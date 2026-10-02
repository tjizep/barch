#include "repo_package.h"

#include "foreign/driver.h"
#include "key_space.h"
#include "local_fs.h"

#include <cmath>
#include <cstdio>
#include <set>

#ifdef BARCH_HAS_SIMDJSON
#include "simdjson.h"
#endif

namespace lfs = barch::localfs;

namespace barch::package {

const char* file_name = "package.luau";

namespace {

/*
 * The `<space>.<setting>` names key_space and the file store read. A package can
 * only write these, so a misspelt one is refused rather than becoming a key that
 * nothing ever reads.
 *
 * foreign_password is left out on purpose: a git repository is the last place a
 * database password belongs, for the same reason a repository's ssh_key has to be
 * a path and not the key. It can still be SET by hand.
 */
const std::set<std::string> settings_known = {
    "shards", "ordered", "hybrid", "compression", "range_sharded", "key_split",
    "missing_ttl", "arena_dir", "arena_map", "aof", "aof_dir",
    "fs_cache_bytes", "fs_source", "fs_source_list",
    "function_deadline_ms", "function_deadline_max_ms",
    "function_slice_insns", "function_slice_max_insns",
    "foreign", "foreign_database", "foreign_dsn", "foreign_host", "foreign_max_inflight",
    "foreign_pool_max_age_ms", "foreign_pool_size", "foreign_port", "foreign_query",
    "foreign_query_timeout_ms", "foreign_script", "foreign_script_insns",
    "foreign_timeout_ms", "foreign_user",
};

#ifdef BARCH_HAS_SIMDJSON
using simdjson::dom::element;
using simdjson::dom::element_type;

// the .get() forms throughout, which work whether simdjson was built with exceptions or not
std::string_view sv_of(const element& e) {
    std::string_view v;
    if (e.get(v) != simdjson::SUCCESS) {}
    return v;
}

simdjson::dom::object obj_of(const element& e) {
    simdjson::dom::object o;
    if (e.get(o) != simdjson::SUCCESS) {}
    return o;
}

std::string kind_of(const element& e) {
    switch (e.type()) {
        case element_type::ARRAY: return "a list";
        case element_type::OBJECT: return "a table";
        case element_type::STRING: return "a string";
        case element_type::BOOL: return "a boolean";
        case element_type::NULL_VALUE: return "nil";
        default: return "a number";
    }
}

/*
 * What a setting is written as. Numbers come out of the JSON as doubles, and a
 * shard count of 7 has to be written "7", not "7.0". Booleans are 1 and 0, which
 * every reader in key_space takes.
 */
bool scalar_text(const element& e, std::string& out) {
    switch (e.type()) {
        case element_type::STRING:
            out = std::string(sv_of(e));
            return true;
        case element_type::BOOL: {
            bool b = false;
            if (e.get(b) != simdjson::SUCCESS) {}
            out = b ? "1" : "0";
            return true;
        }
        case element_type::INT64: {
            int64_t i = 0;
            if (e.get(i) != simdjson::SUCCESS) {}
            out = std::to_string(i);
            return true;
        }
        case element_type::UINT64: {
            uint64_t u = 0;
            if (e.get(u) != simdjson::SUCCESS) {}
            out = std::to_string(u);
            return true;
        }
        case element_type::DOUBLE: {
            double d = 0;
            if (e.get(d) != simdjson::SUCCESS) {}
            if (std::floor(d) == d && std::fabs(d) < 9.0e15) {
                out = std::to_string((int64_t) d);
            } else {
                char buf[64];
                snprintf(buf, sizeof buf, "%.17g", d);
                out = buf;
            }
            return true;
        }
        default:
            return false;
    }
}

bool whole_number(const element& e, int64_t& out) {
    std::string text;
    if (e.type() == element_type::STRING || !scalar_text(e, text))
        return false;
    if (text.empty() || text.find_first_not_of("-0123456789") != std::string::npos)
        return false;
    out = std::stoll(text);
    return true;
}

/*
 * An empty Luau table encodes as `{}` whatever it was meant to be, so a list that
 * came out as an empty table is an empty list.
 */
bool list_of(const element& e, std::vector<element>& out, const std::string& what,
             std::string& err) {
    out.clear();
    if (e.type() == element_type::ARRAY) {
        simdjson::dom::array a;
        if (e.get(a) != simdjson::SUCCESS) {}
        for (auto item : a)
            out.push_back(item);
        return true;
    }
    if (e.type() == element_type::OBJECT && obj_of(e).size() == 0)
        return true;
    err = what + " has to be a list, not " + kind_of(e);
    return false;
}

bool table_of(const element& e, const std::string& what, std::string& err) {
    if (e.type() == element_type::OBJECT)
        return true;
    err = what + " has to be a table, not " + kind_of(e);
    return false;
}

bool text_of(const element& e, const std::string& what, std::string& out, std::string& err) {
    if (e.type() == element_type::STRING) {
        out = std::string(sv_of(e));
        return true;
    }
    err = what + " has to be a string, not " + kind_of(e);
    return false;
}

bool space_name(const std::string& name, const std::string& what, std::string& err) {
    if (name.empty())
        return true;                    // the default space
    if (name == "configuration" || !barch::check_ks_name(name)) {
        err = what + " '" + name + "' is not a key space a package can use";
        return false;
    }
    return true;
}

/** a path inside the checkout: relative, no `..`, "." for the whole of it */
bool checkout_path(const std::string& path, const std::string& what, std::string& err) {
    if (path.empty() || path[0] == '/') {
        err = what + " has to be a path inside the repository";
        return false;
    }
    size_t at = 0;
    while (at <= path.size()) {
        auto slash = path.find('/', at);
        auto part = path.substr(at, slash == std::string::npos ? std::string::npos : slash - at);
        if (part == "..") {
            err = what + " '" + path + "' leaves the repository";
            return false;
        }
        if (slash == std::string::npos)
            break;
        at = slash + 1;
    }
    return true;
}

bool only_fields(const element& e, std::initializer_list<const char*> allowed,
                 const std::string& what, std::string& err) {
    for (auto field : obj_of(e)) {
        bool ok = false;
        for (auto* a : allowed)
            ok = ok || field.key == a;
        if (!ok) {
            err = what + " has no field '" + std::string(field.key) + "'";
            return false;
        }
    }
    return true;
}

bool read_hook(const element& e, const std::string& what, hook& out, std::string& err) {
    if (!table_of(e, what, err) || !only_fields(e, {"space", "call", "args"}, what, err))
        return false;
    element v;
    if (e["space"].get(v) == simdjson::SUCCESS &&
        (!text_of(v, what + ".space", out.space, err) || !space_name(out.space, what + ".space", err)))
        return false;
    if (e["call"].get(v) != simdjson::SUCCESS) {
        err = what + " needs call, the command function to run";
        return false;
    }
    if (!text_of(v, what + ".call", out.call, err))
        return false;
    if (out.call.empty()) {
        err = what + ".call is empty";
        return false;
    }
    if (e["args"].get(v) == simdjson::SUCCESS) {
        std::vector<element> args;
        if (!list_of(v, args, what + ".args", err))
            return false;
        for (const auto& a : args) {
            std::string text;
            if (!scalar_text(a, text)) {
                err = what + ".args holds " + kind_of(a) + ", only strings, numbers and booleans";
                return false;
            }
            out.args.push_back(text);
        }
    }
    return true;
}
#endif

} // namespace

bool known_setting(const std::string& name) {
    return settings_known.count(name) != 0;
}

bool parse(const std::string& json, spec& out, std::string& err) {
    out = spec{};
#ifndef BARCH_HAS_SIMDJSON
    (void) json;
    err = "package.luau needs a build with simdjson";
    return false;
#else
    simdjson::dom::parser parser;
    element doc;
    if (parser.parse(json).get(doc) != simdjson::SUCCESS) {
        err = "setup() returned something that does not read back";
        return false;
    }
    if (!table_of(doc, "setup()'s result", err) ||
        !only_fields(doc, {"spaces", "load", "http", "hooks"}, "setup()'s result", err))
        return false;

    element v;
    if (doc["spaces"].get(v) == simdjson::SUCCESS) {
        if (!table_of(v, "spaces", err))
            return false;
        for (auto space : obj_of(v)) {
            std::string name(space.key);
            std::string what = "spaces." + name;
            if (name.empty()) {
                err = "spaces needs a space name; the default space has no settings of its own";
                return false;
            }
            if (!space_name(name, "spaces", err) || !table_of(space.value, what, err))
                return false;
            out.spaces.push_back(name);
            for (auto field : obj_of(space.value)) {
                std::string setting(field.key);
                if (setting == "foreign_password") {
                    err = what + ".foreign_password belongs in the server's configuration, "
                                 "not in a git repository";
                    return false;
                }
                if (!known_setting(setting)) {
                    err = what + "." + setting + " is not a key space setting";
                    return false;
                }
                std::string text;
                if (!scalar_text(field.value, text)) {
                    err = what + "." + setting + " is " + kind_of(field.value)
                        + ", a setting is a string, a number or a boolean";
                    return false;
                }
                if (setting == "shards") {
                    int64_t n = 0;
                    if (!whole_number(field.value, n) || n < 1 || n > 65536) {
                        err = what + ".shards has to be a whole number from 1 to 65536";
                        return false;
                    }
                }
                out.settings.push_back({name, setting, text});
            }
        }
    }

    if (doc["load"].get(v) == simdjson::SUCCESS) {
        std::vector<element> entries;
        if (!list_of(v, entries, "load", err))
            return false;
        size_t i = 0;
        for (const auto& e : entries) {
            std::string what = "load[" + std::to_string(++i) + "]";
            if (!table_of(e, what, err) ||
                !only_fields(e, {"path", "space", "as", "fs_root"}, what, err))
                return false;
            load_entry one;
            element f;
            // a list entry with a bare path, { "luau", space = "x" }, loses its named
            // fields on the way to JSON - so the path has to be named too
            if (e["path"].get(f) != simdjson::SUCCESS) {
                err = what + " needs path = \"...\"";
                return false;
            }
            if (!text_of(f, what + ".path", one.path, err) ||
                !checkout_path(one.path, what + ".path", err))
                return false;
            if (e["space"].get(f) == simdjson::SUCCESS &&
                (!text_of(f, what + ".space", one.space, err) ||
                 !space_name(one.space, what + ".space", err)))
                return false;
            if (e["as"].get(f) == simdjson::SUCCESS) {
                if (!text_of(f, what + ".as", one.as, err))
                    return false;
                if (one.as != "keys" && one.as != "fs") {
                    err = what + ".as is 'keys' or 'fs'";
                    return false;
                }
            }
            if (e["fs_root"].get(f) == simdjson::SUCCESS) {
                if (!text_of(f, what + ".fs_root", one.fs_root, err))
                    return false;
                if (one.as != "fs") {
                    err = what + ".fs_root only means something with as = \"fs\"";
                    return false;
                }
                if (one.fs_root.empty() || one.fs_root[0] != '/') {
                    err = what + ".fs_root has to start with /";
                    return false;
                }
            }
            out.loads.push_back(std::move(one));
        }
        // the sweep after a file import drops what is under its root and was not
        // imported, so two roots in one space where one holds the other would
        // delete each other's files on every sync
        auto under = [](const std::string& a, const std::string& b) {
            if (a == b || b == "/")
                return true;
            return a.size() > b.size() && a.compare(0, b.size(), b) == 0 && a[b.size()] == '/';
        };
        for (size_t i = 0; i < out.loads.size(); ++i) {
            for (size_t j = i + 1; j < out.loads.size(); ++j) {
                const auto& a1 = out.loads[i];
                const auto& b1 = out.loads[j];
                if (a1.as != "fs" || b1.as != "fs" || a1.space != b1.space)
                    continue;
                if (under(a1.fs_root, b1.fs_root) || under(b1.fs_root, a1.fs_root)) {
                    err = "load[" + std::to_string(i + 1) + "] and load[" + std::to_string(j + 1)
                        + "] both put files under " + a1.fs_root + " and " + b1.fs_root
                        + " in the same space; one root cannot hold the other";
                    return false;
                }
            }
        }
    }

    if (doc["http"].get(v) == simdjson::SUCCESS) {
        if (!table_of(v, "http", err) ||
            !only_fields(v, {"space", "key", "port", "bind"}, "http", err))
            return false;
        http_entry h;
        element f;
        if (v["space"].get(f) == simdjson::SUCCESS &&
            (!text_of(f, "http.space", h.space, err) || !space_name(h.space, "http.space", err)))
            return false;
        if (v["key"].get(f) == simdjson::SUCCESS && !text_of(f, "http.key", h.key, err))
            return false;
        if (v["port"].get(f) == simdjson::SUCCESS) {
            int64_t n = 0;
            if (!whole_number(f, n) || n < 1 || n > 65535) {
                err = "http.port has to be a port number";
                return false;
            }
            h.port = (uint16_t) n;
        }
        if (v["bind"].get(f) == simdjson::SUCCESS && !text_of(f, "http.bind", h.bind, err))
            return false;
        out.http = h;
    }

    if (doc["hooks"].get(v) == simdjson::SUCCESS) {
        if (!table_of(v, "hooks", err) || !only_fields(v, {"before", "after"}, "hooks", err))
            return false;
        element f;
        if (v["before"].get(f) == simdjson::SUCCESS) {
            hook h;
            if (!read_hook(f, "hooks.before", h, err))
                return false;
            out.before = h;
        }
        if (v["after"].get(f) == simdjson::SUCCESS) {
            hook h;
            if (!read_hook(f, "hooks.after", h, err))
                return false;
            out.after = h;
        }
    }
    return true;
#endif
}

bool read(const std::string& dir, bool& present, spec& out, std::string& err) {
    out = spec{};
    std::string path = dir + "/" + file_name;
    present = lfs::is_reg(path);
    if (!present)
        return true;
    std::string source;
    if (!lfs::read_file(path, source)) {
        err = "could not read " + path;
        return false;
    }
    std::string json;
    if (!barch::foreign::package_setup(source, json, err))
        return false;
    return parse(json, out, err);
}

}
