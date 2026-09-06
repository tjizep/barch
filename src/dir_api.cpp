#include "dir_api.h"

#include "function_api.h"
#include "key_space.h"
#include "staged.h"
#include "vk_caller.h"

#include <cstdlib>

namespace {

using access = barch::foreign::store_access;

std::string as_text(art::value_type v) {
    return {v.chars(), v.size};
}

std::string upper(std::string s) {
    for (auto& c : s)
        c = (char) toupper((unsigned char) c);
    return s;
}

/** the key just past everything starting with `prefix` */
std::string past(const std::string& prefix) {
    std::string hi = prefix;
    while (!hi.empty()) {
        auto c = (unsigned char) hi.back();
        if (c != 0xff) {
            hi.back() = (char) (c + 1);
            return hi;
        }
        hi.pop_back();
    }
    return {};
}

/** an optional NAME VALUE pair */
bool option_at(const arg_t& argv, size_t at, const char* name, std::string& value) {
    if (at + 1 >= argv.size())
        return false;
    if (upper(as_text(argv[at])) != name)
        return false;
    value = as_text(argv[at + 1]);
    return true;
}

struct options {
    char sep{':'};
    std::string after;
    size_t limit{0};
};

/** parse [SEP s] [AFTER name] [LIMIT n] from `at` onwards. Non-empty is the error */
std::string read_options(const arg_t& argv, size_t at, options& out) {
    for (; at + 1 < argv.size(); at += 2) {
        std::string v;
        if (option_at(argv, at, "SEP", v)) {
            if (v.size() != 1 || v[0] == '\0' || (unsigned char) v[0] == 0xff)
                return "SEP is one byte, and not NUL or 0xff";
            out.sep = v[0];
        } else if (option_at(argv, at, "AFTER", v)) {
            out.after = v;
        } else if (option_at(argv, at, "LIMIT", v)) {
            out.limit = (size_t) strtoull(v.c_str(), nullptr, 10);
        } else {
            return "expected SEP, AFTER or LIMIT";
        }
    }
    if (at < argv.size())
        return "an option needs a value";
    return {};
}

/** `path` with a trailing separator, which is where its children start */
std::string children_of(const std::string& path, char sep) {
    if (path.empty())
        return {};
    if (path.back() == sep)
        return path;
    return path + sep;
}

/**
 * One level.
 *
 * Unlike the file store a raw key tree can be both at once - `a:b` can hold a value
 * and `a:b:c` can exist under it - so an entry says which it is, and `both` is a
 * real answer rather than an oddity to hide.
 *
 * Two seeks per child, not one read per descendant: the first finds the child, the
 * second asks whether anything lives below it, and then the scan jumps past its
 * whole subtree.
 */
struct child {
    std::string name;
    bool is_key{false};
    bool is_node{false};
    int64_t size{0};
};

void walk_level(const access& acc, const std::string& prefix, char sep,
                const options& opt, std::vector<child>& out) {
    /*
     * The root has no prefix and therefore no `past(prefix)` to stop at, and an
     * empty high bound is an empty range rather than an open one. The largest key
     * in the space is the bound, one byte past it so that it is included.
     */
    std::string hi;
    if (prefix.empty()) {
        std::string biggest;
        if (!acc.max || !acc.max(biggest))
            return;                          // nothing in the space at all
        hi = past(biggest);
    } else {
        hi = past(prefix);
    }
    std::string at = prefix + opt.after;
    std::string skip = opt.after.empty() ? std::string() : at;

    while (opt.limit == 0 || out.size() < opt.limit) {
        heap::vector<std::string> got;
        acc.range(at, hi, 1, got);
        if (got.empty())
            break;
        const std::string& key = got.front();
        if (!prefix.empty() && key.compare(0, prefix.size(), prefix) != 0)
            break;
        if (key == skip) {
            // the key the listing resumed from: step past it by its own name
            at = key + sep;
            skip.clear();
            continue;
        }
        std::string rest = key.substr(prefix.size());
        auto cut = rest.find(sep);
        child c;
        c.name = cut == std::string::npos ? rest : rest.substr(0, cut);
        c.is_key = (cut == std::string::npos);
        if (c.is_key && acc.get) {
            std::string value;
            if (acc.get(key, value) == access::read_state::present)
                c.size = (int64_t) value.size();
        }
        std::string below = prefix + c.name + sep;
        heap::vector<std::string> under;
        acc.range(below, past(below), 1, under);
        c.is_node = !under.empty();
        out.push_back(std::move(c));
        at = below;                          // everything under it, skipped in one step
        std::string jump = below;
        jump.back() = (char) (sep + 1);
        at = jump;
    }
}

/** every key under `prefix`, and the key at `prefix` itself when there is one */
void all_under(const access& acc, const std::string& path, char sep,
               std::vector<std::string>& out) {
    if (acc.exists && acc.exists(path))
        out.push_back(path);
    std::string prefix = children_of(path, sep);
    if (prefix.empty())
        return;
    std::string hi = past(prefix);
    std::string at = prefix;
    std::string seen;
    for (;;) {
        heap::vector<std::string> got;
        acc.range(at, hi, 512, got);
        if (got.empty())
            break;
        size_t took = 0;
        for (const auto& k : got) {
            if (k == seen)
                continue;
            ++took;
            out.push_back(k);
            seen = k;
        }
        at = got.back();
        if (took == 0 || got.size() < 512)
            break;
    }
}

}

/* DIR LS|COUNT|RM <path> [SEP s] [AFTER n] [LIMIT n] | DIR MV|CP <from> <to> [SEP s]
 *
 * See dir_api.h. LS is a line per child - `kind size name`, the name last because
 * it is the only field that can hold a space - which is the shape FS LS and
 * FUNCTIONS COMMANDS already use, and for the same reason: the module caller
 * cannot send a nested array.
 */
int DIR(caller& call, const arg_t& argv) {
    if (argv.size() < 3)
        return call.wrong_arity();
    std::string sub = upper(as_text(argv[1]));
    std::string path = as_text(argv[2]);
    auto space = call.kspace();
    auto acc = barch::functions::store_for_owner(space);

    if (sub == "LS" || sub == "COUNT" || sub == "RM") {
        options opt;
        auto bad = read_options(argv, 3, opt);
        if (!bad.empty())
            return call.push_error(bad.c_str());

        if (sub == "COUNT") {
            std::string prefix = children_of(path, opt.sep);
            if (prefix.empty())
                return call.push_int((int64_t) (acc.size ? acc.size() : 0));
            int64_t n = acc.count ? acc.count(prefix, past(prefix)) : 0;
            if (acc.exists && acc.exists(path))
                ++n;
            return call.push_int(n);
        }
        if (sub == "RM") {
            std::vector<std::string> keys;
            all_under(acc, path, opt.sep, keys);
            if (keys.empty())
                return call.push_int(0);
            barch::staged ops(space);
            for (const auto& k : keys)
                ops.remove(k);
            std::string err;
            if (!ops.commit(err))
                return call.push_error(err.c_str());
            return call.push_int((int64_t) keys.size());
        }

        std::vector<child> got;
        walk_level(acc, children_of(path, opt.sep), opt.sep, opt, got);
        call.start_array();
        for (const auto& c : got) {
            const char* kind = c.is_key && c.is_node ? "both" : (c.is_key ? "key" : "node");
            call.push_string(std::string(kind) + " " + std::to_string(c.size) + " " + c.name);
        }
        return call.end_array();
    }

    if (sub == "MV" || sub == "CP") {
        if (argv.size() < 4)
            return call.wrong_arity();
        std::string to = as_text(argv[3]);
        options opt;
        auto bad = read_options(argv, 4, opt);
        if (!bad.empty())
            return call.push_error(bad.c_str());
        if (to == path)
            return call.push_error("the source and the destination are the same");
        if (to.compare(0, path.size(), path) == 0 && to.size() > path.size()
            && to[path.size()] == opt.sep)
            return call.push_error("cannot move a subtree into itself");

        std::vector<std::string> keys;
        all_under(acc, path, opt.sep, keys);
        if (keys.empty())
            return call.push_int(0);
        barch::staged ops(space);
        for (const auto& k : keys) {
            std::string value;
            if (!acc.get || acc.get(k, value) != barch::foreign::store_access::read_state::present)
                continue;
            ops.set(to + k.substr(path.size()), value);
            if (sub == "MV")
                ops.remove(k);
        }
        std::string err;
        if (!ops.commit(err))
            return call.push_error(err.c_str());
        return call.push_int((int64_t) keys.size());
    }
    return call.push_error("DIR LS|COUNT|RM|MV|CP");
}

int cmd_DIR(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, DIR);
}

void register_dir_api(function_map& r) {
    r["DIR"] = {::DIR, {"read", "write", "keys", "data"}};
}
