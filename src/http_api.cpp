#include "http_api.h"

#include "fs.h"

#include "function_api.h"
#include "foreign/driver.h"
#include "auth_api.h"
#include "lzr_log.h"
#include "key_space.h"
#include "sharded_store.h"

#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#ifdef BARCH_HAS_SIMDJSON
#include <simdjson.h>
#endif

#ifdef BARCH_HAS_CROW
#include <crow.h>
#include <asio.hpp>
#ifdef CROW_ENABLE_SSL
#include <asio/ssl.hpp>
#endif
#endif

namespace {

std::string fold_name(std::string s) {
    for (auto& ch : s)
        ch = (char) toupper((unsigned char) ch);
    return s;
}

std::string as_text(art::value_type v) {
    return {v.chars(), v.size};
}

bool is_port_arg(art::value_type v) {
    if (v.size == 0)
        return false;
    for (size_t i = 0; i < v.size; ++i) {
        if (!std::isdigit((unsigned char) v.chars()[i]))
            return false;
    }
    return true;
}

std::string lower_copy(std::string s) {
    for (auto& ch : s)
        ch = (char) std::tolower((unsigned char) ch);
    return s;
}

bool is_http_kind(const barch::foreign::http_route& r) {
    if (r.kind == "http")
        return true;
    if (r.kind == "resource")
        return false;
    return r.has_transport && !r.has_route;
}

bool is_files_kind(const barch::foreign::http_route& r) {
    return r.kind == "files";
}

bool is_resource_kind(const barch::foreign::http_route& r) {
    if (r.kind == "resource")
        return true;
    if (r.kind == "http" || r.kind == "files")
        return false;
    return r.has_route;
}

/** everything that owns a route, however it is answered */
bool is_route_kind(const barch::foreign::http_route& r) {
    return is_resource_kind(r) || is_files_kind(r);
}

#ifdef BARCH_HAS_CROW

struct http_vm_slot {
    barch::foreign::http_vm vm;
    /** "NAME:VERB" -> lua ref, valid only on this vm */
    std::unordered_map<std::string, int> methods;
    /** what compile_epoch() said when these methods were compiled - TODO 244 */
    uint64_t epoch{0};
};

struct space_http {
    barch::key_space_ptr space;
    std::unique_ptr<crow::SimpleApp> app;
    std::thread thread;
    std::vector<barch::foreign::http_route> routes;
    std::string bind{"0.0.0.0"};
    uint16_t port{18080};
    std::string ssl_proto;
    std::string ssl_cert;
    std::string ssl_key;
    std::string default_user{"web"};
    std::string fail;
    std::atomic<bool> running{false};
    std::mutex pool_mu;
    std::condition_variable pool_cv;
    std::vector<std::shared_ptr<http_vm_slot>> idle;
    /** how many slots the pool was built with, so executing = pool_size - idle */
    size_t pool_size{0};
    /** what this space's VMs hold, counted by the Luau allocator - TODO 181 */
    std::shared_ptr<std::atomic<uint64_t>> luau_bytes{std::make_shared<std::atomic<uint64_t>>(0)};
};

std::shared_ptr<http_vm_slot> pop_vm(space_http& s) {
    std::unique_lock<std::mutex> g(s.pool_mu);
    s.pool_cv.wait(g, [&] { return !s.idle.empty() || !s.running.load(); });
    if (s.idle.empty())
        return nullptr;
    auto v = std::move(s.idle.back());
    s.idle.pop_back();
    return v;
}

void push_vm(space_http& s, std::shared_ptr<http_vm_slot> v) {
    if (!v)
        return;
    {
        std::lock_guard<std::mutex> g(s.pool_mu);
        s.idle.push_back(std::move(v));
    }
    s.pool_cv.notify_one();
}

std::shared_ptr<http_vm_slot> make_vm_slot(const std::string& space,
                                           const barch::foreign::call_interface_ptr& iface,
                                           uint64_t deadline_ms,
                                           std::shared_ptr<std::atomic<uint64_t>> bytes) {
    auto slot = std::make_shared<http_vm_slot>();
    slot->vm.cache = barch::foreign::make_function_states(std::move(bytes));
    slot->vm.space = space;
    slot->vm.deadline_ms = deadline_ms ? deadline_ms : 5000;
    slot->vm.iface = iface;
    return slot;
}

bool load_resource_into(http_vm_slot& slot, const std::string& name,
                        const std::string& source, std::string& err) {
    barch::foreign::http_route spec;
    if (!barch::foreign::http_vm_load(slot.vm, name, source, spec, err))
        return false;
    if (!is_resource_kind(spec))
        return true;
    for (const auto& m : spec.methods)
        slot.methods[spec.name + ":" + m.verb] = m.fn_ref;
    return true;
}

std::mutex http_mu;
heap::string_map<std::shared_ptr<space_http>> http_servers;

/*
 * Templated routes - TODO 222.
 *
 * `route = "/notes/{id}/rev/{n}"` in a resource transport() binds two names out
 * of the path; a trailing `*` binds whatever is left under `*`. Crow cannot do
 * this for us: route_dynamic checks the rule's parameter tag against the
 * handler's arity, and our handler is one lambda for every route, with the
 * routes themselves only known once someone has stored a function. So Crow gets
 * the literal prefix and a `<path>` to swallow the rest, and the matching below
 * runs against req.url, which is the path with the query already split off.
 */

/** percent decoding for a path segment. `+` is a plus here - this is not a query string. */
std::string pct_decode(std::string_view in) {
    std::string out;
    out.reserve(in.size());
    auto hex = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    for (size_t i = 0; i < in.size(); ++i) {
        if (in[i] == '%' && i + 2 < in.size()) {
            int h = hex(in[i + 1]);
            int l = hex(in[i + 2]);
            if (h >= 0 && l >= 0) {
                out.push_back((char) ((h << 4) | l));
                i += 2;
                continue;
            }
        }
        // a stray % that is not followed by two hex digits stays a %, which is
        // what a browser sends for a literal one often enough to matter
        out.push_back(in[i]);
    }
    return out;
}

/** split on '/', dropping the leading empty piece and one trailing slash */
std::vector<std::string_view> path_segments(std::string_view path) {
    std::vector<std::string_view> out;
    size_t i = 0;
    if (!path.empty() && path[0] == '/')
        i = 1;
    while (i <= path.size()) {
        size_t j = path.find('/', i);
        if (j == std::string_view::npos)
            j = path.size();
        out.push_back(path.substr(i, j - i));
        if (j == path.size())
            break;
        i = j + 1;
    }
    if (!out.empty() && out.back().empty())
        out.pop_back();
    return out;
}

/**
 * Fill in spec.segs, spec.crow_route and the two flags from spec.route. An
 * untemplated route comes out with templated=false and goes to Crow whole,
 * exactly as it did before this existed.
 */
bool parse_route(barch::foreign::http_route& spec, std::string& err) {
    spec.segs.clear();
    spec.wild_tail = false;
    spec.templated = false;
    spec.crow_route = spec.route;
    // a `?` in the pattern is someone writing the query in as documentation;
    // the query never takes part in matching, so drop it
    auto q = spec.route.find('?');
    std::string route = q == std::string::npos ? spec.route : spec.route.substr(0, q);
    if (route.find('{') == std::string::npos && route.find('*') == std::string::npos) {
        spec.crow_route = route;
        return true;
    }
    auto raw = path_segments(route);
    std::string prefix;
    bool prefix_open = true;
    for (size_t i = 0; i < raw.size(); ++i) {
        std::string_view seg = raw[i];
        if (seg == "*") {
            if (i + 1 != raw.size()) {
                err = spec.name + ": `*` has to be the last segment of " + spec.route;
                return false;
            }
            spec.wild_tail = true;
            prefix_open = false;
            break;
        }
        if (seg.size() >= 2 && seg.front() == '{' && seg.back() == '}') {
            barch::foreign::http_route_seg s;
            s.hole = true;
            s.name = std::string(seg.substr(1, seg.size() - 2));
            if (s.name.empty()) {
                err = spec.name + ": empty {} in " + spec.route;
                return false;
            }
            if (s.name == "*") {
                err = spec.name + ": {*} is not a name - use a bare * at the end";
                return false;
            }
            for (const auto& done : spec.segs) {
                if (done.hole && done.name == s.name) {
                    err = spec.name + ": {" + s.name + "} appears twice in " + spec.route;
                    return false;
                }
            }
            spec.segs.push_back(std::move(s));
            prefix_open = false;
            continue;
        }
        if (seg.find('{') != std::string_view::npos || seg.find('}') != std::string_view::npos) {
            err = spec.name + ": a {name} has to be a whole segment in " + spec.route;
            return false;
        }
        if (seg.find('*') != std::string_view::npos) {
            err = spec.name + ": `*` has to be a whole segment in " + spec.route;
            return false;
        }
        barch::foreign::http_route_seg s;
        s.text = pct_decode(seg);
        spec.segs.push_back(s);
        if (prefix_open)
            prefix += "/" + std::string(seg);
    }
    spec.templated = true;
    // Crow matches the literal prefix and `<path>` takes the rest. `<path>`
    // needs something to match, which is right: /notes/{id} should not answer
    // for /notes.
    spec.crow_route = prefix + "/<path>";
    return true;
}

/**
 * Match a request path against a parsed route, filling in the bindings. False
 * means the path got past Crow's prefix but does not fit the pattern, which is
 * a 404 rather than a handler call.
 */
bool match_route(const barch::foreign::http_route& spec, std::string_view url,
                 std::vector<barch::foreign::http_binding>& out) {
    auto segs = path_segments(url);
    size_t want = spec.segs.size();
    if (spec.wild_tail ? segs.size() < want : segs.size() != want)
        return false;
    for (size_t i = 0; i < want; ++i) {
        const auto& pat = spec.segs[i];
        std::string got = pct_decode(segs[i]);
        if (!pat.hole) {
            if (got != pat.text)
                return false;
            continue;
        }
        // a {name} binds something; //, which is an empty segment, is not it
        if (got.empty())
            return false;
        out.push_back({pat.name, std::move(got)});
    }
    if (spec.wild_tail) {
        // the rest, decoded segment by segment and joined back up. A %2F inside
        // the tail is a / by the time luau sees it - the price of handing over
        // one string rather than a list.
        std::string rest;
        for (size_t i = want; i < segs.size(); ++i) {
            if (!rest.empty())
                rest += "/";
            rest += pct_decode(segs[i]);
        }
        out.push_back({"*", std::move(rest)});
    }
    return true;
}

const barch::foreign::http_method* find_method(const barch::foreign::http_route& spec,
                                               const std::string& verb) {
    for (const auto& m : spec.methods) {
        if (m.verb == verb)
            return &m;
    }
    return nullptr;
}

void apply_cors(crow::response& res, const barch::foreign::http_route& spec) {
    if (spec.cors.empty())
        return;
    res.set_header("Access-Control-Allow-Origin", spec.cors);
    std::string allow;
    for (const auto& m : spec.methods) {
        if (!allow.empty())
            allow += ", ";
        allow += m.verb;
    }
    if (allow.find("OPTIONS") == std::string::npos) {
        if (!allow.empty())
            allow += ", ";
        allow += "OPTIONS";
    }
    res.set_header("Access-Control-Allow-Methods", allow);
    res.set_header("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

bool accept_ok(const crow::request& req, const barch::foreign::http_route& spec) {
    if (spec.accept.empty())
        return true;
    auto verb = crow::method_name(req.method);
    if (verb != "POST" && verb != "PUT" && verb != "PATCH")
        return true;
    if (req.body.empty())
        return true;
    auto ct = lower_copy(req.get_header_value("Content-Type"));
    auto want = lower_copy(spec.accept);
    return ct.find(want) != std::string::npos;
}

std::string cookie_value(const std::string& header, const std::string& name) {
    size_t nlen = name.size();
    size_t i = 0;
    while (i < header.size()) {
        while (i < header.size() && (header[i] == ' ' || header[i] == ';'))
            ++i;
        if (i + nlen < header.size() && header.compare(i, nlen, name) == 0 &&
            header[i + nlen] == '=') {
            size_t v = i + nlen + 1;
            size_t e = header.find(';', v);
            if (e == std::string::npos)
                e = header.size();
            return header.substr(v, e - v);
        }
        i = header.find(';', i);
        if (i == std::string::npos)
            break;
        ++i;
    }
    return {};
}

std::string session_user(const barch::key_space_ptr& space, const std::string& sid) {
    if (!space || sid.empty())
        return {};
    std::string key = "http:sess:" + sid;
    std::string value;
    auto acc = barch::functions::store_for_owner(space);
    if (acc.get && acc.get(key, value) == barch::foreign::store_access::read_state::present)
        return value;
    return {};
}

/*
 * Serving the file store, in C++ - TODO 235.
 *
 * A `kind = "files"` transport declares a route and a root, and this answers it
 * without entering luau at all. That is not an optimisation: a luau handler holds
 * a VM slot for the whole call and the pool is 2-8, so eight concurrent downloads
 * would exhaust it and every other request on that server would get a 503.
 *
 * What a stored file is lives in fs.h - TODO 256. This used to carry its own
 * metadata struct, its own json parse and its own chunk key builder, which was one
 * of two parsers for the same document and the reason the layout could not be
 * changed in one place.
 */

/**
 * `Range: bytes=a-b`, in the one form worth supporting: a single range. Multipart
 * ranges are legal and no client asks for them. False means "not a range request
 * we understand", and the whole file is the right answer to that.
 */
bool parse_range(const std::string& header, uint64_t size, uint64_t& first, uint64_t& last) {
    if (header.rfind("bytes=", 0) != 0 || size == 0)
        return false;
    auto spec = header.substr(6);
    if (spec.find(',') != std::string::npos)
        return false;                       // multipart, not worth it
    auto dash = spec.find('-');
    if (dash == std::string::npos)
        return false;
    auto from = spec.substr(0, dash);
    auto to = spec.substr(dash + 1);
    try {
        if (from.empty()) {
            // a suffix range: the last N bytes
            if (to.empty()) return false;
            auto n = std::stoull(to);
            if (n == 0) return false;
            first = n >= size ? 0 : size - n;
            last = size - 1;
        } else {
            first = std::stoull(from);
            last = to.empty() ? size - 1 : std::stoull(to);
        }
    } catch (const std::exception&) {
        return false;
    }
    if (last >= size)
        last = size - 1;
    return first <= last;
}

/** what a files route serves for this url, or empty when the url escapes the root */
std::string file_path_for(const barch::foreign::http_route& spec, const std::string& url) {
    // the route's literal prefix comes off, and what is left hangs under the root
    std::string prefix = spec.route;
    auto star = prefix.find('*');
    if (star != std::string::npos)
        prefix = prefix.substr(0, star);
    if (url.size() < prefix.size() || url.compare(0, prefix.size(), prefix) != 0)
        return {};
    std::string rest = pct_decode(url.substr(prefix.size()));
    // no climbing out with .., and no NUL smuggled in through an escape
    if (rest.find("..") != std::string::npos || rest.find('\0') != std::string::npos)
        return {};
    std::string root = spec.root.empty() ? std::string("/") : spec.root;
    if (root.back() != '/')
        root += '/';
    while (!rest.empty() && rest.front() == '/')
        rest.erase(rest.begin());
    // a url naming a directory gets the route's index, when it declared one. That
    // is what makes /browser an entry point rather than a 404 - TODO 253
    if (rest.empty() || rest.back() == '/') {
        if (spec.index.empty())
            return {};
        rest += spec.index;
    }
    return root + rest;
}

void handle_file(const std::shared_ptr<space_http>& server,
                 const barch::foreign::http_route& spec,
                 const crow::request& req, crow::response& res) {
    auto verb = crow::method_name(req.method);
    if (verb != "GET" && verb != "HEAD") {
        res.code = 405;
        res.set_header("Allow", "GET, HEAD");
        res.body = "method not allowed";
        apply_cors(res, spec);
        res.end();
        return;
    }
    auto path = file_path_for(spec, req.url);
    if (path.empty()) {
        res.code = 404;
        res.body = "not found";
        apply_cors(res, spec);
        res.end();
        return;
    }

    // the same identity a luau route would run as, so a file is as private as any
    // other key in the space. No VM is taken to work it out
    barch::functions::http_ident ident;
    ident.sid = cookie_value(req.get_header_value("Cookie"), "sid");
    if (!spec.user.empty())
        ident.user = spec.user;
    else if (!ident.sid.empty())
        ident.user = session_user(server->space, ident.sid);
    if (ident.user.empty())
        ident.user = server->default_user.empty() ? "web" : server->default_user;
    auto acc = barch::functions::store_for(server->space, acl_for_user(ident.user));
    if (!acc.may_read) {
        res.code = 403;
        res.body = "forbidden";
        apply_cors(res, spec);
        res.end();
        return;
    }

    /*
     * A route that declared `source = true` may ask the space's file source for
     * something it does not have. It waits inline and holds this slot while it
     * does, which is why it is opt in - TODO 263.
     */
    if (spec.source) {
        barch::fs::entry got;
        std::string why;
        (void) barch::fs::fetch(server->space, path, got, why);
    }
    barch::fs::file open_file;
    std::string open_err;
    if (!barch::fs::file::open(acc, path, open_file, open_err)) {
        res.code = 404;
        res.body = "not found";
        apply_cors(res, spec);
        res.end();
        return;
    }
    const auto& meta = open_file.meta();

    /*
     * Strong when the file carries a version, since that moves on every write - so a
     * rewrite that lands on the same length gets a new ETag, which is exactly what
     * the weak one could not do. A file whose writer keeps no version falls back to
     * the weak form and says so with the W/ prefix. See TODO 248.
     */
    std::string etag = meta.version
        ? "\"" + std::to_string(meta.version) + "-" + std::to_string(meta.size) + "\""
        : "W/\"" + std::to_string(meta.size) + "-" + std::to_string(meta.chunks) + "\"";
    if (req.get_header_value("If-None-Match") == etag) {
        res.code = 304;
        res.set_header("ETag", etag);
        apply_cors(res, spec);
        res.end();
        return;
    }

    auto type = meta.type.empty() ? barch::fs::type_from_path(path) : meta.type;
    res.set_header("Content-Type", type);
    res.set_header("ETag", etag);
    res.set_header("Accept-Ranges", "bytes");
    apply_cors(res, spec);

    uint64_t first = 0;
    uint64_t last = meta.size ? meta.size - 1 : 0;
    const auto& range_header = req.get_header_value("Range");
    bool ranged = !range_header.empty() && parse_range(range_header, meta.size, first, last);
    if (!range_header.empty() && !ranged && range_header.rfind("bytes=", 0) == 0 &&
        meta.size > 0) {
        // asked for something outside the file: 416 with what the file actually is
        res.code = 416;
        res.set_header("Content-Range", "bytes */" + std::to_string(meta.size));
        res.body.clear();
        res.end();
        return;
    }

    /*
     * No special case for HEAD. Crow's router sets res.skip_body on a HEAD and its
     * end() then takes Content-Length from the body before dropping it, so the
     * answer is the headers a GET would have given with nothing after them - which
     * is what HEAD is. Setting the length here instead fought that and lost.
     */

    /*
     * Assembled into the response body rather than streamed. Crow's response is a
     * string, so a file is held once in memory while it is written - fine for the
     * images and pages this is for, and the reason Range matters for anything
     * bigger. Only the chunks a range actually covers are read.
     */
    std::string body;
    if (meta.size) {
        std::string why;
        if (!open_file.read_at(first, last - first + 1, body, why)) {
            // the metadata says there is a chunk and there is not: a half written
            // file, or one being overwritten while it is read. Nothing here is
            // atomic across chunks - see DONE 226
            res.code = 500;
            res.body = "file is incomplete";
            res.end();
            return;
        }
    }

    if (ranged) {
        res.code = 206;
        res.set_header("Content-Range", "bytes " + std::to_string(first) + "-" +
                                        std::to_string(last) + "/" + std::to_string(meta.size));
    } else {
        res.code = 200;
    }
    res.body = std::move(body);
    res.end();
}

void handle_route(const std::shared_ptr<space_http>& server,
                  const barch::foreign::http_route& spec,
                  const crow::request& req, crow::response& res) {
    std::vector<barch::foreign::http_binding> params;
    if (spec.templated && !match_route(spec, req.url, params)) {
        // Crow only matched the literal prefix, so this is ours to refuse
        res.code = 404;
        res.body = "not found";
        apply_cors(res, spec);
        res.end();
        return;
    }
    auto verb = crow::method_name(req.method);
    if (verb == "HEAD")
        verb = "GET";
    const auto* method = find_method(spec, verb);
    if (!method && req.method == crow::HTTPMethod::Options && !spec.cors.empty()) {
        res.code = 204;
        apply_cors(res, spec);
        res.end();
        return;
    }
    if (!method) {
        res.code = 405;
        res.body = "method not allowed";
        apply_cors(res, spec);
        res.end();
        return;
    }
    if (!accept_ok(req, spec)) {
        res.code = 415;
        res.body = "unsupported media type";
        apply_cors(res, spec);
        res.end();
        return;
    }
    auto slot = pop_vm(*server);
    if (!slot) {
        res.code = 503;
        res.body = "HTTP vm pool empty";
        apply_cors(res, spec);
        res.end();
        return;
    }
    struct put_back {
        space_http* s;
        std::shared_ptr<http_vm_slot> v;
        ~put_back() { push_vm(*s, std::move(v)); }
    } hold{server.get(), std::move(slot)};

    /*
     * A handler is compiled into each slot at HTTP START and called by reference, so
     * it never goes through the compiled cache and the epoch of DONE 234 would never
     * reach it - a rewritten resource function would be picked up by RESP callers and
     * not by the server. So a slot rebuilds its handlers when it finds the epoch has
     * moved, which is once per slot per change.
     *
     * Only the handlers. The routes Crow knows about were registered at START and a
     * running app cannot be re-routed, so a transport() that changes its route, its
     * verbs or its cors still needs a STOP and START. See TODO 244.
     *
     * A source that will not compile leaves the slot as it was and the epoch alone,
     * so the request is answered by the last code that worked and the next one tries
     * again. A broken save should not take a running server down with it.
     */
    if (hold.v->epoch != barch::functions::compile_epoch()) {
        auto want = barch::functions::compile_epoch();
        // which of *these* routes was published, not whether anything anywhere was -
        // a publish of an unrelated function must not rebuild this server's handlers
        bool any = false;
        const auto& canon = server->space->canonical();
        for (const auto& r : server->routes) {
            if (is_files_kind(r))
                continue;
            if (barch::functions::published_at(
                    barch::functions::compiled_key(canon, r.name)) > hold.v->epoch) {
                any = true;
                break;
            }
        }
        if (!any) {
            hold.v->epoch = want;               // caught up, nothing to do
        } else {
            std::string reload_err;
            auto fresh = hold.v->methods;
            hold.v->methods.clear();
            for (const auto& r : server->routes) {
                if (is_files_kind(r))
                    continue;
                std::string src;
                if (!barch::functions::source_in(server->space, r.name, src)) {
                    reload_err = r.name + ": no source";
                    break;
                }
                if (!load_resource_into(*hold.v, r.name, src, reload_err))
                    break;
            }
            if (reload_err.empty()) {
                hold.v->epoch = want;
            } else {
                hold.v->methods = std::move(fresh);
                barch::err({"HTTP could not reload", reload_err});
            }
        }
    }

    barch::functions::http_ident ident;
    ident.sid = cookie_value(req.get_header_value("Cookie"), "sid");
    if (!spec.user.empty()) {
        ident.user = spec.user;
        ident.pinned = true;
    } else if (!ident.sid.empty()) {
        ident.user = session_user(server->space, ident.sid);
    }
    if (ident.user.empty())
        ident.user = server->default_user.empty() ? "web" : server->default_user;
    ident.acl = acl_for_user(ident.user);
    ident.space = server->space;
    auto*& tls = barch::functions::http_ident_tls();
    auto* prev = tls;
    tls = &ident;
    struct drop_ident {
        barch::functions::http_ident** p;
        barch::functions::http_ident* was;
        ~drop_ident() { *p = was; }
    } drop{&tls, prev};

    std::string err;
    auto it = hold.v->methods.find(spec.name + ":" + verb);
    if (it == hold.v->methods.end())
        err = "no handler on this vm";
    else
        barch::foreign::http_vm_call(hold.v->vm, it->second, &req, &res,
                                     spec.templated ? &params : nullptr, err);
    if (ident.sid_new && !ident.sid.empty()) {
        res.add_header("Set-Cookie",
                       "sid=" + ident.sid + "; Path=/; HttpOnly");
    }
    if (!err.empty()) {
        res.code = 500;
        res.body = err;
    }
    if (!spec.send.empty() && res.get_header_value("Content-Type").empty())
        res.set_header("Content-Type", spec.send);
    apply_cors(res, spec);
    if (!res.is_completed())
        res.end();
}

bool port_open(const std::string& bind, uint16_t port) {
    try {
        asio::io_context io;
        asio::ip::tcp::socket sock(io);
        std::string host = bind;
        if (host.empty() || host == "0.0.0.0" || host == "*" || host == "::")
            host = "127.0.0.1";
        asio::ip::tcp::endpoint ep(asio::ip::make_address(host), port);
        sock.connect(ep);
        return true;
    } catch (...) {
        return false;
    }
}

std::string start_space_http(const barch::key_space_ptr& space,
                             const std::string& httpkey,
                             uint16_t port, std::string bind,
                             std::vector<std::string> keys,
                             std::vector<std::string>& reply, std::string& err) {
    auto canon = space->canonical();
    {
        std::lock_guard<std::mutex> g(http_mu);
        auto found = http_servers.find(canon);
        if (found != http_servers.end()) {
            // an entry whose server is not running is not a running server. STATUS
            // reads the same flag, and the two used to disagree - TODO 257
            if (found->second && found->second->running.load()) {
                err = "HTTP already running in this space";
                return err;
            }
            http_servers.erase(found);
        }
    }

    auto server = std::make_shared<space_http>();
    server->space = space;
    auto iface = std::make_shared<barch::foreign::call_interface>();
    iface->running_in = canon;
    iface->defined_in = canon;
    iface->load = [space](const std::string&, const std::string& want, bool,
                          std::string& source) -> bool {
        return barch::functions::source_of(space, want, source);
    };
    iface->store = barch::functions::store_for_owner(space);
    iface->run_command = barch::functions::runner_for_http(space);
    /*
     * `barch.space.other` from a handler. It was not wired here at all, so a route
     * could only ever see the space its own server runs in - which is what stopped
     * the shop example reading its image cache next door, and half of TODO 259.
     *
     * The rights are asked for again in the other space rather than inherited, the
     * same as the RESP path does, so per space overrides still apply and naming a
     * space that does not exist must not build one.
     */
    iface->open_space = [](const std::string& name,
                           barch::foreign::store_access& out) -> bool {
        auto* id = barch::functions::http_ident_tls();
        if (!id || !barch::is_keyspace(name))
            return false;
        auto other = barch::get_keyspace(name);
        if (!other)
            return false;
        auto rights = barch::read_space_overrides(id->user);
        auto found = rights.find(other->get_canonical_name());
        out = barch::functions::store_for(other, found == rights.end()
                                          ? id->acl
                                          : barch::apply_overrides(id->acl, found->second));
        return true;
    };
    uint64_t deadline = space->function_deadline();
    auto slot0 = make_vm_slot(canon, iface, deadline, server->luau_bytes);

    std::vector<std::string> want = keys;
    if (!httpkey.empty()) {
        std::string source;
        barch::foreign::http_route conf;
        if (!barch::functions::source_in(space, httpkey, source)) {
            err = "no such function '" + httpkey + "'";
            return err;
        }
        if (!barch::foreign::http_vm_load(slot0->vm, httpkey, source, conf, err))
            return err;
        if (!conf.has_transport) {
            err = "'" + httpkey + "' has no transport()";
            return err;
        }
        if (is_resource_kind(conf)) {
            err = "'" + httpkey + "' is kind=resource, not http";
            return err;
        }
        if (port == 0 && conf.port)
            port = conf.port;
        if (!conf.user.empty())
            server->default_user = conf.user;
        if (bind.empty() && !conf.bind.empty())
            bind = conf.bind;
        if (server->ssl_proto.empty() && !conf.ssl_proto.empty())
            server->ssl_proto = conf.ssl_proto;
        if (want.empty())
            want = conf.extra_keys;
        server->routes.push_back(std::move(conf));
    }

    if (want.empty() && httpkey.empty()) {
        auto n = barch::functions::names(space);
        want.assign(n.begin(), n.end());
    }

    std::unordered_set<std::string> seen;
    for (const auto& r : server->routes)
        seen.insert(r.name);
    std::vector<std::string> extra;
    for (const auto& n : want) {
        auto folded = fold_name(n);
        if (seen.insert(folded).second)
            extra.push_back(folded);
    }
    // extra_keys on loaded routes
    for (const auto& r : server->routes) {
        for (const auto& n : r.extra_keys) {
            if (seen.insert(n).second)
                extra.push_back(n);
        }
    }

    std::string ssl_cert, ssl_key, ssl_proto;
    for (const auto& r : server->routes) {
        if (ssl_cert.empty() && !r.ssl_cert.empty())
            ssl_cert = r.ssl_cert;
        if (ssl_key.empty() && !r.ssl_key.empty())
            ssl_key = r.ssl_key;
        if (ssl_proto.empty() && !r.ssl_proto.empty())
            ssl_proto = r.ssl_proto;
        if (port == 0 && r.port)
            port = r.port;
        if (bind.empty() && !r.bind.empty())
            bind = r.bind;
    }

    for (size_t i = 0; i < extra.size(); ++i) {
        const auto name = extra[i];
        std::string source;
        if (!barch::functions::source_in(space, name, source))
            continue;
        barch::foreign::http_route spec;
        std::string load_err;
        if (!barch::foreign::http_vm_load(slot0->vm, name, source, spec, load_err)) {
            err = name + ": " + load_err;
            return err;
        }
        if (!spec.has_transport)
            continue;
        if (is_http_kind(spec)) {
            if (ssl_cert.empty() && !spec.ssl_cert.empty())
                ssl_cert = spec.ssl_cert;
            if (ssl_key.empty() && !spec.ssl_key.empty())
                ssl_key = spec.ssl_key;
            if (ssl_proto.empty() && !spec.ssl_proto.empty())
                ssl_proto = spec.ssl_proto;
            if (port == 0 && spec.port)
                port = spec.port;
            if (bind.empty() && !spec.bind.empty())
                bind = spec.bind;
            for (const auto& n : spec.extra_keys) {
                if (seen.insert(n).second)
                    extra.push_back(n);
            }
            continue;
        }
        if (!is_route_kind(spec))
            continue;
        if (ssl_cert.empty() && !spec.ssl_cert.empty())
            ssl_cert = spec.ssl_cert;
        if (ssl_key.empty() && !spec.ssl_key.empty())
            ssl_key = spec.ssl_key;
        if (ssl_proto.empty() && !spec.ssl_proto.empty())
            ssl_proto = spec.ssl_proto;
        if (port == 0 && spec.port)
            port = spec.port;
        if (bind.empty() && !spec.bind.empty())
            bind = spec.bind;
        for (const auto& n : spec.extra_keys) {
            if (seen.insert(n).second)
                extra.push_back(n);
        }
        server->routes.push_back(std::move(spec));
    }

    std::vector<barch::foreign::http_route> live;
    live.reserve(server->routes.size());
    for (auto& r : server->routes) {
        if (is_route_kind(r))
            live.push_back(std::move(r));
    }
    server->routes = std::move(live);
    if (server->routes.empty()) {
        err = "no HTTP functions";
        return err;
    }

    std::unordered_map<std::string, std::string> sources;
    for (const auto& r : server->routes) {
        // a files route is answered in C++ and has no handler to load into a VM
        if (is_files_kind(r))
            continue;
        std::string src;
        if (!barch::functions::source_in(space, r.name, src)) {
            err = "no source for '" + r.name + "'";
            return err;
        }
        sources[r.name] = std::move(src);
        for (const auto& m : r.methods)
            slot0->methods[r.name + ":" + m.verb] = m.fn_ref;
    }

    unsigned pool = std::thread::hardware_concurrency();
    if (pool < 2)
        pool = 2;
    if (pool > 8)
        pool = 8;
    server->pool_size = pool;
    slot0->epoch = barch::functions::compile_epoch();
    server->idle.push_back(std::move(slot0));
    for (unsigned i = 1; i < pool; ++i) {
        auto slot = make_vm_slot(canon, iface, deadline, server->luau_bytes);
        for (const auto& r : server->routes) {
            if (is_files_kind(r))
                continue;
            std::string load_err;
            if (!load_resource_into(*slot, r.name, sources[r.name], load_err)) {
                err = r.name + ": " + load_err;
                return err;
            }
        }
        slot->epoch = barch::functions::compile_epoch();
        server->idle.push_back(std::move(slot));
    }

    if (port == 0)
        port = 18080;
    if (bind.empty())
        bind = "0.0.0.0";
    server->port = port;
    server->bind = bind;
    server->ssl_proto = ssl_proto;
    server->ssl_cert = ssl_cert;
    server->ssl_key = ssl_key;

    server->app = std::make_unique<crow::SimpleApp>();
    server->app->loglevel(crow::LogLevel::Warning);
    server->app->signal_clear();
    server->app->concurrency((uint16_t) pool);
    server->app->timeout(30);
    server->app->server_name("barch");

    for (auto& spec : server->routes) {
        if (!parse_route(spec, err))
            return err;
    }
    for (const auto& spec : server->routes) {
        auto& rule = server->app->route_dynamic(spec.crow_route);
        rule.methods(crow::HTTPMethod::Get, crow::HTTPMethod::Post, crow::HTTPMethod::Put,
                     crow::HTTPMethod::Delete, crow::HTTPMethod::Patch,
                     crow::HTTPMethod::Options, crow::HTTPMethod::Head);
        const bool files = is_files_kind(spec);
        if (spec.templated) {
            // the `<path>` argument is only there because Crow insists the
            // handler's arity match the rule's; the matching itself works off
            // req.url, which is the same text with the prefix still on it
            rule([server, spec, files](const crow::request& req, crow::response& res, std::string) {
                if (files) handle_file(server, spec, req, res);
                else handle_route(server, spec, req, res);
            });
        } else {
            rule([server, spec, files](const crow::request& req, crow::response& res) {
                if (files) handle_file(server, spec, req, res);
                else handle_route(server, spec, req, res);
            });
        }
        /*
         * A files route that declares an index needs the bare prefix as well.
         * Crow's `<path>` has to match at least one character - which is right,
         * /notes/{id} should not answer for /notes - so /browser and /browser/
         * reach no rule at all and Crow answers 404 before any of this is
         * consulted. Both spellings get their own rule, and file_path_for turns
         * an empty remainder into the index. See TODO 253.
         */
        if (files && !spec.index.empty() && spec.wild_tail) {
            std::string prefix = spec.crow_route;
            auto at = prefix.rfind("/<path>");
            if (at != std::string::npos && at > 0) {
                /*
                 * With the trailing slash, which is the useful direction: Crow
                 * registers a rule ending in `/` and adds a redirect to it from
                 * the bare form, so /browser answers 301 to /browser/ and both
                 * work. Registering the bare form instead takes the name and
                 * makes the slashed one a 404, and registering both is refused
                 * with "handler already exists".
                 */
                auto& entry = server->app->route_dynamic(prefix.substr(0, at) + "/");
                entry.methods(crow::HTTPMethod::Get, crow::HTTPMethod::Head);
                entry([server, spec](const crow::request& req, crow::response& res) {
                    handle_file(server, spec, req, res);
                });
            }
        }
    }

#ifdef CROW_ENABLE_SSL
    if (!server->ssl_cert.empty()) {
        try {
            if (server->ssl_cert.find("-----BEGIN") != std::string::npos) {
                asio::ssl::context ctx(asio::ssl::context::tls_server);
                ctx.set_options(asio::ssl::context::default_workarounds |
                                asio::ssl::context::no_sslv2 | asio::ssl::context::no_sslv3);
                ctx.use_certificate_chain(asio::buffer(server->ssl_cert));
                if (!server->ssl_key.empty())
                    ctx.use_private_key(asio::buffer(server->ssl_key), asio::ssl::context::pem);
                server->app->ssl(std::move(ctx));
            } else if (server->ssl_key.empty()) {
                server->app->ssl_file(server->ssl_cert);
            } else {
                server->app->ssl_file(server->ssl_cert, server->ssl_key);
            }
        } catch (const std::exception& e) {
            err = std::string("HTTP ssl: ") + e.what();
            return err;
        }
    } else if (!ssl_proto.empty()) {
        err = "ssl.proto set but ssl.cert is empty";
        return err;
    }
#else
    if (!ssl_cert.empty() || !ssl_proto.empty()) {
        err = "HTTP ssl is not built";
        return err;
    }
#endif

    try {
        server->app->bindaddr(bind).port(port);
    } catch (const std::exception& e) {
        err = e.what();
        return err;
    }

    server->thread = std::thread([server] {
        try {
            server->app->run();
        } catch (const std::exception& e) {
            server->fail = e.what();
        }
        server->running.store(false);
        server->pool_cv.notify_all();
    });

    bool up = false;
    for (int i = 0; i < 100; ++i) {
        if (!server->fail.empty())
            break;
        if (port_open(bind, port)) {
            up = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    /*
     * `port_open` says something is listening, not that it is us - a socket left by
     * anything else answers the same way. So the run thread gets a moment to report
     * a bind failure, and that is believed over the probe.
     */
    if (up && server->fail.empty()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        if (!server->fail.empty())
            up = false;
    }
    if (!up || !server->fail.empty()) {
        try {
            server->app->stop();
        } catch (...) {
        }
        if (server->thread.joinable())
            server->thread.join();
        server->app.reset();
        err = server->fail.empty() ? "HTTP server failed to start" : server->fail;
        return err;
    }
    server->running.store(true);

    {
        std::lock_guard<std::mutex> g(http_mu);
        http_servers[canon] = server;
    }

    reply.push_back("port=" + std::to_string(port));
    reply.push_back("bind=" + bind);
    reply.push_back(std::string("ssl=") + (ssl_cert.empty() ? "off" : (ssl_proto.empty() ? "TLS" : ssl_proto)));
    for (const auto& r : server->routes) {
        std::string line = r.name + " " + r.route;
        for (size_t i = 0; i < r.methods.size(); ++i) {
            line += (i == 0 ? " " : ",");
            line += r.methods[i].verb;
        }
        reply.push_back(std::move(line));
    }
    return {};
}

int status_space_http(caller& call, const barch::key_space_ptr& space) {
    std::shared_ptr<space_http> server;
    {
        std::lock_guard<std::mutex> g(http_mu);
        auto it = http_servers.find(space->canonical());
        if (it != http_servers.end())
            server = it->second;
    }
    if (!server || !server->running.load())
        return call.push_simple("stopped");
    // read both under the one lock, so the pair adds up to the pool
    size_t idle = 0;
    size_t pool_size = 0;
    {
        std::lock_guard<std::mutex> g(server->pool_mu);
        idle = server->idle.size();
        pool_size = server->pool_size;
    }
    size_t executing = pool_size > idle ? pool_size - idle : 0;
    call.start_array();
    call.push_string("port=" + std::to_string(server->port));
    call.push_string("bind=" + server->bind);
    call.push_string(std::string("ssl=") + (server->ssl_proto.empty() ? "off" : server->ssl_proto));
    call.push_string("vms=" + std::to_string(pool_size));
    call.push_string("executing=" + std::to_string(executing));
    call.push_string("idle=" + std::to_string(idle));
    call.push_string("luau_bytes=" + std::to_string(server->luau_bytes->load()));
    for (const auto& r : server->routes) {
        std::string line = r.name + " " + r.route;
        for (size_t i = 0; i < r.methods.size(); ++i) {
            line += (i == 0 ? " " : ",");
            line += r.methods[i].verb;
        }
        call.push_string(line);
    }
    return call.end_array();
}

#endif

} // namespace

namespace barch {

void stop_http_server(const std::string& space) {
#ifdef BARCH_HAS_CROW
    std::shared_ptr<space_http> server;
    {
        std::lock_guard<std::mutex> g(http_mu);
        auto it = http_servers.find(space);
        if (it == http_servers.end())
            return;
        server = std::move(it->second);
        http_servers.erase(it);
    }
    if (server) {
        server->running.store(false);
        server->pool_cv.notify_all();
        if (server->app) {
            try {
                server->app->stop();
            } catch (...) {
            }
        }
    }
    if (server && server->thread.joinable())
        server->thread.join();
    /*
     * The app has to be destroyed here, and not left to the shared_ptr going out of
     * scope, because it cannot go out of scope: every route handler is a lambda
     * capturing this same shared_ptr, the app owns the handlers, and the server owns
     * the app. That cycle kept the whole thing alive after a STOP, and with it the
     * listening socket - so the port went on accepting connections that nothing
     * would ever answer, and the next START saw the port open, decided it had come
     * up, and stored a server whose own bind had failed. See TODO 257.
     *
     * Destroying the app drops the handlers, which drops their references, which is
     * what lets the rest of it go.
     */
    if (server) {
        server->app.reset();
        server->routes.clear();
        server->idle.clear();
    }
#else
    (void) space;
#endif
}

void stop_http_servers() {
#ifdef BARCH_HAS_CROW
    std::vector<std::string> names;
    {
        std::lock_guard<std::mutex> g(http_mu);
        names.reserve(http_servers.size());
        for (const auto& e : http_servers)
            names.push_back(e.first);
    }
    for (const auto& n : names)
        stop_http_server(n);
#endif
}

}

extern "C" {

/* HTTP START [key] [port] [bind] | STOP | STATUS
 *
 * START looks at stored functions' transport() tables and runs a Crow
 * thread for this space. A named key is the config function: its table
 * can set port/bind/ssl and list other keys. Without a key, every
 * function in the space is asked. Keys with no transport() stay ordinary
 * stored functions.
 */
int HTTP(caller& call, const arg_t& argv) {
    if (argv.size() < 2)
        return call.wrong_arity();
    std::string sub = fold_name(as_text(argv[1]));
#ifndef BARCH_HAS_CROW
    (void) call;
    return call.push_error("HTTP is not built");
#else
    if (sub == "STOP") {
        if (argv.size() != 2)
            return call.wrong_arity();
        barch::stop_http_server(call.kspace()->canonical());
        return call.push_simple("OK");
    }
    if (sub == "STATUS") {
        if (argv.size() != 2)
            return call.wrong_arity();
        return status_space_http(call, call.kspace());
    }
    if (sub != "START")
        return call.push_error("HTTP START [key] [port] [bind]|STOP|STATUS");

    std::string httpkey;
    uint16_t port = 0;
    std::string bind;
    std::vector<std::string> keys;
    size_t i = 2;
    if (i < argv.size() && !is_port_arg(argv[i])) {
        httpkey = fold_name(as_text(argv[i]));
        ++i;
    }
    if (i < argv.size() && is_port_arg(argv[i])) {
        port = (uint16_t) std::stoul(as_text(argv[i]));
        ++i;
    }
    if (i < argv.size()) {
        std::string a = as_text(argv[i]);
        if (a.find('.') != std::string::npos || a.find(':') != std::string::npos ||
            a == "*" || a == "localhost") {
            bind = a;
            ++i;
        }
    }
    for (; i < argv.size(); ++i)
        keys.push_back(fold_name(as_text(argv[i])));

    if (!barch::foreign::luau_available())
        return call.push_error("luau not built");

    std::vector<std::string> reply;
    std::string err;
    start_space_http(call.kspace(), httpkey, port, bind, keys, reply, err);
    if (!err.empty())
        return call.push_error(err.c_str());
    call.start_array();
    for (const auto& line : reply)
        call.push_string(line);
    return call.end_array();
#endif
}

}

void register_http_api(function_map& r) {
    r["HTTP"] = {::HTTP, {"write", "data", "function", "admin"}};
}
