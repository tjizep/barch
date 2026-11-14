//
// A stored function's own settings, from its header - TODO 434. See driver.h.
//
#include "driver.h"

#include <simdjson.h>
#include <cstdint>

namespace barch::foreign {

/*
 * The header is what Luau reads before the first token: blank lines and comments.
 * The same walk as wants_native_marker in luau_driver.cpp, looking for a line comment
 * or a block comment that starts with `@barch`.
 */
static bool find_meta(const std::string& s, std::string& json) {
    const size_t n = s.size();
    size_t i = 0;
    while (i < n) {
        const char c = s[i];
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
            ++i;
            continue;
        }
        if (c != '-' || i + 1 >= n || s[i + 1] != '-')
            return false;                       // the first token: the header is over
        size_t p = i + 2;
        if (p < n && s[p] == '[') {
            size_t q = p + 1;
            while (q < n && s[q] == '=')
                ++q;
            if (q < n && s[q] == '[') {         // --[[ … ]] or --[==[ … ]==]
                std::string close = "]" + std::string(q - p - 1, '=') + "]";
                size_t e = s.find(close, q + 1);
                if (e == std::string::npos)
                    return false;
                size_t body = q + 1;
                while (body < e && (s[body] == ' ' || s[body] == '\t' || s[body] == '\r' || s[body] == '\n'))
                    ++body;
                if (s.compare(body, 6, "@barch") == 0) {
                    json = s.substr(body + 6, e - body - 6);
                    return true;
                }
                i = e + close.size();
                continue;
            }
        }
        size_t eol = s.find('\n', i);
        if (eol == std::string::npos)
            eol = n;
        if (s.compare(p, 6, "@barch") == 0) {
            json = s.substr(p + 6, eol - p - 6);
            return true;
        }
        i = eol;
    }
    return false;
}

/** an unsigned LEB128 number of at most 32 bits, moving `at` past it */
static bool read_leb(const std::string& m, size_t& at, uint32_t& out) {
    uint64_t v = 0;
    for (int shift = 0; shift < 35; shift += 7) {
        if (at >= m.size())
            return false;
        const auto b = (uint8_t) m[at++];
        v |= (uint64_t) (b & 0x7f) << shift;
        if (!(b & 0x80)) {
            if (v > UINT32_MAX)
                return false;
            out = (uint32_t) v;
            return true;
        }
    }
    return false;
}

/*
 * The module's sections, walked by hand rather than asked of WAMR, so the header
 * reader works in a build without ADD_WASM and doesn't need a loaded module - TODO 820.
 * Only the framing is checked here; WAMR checks the rest when SETF loads it.
 */
bool read_wasm_sections(const std::string& m, wasm_sections& out, std::string& err) {
    out = wasm_sections{};
    if (!is_wasm_module(m) || m.size() < 8) {
        err = "a WASM module is cut short";
        return false;
    }
    size_t at = 8;  // the magic and the version
    while (at < m.size()) {
        const auto id = (uint8_t) m[at++];
        uint32_t size = 0;
        if (!read_leb(m, at, size) || size > m.size() - at) {
            err = "a WASM module's section runs past its end";
            return false;
        }
        const size_t end = at + size;
        if (id == 8) {
            out.has_start = true;
        } else if (id == 0) {
            uint32_t name_len = 0;
            size_t p = at;
            if (!read_leb(m, p, name_len) || name_len > end - p) {
                err = "a WASM module's custom section has a bad name";
                return false;
            }
            if (m.compare(p, name_len, "barch") == 0 && name_len == 5) {
                if (out.has_meta) {
                    err = "a WASM module has more than one barch section";
                    return false;
                }
                out.has_meta = true;
                out.meta = m.substr(p + name_len, end - p - name_len);
            }
        }
        at = end;
    }
    return true;
}

bool read_function_meta(const std::string& source, function_meta& out, std::string& err) {
    out = function_meta{};
    std::string json;
    // where the settings came from, so a refusal names the right thing
    std::string where = "the --@barch header";
    if (is_wasm_module(source)) {
        wasm_sections sections;
        if (!read_wasm_sections(source, sections, err))
            return false;
        if (!sections.has_meta)
            return true;
        json = std::move(sections.meta);
        where = "the barch section";
    } else if (!find_meta(source, json)) {
        return true;
    }
    out.present = true;
    simdjson::dom::parser parser;
    simdjson::dom::element doc;
    if (parser.parse(json).get(doc) != simdjson::SUCCESS || !doc.is_object()) {
        err = where + " isn't a JSON object";
        return false;
    }
    for (auto [field, into] : {std::pair<const char*, uint64_t*>{"deadline_ms", &out.deadline_ms},
                               {"slice_insns", &out.slice_insns}}) {
        simdjson::dom::element v;
        if (doc[field].get(v) != simdjson::SUCCESS)
            continue;                           // not given
        uint64_t n = 0;
        int64_t sn = 0;
        if (v.get(n) == simdjson::SUCCESS) {
            // a uint64
        } else if (v.get(sn) == simdjson::SUCCESS && sn > 0) {
            n = (uint64_t) sn;
        } else {
            n = 0;
        }
        if (n == 0) {
            err = where + "'s " + field + " is a whole number above 0";
            return false;
        }
        *into = n;
    }
    // TODO 672: whether it writes, as a JSON bool
    simdjson::dom::element w;
    if (doc["write"].get(w) == simdjson::SUCCESS) {
        bool may = true;
        if (w.get(may) != simdjson::SUCCESS) {
            err = where + "'s write is true or false";
            return false;
        }
        out.read_only = !may;
    }
    // TODO 836: whether it may go into overtime, as a JSON bool
    simdjson::dom::element o;
    if (doc["overtime"].get(o) == simdjson::SUCCESS) {
        bool may = true;
        if (o.get(may) != simdjson::SUCCESS) {
            err = where + "'s overtime is true or false";
            return false;
        }
        out.overtime = may;
    }
    // TODO 673: how a writing function is replicated
    simdjson::dom::element r;
    if (doc["replicate"].get(r) == simdjson::SUCCESS) {
        std::string_view how;
        if (r.get(how) != simdjson::SUCCESS || (how != "log" && how != "effects")) {
            err = where + "'s replicate is \"log\" or \"effects\"";
            return false;
        }
        out.replicate_effects = how == "effects";
    }
    return true;
}


/*
 * A WASM module's transport, from its barch section - TODO 823. Read with the same
 * rules as the Luau readers in luau_driver.cpp (read_resp_transport and the rest), so
 * a schedule or a queue means the same whichever language declared it.
 */
namespace {

typedef simdjson::dom::element json_el;

std::string lowered(std::string_view v) {
    std::string out(v);
    for (auto& ch : out)
        ch = (char) tolower((unsigned char) ch);
    return out;
}

/** a string field: false with `err` when it's there and isn't a string */
bool text_field(json_el t, const char* field, const std::string& what, std::string& out, std::string& err) {
    json_el v;
    if (t[field].get(v) != simdjson::SUCCESS || v.is_null())
        return true;
    std::string_view sv;
    if (v.get(sv) != simdjson::SUCCESS) {
        err = what + " field '" + field + "' must be a string";
        return false;
    }
    out = std::string(sv);
    return true;
}

bool flag_field(json_el t, const char* field, const std::string& what, bool& out, std::string& err) {
    json_el v;
    if (t[field].get(v) != simdjson::SUCCESS || v.is_null())
        return true;
    if (v.get(out) != simdjson::SUCCESS) {
        err = what + " field '" + field + "' must be true or false";
        return false;
    }
    return true;
}

bool text_list(json_el v, const std::string& what, std::vector<std::string>& out, std::string& err) {
    simdjson::dom::array a;
    if (v.get(a) != simdjson::SUCCESS) {
        err = what + " must be a list of strings";
        return false;
    }
    for (auto item : a) {
        std::string_view sv;
        if (item.get(sv) != simdjson::SUCCESS) {
            err = what + " must be a list of strings";
            return false;
        }
        out.emplace_back(sv);
    }
    return true;
}

bool read_resp(json_el t, resp_spec& spec, std::string& err) {
    const std::string what = "the barch section's resp transport";
    spec.is_resp = true;
    simdjson::dom::object methods;
    if (t["methods"].get(methods) != simdjson::SUCCESS) {
        err = what + " needs a methods object";
        return false;
    }
    for (auto [key, v] : methods) {
        resp_method m;
        m.name = std::string(key);
        for (auto& ch : m.name)
            ch = (char) toupper((unsigned char) ch);
        if (m.name.empty()) {
            err = what + " has a method with no name";
            return false;
        }
        const std::string mwhat = "resp method '" + m.name + "'";
        if (!v.is_object()) {
            err = mwhat + " must be an object naming its export";
            return false;
        }
        if (!text_field(v, "export", mwhat, m.export_name, err))
            return false;
        if (m.export_name.empty()) {
            err = mwhat + " needs the export it runs";
            return false;
        }
        json_el cats;
        if (v["categories"].get(cats) == simdjson::SUCCESS && !cats.is_null()) {
            if (!text_list(cats, "categories for '" + m.name + "'", m.categories, err))
                return false;
            for (auto& c : m.categories)
                c = lowered(c);
        }
        json_el ar;
        if (v["arity"].get(ar) == simdjson::SUCCESS && !ar.is_null()) {
            int64_t n = 0;
            if (ar.get(n) != simdjson::SUCCESS || n < -1000000 || n > 1000000) {
                err = mwhat + "'s arity must be a whole number";
                return false;
            }
            m.arity = (int) n;
        }
        spec.methods.push_back(std::move(m));
    }
    if (spec.methods.empty()) {
        err = what + " exposes no methods";
        return false;
    }
    return true;
}

bool read_cron(json_el t, cron_spec& spec, std::string& err) {
    const std::string what = "cron transport";
    spec.is_cron = true;
    for (auto [field, into] : {std::pair<const char*, std::string*>{"space", &spec.space},
                               {"call", &spec.call}, {"every", &spec.every}, {"cron", &spec.cron},
                               {"user", &spec.user}, {"jitter", &spec.jitter}}) {
        if (!text_field(t, field, what, *into, err))
            return false;
    }
    std::string overlap, tz;
    if (!text_field(t, "overlap", what, overlap, err) || !text_field(t, "tz", what, tz, err))
        return false;
    if (!overlap.empty())
        spec.overlap = overlap;
    if (!tz.empty())
        spec.tz = tz;
    if (!flag_field(t, "catchup", what, spec.catchup, err) || !flag_field(t, "enabled", what, spec.enabled, err))
        return false;
    json_el args;
    if (t["args"].get(args) == simdjson::SUCCESS && !args.is_null() &&
        !text_list(args, "cron transport args", spec.args, err))
        return false;
    if (spec.space.empty() || spec.call.empty()) {
        err = "cron transport needs a space and a call";
        return false;
    }
    if (spec.every.empty() == spec.cron.empty()) {
        err = "cron transport needs exactly one of every or cron";
        return false;
    }
    if (spec.user.empty()) {
        err = "cron transport needs a user to run as";
        return false;
    }
    if (spec.overlap != "skip" && spec.overlap != "queue" && spec.overlap != "allow") {
        err = "cron transport overlap must be skip, queue or allow";
        return false;
    }
    return true;
}

bool read_queue(json_el t, queue_spec& spec, std::string& err) {
    const std::string what = "queue transport";
    spec.is_queue = true;
    for (auto [field, into] : {std::pair<const char*, std::string*>{"name", &spec.name},
                               {"space", &spec.space}, {"call", &spec.call}, {"user", &spec.user},
                               {"dir", &spec.dir}}) {
        if (!text_field(t, field, what, *into, err))
            return false;
    }
    std::string durability, poll;
    if (!text_field(t, "durability", what, durability, err) || !text_field(t, "poll", what, poll, err))
        return false;
    if (!durability.empty())
        spec.durability = durability;
    if (!poll.empty())
        spec.poll = poll;
    json_el ma;
    if (t["max_attempts"].get(ma) == simdjson::SUCCESS && !ma.is_null()) {
        int64_t n = 0;
        // a whole number of deliveries, at least one, as the Luau reader says
        if (ma.get(n) != simdjson::SUCCESS || n < 1 || n > 1000000) {
            err = "queue transport max_attempts must be a whole number of at least 1";
            return false;
        }
        spec.max_attempts = (uint32_t) n;
    }
    if (!flag_field(t, "enabled", what, spec.enabled, err))
        return false;
    if (spec.name.empty()) {
        err = "queue transport needs a name for senders to publish to";
        return false;
    }
    if (spec.space.empty() || spec.call.empty()) {
        err = "queue transport needs a space and a call";
        return false;
    }
    if (spec.user.empty()) {
        err = "queue transport needs a user to run as";
        return false;
    }
    return true;
}

/** a websocket transport - TODO 901. The same fields as the Luau table */
bool read_websocket(json_el t, http_route& out, std::string& err) {
    const std::string what = "websocket transport";
    out.kind = "websocket";
    out.has_transport = true;
    for (auto [field, into] : {std::pair<const char*, std::string*>{"route", &out.route},
                               {"call", &out.call}, {"user", &out.user}}) {
        if (!text_field(t, field, what, *into, err))
            return false;
    }
    json_el mp;
    if (t["max_payload"].get(mp) == simdjson::SUCCESS && !mp.is_null()) {
        int64_t n = 0;
        if (mp.get(n) != simdjson::SUCCESS || n < 1) {
            err = "websocket transport max_payload must be a whole number of bytes";
            return false;
        }
        out.max_payload = (uint64_t) n;
    }
    json_el methods;
    if (t["methods"].get(methods) == simdjson::SUCCESS && !methods.is_null()) {
        err = "kind=websocket takes no methods; it names a call";
        return false;
    }
    if (!out.route.empty() && out.route[0] != '/')
        out.route.insert(out.route.begin(), '/');
    return check_websocket_route(out, err);
}

/** the barch section's transport object, if it has one */
bool transport_of(const std::string& source, simdjson::dom::parser& parser, json_el& t, bool& has,
                  std::string& err) {
    has = false;
    wasm_sections sections;
    if (!read_wasm_sections(source, sections, err))
        return false;
    if (!sections.has_meta)
        return true;
    json_el doc;
    if (parser.parse(sections.meta).get(doc) != simdjson::SUCCESS || !doc.is_object()) {
        err = "the barch section isn't a JSON object";
        return false;
    }
    // "service" is the newer name, as it is for Luau's service() - TODO 557
    if (doc["service"].get(t) != simdjson::SUCCESS && doc["transport"].get(t) != simdjson::SUCCESS)
        return true;
    if (!t.is_object()) {
        err = "the barch section's transport must be an object";
        return false;
    }
    has = true;
    return true;
}

}  // namespace

bool check_websocket_route(http_route& route, std::string& err) {
    if (route.route.empty() || route.route == "/") {
        err = "kind=websocket needs a route";
        return false;
    }
    // Crow's websocket rule is matched whole and binds nothing - TODO 901
    if (route.route.find_first_of("{}*<>") != std::string::npos) {
        err = "kind=websocket route can't have {holes}, * or <>";
        return false;
    }
    if (!route.methods.empty()) {
        err = "kind=websocket takes no methods; it names a call";
        return false;
    }
    route.has_route = true;
    return true;
}

bool read_wasm_route(const std::string& source, http_route& out, std::string& err) {
    simdjson::dom::parser parser;
    json_el t;
    bool has = false;
    if (!transport_of(source, parser, t, has, err))
        return false;
    if (!has)
        return true;
    std::string_view kind_sv;
    if (t["kind"].get(kind_sv) != simdjson::SUCCESS || lowered(kind_sv) != "websocket")
        return true;
    return read_websocket(t, out, err);
}

bool read_wasm_transport(const std::string& source, resp_spec* resp, cron_spec* cron, queue_spec* queue,
                         std::string& err) {
    simdjson::dom::parser parser;
    json_el t;
    bool has = false;
    if (!transport_of(source, parser, t, has, err))
        return false;
    if (!has)
        return true;
    std::string_view kind_sv;
    if (t["kind"].get(kind_sv) != simdjson::SUCCESS) {
        err = "the barch section's transport needs a kind";
        return false;
    }
    const auto kind = lowered(kind_sv);
    if (resp)
        resp->has_transport = true;
    if (cron)
        cron->has_transport = true;
    if (queue)
        queue->has_transport = true;
    if (kind == "resp")
        return !resp || read_resp(t, *resp, err);
    if (kind == "cron")
        return !cron || read_cron(t, *cron, err);
    if (kind == "queue")
        return !queue || read_queue(t, *queue, err);
    if (kind == "websocket") {
        // checked at SETF, so a bad route is refused before HTTP START - TODO 901
        http_route ws;
        return read_websocket(t, ws, err);
    }
    err = "a WASM function's transport kind is resp, cron, queue or websocket, not " + std::string(kind_sv);
    return false;
}

}
