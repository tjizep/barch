//
// A stored function's own settings, from its header - TODO 434. See driver.h.
//
#include "driver.h"

#include <simdjson.h>

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

bool read_function_meta(const std::string& source, function_meta& out, std::string& err) {
    out = function_meta{};
    std::string json;
    if (!find_meta(source, json))
        return true;
    out.present = true;
    simdjson::dom::parser parser;
    simdjson::dom::element doc;
    if (parser.parse(json).get(doc) != simdjson::SUCCESS || !doc.is_object()) {
        err = "the --@barch header isn't a JSON object";
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
            err = std::string("the --@barch header's ") + field + " is a whole number above 0";
            return false;
        }
        *into = n;
    }
    return true;
}

}
