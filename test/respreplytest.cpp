// The RESP reply parser - TODO 379.
//
// Every RESP2 and RESP3 type, nesting, the limits, garbage, and the thing an
// incremental parser most easily gets wrong: the same stream fed whole, a byte at a
// time and in chunks of every size from 1 to 17 has to give the same replies.
#include "resp_reply.h"

#include <cmath>
#include <cstdio>
#include <string>
#include <vector>

using barch::resp::reply_parser;
using barch::resp::limits;

// sastam.cpp - the heap behind heap::vector - asks these about memory limits. They
// belong to the configuration, which a parser test has no business loading
namespace barch {
bool get_cgroup_memory_control() { return false; }
uint64_t get_cgroup_memory_headroom() { return 0; }
std::string get_cgroup_memory_path() { return {}; }
}

static int failures = 0;

#define CHECK(cond, what)                                                              \
    do {                                                                               \
        if (!(cond)) {                                                                 \
            std::printf("  FAIL %s (%s:%d)\n", what, __FILE__, __LINE__);              \
            ++failures;                                                                \
        }                                                                              \
    } while (0)

/** a reply written back as text, so two parses can be compared and read */
static std::string show(const Variable& v) {
    switch (v.index()) {
        case var_bool: return std::get<bool>(v) ? "#t" : "#f";
        case var_int64: return ":" + std::to_string(std::get<int64_t>(v));
        case var_double: {
            double d = std::get<double>(v);
            if (std::isnan(d)) return ",nan";
            if (std::isinf(d)) return d > 0 ? ",inf" : ",-inf";
            char b[64];
            std::snprintf(b, sizeof b, ",%.17g", d);
            return b;
        }
        case var_string: return "s(" + std::get<std::string>(v) + ")";
        case var_null: return "_";
        case var_error: return "err(" + std::string(std::get<error>(v).what()) + ")";
        case var_verbatim: {
            const auto& vb = std::get<verbatim_t>(v);
            return "v(" + vb.format + ":" + vb.text + ")";
        }
        case var_array:
        case var_map:
        case var_set: {
            std::string s = v.index() == var_array ? "[" : v.index() == var_map ? "{" : "~(";
            bool first = true;
            for (const auto& e : v.elements()) {
                if (!first) s += ",";
                first = false;
                s += show(static_cast<const variable_t&>(e));
            }
            return s + (v.index() == var_array ? "]" : v.index() == var_map ? "}" : ")");
        }
        default: return "?";
    }
}

/** every reply in `stream`, fed `chunk` bytes at a time (0 = all at once) */
static std::vector<std::string> parse_all(const std::string& stream, size_t chunk,
                                          std::string* failed = nullptr, limits lim = {}) {
    reply_parser p(lim);
    std::vector<std::string> out;
    size_t at = 0;
    auto drain = [&]() -> bool {
        for (;;) {
            Variable v;
            std::string err;
            int r = p.next(v, err);
            if (r == 1) { out.push_back(show(v)); continue; }
            if (r < 0) { if (failed) *failed = err; return false; }
            return true;
        }
    };
    if (chunk == 0) {
        p.feed(stream.data(), stream.size());
        drain();
        return out;
    }
    while (at < stream.size()) {
        size_t n = std::min(chunk, stream.size() - at);
        p.feed(stream.data() + at, n);
        at += n;
        if (!drain())
            break;
    }
    return out;
}

static std::string one(const std::string& wire) {
    auto got = parse_all(wire, 0);
    return got.size() == 1 ? got[0] : "<" + std::to_string(got.size()) + " replies>";
}

int main() {
    std::printf("resp reply parser\n");

    std::printf(" every type\n");
    CHECK(one("+OK\r\n") == "s($OK)", "simple string, marked like a bulk one");
    CHECK(one("+$looks like bulk\r\n") == "s($$looks like bulk)", "a simple string starting with $");
    CHECK(one("-ERR no\r\n") == "err(ERR no)", "error");
    CHECK(one(":-42\r\n") == ":-42", "integer");
    CHECK(one("$5\r\nhello\r\n") == "s($hello)", "bulk");
    CHECK(one("$0\r\n\r\n") == "s($)", "empty bulk");
    CHECK(one(std::string("$5\r\na\r\n\0b\r\n", 11)) == std::string("s($a\r\n\0b)", 9),
          "a bulk holding CRLF and NUL");
    CHECK(one("$-1\r\n") == "_", "RESP2 null bulk");
    CHECK(one("*-1\r\n") == "_", "RESP2 null array");
    CHECK(one("*0\r\n") == "[]", "empty array");
    CHECK(one("*3\r\n:1\r\n$1\r\nx\r\n*1\r\n+y\r\n") == "[:1,s($x),[s($y)]]", "nested array");
    CHECK(one("_\r\n") == "_", "RESP3 null");
    CHECK(one("#t\r\n") == "#t" && one("#f\r\n") == "#f", "boolean");
    CHECK(one(",3.25\r\n") == ",3.25", "double");
    CHECK(one(",inf\r\n") == ",inf" && one(",-inf\r\n") == ",-inf" && one(",nan\r\n") == ",nan",
          "infinities and nan");
    CHECK(one("(3492890328409238509324850943850943825024385\r\n")
          == "s($3492890328409238509324850943850943825024385)", "big number stays text");
    CHECK(one("!21\r\nSYNTAX invalid syntax\r\n") == "err(SYNTAX invalid syntax)", "blob error");
    CHECK(one("=15\r\ntxt:Some string\r\n") == "v(txt:Some string)", "verbatim");
    CHECK(one("%2\r\n+a\r\n:1\r\n+b\r\n*2\r\n:2\r\n:3\r\n") == "{s($a),:1,s($b),[:2,:3]}", "map");
    CHECK(one("~2\r\n+x\r\n+y\r\n") == "~(s($x),s($y))", "set");
    CHECK(one("%0\r\n") == "{}" && one("~0\r\n") == "~()", "empty map and set");

    std::printf(" attributes and pushes are not replies\n");
    CHECK(one("|1\r\n+ttl\r\n:3600\r\n$3\r\nval\r\n") == "s($val)", "an attribute before a value");
    CHECK(one("*2\r\n|1\r\n+a\r\n+b\r\n:1\r\n:2\r\n") == "[:1,:2]",
          "an attribute inside an array doesn't take a slot");
    {
        auto got = parse_all(">3\r\n+message\r\n+chan\r\n+hi\r\n+OK\r\n", 0);
        CHECK(got.size() == 1 && got[0] == "s($OK)", "a push before a reply is dropped");
    }

    std::printf(" the same stream split every way\n");
    const std::string stream =
        "+OK\r\n" "-ERR bad\r\n" ":123456789012\r\n" "$11\r\nhello world\r\n" "$-1\r\n"
        "*3\r\n$1\r\na\r\n*2\r\n:1\r\n_\r\n%1\r\n+k\r\n~1\r\n#t\r\n"
        ",1.5\r\n" "=7\r\nmkd:*x*\r\n" "|1\r\n+a\r\n+b\r\n:7\r\n"
        ">2\r\n+p\r\n+q\r\n" "*0\r\n" "$0\r\n\r\n" "!3\r\nbad\r\n" "(99\r\n";
    auto whole = parse_all(stream, 0);
    CHECK(whole.size() == 13, "13 replies in the stream");
    for (size_t chunk = 1; chunk <= 17; ++chunk) {
        auto got = parse_all(stream, chunk);
        if (got != whole) {
            std::printf("  FAIL chunk size %zu gave %zu replies\n", chunk, got.size());
            ++failures;
        }
    }

    std::printf(" state between replies\n");
    {
        reply_parser p;
        p.feed("+a\r\n+b\r\n*2\r\n:1\r\n", 16);
        Variable v;
        std::string err;
        CHECK(p.next(v, err) == 1 && show(v) == "s($a)", "first");
        CHECK(p.buffered() > 0 && !p.idle(), "more waiting");
        CHECK(p.next(v, err) == 1 && show(v) == "s($b)", "second");
        CHECK(p.next(v, err) == 0 && !p.idle(), "an array half here");
        p.feed(":2\r\n", 4);
        CHECK(p.next(v, err) == 1 && show(v) == "[:1,:2]", "and finished");
        CHECK(p.idle(), "nothing left");
    }

    std::printf(" what it refuses\n");
    auto refuses = [](const std::string& wire, const char* want, limits lim = {}) {
        std::string err;
        parse_all(wire, 0, &err, lim);
        return err.find(want) != std::string::npos;
    };
    CHECK(refuses("?what\r\n", "not a RESP reply"), "an unknown type byte");
    CHECK(refuses(":12x\r\n", "integer"), "a bad integer");
    CHECK(refuses("$3\r\nabcde\r\n", "without its CRLF"), "a bulk longer than it said");
    CHECK(refuses("$-2\r\n", "length"), "a negative length");
    CHECK(refuses("*-5\r\n", "length"), "a negative count");
    CHECK(refuses("#x\r\n", "boolean"), "a bad boolean");
    CHECK(refuses(",1.2.3\r\n", "double"), "a bad double");
    CHECK(refuses("=3\r\nabc\r\n", "verbatim"), "a verbatim without its format");
    CHECK(refuses("_x\r\n", "null"), "a null with something in it");
    CHECK(refuses("(12a\r\n", "big number"), "a bad big number");
    CHECK(refuses("+" + std::string(70000, 'x'), "no end"), "a line that never ends");
    limits small;
    small.max_bulk = 10;
    small.max_elements = 4;
    small.max_depth = 3;
    small.max_reply = 64;
    CHECK(refuses("$11\r\nhello world\r\n", "larger than the limit", small), "max_bulk");
    CHECK(refuses("*5\r\n", "larger than the limit", small), "max_elements");
    CHECK(refuses("%3\r\n", "larger than the limit", small), "a map counts keys and values");
    CHECK(refuses("*1\r\n*1\r\n*1\r\n*1\r\n:1\r\n", "deeper", small), "max_depth");
    CHECK(refuses("*4\r\n$10\r\n0123456789\r\n$10\r\n0123456789\r\n$10\r\n0123456789\r\n$10\r\n"
                  "0123456789\r\n", "reply larger", small), "max_reply across an aggregate");
    {
        // a bulk announced as huge isn't waited for or allocated
        limits l;
        l.max_bulk = 1000;
        CHECK(refuses("$999999999\r\n", "larger than the limit", l), "a huge announced bulk");
    }
    {
        // after an error the parser stays failed: the stream has lost its place
        reply_parser p;
        p.feed("?\r\n+OK\r\n", 8);
        Variable v;
        std::string err;
        CHECK(p.next(v, err) == -1, "fails");
        CHECK(p.next(v, err) == -1, "and stays failed");
    }

    if (failures) {
        std::printf("resp reply parser: %d failures\n", failures);
        return 1;
    }
    std::printf("resp reply parser: all passed\n");
    return 0;
}
