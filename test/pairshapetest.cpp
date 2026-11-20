// How replies lay out member-score and field-value pairs - TODO 948.
//
// src/pair_shape_core.h picks flat or nested pairs and opens and closes each one. This
// runs every protocol from 0 to 4, a list of pairs or a lone pair, and 0 to 3 pairs
// through it, into a builder that writes the reply as a shape string. It checks:
//   - under RESP3 a list nests each pair: *[*[m s] *[m s]];
//   - under RESP2 and below the same list is flat: *[m s m s];
//   - a lone pair is flat under every protocol, as ZPOPMIN without a count;
//   - every array closed was opened, and none is left open.
//
// It also runs rules that are known to be wrong and checks that it catches each:
// nesting under RESP2, ignoring the lone pair, nesting only at exactly 3, and a close
// that does not match its open.
#include "pair_shape_core.h"

#include <cstdio>
#include <functional>
#include <string>

using namespace barch::pair_shape_core;

static int failures = 0;

static void check(bool ok, const char* what) {
    std::printf("%-72s %s\n", what, ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

namespace {
    /** writes a reply as a shape string and counts open arrays */
    struct builder {
        std::string out;
        int depth = 0;
        bool underflow = false;

        void sep() {
            if (!out.empty() && out.back() != '[') out += ' ';
        }
        void start_array() {
            sep();
            out += "*[";
            ++depth;
        }
        void end_array() {
            if (depth == 0) underflow = true;
            else --depth;
            out += ']';
        }
        void value(const char* v) {
            sep();
            out += v;
        }
        bool balanced() const { return depth == 0 && !underflow; }
    };

    struct rules {
        std::function<layout(int, bool)> pairs;
        std::function<void(builder&, layout)> open;
        std::function<void(builder&, layout)> close;
    };

    const rules real{
        [](int p, bool l) { return barch::pair_shape_core::pairs(p, l); },
        [](builder& b, layout l) { open_pair(b, l); },
        [](builder& b, layout l) { close_pair(b, l); },
    };

    /** the reply the commands build: an outer array and n pairs */
    builder reply(const rules& r, int protocol, bool list, int n) {
        builder b;
        const layout l = r.pairs(protocol, list);
        b.start_array();
        for (int i = 0; i < n; ++i) {
            r.open(b, l);
            b.value("m");
            b.value("s");
            r.close(b, l);
        }
        b.end_array();
        return b;
    }

    std::string expected(int protocol, bool list, int n) {
        const bool nest = protocol >= 3 && list;
        std::string out = "*[";
        for (int i = 0; i < n; ++i) {
            if (i) out += ' ';
            out += nest ? "*[m s]" : "m s";
        }
        return out + "]";
    }

    /** the number of cases the rules get wrong */
    int wrong(const rules& r) {
        int bad = 0;
        for (int protocol = 0; protocol <= 4; ++protocol)
            for (bool list : {false, true})
                for (int n = 0; n <= 3; ++n) {
                    // a lone pair is one pair, or none for a missing key
                    if (!list && n > 1) continue;
                    builder b = reply(r, protocol, list, n);
                    if (!b.balanced() || b.out != expected(protocol, list, n)) ++bad;
                }
        return bad;
    }
}

int main() {
    check(wrong(real) == 0, "every protocol, list or lone, 0 to 3 pairs has its shape");

    check(reply(real, 3, true, 2).out == "*[*[m s] *[m s]]", "RESP3 list nests each pair");
    check(reply(real, 2, true, 2).out == "*[m s m s]", "RESP2 list stays flat");
    check(reply(real, 3, false, 1).out == "*[m s]", "RESP3 lone pair stays flat");
    check(reply(real, 3, true, 0).out == "*[]", "RESP3 empty list is an empty array");

    rules m = real;
    m.pairs = [](int p, bool l) { return p >= 2 && l ? layout::nested : layout::flat; };
    check(wrong(m) > 0, "mutant: nesting under RESP2 is caught");

    m = real;
    m.pairs = [](int p, bool) { return p >= 3 ? layout::nested : layout::flat; };
    check(wrong(m) > 0, "mutant: nesting a lone pair is caught");

    m = real;
    m.pairs = [](int p, bool l) { return p == 3 && l ? layout::nested : layout::flat; };
    check(wrong(m) > 0, "mutant: nesting only at exactly 3 is caught");

    m = real;
    m.close = [](builder& b, layout) { b.end_array(); };
    check(wrong(m) > 0, "mutant: a close that always closes is caught");

    m = real;
    m.close = [](builder&, layout) {};
    check(wrong(m) > 0, "mutant: a close that never closes is caught");

    std::printf("%s\n", failures ? "FAILED" : "all passed");
    return failures ? 1 : 0;
}
