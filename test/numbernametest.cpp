// Which names are stored as numbers, over every short text - TODO 952.
//
// src/number_name_core.h stores a key, member or field as a number only when the number
// prints back as the same text. This parses and prints the way src/conversion.cpp does,
// with fmt, and runs the rule over every text of up to five characters from
// `0 1 9 - + . e`, plus the edges: 2^63 and its neighbours, -0, inf and nan. It checks:
//   - no two texts get the same stored form;
//   - a stored number prints back as its text;
//   - with noint set, nothing is an integer;
//   - plain numbers like 10, -3 and 1.5 are still numbers, so they keep numeric order.
//
// It also runs rules that are known to be wrong and checks that it catches each: the old
// rule, which stored whatever parsed; a rule that checks only the integer print; and a
// rule that prefers a real to an integer.
#include "number_name_core.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <charconv>
#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <tuple>
#include <vector>

#include <fmt/format.h>

using namespace barch::number_name_core;

static int failures = 0;

static void check(bool ok, const char* what) {
    std::printf("%-72s %s\n", what, ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

namespace {
    using rule = std::function<form(std::string_view, bool, reading, reading)>;

    /** the stored form of a name: what it is, and the bits of its number */
    struct stored {
        form f = form::string;
        uint64_t bits = 0;
        bool operator<(const stored& o) const { return std::tie(f, bits) < std::tie(o.f, o.bits); }
    };

    /** what conversion::convert keeps for a text, under a rule */
    stored store(const rule& r, const std::string& text, bool noint, std::string* printed = nullptr) {
        if (text.empty() || !((text[0] >= '0' && text[0] <= '9') || text[0] == '-')) return {};
        const char* b = text.data();
        const char* e = b + text.size();
        int64_t i = 0;
        double d = 0;
        auto ir = std::from_chars(b, e, i);
        auto dr = std::from_chars(b, e, d);
        bool iok = ir.ec == std::errc{} && ir.ptr == e;
        bool dok = dr.ec == std::errc{} && dr.ptr == e;
        std::string ip = iok ? fmt::format("{}", i) : std::string{};
        std::string dp = dok ? fmt::format("{}", d) : std::string{};
        switch (r(text, noint, reading{iok, ip}, reading{dok, dp})) {
            case form::integer:
                if (printed) *printed = ip;
                return {form::integer, static_cast<uint64_t>(i)};
            case form::real: {
                if (printed) *printed = dp;
                uint64_t bits;
                std::memcpy(&bits, &d, sizeof bits);
                return {form::real, bits};
            }
            default:
                return {};
        }
    }

    std::vector<std::string> texts() {
        std::vector<std::string> out{"9223372036854775807", "9223372036854775808",
                                     "9223372036854775809", "-9223372036854775808",
                                     "-9223372036854775809", "-0", "-0.0", "inf", "-inf",
                                     "nan", "-nan", "1e308", "1e309", "5e-324", "0.1"};
        const char alphabet[] = "019-+.e";
        std::vector<std::string> layer{""};
        for (int len = 1; len <= 5; ++len) {
            std::vector<std::string> next;
            for (auto& t : layer)
                for (const char* c = alphabet; *c; ++c) next.push_back(t + *c);
            out.insert(out.end(), next.begin(), next.end());
            layer = std::move(next);
        }
        std::sort(out.begin(), out.end());
        out.erase(std::unique(out.begin(), out.end()), out.end());
        return out;
    }

    const std::vector<std::string> all = texts();

    /** the number of ways the rule gets names wrong; tell names the first few */
    int wrong(const rule& r, bool tell = false) {
        int bad = 0;
        auto note = [&](const std::string& t, const char* why) {
            if (tell && bad <= 5) std::printf("  %s: %s\n", t.c_str(), why);
        };
        for (bool noint : {false, true}) {
            std::map<stored, std::string> seen;
            for (auto& t : all) {
                std::string printed;
                stored s = store(r, t, noint, &printed);
                if (s.f == form::string) continue;
                if (!seen.emplace(s, t).second) ++bad, note(t, "shares a key");
                if (printed != t) ++bad, note(t, "does not come back");
                if (noint && s.f == form::integer) ++bad, note(t, "is an integer under noint");
            }
        }
        struct want { const char* text; form f; };
        for (auto [text, f] : {want{"10", form::integer}, want{"-3", form::integer},
                               want{"0", form::integer}, want{"1.5", form::real},
                               want{"-0", form::real}, want{"010", form::string},
                               want{"1e3", form::string}, want{"+1", form::string}}) {
            if (store(r, text, false).f != f) ++bad, note(text, "has the wrong form");
        }
        return bad;
    }
}

int main() {
    const rule real = choose;
    check(wrong(real, true) == 0, "every short text keeps its bytes, and plain numbers stay numbers");
    std::printf("  %zu texts\n", all.size());

    rule old = [](std::string_view, bool noint, reading i, reading d) {
        if (!noint && i.parsed) return form::integer;
        if (d.parsed) return form::real;
        return form::string;
    };
    check(wrong(old) > 0, "mutant: storing whatever parses, the old rule, is caught");

    rule int_only = [](std::string_view text, bool noint, reading i, reading d) {
        if (!noint && i.parsed && i.printed == text) return form::integer;
        if (d.parsed) return form::real;
        return form::string;
    };
    check(wrong(int_only) > 0, "mutant: checking only the integer print is caught");

    rule prefer_real = [](std::string_view text, bool noint, reading i, reading d) {
        if (d.parsed && d.printed == text) return form::real;
        if (!noint && i.parsed && i.printed == text) return form::integer;
        return form::string;
    };
    check(wrong(prefer_real) > 0, "mutant: preferring a real to an integer is caught");

    std::printf("%s\n", failures ? "FAILED" : "all passed");
    return failures ? 1 : 0;
}
