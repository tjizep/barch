#pragma once
// Whether a name is stored as a number - TODO 952.
//
// A key, member or field that is a number sorts as a number. Before TODO 952 any text
// that parsed as one was stored as one, so `010` and `10` were one key, `1e3` came
// back as `1000`, and two integers past 2^63 shared a double. A name is now a number
// only when it prints back as the same text, so the stored form gives the name back
// byte for byte and two names never share a key.
//
// The caller parses and prints; this decides. test/numbernametest.cpp runs it over
// every short text, and TestCoreBoundary keeps it free of I/O.
#include <string_view>

namespace barch::number_name_core {
    enum class form { string, integer, real };

    /** one way of reading the text: whether it parsed, and how the number prints */
    struct reading {
        bool parsed = false;
        std::string_view printed{};
    };

    /** noint asks for a real even where the text is an integer, as a score does */
    constexpr form choose(std::string_view text, bool noint, reading as_integer, reading as_real) {
        if (!noint && as_integer.parsed && as_integer.printed == text) return form::integer;
        if (as_real.parsed && as_real.printed == text) return form::real;
        return form::string;
    }
}
