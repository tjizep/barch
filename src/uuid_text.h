#pragma once

#include <cstdint>

namespace barch {

/**
 * A UUIDv7's 36 characters into `out`, from its three parts: `ms` (48 bits), `a`
 * (12 bits) and `b` (62 bits). Version 7 goes in the 4 bits above a, variant 10 in
 * the 2 bits above b. No terminating NUL.
 *
 * A table rather than snprintf, which parses its format on every call and was 7x
 * slower - TODO 894.
 */
inline void uuid_text(char* out, uint64_t ms, uint64_t a, uint64_t b) {
    static constexpr char digits[] = "0123456789abcdef";
    // `n` hex digits of `v`, most significant first
    const auto hex = [](char* p, uint64_t v, int n) {
        for (int i = n - 1; i >= 0; --i) {
            p[i] = digits[v & 0xf];
            v >>= 4;
        }
    };
    hex(out, ms >> 16, 8);
    out[8] = '-';
    hex(out + 9, ms & 0xffff, 4);
    out[13] = '-';
    hex(out + 14, 0x7000 | a, 4);
    out[18] = '-';
    hex(out + 19, 0x8000 | (b >> 48), 4);
    out[23] = '-';
    hex(out + 24, b & 0xffffffffffffull, 12);
}

}
