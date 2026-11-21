#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace barch {

/**
 * Write a UUIDv7's 36 lowercase characters into `out`, with no NUL terminator.
 * Inputs: `ms` (48 bits), `a` (12 bits), and `b` (62 bits).
 * Version 7 goes in the 4 bits above `a`. Variant 10 goes in the 2 bits above `b`.
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

/**
 * Write a UUIDv7's 16 raw bytes into `out` in big-endian RFC 9562 order.
 * Inputs: `ms` (48 bits), `a` (12 bits), and `b` (62 bits).
 */
inline void uuid_bytes(uint8_t* out, uint64_t ms, uint64_t a, uint64_t b) {
    out[0] = static_cast<uint8_t>(ms >> 40);
    out[1] = static_cast<uint8_t>(ms >> 32);
    out[2] = static_cast<uint8_t>(ms >> 24);
    out[3] = static_cast<uint8_t>(ms >> 16);
    out[4] = static_cast<uint8_t>(ms >> 8);
    out[5] = static_cast<uint8_t>(ms);
    out[6] = static_cast<uint8_t>(0x70 | ((a >> 8) & 0x0f));
    out[7] = static_cast<uint8_t>(a & 0xff);
    out[8] = static_cast<uint8_t>(0x80 | ((b >> 56) & 0x3f));
    out[9] = static_cast<uint8_t>(b >> 48);
    out[10] = static_cast<uint8_t>(b >> 40);
    out[11] = static_cast<uint8_t>(b >> 32);
    out[12] = static_cast<uint8_t>(b >> 24);
    out[13] = static_cast<uint8_t>(b >> 16);
    out[14] = static_cast<uint8_t>(b >> 8);
    out[15] = static_cast<uint8_t>(b);
}

/**
 * Format 16 raw UUID bytes into 36 lowercase characters (`8-4-4-4-12`), no NUL.
 */
inline void uuid_format(char* out, const uint8_t* in) {
    static constexpr char digits[] = "0123456789abcdef";
    size_t pos = 0;
    for (int i = 0; i < 16; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10)
            out[pos++] = '-';
        out[pos++] = digits[in[i] >> 4];
        out[pos++] = digits[in[i] & 0xf];
    }
}

inline int uuid_hex_nibble(unsigned char c) {
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    return -1;
}

/**
 * Parse a 36-character hyphenated UUID (`8-4-4-4-12`) or 32-character hex UUID
 * into 16 raw bytes. Hex digits are case-insensitive. Leaves `out` unchanged on
 * failure and returns false.
 */
inline bool uuid_parse(uint8_t* out, const char* in, size_t len) {
    if (!in || (len != 36 && len != 32))
        return false;
    const bool dashed = (len == 36);
    if (dashed && (in[8] != '-' || in[13] != '-' || in[18] != '-' || in[23] != '-'))
        return false;
    uint8_t tmp[16];
    size_t pos = 0;
    for (int i = 0; i < 16; ++i) {
        if (dashed && (pos == 8 || pos == 13 || pos == 18 || pos == 23))
            ++pos;
        const int hi = uuid_hex_nibble(static_cast<unsigned char>(in[pos]));
        const int lo = uuid_hex_nibble(static_cast<unsigned char>(in[pos + 1]));
        if (hi < 0 || lo < 0)
            return false;
        tmp[i] = static_cast<uint8_t>((hi << 4) | lo);
        pos += 2;
    }
    std::memcpy(out, tmp, 16);
    return true;
}

/**
 * Return the 4-bit version field (0..15) of a 16-byte UUID.
 */
inline uint8_t uuid_version(const uint8_t* in) {
    return static_cast<uint8_t>(in[6] >> 4);
}

/**
 * Read the 48-bit Unix timestamp in ms from a 16-byte UUIDv7.
 * Returns false if the version is not 7 or the variant is not RFC 9562 (`10xx`).
 */
inline bool uuid_time_ms(const uint8_t* in, uint64_t& ms) {
    if ((in[6] >> 4) != 7 || (in[8] & 0xc0) != 0x80)
        return false;
    ms = (static_cast<uint64_t>(in[0]) << 40) |
         (static_cast<uint64_t>(in[1]) << 32) |
         (static_cast<uint64_t>(in[2]) << 24) |
         (static_cast<uint64_t>(in[3]) << 16) |
         (static_cast<uint64_t>(in[4]) << 8) |
         static_cast<uint64_t>(in[5]);
    return true;
}

}
