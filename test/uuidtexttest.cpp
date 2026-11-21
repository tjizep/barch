// uuid_text writes the same 36 characters snprintf did - TODO 894
#include "uuid_text.h"
#include <cstdio>
#include <cstring>
#include <random>
#include <string>

using namespace barch;

static int failures = 0;
static void check(bool ok, const std::string& what) {
    std::printf("  %-58s %s\n", what.c_str(), ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

// the old way, kept here as the reference
static std::string by_snprintf(uint64_t ms, uint64_t a, uint64_t b) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%08llx-%04llx-%04llx-%04llx-%012llx",
                  (unsigned long long) (ms >> 16),
                  (unsigned long long) (ms & 0xffff),
                  (unsigned long long) (0x7000 | a),
                  (unsigned long long) (0x8000 | (b >> 48)),
                  (unsigned long long) (b & 0xffffffffffffull));
    return buf;
}

static std::string by_table(uint64_t ms, uint64_t a, uint64_t b) {
    char buf[37];
    std::memset(buf, '#', sizeof(buf));
    uuid_text(buf, ms, a, b);
    check(buf[36] == '#', "writes 36 characters and no more");
    return {buf, 36};
}

int main() {
    constexpr uint64_t ms_max = (1ull << 48) - 1, a_max = 0xfff, b_max = (1ull << 62) - 1;

    std::printf("edges\n");
    const uint64_t mss[] = {0, 1, 0xffff, 0x10000, 0x018f3a6c1b2eull, ms_max};
    const uint64_t as[] = {0, 1, 0xabc, a_max};
    const uint64_t bs[] = {0, 1, 0xffffffffffffull, 1ull << 48, 0x1a8b2c3d4e5f6071ull, b_max};
    int same = 0, total = 0;
    for (uint64_t ms : mss)
        for (uint64_t a : as)
            for (uint64_t b : bs) {
                ++total;
                same += by_table(ms, a, b) == by_snprintf(ms, a, b);
            }
    check(same == total, "matches snprintf at every edge (" + std::to_string(same) + " of " +
                                 std::to_string(total) + ")");
    check(by_table(0x018f3a6c1b2eull, 0xc4d, 0x1a8b2c3d4e5f6071ull) == "018f3a6c-1b2e-7c4d-9a8b-2c3d4e5f6071",
          "a known uuid");

    std::printf("random\n");
    std::mt19937_64 r{894};
    same = 0;
    int bytes_same = 0;
    int roundtrip = 0;
    int time_ok = 0;
    constexpr int n = 100000;
    for (int i = 0; i < n; ++i) {
        const uint64_t ms = r() & ms_max, a = r() & a_max, b = r() & b_max;
        char buf[36];
        uuid_text(buf, ms, a, b);
        const std::string expected = by_snprintf(ms, a, b);
        same += std::string(buf, 36) == expected;

        uint8_t raw[16];
        uuid_bytes(raw, ms, a, b);
        char fmt[36];
        uuid_format(fmt, raw);
        bytes_same += std::string(fmt, 36) == expected;

        uint8_t parsed[16];
        if (uuid_parse(parsed, fmt, 36) && std::memcmp(raw, parsed, 16) == 0)
            ++roundtrip;

        uint64_t got_ms = 0;
        if (uuid_version(raw) == 7 && uuid_time_ms(raw, got_ms) && got_ms == ms)
            ++time_ok;
    }
    check(same == n, "matches snprintf on 100000 random uuids");
    check(bytes_same == n, "uuid_bytes + uuid_format matches snprintf on 100000");
    check(roundtrip == n, "uuid_parse round-trips 100000 uuids");
    check(time_ok == n, "uuid_version and uuid_time_ms match on 100000 uuids");

    std::printf("parse and inspect\n");
    {
        uint8_t a[16], b[16], c[16];
        check(uuid_parse(a, "018f3a6c-1b2e-7c4d-9a8b-2c3d4e5f6071", 36), "parse lowercase dashed");
        check(uuid_parse(b, "018F3A6C-1B2E-7C4D-9A8B-2C3D4E5F6071", 36), "parse uppercase dashed");
        check(uuid_parse(c, "018f3a6c1b2e7c4d9a8b2c3d4e5f6071", 32), "parse compact 32-hex");
        check(std::memcmp(a, b, 16) == 0 && std::memcmp(a, c, 16) == 0, "all three parse to identical bytes");
        const uint8_t expected_bytes[16] = {
            0x01, 0x8f, 0x3a, 0x6c, 0x1b, 0x2e, 0x7c, 0x4d,
            0x9a, 0x8b, 0x2c, 0x3d, 0x4e, 0x5f, 0x60, 0x71
        };
        check(std::memcmp(a, expected_bytes, 16) == 0, "known bytes match");

        uint64_t ms = 0;
        check(uuid_version(a) == 7 && uuid_time_ms(a, ms) && ms == 0x018f3a6c1b2eull,
              "uuid_time_ms extracts 48-bit ms");

        // v4 uuid: parses and reports version 4, but uuid_time_ms rejects it
        uint8_t v4[16];
        check(uuid_parse(v4, "550e8400-e29b-41d4-a716-446655440000", 36), "parse v4 uuid");
        check(uuid_version(v4) == 4, "v4 reports version 4");
        check(!uuid_time_ms(v4, ms), "uuid_time_ms rejects v4");

        // non-RFC-9562 variant with version nibble 7 is rejected by uuid_time_ms
        uint8_t bad_var[16];
        std::memcpy(bad_var, a, 16);
        bad_var[8] = 0x00;
        check(!uuid_time_ms(bad_var, ms), "uuid_time_ms rejects non-RFC variant 00");
        bad_var[8] = 0xc0;
        check(!uuid_time_ms(bad_var, ms), "uuid_time_ms rejects non-RFC variant 11");

        // nil and max uuids
        uint8_t zero[16], maxu[16];
        check(uuid_parse(zero, "00000000-0000-0000-0000-000000000000", 36) && uuid_version(zero) == 0,
              "parse nil uuid");
        check(uuid_parse(maxu, "ffffffff-ffff-ffff-ffff-ffffffffffff", 36) && uuid_version(maxu) == 15,
              "parse max uuid");

        // bad inputs leave output untouched
        uint8_t guard[16];
        std::memset(guard, 0xaa, 16);
        const char* bads[] = {
            "",
            "018f3a6c-1b2e-7c4d-9a8b-2c3d4e5f607",
            "018f3a6c-1b2e-7c4d-9a8b-2c3d4e5f60710",
            "018f3a6c1b2e-7c4d-9a8b-2c3d4e5f6071-",
            "018f3a6c_1b2e_7c4d_9a8b_2c3d4e5f6071",
            "018f3a6g-1b2e-7c4d-9a8b-2c3d4e5f6071",
            " 018f3a6c-1b2e-7c4d-9a8b-2c3d4e5f6071",
            "018f3a6c-1b2e-7c4d-9a8b-2c3d4e5f6071 ",
            "018f3a6c1b2e7c4d9a8b2c3d4e5f607g",
            "{018f3a6c-1b2e-7c4d-9a8b-2c3d4e5f6071}",
        };
        int rejected = 0;
        for (const char* s : bads)
            rejected += !uuid_parse(guard, s, std::strlen(s));
        const char nul_in[36] = {'0','1','8','f','3','a','6','c','-','1','b','2','e','-',
                                 '7','c','4','d','-','9','a','8','b','-','2','c','3','d',
                                 '4','e','5','f','6','0','\0','1'};
        rejected += !uuid_parse(guard, nul_in, 36);
        rejected += !uuid_parse(guard, nullptr, 36);
        check(rejected == (int)(sizeof(bads) / sizeof(bads[0])) + 2, "rejects all malformed strings");
        uint8_t all_aa[16];
        std::memset(all_aa, 0xaa, 16);
        check(std::memcmp(guard, all_aa, 16) == 0, "failed parse leaves output untouched");
    }

    std::printf("%s\n", failures ? "FAILED" : "all pass");
    return failures ? 1 : 0;
}
