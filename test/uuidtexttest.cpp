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
    constexpr int n = 100000;
    for (int i = 0; i < n; ++i) {
        const uint64_t ms = r() & ms_max, a = r() & a_max, b = r() & b_max;
        char buf[36];
        uuid_text(buf, ms, a, b);
        same += std::string(buf, 36) == by_snprintf(ms, a, b);
    }
    check(same == n, "matches snprintf on 100000 random uuids");

    std::printf("%s\n", failures ? "FAILED" : "all pass");
    return failures ? 1 : 0;
}
