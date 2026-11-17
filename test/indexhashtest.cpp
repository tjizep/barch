// the two key hashes - TODO 912. The routing hash is fixed, since saved shards
// hold the keys it sent there. The index hash is seeded at random in each
// process, so nobody outside can pick keys that land in one bucket.
#include "key_hash.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

using namespace barch;

static int failures = 0;
static void check(bool ok, const std::string& what) {
    std::printf("  %-58s %s\n", what.c_str(), ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

static const char* probe = "user:secret:default";

static uint64_t index_of(const char* k) {
    return index_hash(k, std::strlen(k));
}

// the probe's index hash in a new process of this test
static uint64_t child_index(const char* self) {
    const std::string cmd = std::string("\"") + self + "\" --print";
    FILE* p = popen(cmd.c_str(), "r");
    if (!p) return 0;
    unsigned long long v = 0;
    if (std::fscanf(p, "%llu", &v) != 1) v = 0;
    pclose(p);
    return v;
}

int main(int argc, char** argv) {
    if (argc > 1 && std::strcmp(argv[1], "--print") == 0) {
        std::printf("%llu\n", (unsigned long long) index_of(probe));
        return 0;
    }

    std::printf("the routing hash is fixed\n");
    {
        const size_t n = std::strlen(probe);
        check(key_hash(probe, n) == a5hash(probe, n, 0), "key_hash is a5hash with seed 0");
    }

    std::printf("the index hash is seeded\n");
    {
        check(index_of(probe) == index_of(probe), "the same key hashes the same in one process");
        int same = 0;
        char k[32];
        for (int i = 0; i < 64; ++i) {
            std::snprintf(k, sizeof k, "key:%d", i);
            if (index_of(k) == key_hash(k, std::strlen(k))) ++same;
        }
        check(same == 0, "no key's index hash is its routing hash");
    }

    std::printf("each process has its own seed\n");
    {
        const uint64_t a = child_index(argv[0]);
        const uint64_t b = child_index(argv[0]);
        check(a != 0 && b != 0, "both children answered");
        check(a != b, "two processes hash the same key differently");
        check(a != index_of(probe) && b != index_of(probe), "and differently from this one");
    }

    std::printf(failures ? "%d failed\n" : "all passed\n", failures);
    return failures ? 1 : 0;
}
