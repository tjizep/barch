// a new leaf defines every byte it owns - TODO 965
//
// make_leaf writes the header, key, value and expiry over the whole leaf, so the
// allocator does not zero those bytes first. This checks what that leaves behind:
//   - memory handed back dirty is reused for the next leaf of that size;
//   - every byte of the new leaf, padding included, matches a leaf built in zeroed
//     memory.
// It covers small and large leaves, padded sizes, expiry and the volatile flag.
#include "art/nodes.h"
#include "logical_allocator.h"

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

static int failures = 0;
static void check(bool ok, const std::string& what) {
    std::printf("  %-66s %s\n", what.c_str(), ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

struct leaf_case {
    const char* name;
    size_t key_len;
    size_t val_len;
    int64_t ttl;
    bool is_volatile;
};

// the tree's own pair removes leaves from its index; none are removed here
struct test_pair : alloc_pair {
    test_pair() : alloc_pair(0, "leafbytes") {}
    void remove_leaf(const logical_address&) override {}
};

int main() {
    test_pair ap;
    auto& leaves = ap.get_leaves();
    const leaf_case cases[] = {
        {"small", 3, 10, 0, false},
        {"padded", 20, 1500, 0, false},
        {"large key", 300, 40, 0, false},
        {"large value", 8, 70000, 0, false},
        {"expiry", 12, 2000, 1000000, false},
        {"volatile", 12, 33, 0, true},
        {"large, expiry, volatile", 400, 5000, 1000000, true},
    };
    for (const auto& c : cases) {
        std::string key(c.key_len, 'k');
        for (size_t i = 0; i < key.size(); ++i) key[i] = char('a' + i % 26);
        std::string val(c.val_len, 'v');
        for (size_t i = 0; i < val.size(); ++i) val[i] = char(i * 7);
        // the key's size counts the hidden 0 at its end
        art::value_type k(key.c_str(), key.size() + 1);
        art::value_type v(val);

        size_t size = art::leaf::make_size(k.length(), v.size, c.ttl > 0, c.is_volatile);
        size_t padded = alloc_pad(size);

        // dirty a block of this size and hand it back
        logical_address dirty{&ap};
        uint8_t* d = leaves.new_address(dirty, size);
        std::memset(d, 0xA5, padded);
        leaves.free(dirty, size);

        auto n = art::make_leaf(ap, k, v, c.ttl, c.is_volatile, false);
        check(n.logical == dirty, std::string(c.name) + ": leaf reuses the dirty block");

        std::vector<uint8_t> ref(padded, 0);
        auto* r = new(ref.data()) art::leaf(k.length(), v.size, c.ttl, c.is_volatile, false);
        r->set_key(k);
        r->set_value(v);

        const uint8_t* got = leaves.modify<uint8_t>(n.logical);
        size_t first = padded;
        for (size_t i = 0; i < padded; ++i) {
            if (got[i] != ref[i]) { first = i; break; }
        }
        check(first == padded, std::string(c.name) + ": all " + std::to_string(padded) +
              " bytes match" + (first < padded ? " (first diff at " + std::to_string(first) + ")" : ""));
    }
    std::printf("%s\n", failures ? "FAILED" : "ok");
    return failures ? 1 : 0;
}
