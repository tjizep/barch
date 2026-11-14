// a text range holds a bounded number of keys - TODO 905
//
// A range that reaches both regions, plain keys and keys holding the split
// character, merges the two. It used to gather each region whole and merge in
// memory, so a range with no limit held every key in it, and every value for a
// range with values. These check how many keys the merge held at once, by the
// count key_range keeps, and that what comes back is what it was.
#include "key_space.h"
#include "function_api.h"
#include "foreign/driver.h"
#include "key_range.h"

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

static int failures = 0;
static void check(bool ok, const std::string& what) {
    std::printf("  %-66s %s\n", what.c_str(), ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

static std::string num(int i) {
    char b[16];
    std::snprintf(b, sizeof b, "k%05d", i);
    return b;
}

// the order a range answers in: a plain key, then the split key under it
static std::vector<std::string> expected(int n) {
    std::vector<std::string> out;
    for (int i = 0; i < n; ++i) {
        out.push_back(num(i));
        out.push_back(num(i) + " x");
    }
    return out;
}

static std::vector<std::string> as_vector(const heap::vector<std::string>& v) {
    return {v.begin(), v.end()};
}

// a batch is at most this many keys from each region, and one more on a resume
static constexpr size_t most_held = 2 * (256 + 1);

static void keys() {
    std::printf("keys in both regions\n");
    const int n = 3000;
    auto space = barch::key_space::make_scratch();
    auto acc = barch::functions::store_for_owner(space);
    std::string err;
    for (int i = 0; i < n; ++i) {
        acc.set(num(i), "p" + std::to_string(i), err);
        acc.set(num(i) + " x", "c" + std::to_string(i), err);
    }
    const auto want = expected(n);
    barch::text_held_peak();

    heap::vector<std::string> got;
    acc.range("k", "l", 0, got);
    const size_t peak = barch::text_held_peak();
    check(as_vector(got) == want, "a range with no limit gives every key in order (" + std::to_string(got.size()) + ")");
    check(peak <= most_held, "and held at most " + std::to_string(most_held) + " keys (" + std::to_string(peak) + ")");

    got.clear();
    acc.range_from("k", "l", 10, 4321, got);
    check(as_vector(got) == std::vector<std::string>(want.begin() + 4321, want.begin() + 4331),
          "an offset deep in the range lands where it did");
    check(barch::text_held_peak() <= most_held, "and stepping over it is bounded too");

    // page through by offset and by last key, as callers do
    std::vector<std::string> paged;
    for (int64_t off = 0;; off += 700) {
        got.clear();
        acc.range_from("k", "l", 700, off, got);
        if (got.empty())
            break;
        paged.insert(paged.end(), got.begin(), got.end());
    }
    check(paged == want, "pages by offset add up to the whole range");

    got.clear();
    acc.range("k00100", "k00103", 0, got);
    check(as_vector(got) == std::vector<std::string>{"k00100", "k00100 x", "k00101", "k00101 x", "k00102", "k00102 x"},
          "a short range gives both regions");
    got.clear();
    acc.range("k00100 a", "k00100 z", 0, got);
    check(as_vector(got) == std::vector<std::string>{"k00100 x"}, "bounds holding the split stay in that region");

    // a filter sees each key once, in both the range and the count
    size_t asked = 0;
    barch::key_filter all = [&asked](art::value_type) { ++asked; return true; };
    barch::text_held_peak();
    const int64_t counted = barch::text_count(space, art::value_type{"k", 1}, art::value_type{"l", 1}, all);
    const size_t count_peak = barch::text_held_peak();
    check(counted == 2 * n, "a filtered count counts both regions (" + std::to_string(counted) + ")");
    check(asked == (size_t) 2 * n, "asking the filter once a key (" + std::to_string(asked) + ")");
    check(count_peak == 0, "and holds no keys to count them (" + std::to_string(count_peak) + ")");

    barch::key_filter plain_only = [](art::value_type k) { return memchr(k.bytes, 'x', k.size) == nullptr; };
    check(barch::text_count(space, art::value_type{"k", 1}, art::value_type{"l", 1}, plain_only) == n,
          "a filter that leaves out the split keys counts the rest");
    int64_t seen = 0;
    barch::text_range(space, art::value_type{"k", 1}, art::value_type{"l", 1}, 0,
                      [&seen](art::value_type) { ++seen; }, plain_only, 5);
    check(seen == n - 5, "a filtered range with an offset counts the offset after the filter");
    // the composite region is all filtered out, a batch at a time, before the plain
    // keys can stream on their own
    check(barch::text_held_peak() <= most_held, "and holds a bounded number while it filters");

    // a filter that drops keys in both regions, so batches resume past dropped keys
    barch::key_filter some = [](art::value_type k) { return memmem(k.bytes, k.size, "7", 1) == nullptr; };
    int64_t kept = 0;
    for (int i = 0; i < n; ++i)
        kept += num(i).find('7') == std::string::npos ? 2 : 0;
    std::vector<std::string> whole, pages;
    auto into = [](std::vector<std::string>& v) {
        return [&v](art::value_type k) { v.emplace_back(k.chars(), k.size); };
    };
    barch::text_range(space, art::value_type{"k", 1}, art::value_type{"l", 1}, 0, into(whole), some);
    for (int64_t off = 0; off < kept + 300; off += 300)
        barch::text_range(space, art::value_type{"k", 1}, art::value_type{"l", 1}, 300, into(pages), some, off);
    check((int64_t) whole.size() == kept, "a filter in both regions keeps what it should (" + std::to_string(whole.size()) + ")");
    check(pages == whole, "and its pages by offset add up to it");
    check(barch::text_held_peak() <= most_held, "holding a bounded number");
}

static void values() {
    std::printf("large values in both regions\n");
    const int n = 200;
    const std::string big(64 * 1024, 'v');
    auto space = barch::key_space::make_scratch();
    auto acc = barch::functions::store_for_owner(space);
    std::string err;
    for (int i = 0; i < n; ++i) {
        acc.set(num(i), big + "p" + std::to_string(i), err);
        acc.set(num(i) + " x", big + "c" + std::to_string(i), err);
    }
    barch::text_held_peak();
    heap::vector<std::pair<std::string, std::string>> got;
    acc.range_values("k", "l", 0, 0, got);
    const size_t peak = barch::text_held_peak();
    bool right = got.size() == (size_t) 2 * n;
    for (int i = 0; right && i < n; ++i) {
        right = got[2 * i].first == num(i) && got[2 * i].second == big + "p" + std::to_string(i)
                && got[2 * i + 1].first == num(i) + " x" && got[2 * i + 1].second == big + "c" + std::to_string(i);
    }
    check(right, "every key and value comes back in order (" + std::to_string(got.size()) + ")");
    // 4 MiB of values a region, and one past it
    const size_t cap = 2 * ((4u << 20) / big.size() + 1);
    check(peak <= cap, "a batch stops at its bytes, so " + std::to_string(cap) + " keys at most (" + std::to_string(peak) + ")");
}

int main() {
    keys();
    values();
    std::printf(failures ? "FAILED\n" : "all passed\n");
    return failures ? 1 : 0;
}
