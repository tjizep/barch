// containers over a scratch space - TODO 597
#include "key_space.h"
#include "function_api.h"
#include "foreign/driver.h"
#include "scratch_container.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

static int failures = 0;
static void check(bool ok, const std::string& what) {
    std::printf("  %-62s %s\n", what.c_str(), ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

using read_state = barch::foreign::store_access::read_state;

/*
 * What the containers lean on, checked against store_access itself: whether a key
 * or a value holding a space or binary bytes comes back as written, and whether a
 * range shows a key that was removed.
 */
static void store_facts() {
    std::printf("the store underneath\n");
    auto space = barch::key_space::make_scratch();
    auto acc = barch::functions::store_for_owner(space);
    std::string err, got;
    const std::string spaced_key = "a key";
    const std::string binary(std::string("x\0y\xff z", 6));
    check(acc.set(spaced_key, "v", err), "a key with a space is written");
    check(acc.get(spaced_key, got) == read_state::present && got == "v",
          "a key with a space reads back");
    heap::vector<std::string> keys;
    acc.range("", "\xff", 10, keys);
    check(keys.size() == 1 && keys[0] == spaced_key, "range gives a spaced key back as written");
    check(acc.set("k", binary, err), "a binary value is written");
    check(acc.get("k", got) == read_state::present && got == binary,
          "a binary value with a space reads back as written");
    check(acc.set(binary, "b", err), "a binary key is written");
    check(acc.get(binary, got) == read_state::present && got == "b", "a binary key reads back");

    auto s2 = barch::key_space::make_scratch();
    auto a2 = barch::functions::store_for_owner(s2);
    for (int i = 0; i < 5; ++i)
        a2.set("k" + std::to_string(i), "1", err);
    a2.remove("k2");
    keys.clear();
    a2.range("", "\xff", 100, keys);
    check(keys.size() == 4, "range leaves out a removed key (" + std::to_string(keys.size()) + ")");
    a2.set("k2", "1", err);
    a2.remove("k2");
    keys.clear();
    a2.range("", "\xff", 100, keys);
    check(keys.size() == 4, "and one removed, set again and removed again (" +
                            std::to_string(keys.size()) + ")");
    a2.set("k2", "1", err);
    keys.clear();
    a2.range("", "\xff", 100, keys);
    check(keys.size() == 5, "and one set again after a remove (" + std::to_string(keys.size()) + ")");
    check(a2.size() == 5, "size counts it once (" + std::to_string(a2.size()) + ")");
}

static std::mt19937_64 rnd(597);

template<typename K>
static bool same_set(const barch::scratch::set<K>& got, const std::set<K>& want) {
    if (got.size() != want.size())
        return false;
    auto w = want.begin();
    size_t n = 0;
    for (auto k : got) {
        if (w == want.end() || k != *w)
            return false;
        ++w;
        ++n;
    }
    return n == want.size() && w == want.end();
}

template<typename K, typename V>
static bool same_map(const barch::scratch::map<K, V>& got, const std::map<K, V>& want) {
    if (got.size() != want.size())
        return false;
    auto w = want.begin();
    for (auto [k, v] : got) {
        if (w == want.end() || k != w->first || v != w->second)
            return false;
        ++w;
    }
    return w == want.end();
}

/** insert, erase about a third, put some back, comparing with std::set each time */
template<typename K, typename Gen>
static void set_round(const char* what, Gen gen, size_t n) {
    barch::scratch::set<K> got;
    std::set<K> want;
    bool inserts_agree = true;
    for (size_t i = 0; i < n; ++i) {
        K k = gen();
        inserts_agree = inserts_agree && got.insert(k) == want.insert(k).second;
    }
    check(inserts_agree, std::string(what) + ": insert says new exactly when std::set does");
    check(same_set(got, want), std::string(what) + ": " + std::to_string(want.size()) +
                               " keys iterate in std::set's order");
    std::vector<K> all(want.begin(), want.end());
    bool erases_agree = true;
    for (size_t i = 0; i < all.size(); i += 3)
        erases_agree = erases_agree && got.erase(all[i]) == (want.erase(all[i]) == 1);
    erases_agree = erases_agree && !got.erase(all[0]);
    check(erases_agree, std::string(what) + ": erase says there exactly when std::set does");
    for (size_t i = 0; i < all.size(); i += 6) {
        got.insert(all[i]);
        want.insert(all[i]);
    }
    check(same_set(got, want), std::string(what) + ": after erasing and putting some back");
    bool found = true;
    for (size_t i = 0; i < all.size(); ++i)
        found = found && got.contains(all[i]) == (want.count(all[i]) == 1);
    check(found, std::string(what) + ": contains agrees for every key");
    // lower_bound from a few places, the first and past the last included
    bool bounds = true;
    for (size_t i = 0; i < all.size(); i += all.size() / 7 + 1) {
        auto g = got.lower_bound(all[i]);
        auto w = want.lower_bound(all[i]);
        bounds = bounds && ((g == got.end()) == (w == want.end())) &&
                 (w == want.end() || *g == *w);
    }
    check(bounds, std::string(what) + ": lower_bound lands where std::set's does");
    got.clear();
    check(got.empty() && got.begin() == got.end(), std::string(what) + ": clear empties it");
}

static std::string random_bytes() {
    // mostly short, from a small alphabet that has the awkward ones in it, so keys
    // share prefixes and some are prefixes of others: spaces, NUL, the escape byte
    // '!' and what it escapes to, digits so some read as numbers, 0xff. And now and
    // then any byte at all
    static const char pool[] = {' ', '\0', '\xff', 'a', '1', '!', '\x01', '0',
                                '-', '.', 'Q', '\x7f'};
    size_t n = rnd() % 6;
    std::string s;
    for (size_t i = 0; i < n; ++i)
        s.push_back(rnd() % 8 ? pool[rnd() % sizeof pool] : (char) (rnd() % 256));
    return s;
}

static void sets() {
    std::printf("scratch::set\n");
    set_round<uint64_t>("uint64_t", [] {
        // whole range, plus small ones, plus ones made of 0x20 bytes
        switch (rnd() % 4) {
            case 0: return (uint64_t) (rnd() % 1000);
            case 1: return (uint64_t) 0x2020202020202020ull ^ (rnd() % 256);
            case 2: return std::numeric_limits<uint64_t>::max() - rnd() % 3;
            default: return (uint64_t) rnd();
        }
    }, 20000);
    set_round<int64_t>("int64_t", [] {
        switch (rnd() % 4) {
            case 0: return (int64_t) (rnd() % 2001) - 1000;
            case 1: return std::numeric_limits<int64_t>::min() + (int64_t) (rnd() % 3);
            case 2: return std::numeric_limits<int64_t>::max() - (int64_t) (rnd() % 3);
            default: return (int64_t) rnd();
        }
    }, 20000);
    set_round<std::string>("std::string", random_bytes, 20000);
}

static void maps() {
    std::printf("scratch::map\n");
    barch::scratch::map<std::string, std::string> sm;
    std::map<std::string, std::string> wm;
    for (int i = 0; i < 5000; ++i) {
        auto k = random_bytes();
        auto v = random_bytes() + std::to_string(i);
        if (i % 2) {
            sm.set(k, v);
            wm[k] = v;
        } else {
            sm.insert(k, v);
            wm.insert({k, v});
        }
    }
    check(same_map(sm, wm), "string -> string: set and insert agree with std::map");
    bool gets = true;
    for (const auto& [k, v] : wm)
        gets = gets && sm.get(k) == v;
    check(gets && !sm.get("not \xff there"), "get answers each value, and nothing for a missing key");

    barch::scratch::map<uint64_t, int64_t> counts;
    std::map<uint64_t, int64_t> want;
    for (int i = 0; i < 20000; ++i) {
        uint64_t k = rnd() % 700;
        int64_t d = (int64_t) (rnd() % 21) - 10;
        counts.update(k, 0, [d](int64_t& n) { n += d; });
        want[k] += d;
    }
    check(same_map(counts, want), "uint64_t -> int64_t: update counts the way m[k] += d does");
    for (uint64_t k = 0; k < 700; k += 2) {
        counts.erase(k);
        want.erase(k);
    }
    check(same_map(counts, want), "and after erasing every other key");

    barch::scratch::map<int64_t, uint64_t> big;
    std::map<int64_t, uint64_t> wbig;
    for (int i = 0; i < 3000; ++i) {
        auto k = (int64_t) rnd();
        big.set(k, (uint64_t) i);
        wbig[k] = (uint64_t) i;
    }
    check(same_map(big, wbig), "int64_t -> uint64_t over many pages");
}

/** what one key comes back as from a range, printed, for working out the encoding */
static void show(const char* what, const std::string& k) {
    auto space = barch::key_space::make_scratch();
    auto acc = barch::functions::store_for_owner(space);
    std::string err, got;
    bool ok = acc.set(k, "v", err);
    heap::vector<std::string> keys;
    acc.range("", "\xff\xff", 10, keys);
    std::printf("    %-8s set=%d get=%d range=%zu same=%d bytes:", what, ok,
                (int) (acc.get(k, got) == read_state::present), keys.size(),
                (int) (keys.size() == 1 && keys[0] == k));
    for (unsigned char c : (keys.empty() ? std::string() : keys[0]))
        std::printf(" %02x", c);
    std::printf(" %s\n", err.c_str());
}

int main() {
    if (getenv("SHOW_KEYS")) {
        show("digits", "123");
        show("neg", "-5");
        show("float", "1.5");
        show("space", "a b");
        show("lead sp", " a");
        show("empty", "");
        show("x7f", "\x7f");
        show("xff", "\xff");
        show("x0102", "\x01\x02");
        show("x8010", std::string("\x80\x10", 2));
        show("int 3", std::string("\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x03", 15));
    }
    store_facts();
    sets();
    maps();
    {
        // what one costs to make and drop, which decides where one is worth using
        auto t0 = std::chrono::steady_clock::now();
        for (int i = 0; i < 200; ++i) {
            barch::scratch::set<uint64_t> s;
            s.insert((uint64_t) i);
        }
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - t0).count() / 200;
        std::printf("  (one container made, used once and dropped: about %lld us)\n",
                    (long long) us);
    }
    std::printf("%s\n", failures ? "FAILURES above" : "all passed");
    return failures ? 1 : 0;
}
