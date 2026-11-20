// The eviction sweeps' rules, every key state and every small page - TODO 940.
//
// src/eviction_core.h decides whether a sweep runs, which pages it draws from, which
// keys on a page it takes, and how far volatile-ttl goes. This runs:
//   - every policy against every state a leaf can be in (32 states);
//   - every page of up to five keys, each with or without an expiry, at expiries 0..3,
//     through volatile-ttl the way run_evict_volatile_ttl_keys walks it;
//   - every page count from 0 to 8 through the random pick.
// It checks:
//   - no sweep takes a deleted key or one may_evict keeps;
//   - the volatile policies take only keys with an expiry - TODO 893;
//   - LRU and LFU run on the clock, and never take a key read since it last
//     passed - TODO 456. LFU's sweep read a stub page and took nothing;
//   - the expiry sweep takes only expired keys, and a Raft space only those;
//   - volatile-ttl takes the nearer half: nothing later than a key it leaves, and
//     at least half the candidates;
//   - a random pick never draws from an empty range, and never page 0.
//
// It also runs rules that are known to be wrong and checks that it catches each. One
// is the page pick as it was before TODO 940: [1, max] with max 0. Another is LFU
// off the clock, as before TODO 456.
#include "eviction_core.h"

#include <algorithm>
#include <cstdio>
#include <functional>
#include <string>
#include <vector>

using namespace barch::eviction_core;

static int failures = 0;

static void check(bool ok, const char* what) {
    std::printf("%-72s %s\n", what, ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

namespace {
    struct rules {
        std::function<bool(size_t, size_t)> under_pressure;
        std::function<std::optional<std::pair<size_t, size_t>>(size_t)> page_range;
        std::function<bool(policy, const key&)> takes;
        std::function<size_t(size_t)> ttl_cut;
        std::function<bool(bool)> raft_may_evict;
        std::function<bool(policy)> on_clock;
    };

    const policy policies[] = {policy::all_random, policy::volatile_random, policy::all_lfu,
                               policy::volatile_lfu, policy::volatile_ttl, policy::all_lru,
                               policy::volatile_lru, policy::expired};

    bool is_volatile(policy p) {
        return p == policy::volatile_random || p == policy::volatile_lfu || p == policy::volatile_ttl ||
               p == policy::volatile_lru;
    }

    bool clocked(policy p) {
        return p == policy::all_lru || p == policy::volatile_lru || p == policy::all_lfu ||
               p == policy::volatile_lfu;
    }

    key state(unsigned bits) {
        return key{(bits & 1) != 0, (bits & 2) != 0, (bits & 4) != 0, (bits & 8) != 0, (bits & 16) != 0};
    }

    std::string every_key(const rules& r) {
        for (auto p : policies)
            if (r.on_clock(p) != clocked(p))
                return clocked(p) ? "an LRU or LFU policy is off the clock" : "a policy with no clock is on it";
        for (auto p : policies)
            for (unsigned b = 0; b < 32; ++b) {
                const auto k = state(b);
                // an expired key without an expiry can't be
                if (k.expired && !k.has_expiry)
                    continue;
                if (!r.takes(p, k))
                    continue;
                if (k.deleted)
                    return "a sweep took a deleted key";
                if (k.kept)
                    return "a sweep took a key may_evict keeps";
                if (is_volatile(p) && !k.has_expiry)
                    return "a volatile policy took a key with no expiry";
                if (clocked(p) && k.read)
                    return "the clock took a key read since it passed";
                if (p == policy::expired && !k.expired)
                    return "the expiry sweep took a live key";
            }
        // a candidate nobody stops is taken: a policy that takes nothing is no policy
        for (auto p : policies) {
            key k{false, false, true, p == policy::expired, false};
            if (!r.takes(p, k))
                return "a policy took nothing it should";
        }
        if (r.raft_may_evict(false))
            return "a Raft space evicted a live key";
        if (!r.raft_may_evict(true))
            return "a Raft space kept an expired key";
        return {};
    }

    /** a page's keys: an expiry 0..3, or -1 for none */
    std::string every_ttl_page(const rules& r) {
        for (size_t n = 1; n <= 5; ++n) {
            std::vector<int> page(n, -1);
            while (true) {
                // as run_evict_volatile_ttl_keys: the candidates' expiries, the cut, the walk
                std::vector<int> expiries;
                for (auto e : page)
                    if (r.takes(policy::volatile_ttl, key{false, false, e >= 0, false, false}))
                        expiries.push_back(e);
                if (!expiries.empty()) {
                    const size_t cut = r.ttl_cut(expiries.size());
                    if (cut >= expiries.size())
                        return "volatile-ttl cut past the candidates";
                    auto sorted = expiries;
                    std::sort(sorted.begin(), sorted.end());
                    const int last = sorted[cut];
                    size_t taken = 0;
                    int latest_taken = -1, earliest_left = 99;
                    for (auto e : expiries) {
                        if (e <= last) {
                            ++taken;
                            latest_taken = std::max(latest_taken, e);
                        } else {
                            earliest_left = std::min(earliest_left, e);
                        }
                    }
                    if (latest_taken > earliest_left)
                        return "volatile-ttl took a key later than one it left";
                    if (taken * 2 < expiries.size())
                        return "volatile-ttl took less than half";
                    // with every expiry different, the nearer half exactly
                    std::vector<int> u = sorted;
                    u.erase(std::unique(u.begin(), u.end()), u.end());
                    if (u.size() == sorted.size() && taken != (sorted.size() + 1) / 2)
                        return "volatile-ttl took more than the nearer half";
                }
                size_t i = 0;
                while (i < n && ++page[i] == 4)
                    page[i++] = -1;
                if (i == n)
                    break;
            }
        }
        return {};
    }

    std::string every_page_count(const rules& r) {
        for (size_t max = 0; max <= 8; ++max) {
            const auto range = r.page_range(max);
            if (!range)
                continue;
            if (range->first > range->second)
                return "a random pick from an empty range";
            if (range->first == 0)
                return "a random pick could draw page 0";
            if (range->second > max)
                return "a random pick past the last page";
        }
        if (r.page_range(3) == std::nullopt)
            return "a shard with pages drew none";
        return {};
    }

    std::string pressure(const rules& r) {
        if (r.under_pressure(9, 10))
            return "a sweep ran under the threshold";
        if (!r.under_pressure(10, 10) || !r.under_pressure(11, 10))
            return "a sweep didn't run at the threshold";
        return {};
    }

    std::string all(const rules& r) {
        for (auto f : {pressure, every_page_count, every_key, every_ttl_page}) {
            auto got = f(r);
            if (!got.empty())
                return got;
        }
        return {};
    }
}

int main() {
    const rules now{under_pressure, page_range, takes, ttl_cut, raft_may_evict, on_clock};

    std::printf("the rules directly\n");
    {
        check(!page_range(0) && page_range(1) == std::make_pair(size_t{1}, size_t{1}),
              "no pages, no pick; one page, page 1");
        check(ttl_cut(1) == 0 && ttl_cut(2) == 0 && ttl_cut(3) == 1, "volatile-ttl cuts at the median");
        check(takes(policy::all_lru, key{}) && !takes(policy::all_lru, key{false, false, false, false, true}),
              "LRU takes an unread key, not a read one");
        check(takes(policy::all_lfu, key{}) && !takes(policy::all_lfu, key{false, false, false, false, true}),
              "and LFU the same - TODO 456");
    }

    std::printf("every key state, ttl page and page count\n");
    {
        const auto got = all(now);
        if (!got.empty())
            std::printf("  %s\n", got.c_str());
        check(got.empty(), "as now");
    }

    std::printf("rules known to be wrong, each caught\n");
    {
        struct mutant {
            const char* what;
            rules r;
        };
        auto with = [&](auto change) {
            rules r = now;
            change(r);
            return r;
        };
        const std::vector<mutant> mutants{
                {"pages [1, max] with no pages, as before TODO 940", with([](rules& r) {
                    r.page_range = [](size_t max) { return std::make_optional(std::make_pair(size_t{1}, max)); };
                })},
                {"pages from 0", with([](rules& r) {
                    r.page_range = [](size_t max) {
                        return max ? std::make_optional(std::make_pair(size_t{0}, max)) : std::nullopt;
                    };
                })},
                {"a volatile policy takes any key, as before TODO 893", with([](rules& r) {
                    r.takes = [](policy p, const key& k) {
                        return p == policy::volatile_random ? !k.deleted && !k.kept : takes(p, k);
                    };
                })},
                {"a sweep takes a kept key", with([](rules& r) {
                    r.takes = [](policy p, const key& k) { return takes(p, key{k.deleted, false, k.has_expiry, k.expired, k.read}); };
                })},
                {"a sweep takes a deleted key", with([](rules& r) {
                    r.takes = [](policy p, const key& k) { return takes(p, key{false, k.kept, k.has_expiry, k.expired, k.read}); };
                })},
                {"LRU takes a read key", with([](rules& r) {
                    r.takes = [](policy p, const key& k) { return takes(p, key{k.deleted, k.kept, k.has_expiry, k.expired, false}); };
                })},
                {"LFU off the clock, as before TODO 456", with([](rules& r) {
                    r.on_clock = [](policy p) { return p == policy::all_lru || p == policy::volatile_lru; };
                })},
                {"LFU takes a read key", with([](rules& r) {
                    r.takes = [](policy p, const key& k) {
                        const bool lfu = p == policy::all_lfu || p == policy::volatile_lfu;
                        return takes(p, key{k.deleted, k.kept, k.has_expiry, k.expired, k.read && !lfu});
                    };
                })},
                {"the expiry sweep takes any key", with([](rules& r) {
                    r.takes = [](policy p, const key& k) {
                        return p == policy::expired ? !k.deleted && !k.kept : takes(p, k);
                    };
                })},
                {"volatile-ttl cuts past the median", with([](rules& r) {
                    r.ttl_cut = [](size_t n) { return n / 2; };
                })},
                {"volatile-ttl takes only the nearest", with([](rules& r) {
                    r.ttl_cut = [](size_t) { return size_t{0}; };
                })},
                {"a Raft space evicts a live key", with([](rules& r) {
                    r.raft_may_evict = [](bool) { return true; };
                })},
                {"a sweep runs only past the threshold", with([](rules& r) {
                    r.under_pressure = [](size_t m, size_t t) { return m > t; };
                })},
        };
        for (const auto& m : mutants) {
            const auto got = all(m.r);
            char line[160];
            std::snprintf(line, sizeof line, "%s: %s", m.what, got.empty() ? "missed" : got.c_str());
            check(!got.empty(), line);
        }
    }

    std::printf(failures ? "FAILED\n" : "all passed\n");
    return failures ? 1 : 0;
}
