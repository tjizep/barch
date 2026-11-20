#ifndef BARCH_EVICTION_CORE_H
#define BARCH_EVICTION_CORE_H
/*
 * The eviction sweeps' rules, with nothing else in them - TODO 940.
 *
 * Under memory pressure each maintenance tick a sweep takes one page of leaves and
 * evicts some of the keys on it. Which page, and which keys, is decided here from
 * plain values. shard.cpp holds the latch, reads the leaves and evicts.
 *
 * This header includes no locks and no files, and TestCoreBoundary fails if it
 * starts to. test/evictiontest.cpp runs every key state and every small page
 * through it.
 */
#include <cstddef>
#include <optional>
#include <utility>

namespace barch::eviction_core {
    enum class policy {
        all_random,
        volatile_random,
        all_lfu,
        volatile_lfu,
        volatile_ttl,
        all_lru,
        volatile_lru,
        expired         // the expiry sweep, which runs whatever the policy
    };

    /** what a sweep knows about one leaf on its page */
    struct key {
        bool deleted{false};
        bool kept{false};        // may_evict says no: a function, a meta key, a file's or graph's key
        bool has_expiry{false};  // what the volatile policies mean by volatile - TODO 893
        bool expired{false};
        bool read{false};        // the LRU bit: read since the clock last passed
    };

    /** whether a sweep runs: memory at or past the pre-evict threshold */
    inline bool under_pressure(size_t memory, size_t threshold) {
        return memory >= threshold;
    }

    /*
     * The pages a random pick is drawn from, or none. Page 0 is the null base and
     * holds no leaves. A shard with no pages has none to draw: the range [1, 0] is
     * undefined, and with _GLIBCXX_ASSERTIONS it aborts - TODO 940. Memory is counted
     * for the whole server, so an empty shard sweeps when the others are full.
     */
    inline std::optional<std::pair<size_t, size_t>> page_range(size_t max_page) {
        if (max_page == 0)
            return std::nullopt;
        return std::make_pair(size_t{1}, max_page);
    }

    /*
     * Whether a policy runs on the LRU clock: the read bit and the sweep that
     * clears it. LFU does too - TODO 456. Its sweep read a page source that was a
     * stub and took nothing. A true count needs a byte on every leaf and a change
     * to the file format, and the leaf's flags byte is full.
     */
    inline bool on_clock(policy p) {
        return p == policy::all_lru || p == policy::volatile_lru
            || p == policy::all_lfu || p == policy::volatile_lfu;
    }

    /*
     * Whether a sweep takes this key off its page. A key that was read keeps its
     * place on the clock: the sweep clears its bit instead, and takes it next time
     * if nobody read it since. volatile-ttl takes the candidates this says yes to
     * only as far as ttl_cut.
     */
    inline bool takes(policy p, const key& k) {
        if (k.deleted || k.kept)
            return false;
        switch (p) {
            case policy::all_random:
                return true;
            case policy::volatile_random:
            case policy::volatile_ttl:
                return k.has_expiry;
            case policy::all_lru:
            case policy::all_lfu:
                return !k.read;
            case policy::volatile_lru:
            case policy::volatile_lfu:
                return !k.read && k.has_expiry;
            case policy::expired:
                return k.expired;
        }
        return false;
    }

    /**
     * volatile-ttl: of `n` candidates sorted by expiry, the index of the last one it
     * takes. The nearer half, the median with it. n > 0
     */
    inline size_t ttl_cut(size_t n) {
        return (n - 1) / 2;
    }

    /*
     * In a Raft space only an expired key goes - TODO 610. Its expiry is in the
     * record, so every member takes it at the same moment. A live key taken for
     * memory is this node's own choice and would leave the members apart.
     */
    inline bool raft_may_evict(bool expired) {
        return expired;
    }
}
#endif //BARCH_EVICTION_CORE_H
