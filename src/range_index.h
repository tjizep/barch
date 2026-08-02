//
// Created by teejip on 8/2/26.
//

#ifndef BARCH_RANGE_INDEX_H
#define BARCH_RANGE_INDEX_H

#include <atomic>
#include <memory>

#include "abstract_shard.h"

namespace barch {

    /**
     * The routing form of a key.
     *
     * Storage appends a null terminator to a key that does not already have one - see
     * art::s_filter_key - so the same key reaches the router in two forms depending on
     * where it came from: `abc` from a command, `abc\0` read back out of a leaf.
     * Comparing one against the other gives the wrong answer, because `abc` sorts below
     * `abc\0`, and a boundary compared the wrong way sends a key to the shard next door.
     *
     * So both the boundaries in the table and the key being routed are reduced to the
     * same form first. Dropping a trailing null is order preserving: a key may not
     * contain an interior null, so for any two keys a and b, a+0 < b+0 exactly when
     * a < b.
     */
    inline art::value_type route_key(art::value_type key) {
        if (key.size > 1 && key.bytes[key.size - 1] == 0) {
            return {key.bytes, (size_t)(key.size - 1)};
        }
        return key;
    }

    /**
     * Which shard of a range sharded key space owns which span of the key order.
     *
     * The table is the minimum key of every shard above 0, in key order, so a route is a
     * binary search for the last boundary not above the key. A key below every boundary
     * belongs to shard 0, which is why shard 0 has no entry: it is the span that is open
     * at the bottom.
     *
     * Three things about it are worth stating, because they are what make the rest of
     * this cheap:
     *
     *  - **it is never persisted.** It is a function of the shards - each shard's first
     *    key, which an art finds walking down its left spine - so a load rebuilds it
     *    rather than reading it back. There is no index file to version, to write
     *    atomically, or to find out of step with the data it describes.
     *  - **it is a flat vector, not a map.** At most shard_count entries, read on every
     *    route and written only by a rebalance, so the memmove on a write is paid rarely
     *    and the binary search touches a couple of cache lines.
     *  - **it is replaced, never mutated.** A rebalance builds a whole new table and
     *    swaps it in behind an atomic pointer. Routing threads therefore never see a
     *    half written table and never take a lock to read one; a route holds the table
     *    it loaded for the duration of the call, and that table stays alive because it
     *    is a shared_ptr.
     *
     * Swapping the table is not on its own enough to route correctly, because a router
     * can load the table, then be descheduled, and take its lock after the boundary it
     * routed by has moved. What closes that gap is the rule the writers below keep:
     * *the table is only ever changed while holding a write lock on every shard whose
     * span is changing*. A caller that routes, locks, and then re-routes against a
     * freshly loaded table either sees the same shard - in which case no rebalance can
     * have moved that key, since it would have needed the lock the caller is holding -
     * or sees a different one and retries. sharded_store does exactly that.
     */
    class range_index {
    public:
        struct entry {
            /** the smallest key in `shard`, in routing form */
            heap::vector<uint8_t> key{};
            size_t shard{};
            [[nodiscard]] art::value_type value() const { return art::value_type(key); }
        };
        typedef heap::vector<entry> table;
        typedef std::shared_ptr<const table> table_ptr;

        range_index();
        range_index(const range_index&) = delete;
        range_index& operator=(const range_index&) = delete;

        /** the table as it is now. Hold it for as long as the routing decision matters */
        [[nodiscard]] table_ptr get() const { return current.load(std::memory_order_acquire); }

        /**
         * the position of the first boundary above key.
         *
         * This is the lower bound over the shards, and the ordered operations are built
         * on it rather than on route() because they need both of the shards it names:
         * the entry before it owns key's span, and the entry at it - if there is one -
         * is the next shard up. Since every entry is a shard's minimum, that second one
         * is also the smallest key in the space above key that key's own shard does not
         * hold itself.
         */
        static size_t upper(const table& t, art::value_type key);

        /** the shard owning key according to `t` */
        static size_t route(const table& t, art::value_type key);
        /** the shard owning key according to the table as it is now */
        [[nodiscard]] size_t route(art::value_type key) const { return route(*get(), key); }

        /**
         * rebuild the table from the shards themselves, which is what a load does.
         * The caller must hold the space write lock: this reads every shard's minimum
         * and the answer is only meaningful if nothing moves while it does.
         * @return false if the shards are not an ordered partition, in which case the
         *         table published is still the best available but routing by it would
         *         lose keys - see repartition
         */
        bool rebuild(const heap::vector<shard_ptr>& shards);

        /**
         * true if the shards are an ordered partition: every key in shard i below every
         * key in shard j, for i < j. The caller must hold the space read lock.
         */
        [[nodiscard]] static bool partitioned(const heap::vector<shard_ptr>& shards);

        /**
         * move every key that the current table says is in the wrong shard, so that the
         * shards become the partition the table describes. This is what turns a space
         * that was hash sharded when it was written into one that can be range routed,
         * and it moves nearly every key, so it is a load time operation and says so in
         * the log. The caller must hold the space write lock.
         * @return the number of keys moved
         */
        size_t repartition(const heap::vector<shard_ptr>& shards);

        struct sweep_result {
            size_t moved = 0;      // keys moved
            size_t sheds = 0;      // lock pairs taken
            bool balanced = true;  // false if the sweep ran out of budget with work left
        };

        /**
         * one rebalancing sweep, run from the maintenance thread.
         *
         * Repeatedly finds the largest shard and cascades away from it - shedding into
         * whichever neighbour has more room, then looking at that neighbour in turn -
         * until no shard is over the threshold or `max_sheds` lock pairs have been
         * taken. Two things about the shape of it, both settled by the prototype in
         * test/rangeshard_prototype.cpp and neither obvious:
         *
         *  - **a shed moves only enough to meet its neighbour half way**, not everything
         *    above the threshold. Shedding down to the threshold dumps a block into the
         *    neighbour and pushes the whole cascade over at once, which costs moves
         *    quadratic in the shard count. Half way makes it linear.
         *  - **the work is driven by the skew, not by a fixed pass count.** An arbitrary
         *    number of inserts can arrive between two sweeps, so a sweep that does a
         *    fixed amount of work quietly stops keeping up rather than falling behind
         *    visibly. `max_sheds` is the cap that stops one sweep hogging the thread; it
         *    is not the amount of work a sweep does.
         *
         * Each shed holds a write lock on exactly two shards and moves at most `budget`
         * keys under it, and the locks are dropped between sheds, so a sweep with a lot
         * to do does not hold the space still while it does it.
         */
        sweep_result sweep(const heap::vector<shard_ptr>& shards, size_t budget,
                           double tolerance, size_t max_sheds);

    private:
        /** rebuild the entries for shards a and b and publish the result */
        void refresh(const heap::vector<shard_ptr>& shards, size_t a, size_t b);
        void publish(const std::shared_ptr<table>& t);

        std::atomic<table_ptr> current;
    };

    /**
     * how many keys one shed may move under a single pair of locks, and how far a shard
     * may be above the average before it is considered over. At exactly the average a
     * shard is over the moment it is one key above it and thrashes; the slack is what
     * stops that.
     */
    size_t get_range_shard_budget();
    double get_range_shard_tolerance();
}

#endif //BARCH_RANGE_INDEX_H
