//
// Created by teejip on 10/22/25.
//

#ifndef BARCH_ITERATOR_H
#define BARCH_ITERATOR_H
#include <algorithm>
#include <cstring>
#include "../abstract_shard.h"
namespace art {
    struct iterator {
        barch::shard_ptr t;
        art::trace_list tl{};
        art::node_ptr c{};

        /**
         * performs a lower-bound search and returns an iterator that may not always be valid
         * the life-time of the iterator must not exceed that of the t or key parameters
         * @param t the art tree
         * @param key iterator will start at keys not less than key
         * @return an iterator
         */

        iterator(barch::shard_ptr t, art::value_type key);

        /**
         * starts the iterator at the first key (left most) in the tree
         */
        explicit iterator(barch::shard_ptr t);

        iterator(const iterator &it) = default;

        iterator(iterator &&it) = default;

        iterator &operator=(iterator &&it) = default;

        iterator &operator=(const iterator &it) = default;
        // TODO: skip tombstones
        bool next();

        bool previous();

        bool last();

        [[nodiscard]] const leaf *l() const;

        [[nodiscard]] value_type key() const;

        [[nodiscard]] value_type value() const;

        [[nodiscard]] bool end() const;

        [[nodiscard]] bool ok() const;

        [[nodiscard]] node_ptr current() const;

#if 0
        bool update(std::function<node_ptr(const leaf *l)> updater);

        bool update(value_type value);

        bool update(value_type value, int64_t ttl, bool volat);

        bool update(int64_t ttl, bool volat);

        bool update(int64_t ttl);
#endif

        [[nodiscard]] bool remove() const;

        [[nodiscard]] int64_t distance(const iterator &other) const;

        [[nodiscard]] int64_t distance(value_type other, bool traced = false) const;

        [[nodiscard]] int64_t fast_distance(const iterator &other) const;

        /**
         * Move forward n keys - where n calls to next() would land - without visiting
         * the keys in between. Uses the `descendants` count every internal node keeps,
         * the same numbers fast_distance reads, so it costs about one pass up and one
         * pass down the tree instead of n steps.
         *
         * Like next(), it counts every leaf, tombstones and expired ones included.
         * @return how many keys it moved. Less than n means it ran off the end, and
         * the iterator is then at the end.
         */
        int64_t skip(int64_t n);

        void log_trace() const;

    };

    /**
     * iterates over one or more other iterators simultaneously
     * each iterator can share keys
     *
     * On equal keys the lowest numbered iterator's leaf is handed out and the rest are
     * skipped, so with the newest version of a key first, the newest wins - TODO 701.
     * A heap on (key, index) finds the next one, where it used to compare every
     * iterator on every step: with a space's shards and their versions that's hundreds.
     * With an upper bound, the walk ends at the first key not below it.
     */
    struct merge_iterator {

        heap::vector<iterator> others;
        /*
         * The key last handed out, owned - see TODO 323.
         *
         * This used to be a set of every key the merge had ever produced, and
         * `next` skipped a key that set already held. Two things were wrong with
         * that. The small one is cost: a set entry and a hash per key walked, for
         * a question that only ever concerns two neighbours. The large one is
         * that a `value_type` is a *view* into a leaf, and a leaf lives in an
         * arena page that the walk itself can have relocated - so the set filled
         * up with views into memory that had since been reused, and a later key
         * whose bytes happened to match one of them was dropped as a duplicate.
         * `RANGE` with a limit lost four keys out of a thousand that way.
         *
         * Both streams are ordered, so a duplicate can only be adjacent, and the
         * previous key is all that has to be remembered. Copied, because the view
         * it came from does not stay valid.
         */
        heap::vector<uint8_t> last_key{};
        bool has_last = false;
        size_t min_i = std::numeric_limits<size_t>::max();
        // the iterators that have a key in bound, a min-heap on (key, index); the keys
        // are read from the iterators, which only move once they're off the heap
        heap::vector<size_t> pending{};
        heap::vector<uint8_t> upper{};
        bool bounded = false;

        void remember() {
            auto k = others[min_i].key();
            last_key.assign(k.bytes, k.bytes + k.size);
            has_last = true;
        }
        [[nodiscard]] bool is_last(value_type k) const {
            return has_last && k.size == last_key.size()
                   && memcmp(k.bytes, last_key.data(), k.size) == 0;
        }
        [[nodiscard]] bool in_bound(size_t i) const {
            if (others[i].end()) return false;
            return !bounded || others[i].key() < value_type{upper.data(), upper.size()};
        }
        // std::*_heap make a max-heap of `less`, so this is "after": the larger key, or
        // on equal keys the larger index
        [[nodiscard]] bool after(size_t a, size_t b) const {
            const auto ka = others[a].key(), kb = others[b].key();
            if (kb < ka) return true;
            if (ka < kb) return false;
            return a > b;
        }
        void push(size_t i) {
            pending.push_back(i);
            std::push_heap(pending.begin(), pending.end(), [this](size_t a, size_t b) { return after(a, b); });
        }
        size_t pop() {
            std::pop_heap(pending.begin(), pending.end(), [this](size_t a, size_t b) { return after(a, b); });
            const size_t i = pending.back();
            pending.pop_back();
            return i;
        }
        void start(value_type hi) {
            if (!hi.empty()) {
                upper.assign(hi.bytes, hi.bytes + hi.size);
                bounded = true;
            }
            pending.reserve(others.size());
            for (size_t i = 0; i < others.size(); ++i)
                if (in_bound(i))
                    pending.push_back(i);
            std::make_heap(pending.begin(), pending.end(), [this](size_t a, size_t b) { return after(a, b); });
            min_i = pending.empty() ? others.size() : pending.front();
            if (ok()) {
                remember();
            }
        }

        explicit merge_iterator(const heap::vector<iterator>& iters, value_type hi = {}) : others(iters) {
            start(hi);
        }

        merge_iterator(const std::initializer_list<iterator>& iters) : others(iters) {
            start({});
        }

        merge_iterator(const merge_iterator&) = default;
        merge_iterator(merge_iterator&&) = default;

        merge_iterator& operator=(const merge_iterator&) = default;
        merge_iterator& operator=(merge_iterator&&) = default;

        void check_end() const {
            if (end()) {
                throw_exception<std::out_of_range>("invalid merge iterator");
            }
        }
        bool next() {

            if (end()) return false;

            // the one handed out is the heap's top: move it on
            size_t i = pop();
            others[i].next();
            if (in_bound(i))
                push(i);
            // a duplicate is the same key in a later iterator, which an ordered merge
            // brings straight after it
            while (!pending.empty() && is_last(others[pending.front()].key())) {
                i = pop();
                others[i].next();
                if (in_bound(i))
                    push(i);
            }
            min_i = pending.empty() ? others.size() : pending.front();
            if (ok()) {
                remember();
            }
            return ok();
        }

        static bool last() {
            return false;
        }

        [[nodiscard]] const leaf *l() const {
            check_end();
            return others[min_i].l();
        }

        [[nodiscard]] value_type key() const {
            check_end();
            return others[min_i].key();
        };

        [[nodiscard]] value_type value() const {
            check_end();
            return others[min_i].value();
        }

        [[nodiscard]] bool end() const {
            return min_i >= others.size();
        }

        [[nodiscard]] bool ok() const {
            return !end();
        };

        [[nodiscard]] node_ptr current() const {
            check_end();
            return others[min_i].current();
        };

        [[nodiscard]] int64_t distance(const iterator &other) const {
            check_end();
            auto r = 0ll;
            for (auto& i: others) {
                r += i.distance(other);
            }
            return r;
        };

        [[nodiscard]] int64_t distance(value_type other, bool traced = false) const {
            check_end();
            auto r = 0ll;
            for (auto& i: others) {
                r += i.distance(other, traced);
            }
            return r;
        }

        [[nodiscard]] int64_t fast_distance(const merge_iterator &other) const {
            check_end();
            auto r = 0ll;
            size_t n = 0;
            for (auto& i: others) {
                r += i.fast_distance(other.others[n]);
                ++n;
            }
            return r;
        }

    };
}
#endif //BARCH_ITERATOR_H