//
// Created by teejip on 10/22/25.
//

#ifndef BARCH_ITERATOR_H
#define BARCH_ITERATOR_H
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

        void log_trace() const;

    };

    /**
     * iterates over one or more other iterators simultaneously
     * each iterator can share keys
     */
    struct merge_iterator {

        heap::vector<iterator> others;
        heap::unordered_set<value_type, vt_hash> visited;
        size_t min_i = std::numeric_limits<size_t>::max();

        explicit merge_iterator(const heap::vector<iterator>& iters) : others(iters) {
            min_i = min_current();
            if (ok()) {
                visited.insert(others[min_i].key());
            }
        }

        merge_iterator(const std::initializer_list<iterator>& iters) : others(iters) {
            min_i = min_current();
            if (ok()) {
                visited.insert(others[min_i].key());
            }
        }

        merge_iterator(const merge_iterator&) = default;
        merge_iterator(merge_iterator&&) = default;

        merge_iterator& operator=(const merge_iterator&) = default;
        merge_iterator& operator=(merge_iterator&&) = default;

        [[nodiscard]] size_t min_current() const {
            value_type min_k{};
            size_t m = others.size();
            for (size_t i = 0; i < others.size(); ++i) {
                if (others[i].end()) continue;

                value_type ok = others[i].key();

                if (ok.empty()) {
                    m = i;
                    break; // there's no smaller key than the empty key
                }

                if (min_k.empty() || ok < min_k) {
                    min_k = ok;
                    m = i;
                }
            }
            return m;
        }
        void check_end() const {
            if (end()) {
                throw_exception<std::out_of_range>("invalid merge iterator");
            }
        }
        bool next() {

            if (end()) return false;

            others[min_i].next();

            min_i = min_current();
            if (ok()) {
                if (visited.contains(others[min_i].key())) {
                    return next();
                }
                visited.insert(others[min_i].key());
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