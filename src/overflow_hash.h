//
// Created by teejip on 8/24/25.
//

#ifndef BARCH_DOUBLE_HASH_H
#define BARCH_DOUBLE_HASH_H
#include <cstdint>
#include <type_traits>
#include <functional>
#include <vector>
#include <sastam.h>

/**
 * the double hash gives good performance at much reduced memory use
 * an ordered set can also be used as the second set
 * works because the data it indexes is much larger than
 * the ht itself
 */
namespace oh {
    /**
     * a comparator that can warm what a compare will read. a stored key may be
     * only an address, so a compare reads memory somewhere else. see find()
     */
    template<typename EQ, typename K>
    concept prefetching = requires(const EQ &eq, const K &k) { eq.prefetch(k); };

    template<typename K, typename H = std::hash<K>, typename EQ = std::equal_to<K>>
    struct unordered_set {
    public:
        /**
         * where find_for_insert saw room for the key it did not find. no slot
         * means the probes were full and the key goes the slow way.
         */
        struct claim {
            static constexpr size_t no_slot = ~size_t(0);
            size_t slot = no_slot;
            uint8_t tag = 0;
        };
    private:
        enum {
            PROBES = 3,
            INI_SIZE = 8192
        };
        using key_type = K;
        using hash_type = H;
        using key_compare = EQ;
        typedef heap::unordered_set<K, H, key_compare> H2;

        struct data {
            float max_leakage = 0.15f;
            float max_load_factor = 0.75f;
            float max_rehash_multiplier = 16; // a large multiplier is ok
            // because the hash itself is small relative to the data it indexes
            // it wont work well where the data is small
            key_compare eq{};
            hash_type hash{};
            data(key_compare eq, hash_type hash ) : eq(eq), hash(hash) {
                H2 t;
                h2 = H2(t.begin(), t.end(), size_t(0), hash, eq);
            }
            size_t size{};
            /**
             * one byte per slot: 0 when the slot is empty, else the top 8 bits of
             * the key's hash (0 is moved to 1). a stored key is only an address,
             * so a compare reads memory somewhere else. a probe compares only
             * where the tag matches, which skips all but 1 in 255 of the slots
             * that hold some other key. abseil does the same. TODO 839
             */
            heap::vector<uint8_t> tags{};
            heap::vector<key_type> keys{};
            H2 h2{};
            size_t rehashed = 0;
            size_t modifier(size_t i) {
                return i;
            }
            static uint8_t tag_of(size_t h) {
                auto t = uint8_t(h >> (sizeof(size_t) * 8 - 8));
                return t ? t : 1;
            }
            bool insert(const key_type &k) {
                if (keys.empty()) {
                    resize(0);
                }
                if (h2.contains(k)) return false;

                size_t pos = hash(k);
                uint8_t tag = tag_of(pos);
                size_t fopen = keys.size();
                size_t s = keys.size();
                for (size_t i = 0; i < PROBES; ++i) {
                    size_t at = (pos + i) % s;
                    if (tags[at] == 0) {
                        if (fopen == s) {
                            fopen = at;
                        }
                    }else if (tags[at] == tag && eq(keys[at], k)) {
                        return false;
                    }
                }
                if (fopen < s) {
                    tags[fopen] = tag;
                    keys[fopen] = k;
                    ++size;
                    return true;
                }
                size_t sb = h2.size();
                h2.insert(k);
                return h2.size() == sb + 1;
            }
            key_type* find_or_insert(const key_type &k) {
                if (keys.empty()) {
                    resize(0);
                }
                auto i = h2.find(k);
                if (i != h2.end()) {
                    return (key_type*)&(*i);
                }

                size_t pos = hash(k);
                uint8_t tag = tag_of(pos);
                size_t fopen = keys.size();
                size_t s = keys.size();
                for (size_t i = 0; i < PROBES; ++i) {
                    size_t at = (pos + i) % s;
                    if (tags[at] == 0) {
                        if (fopen == s) {
                            fopen = at;
                        }
                    }else if (tags[at] == tag && eq(keys[at], k)) {
                        return &keys[at];
                    }
                }
                if (fopen < s) {
                    tags[fopen] = tag;
                    keys[fopen] = k;
                    ++size;
                    return nullptr;
                }
                h2.insert(k);
                return nullptr;
            }

            /**
             * if we know that all keys are unique beforehand
             * we can use this function (like during rehash)
             * @param k needs to be unique
             * @return true (always)
             */
            bool insert_unique(const key_type &k) {
                if (keys.empty()) {
                    resize(0);
                }
                size_t pos = hash(k);
                size_t s = keys.size();
                for (size_t i = 0; i < PROBES; ++i) {
                    size_t at = (pos + i) % s;
                    if (tags[at] == 0) {
                        tags[at] = tag_of(pos);
                        keys[at] = k;
                        ++size;
                        return true;
                    }
                }
                size_t sb = h2.size();
                h2.insert(k);
                return h2.size() == sb + 1;
            }

            void remove(const key_type *k) {
                if (k) {
                    remove(*k);
                }
            }
            /**
             * Q is either key_type or any query type the hash and the comparator
             * both understand - see find(). pointers are excluded so that the
             * remove(const key_type*) overload above still wins for them: it is a
             * better match than this template only once pointers are off the table.
             */
            template<typename Q, std::enable_if_t<!std::is_pointer_v<Q>, bool> = true>
            size_t remove(const Q &k) {
                if (keys.empty()) { return 0; }
                size_t pos = hash(k);
                uint8_t tag = tag_of(pos);
                for (size_t i = 0; i < PROBES; ++i) {
                    size_t at = (pos + i) % keys.size();
                    if (tags[at] == tag && eq(keys[at], k)) {
                        keys[at] = K();
                        tags[at] = 0;
                        --size;
                        return 1;
                    }
                }
                return h2.erase(k);

            }
            bool scan(const key_type &q) {
                size_t h = 0;
                for (auto&k : keys) {
                    if (tags[h++] && k == q) {

                        return true;
                    }
                }
                return false;
            }
            /**
             * heterogeneous lookup. Q may be key_type, or any query type that the
             * hash and the comparator both accept - so a caller can look a key up
             * by the bytes it already holds, without having to build (and therefore
             * without having to allocate storage for) a key_type first.
             *
             * the compares run one after another, and each can miss the cache.
             * the probes are known up front, so their reads are started together
             * first and the misses overlap. TODO 826
             */
            void prefetch_probes(size_t pos, size_t s) const {
                if constexpr (prefetching<key_compare, key_type>) {
                    uint8_t tag = tag_of(pos);
                    for (size_t i = 0; i < PROBES; ++i) {
                        size_t at = (pos + i) % s;
                        if (tags[at] == tag) eq.prefetch(keys[at]);
                    }
                }
            }
            /**
             * the two steps of a find, without the compare, for a caller that
             * looks up many keys at once: the slots of every key, then the leaves
             * of every key. done in that order, each step reads memory the step
             * before already asked for. TODO 827
             */
            template<typename Q>
            void prefetch_slots(const Q &k) const {
                if (keys.empty()) return;
                size_t at = hash(k) % keys.size();
                // the probes run on past the home slot, so the next line too
                __builtin_prefetch(&keys[at]);
                __builtin_prefetch(&keys[(at + PROBES - 1) % keys.size()]);
            }
            /**
             * the lines an insert writes: the tags its probes read and the key slot.
             * for a caller that knows the keys of the next few SETs. TODO 899
             */
            template<typename Q>
            void prefetch_insert(const Q &k) const {
                if (keys.empty()) return;
                size_t at = hash(k) % keys.size();
                __builtin_prefetch(&tags[at], 1);
                __builtin_prefetch(&tags[(at + PROBES - 1) % keys.size()], 1);
                __builtin_prefetch(&keys[at], 1);
            }
            template<typename Q>
            void prefetch_leaves(const Q &k) const {
                if (keys.empty()) return;
                prefetch_probes(hash(k), keys.size());
            }
            template<typename Q>
            K* find(const Q &k) {
                if (keys.empty()) { return nullptr; }
                size_t pos = hash(k);
                size_t s = keys.size();
                uint8_t tag = tag_of(pos);
                prefetch_probes(pos, s);
                for (size_t i = 0; i < PROBES; ++i) {
                    size_t at = (pos + i) % s;
                    if (tags[at] == tag && eq(keys[at], k)) {
                        return &keys[at];
                    }
                }

                auto i = h2.find(k);
                if (i != h2.end()) {
                    return (key_type*)&(*i);
                }
                return nullptr;
            }
            template<typename Q>
            const K* find(const Q &k) const {
                if (keys.empty()) { return nullptr; }
                size_t pos = hash(k);
                size_t s = keys.size();
                uint8_t tag = tag_of(pos);
                prefetch_probes(pos, s);
                for (size_t i = 0; i < PROBES; ++i) {
                    size_t at = (pos + i) % s;
                    if (tags[at] == tag && eq(keys[at], k)) {
                        return &keys[at];
                    }
                }

                auto i = h2.find(k);
                if (i != h2.end()) {
                    return (key_type*)&(*i);
                }
                return nullptr;
            }

            /**
             * a find that also notes the first empty probe slot, so an insert of a
             * key that was not found needs no second hash and no second probe. TODO 898
             */
            template<typename Q>
            K* find_for_insert(const Q &k, claim& c) {
                if (keys.empty()) {
                    resize(0);
                }
                size_t pos = hash(k);
                size_t s = keys.size();
                uint8_t tag = tag_of(pos);
                prefetch_probes(pos, s);
                c = claim{claim::no_slot, tag};
                for (size_t i = 0; i < PROBES; ++i) {
                    size_t at = (pos + i) % s;
                    if (tags[at] == 0) {
                        if (c.slot == claim::no_slot) c.slot = at;
                    } else if (tags[at] == tag && eq(keys[at], k)) {
                        return &keys[at];
                    }
                }
                if (!h2.empty()) {
                    auto i = h2.find(k);
                    if (i != h2.end()) {
                        return (key_type*)&(*i);
                    }
                }
                return nullptr;
            }
            // k must not be in the set, and nothing may have changed it since the claim
            bool place(const claim& c, const key_type &k) {
                if (c.slot == claim::no_slot) {
                    return insert_unique(k);
                }
                tags[c.slot] = c.tag;
                keys[c.slot] = k;
                ++size;
                return true;
            }

            void swap(data& with) {
                std::swap(tags, with.tags);
                std::swap(keys, with.keys);
                std::swap(h2, with.h2);
                std::swap(size, with.size);
                std::swap(rehashed, with.rehashed);
            }
            void resize(size_t new_size) {
                if (new_size == 0) {
                    new_size = heap::random_range(INI_SIZE/2,INI_SIZE);
                }
                keys.resize(new_size);
                tags.resize(new_size, 0);
            }

            size_t get_size() const {
                return size + h2.size();
            }
            void rehash(data& to) {

                auto mul = std::max<float>(2.0, max_rehash_multiplier) ;
                to.resize(get_size() * mul);
                for (size_t i = 0; i < keys.size(); i++) {
                    if (tags[i]) {
                        auto before = to.get_size();
                        if (!to.insert_unique(keys[i])) {
                            abort_with("insert failed");
                        }
                        if (to.get_size() != before + 1) {
                            abort_with("rehash failed (size wrong)");
                        }
                    }
                }
                for (auto &k : h2) {
                    auto before = to.get_size();
                    if (!to.insert(k)) {
                        to.insert(k);
                        abort_with("insert failed");
                    }
                    if (to.get_size() != before + 1) {
                        abort_with("rehash failed (size wrong)");
                    }
                }
                if (to.get_size() != get_size()) {
                    barch::log({"resize failed",get_size(), to.get_size()});
                    abort_with("rehash failed");
                }
                to.rehashed = ++rehashed;
            }
            bool tolarge() const {
                // use the max_load_factor or max_ratio to determine rehash
                return size >= max_load_factor*keys.size() || size*max_leakage <= h2.size();
            }
            template<typename Q>
            bool contains(const Q &k) const {
                return find(k) != nullptr;
            }
            void clear() {
                data d0{eq, hash};
                this->swap(d0);
            }
        };
        key_compare eq{};
        hash_type hash{};
        data d_{};
        void rehash(data& from, data& to) {
            from.rehash(to);
        }
        template<typename Q>
        void check(const Q& unused(k)) const {

        }
    public:
        void swap(unordered_set& with) {
            d_.swap(with.d_);
            std::swap(eq, with.eq);
            std::swap(hash, with.hash);
        }
        unordered_set(EQ eq, H h) : eq(eq), hash(h), d_(eq, h) {};
        bool insert(const key_type &k) {
            check(k);
            bool r = d_.insert(k);
            if (d_.tolarge()) {
                data to{d_.eq, d_.hash};
                d_.rehash(to);
                d_.swap(to);
                d_.eq = this->eq;
                d_.hash = this->hash;
            }
            check(k);
            return r;
        }
        bool insert_unique(const key_type &k) {
            check(k);
            bool r = d_.insert_unique(k);
            if (d_.tolarge()) {
                data to{d_.eq, d_.hash};
                d_.rehash(to);
                d_.swap(to);
            }
            check(k);
            return r;
        }
        key_type* find_or_insert(const key_type &k) {
            check(k);
            auto r = d_.find_or_insert(k);
            if (d_.tolarge()) {
                data to;
                d_.rehash(to);
                d_.swap(to);
                if (r) {
                    r = find(k);
                }
            }

            return r;
        }

        template<typename Q>
        K* find(const Q &k) {
            check(k);
            auto r = d_.find(k);
            return r;
        }
        template<typename Q>
        const K* find(const Q &k) const {
            check(k);
            auto r = d_.find(k);
            return r;
        }
        template<typename Q>
        K* find_for_insert(const Q &k, claim& c) {
            check(k);
            return d_.find_for_insert(k, c);
        }
        bool insert_claimed(const claim& c, const key_type &k) {
            check(k);
            bool r = d_.place(c, k);
            if (d_.tolarge()) {
                data to{d_.eq, d_.hash};
                d_.rehash(to);
                d_.swap(to);
            }
            check(k);
            return r;
        }
        template<typename Q>
        void prefetch_slots(const Q &k) const {
            d_.prefetch_slots(k);
        }
        template<typename Q>
        void prefetch_insert(const Q &k) const {
            d_.prefetch_insert(k);
        }
        template<typename Q>
        void prefetch_leaves(const Q &k) const {
            d_.prefetch_leaves(k);
        }

        [[nodiscard]] size_t size() const {
            return d_.get_size();
        }
        size_t erase(const key_type *k) {
            size_t n = d_.get_size();
            check(*k);
            d_.remove(k);
            return n - d_.get_size();
        }

        template<typename Q, std::enable_if_t<!std::is_pointer_v<Q>, bool> = true>
        size_t erase(const Q &k) {
            check(k);
            auto n = d_.get_size();
            d_.remove(k);
            check(k);
            return n - d_.get_size();;
        }
        void clear() {
            d_.clear();
        }
        K* end() const {
            return nullptr;
        }

        /**
         * ratios should be less than 0.2 to be effective
         * @param f number between 0 and 1
         */
        void set_max_leakage(float f) {
            d_.max_leakage = f;
        }

        /**
         * set the load factor at which rehashing takes place
         * @param f value between 0 and 1
         */
        void set_max_load_factor(float f) {
            d_.max_load_factor = f;
        }

        /**
         * the rehash multiplier determines initial expansion rate
         * which tapers of to two
         * @param f value > 1
         */
        void set_rehash_multiplier(int f) {
            d_.rehash_multiplier = f;
        }

        /**
         * return current value
         * @return max load factor
         */
        float max_load_factor() const {
            return d_.max_load_factor;
        }
        /**
         * return current value
         * @return max_ratio
         */
        float max_leakage() const {
            return d_.max_leakage;
        }
        /**
         * return current value
         * @return rehash_multiplier
         */
        float rehash_multiplier() const {
            return d_.rehash_multiplier;
        }
    };
}

#endif //BARCH_DOUBLE_HASH_H