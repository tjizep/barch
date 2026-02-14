//
// Created by teejip on 8/24/25.
//

#ifndef BARCH_DOUBLE_HASH_H
#define BARCH_DOUBLE_HASH_H
#include <cstdint>
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
    template<typename K, typename H = std::hash<K>, typename EQ = std::equal_to<K>>
    struct unordered_set {
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
            float max_leakage = 0.01f;
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
            std::vector<bool> has{};
            heap::vector<key_type> keys{};
            H2 h2{};
            size_t rehashed = 0;
            size_t modifier(size_t i) {
                return i;
            }
            bool insert(const key_type &k) {
                if (keys.empty()) {
                    resize(0);
                }
                if (h2.contains(k)) return false;

                size_t pos = hash(k);
                size_t fopen = keys.size();
                size_t s = keys.size();
                for (size_t i = 0; i < PROBES; ++i) {
                    size_t at = (pos + i) % s;
                    if (!has[at]) {
                        if (fopen == s) {
                            fopen = at;
                        }
                    }else if (eq(keys[at], k)) {
                        return false;
                    }
                }
                if (fopen < s) {
                    has[fopen] = true;
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
                size_t fopen = keys.size();
                size_t s = keys.size();
                for (size_t i = 0; i < PROBES; ++i) {
                    size_t at = (pos + i) % s;
                    if (!has[at]) {
                        if (fopen == s) {
                            fopen = at;
                        }
                    }else if (eq(keys[at], k)) {
                        return &keys[at];
                    }
                }
                if (fopen < s) {
                    has[fopen] = true;
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
                    if (!has[at]) {
                        has[at] = true;
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
            size_t remove(const key_type &k) {
                if (keys.empty()) { return 0; }
                size_t pos = hash(k);
                for (size_t i = 0; i < PROBES; ++i) {
                    size_t at = (pos + i) % keys.size();
                    if (has[at] && eq(keys[at], k)) {
                        keys[at] = K();
                        has[at] = false;
                        --size;
                        return 1;
                    }
                }
                return h2.erase(k);

            }
            bool scan(const key_type &q) {
                size_t h = 0;
                for (auto&k : keys) {
                    if (has[h++] && k == q) {

                        return true;
                    }
                }
                return false;
            }
            K* find(const key_type &k) {
                if (keys.empty()) { return nullptr; }
                size_t pos = hash(k);
                size_t s = keys.size();
                for (size_t i = 0; i < PROBES; ++i) {
                    size_t at = (pos + i) % s;
                    if (has[at] && eq(keys[at], k)) {
                        return &keys[at];
                    }
                }

                auto i = h2.find(k);
                if (i != h2.end()) {
                    return (key_type*)&(*i);
                }
                return nullptr;
            }
            const K* find(const key_type &k) const {
                if (keys.empty()) { return nullptr; }
                size_t pos = hash(k);
                size_t s = keys.size();
                for (size_t i = 0; i < PROBES; ++i) {
                    size_t at = (pos + i) % s;
                    if (has[at] && eq(keys[at], k)) {
                        return &keys[at];
                    }
                }

                auto i = h2.find(k);
                if (i != h2.end()) {
                    return (key_type*)&(*i);
                }
                return nullptr;
            }

            void swap(data& with) {
                std::swap(has, with.has);
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
                has.resize(new_size,false);
            }

            size_t get_size() const {
                return size + h2.size();
            }
            void rehash(data& to) {

                auto mul = std::max<float>(2.0, max_rehash_multiplier) ;
                to.resize(get_size() * mul);
                for (size_t i = 0; i < keys.size(); i++) {
                    if (has[i]) {
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
                    barch::std_log("resize failed",get_size(), to.get_size());
                    abort_with("rehash failed");
                }
                to.rehashed = ++rehashed;
            }
            bool tolarge() const {
                // use the max_load_factor or max_ratio to determine rehash
                return size >= max_load_factor*keys.size() || size*max_leakage <= h2.size();
            }
            bool contains(const key_type &k) const {
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
        void check(const key_type& unused(k)) const {

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

        K* find(const key_type &k) {
            check(k);
            auto r = d_.find(k);
            return r;
        }
        const K* find(const key_type &k) const {
            check(k);
            auto r = d_.find(k);
            return r;
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

        size_t erase(const key_type &k) {
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