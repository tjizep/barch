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
    template<typename K, typename H = std::hash<K>, typename EQ = std::equal_to<K>, int PROBES = 4>
    struct unordered_set {
    private:

        using key_type = K;
        using hash_type = H;
        using key_compare = EQ;
        typedef heap::set<K, H> H2;

        struct data {
            float max_leakage = 0.075f;
            float max_load_factor = 0.8f;
            float max_rehash_multiplier = 7; // a large multiplier is ok
            // because the hash itself is small relative to the data it indexes
            // it wont work well where the data is small
            EQ eq{};
            hash_type hash{};
            size_t size{};
            heap::vector<bool> has{};
            heap::vector<key_type> keys{};
            H2 h2{};
            float rehashed = 0;
            size_t modifier(size_t i) {
                return i;
            }
            bool insert(const key_type &k) {
                if (keys.empty()) {
                    resize(4);
                }

                size_t pos = hash(k);
                size_t fopen = keys.size();
                size_t s = keys.size();
                for (size_t i = 0; i < PROBES; ++i) {
                    size_t at = (pos + modifier(i)) % s;
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

            /**
             * if we know that all keys are unique beforehand
             * we can use this function (like during rehash)
             * @param k needs to be unique
             * @return true (always)
             */
            bool insert_unique(const key_type &k) {
                if (keys.empty()) {
                    resize(4);
                }
                size_t pos = hash(k);
                size_t s = keys.size();
                size_t start = pos % s;
                size_t end = std::min(s,start + PROBES);

                for (size_t i = start; i < end; ++i) {
                    if (!has[i]) {
                        has[i] = true;
                        keys[i] = k;
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
            void remove(const key_type &k) {
                if (keys.empty()) { return; }
                size_t pos = hash(k);
                for (size_t i = 0; i < PROBES; ++i) {
                    size_t at = (pos + modifier(i)) % keys.size();
                    if (has[at] && eq(keys[at], k)) {
                        keys[at] = K();
                        has[at] = false;
                        --size;
                        return;
                    }
                }
                h2.erase(k);
            }
            K* find(const key_type &k) {
                if (keys.empty()) { return nullptr; }
                size_t pos = hash(k);
                size_t start = pos % keys.size();
                size_t end = std::min(keys.size(),start + PROBES);
                for (size_t i = start; i < end; ++i) {
                    if (has[i] && eq(keys[i], k)) {
                        return &keys[i];
                    }
                }
                auto i = h2.find(k);
                if (i != h2.end()) {
                    return (key_type*)&(*i);
                }
                return nullptr;
            }
            const K* find(const key_type &k) const {
                return find(k);
            }

            void swap(data& with) {
                std::swap(has, with.has);
                std::swap(keys, with.keys);
                std::swap(h2, with.h2);
                std::swap(size, with.size);
                std::swap(rehashed, with.rehashed);
            }
            void resize(size_t new_size) {
                keys.resize(new_size);
                has.resize(new_size,false);
            }
            size_t get_size() const {
                return size + h2.size();
            }
            void rehash(data& to) {
                //art::std_log("fullness",(float)size/ (float)keys.size(), keys.size(), h2.size(),"leakage",(float)h2.size()/(float)size,"rehashed", rehashed);
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
                        abort_with("insert failed");
                    }
                    if (to.get_size() != before + 1) {
                        abort_with("rehash failed (size wrong)");
                    }
                }
                if (to.get_size() != get_size()) {
                    art::std_log("resize failed",get_size(), to.get_size());
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
                data d0;
                this->swap(d0);
            }
        };
        data d_{};

        void rehash(data& from, data& to) {
            from.rehash(to);
        }

    public:
        unordered_set() = default;
        bool insert(const key_type &k) {
            bool r = d_.insert(k);

            if (d_.tolarge()) {

                data to;
                d_.rehash(to);
                d_.swap(to);
            }
            return r;
        }

        K* find(const key_type &k) {
            return d_.find(k);
        }
        const K* find(const key_type &k) const {
            return d_.find(k);
        }

        size_t size() const {
            return d_.get_size();
        }
        void erase(const key_type *k) {
            d_.remove(k);
        }

        void erase(const key_type &k) {
            d_.remove(k);
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