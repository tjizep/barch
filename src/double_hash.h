//
// Created by teejip on 8/24/25.
//

#ifndef BARCH_DOUBLE_HASH_H
#define BARCH_DOUBLE_HASH_H
#include <cstdint>
#include <functional>
#include <vector>
#include <sastam.h>
namespace dh {
    template<typename K, typename H = std::hash<K>, typename EQ = std::equal_to<K>, int PROBES = 4>
    struct set {
    private:

        using key_type = K;
        using hash_type = H;
        using key_compare = EQ;
        typedef heap::set<K, H> H2;

        struct data {
            const float max_ratio = 0.05f;
            const float max_load_factor = 0.75f;
            EQ eq{};
            hash_type hash{};
            size_t size{};
            heap::vector<bool> has{};
            heap::vector<key_type> keys{};
            H2 h2{};
            bool insert(const key_type &k) {
                if (keys.empty()) {
                    resize(4);
                }

                size_t pos = hash(k);
                size_t fopen = keys.size();
                for (size_t i = 0; i < PROBES; ++i) {
                    size_t at = (pos + i) % keys.size();
                    if (!has[at]) {
                        if (fopen == keys.size()) {
                            fopen = at;
                        }
                    }else if (eq(keys[at], k)) {
                        return false;
                    }
                }
                if (fopen < keys.size()) {
                    has[fopen] = true;
                    keys[fopen] = k;
                    ++size;
                    return true;
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
                    size_t at = (pos + i) % keys.size();
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
                for (size_t i = 0; i < PROBES; ++i) {
                    size_t at = (pos + i) % keys.size();
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
                return find(k);
            }

            void swap(data& with) {
                std::swap(has, with.has);
                std::swap(keys, with.keys);
                std::swap(h2, with.h2);
                std::swap(size, with.size);
            }
            void resize(size_t new_size) {
                keys.resize(new_size);
                has.resize(new_size,false);
            }
            size_t get_size() const {
                return size + h2.size();
            }
            void rehash(data& to) {
                to.resize(get_size()*7);
                for (size_t i = 0; i < keys.size(); i++) {
                    if (has[i]) {
                        auto before = to.get_size();
                        if (!to.insert(keys[i])) {
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
            }
            bool tolarge() const {
                //
                return size >= max_load_factor*keys.size() || size*max_ratio <= h2.size();
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
        set() = default;
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
    };
}

#endif //BARCH_DOUBLE_HASH_H