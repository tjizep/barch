#pragma once
//
// Sets and maps kept in a scratch space rather than on the heap - TODO 597.
//
// key_space::make_scratch() is a private one-shard space: not named, not saved,
// not replicated, its files gone when it is. What goes in it lives in the arenas
// the server manages rather than in malloc'd nodes, so a temporary that turns out
// large - every node a walk has seen, every version a collector has to weigh - is
// held the way the server's own data is, and a process running under a tight
// cgroup is less likely to be killed for it.
//
// It is not faster than std::set or std::map; every operation is a store call and
// a copy. And it can't hand out references, since nothing lives in memory as a K or
// a V: lookups answer with values, `*it` is a value, and changing an entry is
// `set` or `update`, never assignment through a reference.
//
//     barch::scratch::set<uint64_t> seen;
//     if (seen.insert(id)) ...
//     barch::scratch::map<std::string, int64_t> counts;
//     counts.update("x", 0, [](int64_t& n) { ++n; });
//     for (auto [k, v] : counts) ...
//
// Keys and values are uint64_t, int64_t or std::string. Iteration is in key order:
// unsigned for uint64_t, signed for int64_t, bytewise for strings. Integers are
// stored as eight big-endian bytes (an int64_t with its sign bit flipped), so the
// store's own byte order is the numeric one; strings are stored as they are, any
// bytes at all. See test/scratchcontainertest.cpp for what was checked.
//
// Not safe to change from two threads at once, the same as the std containers, and
// an iterator taken before a change may or may not see it.
//
#include "foreign/driver.h"
#include "key_space.h"

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace barch::scratch {

/** how a K or a V is written into the store and read back */
template<typename T> struct codec;

template<> struct codec<uint64_t> {
    static std::string encode(uint64_t v) {
        std::string out(8, '\0');
        for (int i = 7; i >= 0; --i) {
            out[(size_t) i] = (char) (v & 0xff);
            v >>= 8;
        }
        return out;
    }
    static uint64_t decode(const std::string& s) {
        uint64_t v = 0;
        for (size_t i = 0; i < 8 && i < s.size(); ++i)
            v = (v << 8) | (uint8_t) s[i];
        return v;
    }
};

template<> struct codec<int64_t> {
    // the sign bit flipped, so a negative number sorts before a positive one
    static std::string encode(int64_t v) {
        return codec<uint64_t>::encode((uint64_t) v ^ (1ull << 63));
    }
    static int64_t decode(const std::string& s) {
        return (int64_t) (codec<uint64_t>::decode(s) ^ (1ull << 63));
    }
};

template<> struct codec<std::string> {
    static const std::string& encode(const std::string& v) { return v; }
    static const std::string& decode(const std::string& s) { return s; }
};

template<typename T>
constexpr bool supported = std::is_same_v<T, uint64_t> || std::is_same_v<T, int64_t> ||
                           std::is_same_v<T, std::string>;

/**
 * The scratch space and its access, which both containers stand on. Raw keys and
 * values; the containers do the encoding.
 */
class store {
public:
    store();
    store(const store&) = delete;
    store& operator=(const store&) = delete;
    store(store&&) noexcept = default;
    store& operator=(store&&) noexcept = default;

    /** true when the key was not there before. Throws when the store refuses */
    bool put(const std::string& key, const std::string& value);
    bool get(const std::string& key, std::string& value) const;
    bool has(const std::string& key) const;
    /** true when the key was there */
    bool drop(const std::string& key);
    size_t size() const { return count; }
    /** a fresh space: the old one, and its files, go */
    void reset();
    /**
     * up to `want` keys in order: from `from` itself when `inclusive`, otherwise
     * after it. Empty means the end.
     */
    void page(const std::string& from, bool inclusive, size_t want,
              heap::vector<std::string>& out) const;

private:
    key_space_ptr space{};
    foreign::store_access acc{};
    size_t count{0};
};

/** what an iterator walks with: the keys a page at a time, decoded on the way out */
template<typename Out, typename Make>
class iterator {
public:
    using iterator_category = std::input_iterator_tag;
    using value_type = Out;
    using difference_type = std::ptrdiff_t;
    using pointer = void;
    using reference = Out;

    iterator() = default;
    iterator(const store* s, const std::string& from, bool inclusive, Make make)
        : st(s), make(std::move(make)) {
        fill(from, inclusive);
    }
    /** by value: there is nothing in memory to refer to */
    Out operator*() const { return make(*st, keys[at]); }
    iterator& operator++() {
        if (++at >= keys.size()) {
            std::string last = std::move(keys.back());
            fill(last, false);
        }
        return *this;
    }
    void operator++(int) { ++*this; }
    bool operator==(const iterator& o) const {
        if (!st || !o.st)
            return st == o.st;
        return st == o.st && keys[at] == o.keys[o.at];
    }
    bool operator!=(const iterator& o) const { return !(*this == o); }

private:
    static constexpr size_t page_size = 256;
    void fill(const std::string& from, bool inclusive) {
        keys.clear();
        at = 0;
        st->page(from, inclusive, page_size, keys);
        if (keys.empty())
            st = nullptr;               // the end
    }
    const store* st{nullptr};
    Make make{};
    heap::vector<std::string> keys{};
    size_t at{0};
};

/** an ordered set of K, kept in a scratch space */
template<typename K>
class set {
    static_assert(supported<K>, "scratch::set keys are uint64_t, int64_t or std::string");
    struct make_key {
        K operator()(const store&, const std::string& raw) const {
            return K(codec<K>::decode(raw));
        }
    };

public:
    using key_type = K;
    using value_type = K;
    using const_iterator = scratch::iterator<K, make_key>;
    using iterator = const_iterator;

    set() = default;

    /** true when it was not there before */
    bool insert(const K& k) { return st.put(codec<K>::encode(k), std::string()); }
    bool contains(const K& k) const { return st.has(codec<K>::encode(k)); }
    size_t count(const K& k) const { return contains(k) ? 1 : 0; }
    /** true when it was there */
    bool erase(const K& k) { return st.drop(codec<K>::encode(k)); }
    size_t size() const { return st.size(); }
    bool empty() const { return st.size() == 0; }
    void clear() { st.reset(); }

    iterator begin() const { return iterator(&st, std::string(), true, make_key{}); }
    iterator end() const { return iterator(); }
    /** the first key not less than `k` */
    iterator lower_bound(const K& k) const {
        return iterator(&st, std::string(codec<K>::encode(k)), true, make_key{});
    }

private:
    store st;
};

/** an ordered map from K to V, kept in a scratch space */
template<typename K, typename V>
class map {
    static_assert(supported<K>, "scratch::map keys are uint64_t, int64_t or std::string");
    static_assert(supported<V>, "scratch::map values are uint64_t, int64_t or std::string");
    struct make_pair {
        std::pair<K, V> operator()(const store& s, const std::string& raw) const {
            std::string v;
            s.get(raw, v);
            return {K(codec<K>::decode(raw)), V(codec<V>::decode(v))};
        }
    };

public:
    using key_type = K;
    using mapped_type = V;
    using value_type = std::pair<K, V>;
    using const_iterator = scratch::iterator<value_type, make_pair>;
    using iterator = const_iterator;

    map() = default;

    /** add k -> v unless k is there already, like std::map::insert. True when added */
    bool insert(const K& k, const V& v) {
        const std::string key(codec<K>::encode(k));
        if (st.has(key))
            return false;
        return st.put(key, std::string(codec<V>::encode(v)));
    }
    /** k -> v whether or not k was there. True when it wasn't */
    bool set(const K& k, const V& v) {
        return st.put(std::string(codec<K>::encode(k)), std::string(codec<V>::encode(v)));
    }
    bool insert_or_assign(const K& k, const V& v) { return set(k, v); }
    std::optional<V> get(const K& k) const {
        std::string raw;
        if (!st.get(std::string(codec<K>::encode(k)), raw))
            return std::nullopt;
        return V(codec<V>::decode(raw));
    }
    V value_or(const K& k, const V& otherwise) const {
        auto v = get(k);
        return v ? *v : otherwise;
    }
    /**
     * Change the value in place, as far as that goes here: read it (`initial` when
     * k is not there), let `f` change the copy, write it back. What `m[k] += 1`
     * would be on a std::map.
     */
    template<typename F>
    V update(const K& k, const V& initial, F&& f) {
        V v = value_or(k, initial);
        f(v);
        set(k, v);
        return v;
    }
    bool contains(const K& k) const { return st.has(std::string(codec<K>::encode(k))); }
    size_t count(const K& k) const { return contains(k) ? 1 : 0; }
    bool erase(const K& k) { return st.drop(std::string(codec<K>::encode(k))); }
    size_t size() const { return st.size(); }
    bool empty() const { return st.size() == 0; }
    void clear() { st.reset(); }

    iterator begin() const { return iterator(&st, std::string(), true, make_pair{}); }
    iterator end() const { return iterator(); }
    iterator lower_bound(const K& k) const {
        return iterator(&st, std::string(codec<K>::encode(k)), true, make_pair{});
    }

private:
    store st;
};

}
