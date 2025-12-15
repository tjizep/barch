//
// Created by linuxlite on 2/10/25.
//

#ifndef SASTAM_H
#define SASTAM_H
#include <algorithm>
#include <cstdlib>
#include <atomic>
#include <cstring> // for memcpy
#include <new> // bad_alloc, bad_array_new_length
#include <memory>
#include <statistics.h>
#include <stdexcept>
#include <vector>
#include <ankerl/unordered_dense.h>
#include <jg/dense_hash_map.hpp>
#define unused_arg
#define unused(x)

#    if defined(__GNUC__) || defined(__INTEL_COMPILER) || defined(__clang__)
#        define LIKELY(x) __builtin_expect(x, 1)   // NOLINT(cppcoreguidelines-macro-usage)
#        define UNLIKELY(x) __builtin_expect(x, 0) // NOLINT(cppcoreguidelines-macro-usage)
#    else
#        define LIKELY(x) (x)   // NOLINT(cppcoreguidelines-macro-usage)
#        define UNLIKELY(x) (x) // NOLINT(cppcoreguidelines-macro-usage)
#    endif

namespace heap {

    uint64_t random_range(uint64_t lower, uint64_t upper);

    typedef std::shared_timed_mutex shared_mutex;
    uint64_t get_physical_memory_bytes();

    double get_physical_memory_ratio();

    extern std::atomic<uint64_t> allocated;

    void *allocate(size_t size);

    template<typename T>
    T *allocate(size_t count) {
        return static_cast<T *>(allocate(count * sizeof(T)));
    }

    void free(void *ptr, size_t size);

    void free(void *ptr);

    void check_ptr(void *ptr, size_t size);

    bool valid_ptr(void *ptr, size_t size);

    template<class T>
    struct allocator : std::allocator_traits<std::allocator<T> > {
        typedef T value_type;

        allocator() noexcept = default; //default ctor not required by C++ Standard Library
        // allocator(const allocator&) noexcept = default; //default ctor not required by C++ Standard Library

        // A converting copy constructor:
        // adding explicit causes compilation to fail
        template<class U>
        allocator(const allocator<U> &) noexcept {
        }

        template<class U>
        bool operator==(const allocator<U> &) const noexcept {
            return true;
        }

        template<class U>
        bool operator!=(const allocator<U> &) const noexcept {
            return false;
        }

        T *allocate(size_t n) const;

        void deallocate(T *p, size_t) const noexcept;
    };

    template<class T>
    T *allocator<T>::allocate(const size_t n) const {
        if (n == 0) {
            return nullptr;
        }
        if (n > static_cast<size_t>(-1) / sizeof(T)) {
            throw std::bad_array_new_length();
        }
        void *const pv = heap::allocate(n * sizeof(T));
        if (!pv) { throw std::bad_alloc(); }
        return static_cast<T *>(pv);
    }

    template<class T>
    void allocator<T>::deallocate(T *const p, size_t n) const noexcept {
        heap::free(p, n * sizeof(T));
    }

    template<typename T>
    struct buffer {
        T *ptr{nullptr};
        size_t count{0};

        buffer() noexcept = default;

        buffer(buffer &&other) noexcept {
            *this = std::move(other);
        }

        explicit buffer(size_t size) : ptr(nullptr), count(size) {
            if (size) {
                ptr = (T *) allocate(byte_size());
            }
        }

        explicit buffer(const T *data, size_t size) : ptr(nullptr), count(size) {
            if (size) {
                ptr = (T *) allocate(byte_size());
                std::memcpy(ptr, data, byte_size());
            }
        }

        buffer(const buffer &other) {
            *this = other;
        };

        buffer &operator=(buffer &&other) noexcept {
            /// TODO: should we release here if things are not empty
            release();
            ptr = other.ptr;
            count = other.count;
            other.clear();
            return *this;
        }

        buffer &operator=(const buffer &other) {
            if (this == &other) return *this;
            this->release();
            if (other.empty()) {
                return *this;
            }
            ptr = (T *) allocate(other.byte_size());
            count = other.size();
            emplace(other.size(), other.data());
            return *this;
        };

        ~buffer() {
            release();
        }

        [[nodiscard]] size_t byte_size() const {
            return count * sizeof(T);
        }

        [[nodiscard]] size_t size() const {
            return count;
        }

        void emplace(size_t start, size_t cnt, const T *src) {
            if (!src) return;
            if (start + cnt > size()) {
                abort();
            }
            ::memcpy(ptr + start, src, cnt * sizeof(T));
        }

        void emplace(size_t cnt, const T *src) {
            return emplace(0, cnt, src);
        }

        size_t read(size_t start, const buffer &src) {
            if (src.empty()) return 0;

            return read(src.data(), start, src.size());
        }

        size_t read(const T *src, size_t start, size_t cnt) {
            if (!src) return 0;
            if (start + cnt > size()) {
                abort();
            }
            memcpy(data() + start, src, cnt);
            return cnt;
        }
        ;

        int compare(const buffer &other) const {
            return partial_compare(other.count, other);
        }

        int partial_compare(size_t part, const buffer &other) const {
            if (this == &other) return 0;
            if (ptr == nullptr && other.ptr) return -1;
            if (ptr && other.ptr == nullptr) return 1;

            int r = memcmp(ptr, other.ptr, std::min<size_t>(count * sizeof(T), part * sizeof(T)));
            if (r == 0) {
                if (count < part) return -1;
                if (count > part) return 1;
                return 0;
            }
            return r;
        }

        struct safe_iterator {
            T *ptr{nullptr};
            size_t count{0};
            size_t offset{0};

            safe_iterator() noexcept = default;

            safe_iterator(T *ptr_, size_t count, size_t offset) : ptr(ptr_), count(count), offset(offset) {
            };

            operator T *() {
                return ptr + offset;
            }

            operator const T *() const {
                return ptr + offset;
            }

            T *operator ->() {
                if (!valid()) {
                    abort();
                }
                return ptr + offset;
            }

            const T *operator ->() const {
                if (!valid()) {
                    abort();
                }
                return ptr + offset;
            }

            [[nodiscard]] bool valid() const {
                if (!ptr || (offset >= count)) {
                    return false;
                };
                return heap::valid_ptr(ptr, count);
            }

            bool operator==(const safe_iterator &other) const {
                return ptr == other.ptr && offset == other.offset && count == other.count;
            }

            bool operator!=(const safe_iterator &other) const {
                return ptr != other.ptr || offset != other.offset || count != other.count;
            }

            safe_iterator &operator++() {
                ++offset;
                return *this;
            }

            safe_iterator &operator--() {
                --offset;
                return *this;
            }

            safe_iterator operator++(int) {
                safe_iterator tmp = *this;
                ++tmp.offset;
                return tmp;
            }

            safe_iterator operator--(int) {
                safe_iterator tmp = *this;
                --tmp.offset;
                return tmp;
            }

            safe_iterator &operator+=(size_t n) {
                offset += n;
                return *this;
            }

            safe_iterator &operator-=(size_t n) {
                offset -= n;
                return *this;
            }

            safe_iterator operator+(int n) const { return safe_iterator(ptr, count, offset + n); }
            safe_iterator operator-(int n) const { return safe_iterator(ptr, count, offset - n); }
        };

        T *data() {
            return ptr;
        }

        T *begin() {
            return ptr;
        }

        const T *begin() const {
            return ptr;
        }

        T *end() {
            return ptr + size();
        }

        const T *end() const {
            return ptr + size();
        }

        [[nodiscard]] const T *cbegin() const {
            return ptr;
        }

        [[nodiscard]] const T *cend() const {
            return ptr + size();
        }

        [[nodiscard]] const T *data() const {
            return ptr;
        }

        T *move() {
            if (count && !ptr) {
                abort();
            }
            T *tmp = ptr;

            clear();
            return tmp;
        }

        void clear() {
            if (count && !ptr) {
                abort();
            }

            ptr = nullptr;
            count = 0;
        }

        void release() {
            if (count && !ptr) {
                abort();
            }
            if (ptr)
                free(ptr, byte_size());
            clear();
        }

        [[nodiscard]] bool empty() const {
            return count == 0;
        }

        T &operator[](size_t idx) {
            if (idx >= count) {
                abort();
            }
            return ptr[idx];
        }

        const T &operator[](size_t idx) const {
            if (idx >= count) {
                abort();
            }
            return ptr[idx];
        }
    };

    /// a checked vector with automatic heap allocator
    template<typename T, int StaticSize = 8>
    struct small_vector {
    private:
        typedef std::vector<T, allocator<T> > vtype;
        typedef std::array<T, StaticSize> stype;
        stype scontent{};
        size_t ssize{0};
        vtype content{};

    public:
        typedef T value_type;
        typedef size_t size_type;
        typedef ptrdiff_t difference_type;
        typedef T *pointer;
        typedef const T *const_pointer;
        typedef T &reference;
        typedef const T &const_reference;
        typedef __gnu_cxx::__normal_iterator<pointer, small_vector> iterator;
        typedef __gnu_cxx::__normal_iterator<const_pointer, small_vector>
        const_iterator;
        typedef std::reverse_iterator<const_iterator>	const_reverse_iterator;
        typedef std::reverse_iterator<iterator>		reverse_iterator;
        small_vector() {
        };

        explicit small_vector(size_t r) {
            resize(r);
        }

        ~small_vector() {
            clear();
        }
        small_vector(const std::initializer_list<T>& data) {
            for (auto& d : data) {
                emplace_back(d);
            }
        }
        small_vector& operator=(const std::initializer_list<T>& data) {
            for (auto& d : data) {
                emplace_back(d);
            }
            return *this;
        }
        small_vector(const small_vector &other) noexcept {
            *this = other;
        }

        small_vector(small_vector &&other) noexcept {
            *this = std::move(other);
        }

        small_vector &operator=(const small_vector &other) noexcept {
            content = other.content;
            ssize = other.ssize;
            scontent = other.scontent;
            return *this;
        }

        small_vector &operator=(small_vector &&other) noexcept {
            content = std::move(other.content);
            ssize = other.ssize;
            scontent = std::move(other.scontent);
            return *this;
        }

        [[nodiscard]] bool empty() const {
            return ssize == 0;
        }

        [[nodiscard]] size_t capacity() const {
            if (ssize < scontent.size())
                return scontent.size();
            return content.capacity();
        };

        [[nodiscard]] size_t size() const {
            return ssize;
        }

        void resize(size_t n) {
            if (ssize < scontent.size()) {
                if (n >= scontent.size()) {
                    content.clear();
                    content.assign(scontent.begin(), scontent.end());
                    content.resize(n);
                }
            } else {
                if (n < scontent.size()) {
                    content.resize(n);
                    std::copy(content.begin(), content.end(), scontent.begin());
                    content = vtype();
                } else {
                    content.resize(n);
                }
            }
            ssize = n;
        }

        bool operator!=(const small_vector &other) const {
            if (ssize != other.size()) return true;
            for (size_t i = 0; i < size(); ++i) {
                if ((*this)[i] != other[i]) return true;
            }
            return false;
        }

        bool operator<(const small_vector &other) const {
            size_t ms = std::min(size(), other.size());
            for (size_t i = 0; i < ms; ++i) {
                if ((*this)[i] == other[i]) continue;
                if ((*this)[i] < other[i]) return true;
                return false;
            }
            return size() < other.size();
        }

        bool starts_with(const small_vector &other) const {
            size_t ms = std::min(size(), other.size());
            for (size_t i = 0; i < ms; ++i) {
                if ((*this)[i] != other[i]) return false;
            }
            return true;
        }

        void reserve(size_t n) {
            if (n >= scontent.size())
                content.reserve(n);
        }

        void clear() {
            ssize = 0;
            content = vtype();
        }

        void push_back(const T &x) {
            resize(ssize + 1);
            at(ssize - 1) = x;
        }

        template<typename... Args>
        void emplace_back(Args &&... arg) {
            resize(ssize + 1);
            at(ssize - 1) = T(std::forward<Args>(arg)...);
        }

        template<typename VType>
        void append(const VType &other) {
            if (other.empty()) return;
            if (ssize + other.size() > scontent.size()) {
                content.resize(ssize + other.size());
                std::copy(other.begin(), other.end(), content.begin() + ssize);
                ssize += other.size();
            } else {
                std::copy(other.begin(), other.end(), scontent.begin() + ssize);
                ssize += other.size();
            }
        }

        void append(const T* data, size_t size) {
            if (!size) return;
            if (ssize + size > scontent.size()) {
                content.resize(ssize + size);
                std::copy(data, data + size, content.begin() + ssize);
                ssize += size;
            } else {
                std::copy(data, data + size, scontent.begin() + ssize);
                ssize += size;
            }
        }

        void emplace_back() {
            emplace_back(T());
        }

        T &back() {
            return at(size() - 1);
        }

        const T &back() const {
            return at(size() - 1);
        }

        void pop_back() {
            if (!empty()) {
                resize(size() - 1);
            }
        }

        T &operator [](size_t ix) {
            return at(ix);
        }

        const T &operator [](size_t ix) const {
            return at(ix);
        }

        T &at(size_t ix) {
            if (ix >= size()) {
                throw_exception<std::out_of_range>("at()");
            }
            if (ssize < scontent.size()) {
                return scontent[ix];
            }
            return content[ix];
        }

        const T &at(size_t ix) const {
            if (ix >= size())
            {
                throw_exception<std::out_of_range> ("at()");
            }
            if (ssize < scontent.size()) {
                return scontent[ix];
            }
            return content[ix];
        }

        T *data() {
            return &at(0);
        }

        const T *data() const {
            return &at(0);
        }

        iterator raw_begin() noexcept {
            if (ssize < scontent.size()) {
                return iterator{ scontent.begin()};
            }
            return iterator{&content[0]};
        };

        const_iterator raw_begin() const noexcept {
            if (ssize < scontent.size()) {
                return const_iterator(&*scontent.cbegin());
            }
            return const_iterator(&*content.cbegin());
        };

        iterator begin() {
            if (empty()) return {};

            return raw_begin();
        };

        const_iterator begin() const {
            if (empty()) {
                throw_exception<std::out_of_range>("begin()");
            }
            return raw_begin();
        }

        const_iterator cbegin() {
            if (empty()) return {};

            return raw_begin();
        }

        reverse_iterator rbegin() {
            if (empty()) return {};

            return --end();
        }

        const_reverse_iterator rbegin() const {
            if (empty()) return {};

            return --end();
        }

        reverse_iterator rend() {
            if (empty()) return {};

            return --begin();
        }

        const_reverse_iterator rend() const {
            if (empty()) return {};
            return --begin();
        }

        iterator end() {
            if (empty()) return {};
            T* r = &at(0) + size();
            return iterator{r};
        }

        const_iterator end() const {
            if (empty()) {
                throw_exception<std::out_of_range>("end()");
            }
            return const_iterator(&at(0) + size());
        }

        const_iterator cend() const {
            if (empty()) return {};
            return &at(0) + size();
        }
    };

    struct string_hash{
        size_t operator()(const std::string& k) const {
            uint64_t hash = ankerl::unordered_dense::detail::wyhash::hash(k.data(), k.size());
            return hash;
        }
    };

    template<typename K, typename V >
    using map = ankerl::unordered_dense::map
    <     K
        , V
        , std::hash<K>
        , std::equal_to<K>
        , allocator<std::pair<K, V> >
    >;
    template<typename V >
    using string_map = ankerl::unordered_dense::map
    <     std::string
        , V
        , string_hash
        , std::equal_to<std::string>
        , allocator<std::pair<std::string, V> >
    >;

    template<typename K, typename H = std::hash<K>, typename E = std::equal_to<K>>
    using unordered_set = ankerl::unordered_dense::set
    <       K
        ,   H
        ,   E
        ,   allocator<K>
    >;
    template<typename K, typename V, typename H = std::hash<K>>
    using jmap = jg::dense_hash_map
    <K
    , V
    , H
    , std::equal_to<K>
    , allocator<K>
    >;

    using string_set = ankerl::unordered_dense::set
    <   std::string
        , string_hash
        , std::equal_to<std::string>
        , allocator<std::string>
    >;
    template<typename K>
    using std_vector = std::vector
    <K
        , allocator<K>
    >;
    template<typename K>
    using vector = std::vector
    <K
        ,   allocator<K>
    >;

}

extern void abort_with(const char *message) __THROW __attribute__ ((__noreturn__));
#endif //SASTAM_H
