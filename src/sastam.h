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
#include <stdexcept>
#include <vector>

namespace heap
{
    uint64_t get_physical_memory_bytes();
    double get_physical_memory_ratio();
    extern std::atomic<uint64_t> allocated;
    void* allocate(size_t size);

    template <typename T>
    T* allocate(size_t count)
    {
        return static_cast<T*>(allocate(count * sizeof(T)));
    }

    void free(void* ptr, size_t size);
    void free(void* ptr);
    void check_ptr(void* ptr, size_t size);
    bool valid_ptr(void* ptr, size_t size);

    template <class T>
    struct allocator : std::allocator_traits<std::allocator<T>>
    {
        typedef T value_type;
        allocator() noexcept = default; //default ctor not required by C++ Standard Library
        // allocator(const allocator&) noexcept = default; //default ctor not required by C++ Standard Library

        // A converting copy constructor:
        // adding explicit causes compilation to fail
        template <class U>
        allocator(const allocator<U>&) noexcept
        {
        }

        template <class U>
        bool operator==(const allocator<U>&) const noexcept
        {
            return true;
        }

        template <class U>
        bool operator!=(const allocator<U>&) const noexcept
        {
            return false;
        }

        T* allocate(size_t n) const;
        void deallocate(T* p, size_t) const noexcept;
    };

    template <class T>
    T* allocator<T>::allocate(const size_t n) const
    {
        if (n == 0)
        {
            return nullptr;
        }
        if (n > static_cast<size_t>(-1) / sizeof(T))
        {
            throw std::bad_array_new_length();
        }
        void* const pv = heap::allocate(n * sizeof(T));
        if (!pv) { throw std::bad_alloc(); }
        return static_cast<T*>(pv);
    }

    template <class T>
    void allocator<T>::deallocate(T* const p, size_t n) const noexcept
    {
        heap::free(p, n * sizeof(T));
    }

    template <typename T>
    struct buffer
    {
        T* ptr{nullptr};
        size_t count{0};
        buffer() noexcept = default;

        buffer(buffer&& other) noexcept
        {
            *this = std::move(other);
        }

        explicit buffer(size_t size) : ptr(nullptr), count(size)
        {
            if (size)
            {
                ptr = (T*)allocate(byte_size());
            }
        }

        buffer(const buffer& other)
        {
            *this = other;
        };

        buffer& operator=(buffer&& other) noexcept
        {
            /// TODO: should we release here if things are not empty
            release();
            ptr = other.ptr;
            count = other.count;
            other.clear();
            return *this;
        }

        buffer& operator=(const buffer& other)
        {
            if (this == &other) return *this;
            this->release();
            if (other.empty())
            {
                return *this;
            }
            ptr = (T*)allocate(other.byte_size());
            count = other.size();
            emplace(other.size(), other.data());
            return *this;
        };

        ~buffer()
        {
            release();
        }

        [[nodiscard]] size_t byte_size() const
        {
            return count * sizeof(T);
        }

        [[nodiscard]] size_t size() const
        {
            return count;
        }

        void emplace(size_t start, size_t cnt, const T* src)
        {
            if (!src) return;
            if (start + cnt > size())
            {
                abort();
            }
            ::memcpy(ptr + start, src, cnt * sizeof(T));
        }

        void emplace(size_t cnt, const T* src)
        {
            return emplace(0, cnt, src);
        }
        size_t read(size_t start, const buffer& src)
        {
            if (src.empty()) return 0;

            return read(src.data(), start, src.size());
        }
        size_t read(const T* src, size_t start, size_t cnt)
        {
            if (!src) return 0;
            if (start + cnt > size())
            {
                abort();
            }
            memcpy(data() + start, src, cnt);
            return cnt;
        }
        ;

        int compare(const buffer& other) const
        {
            return partial_compare(other.count, other);
        }

        int partial_compare(size_t part, const buffer& other) const
        {
            if (this == &other) return 0;
            if (ptr == nullptr && other.ptr) return -1;
            if (ptr && other.ptr == nullptr) return 1;

            int r = memcmp(ptr, other.ptr, std::min<size_t>(count * sizeof(T), part * sizeof(T)));
            if (r == 0)
            {
                if (count < part) return -1;
                if (count > part) return 1;
                return 0;
            }
            return r;
        }

        struct safe_iterator
        {
            T* ptr{nullptr};
            size_t count{0};
            size_t offset{0};
            safe_iterator() noexcept = default;

            safe_iterator(T* ptr_, size_t count, size_t offset) : ptr(ptr_), count(count), offset(offset)
            {
            };

            operator T*()
            {
                return ptr + offset;
            }

            operator const T*() const
            {
                return ptr + offset;
            }

            T* operator ->()
            {
                if (!valid())
                {
                    abort();
                }
                return ptr + offset;
            }

            const T* operator ->() const
            {
                if (!valid())
                {
                    abort();
                }
                return ptr + offset;
            }

            [[nodiscard]] bool valid() const
            {
                if (!ptr || (offset >= count))
                {
                    return false;
                };
                return heap::valid_ptr(ptr, count);
            }

            bool operator==(const safe_iterator& other) const
            {
                return ptr == other.ptr && offset == other.offset && count == other.count;
            }

            bool operator!=(const safe_iterator& other) const
            {
                return ptr != other.ptr || offset != other.offset || count != other.count;
            }

            safe_iterator& operator++()
            {
                ++offset;
                return *this;
            }

            safe_iterator& operator--()
            {
                --offset;
                return *this;
            }

            safe_iterator operator++(int)
            {
                safe_iterator tmp = *this;
                ++tmp.offset;
                return tmp;
            }

            safe_iterator operator--(int)
            {
                safe_iterator tmp = *this;
                --tmp.offset;
                return tmp;
            }

            safe_iterator& operator+=(size_t n)
            {
                offset += n;
                return *this;
            }

            safe_iterator& operator-=(size_t n)
            {
                offset -= n;
                return *this;
            }

            safe_iterator operator+(int n) const { return safe_iterator(ptr, count, offset + n); }
            safe_iterator operator-(int n) const { return safe_iterator(ptr, count, offset - n); }
        };

        T* data()
        {
            return ptr;
        }

        T* begin()
        {
            return ptr;
        }

        const T* begin() const
        {
            return ptr;
        }

        T* end()
        {
            return ptr + size();
        }

        const T* end() const
        {
            return ptr + size();
        }

        [[nodiscard]] const T* cbegin() const
        {
            return ptr;
        }

        [[nodiscard]] const T* cend() const
        {
            return ptr + size();
        }

        [[nodiscard]] const T* data() const
        {
            return ptr;
        }

        T* move()
        {
            if (count && !ptr)
            {
                abort();
            }
            T* tmp = ptr;

            clear();
            return tmp;
        }

        void clear()
        {
            if (count && !ptr)
            {
                abort();
            }

            ptr = nullptr;
            count = 0;
        }

        void release()
        {
            if (count && !ptr)
            {
                abort();
            }
            if (ptr)
                free(ptr, byte_size());
            clear();
        }

        [[nodiscard]] bool empty() const
        {
            return count == 0;
        }

        T& operator[](size_t idx)
        {
            if (idx >= count)
            {
                abort();
            }
            return ptr[idx];
        }

        const T& operator[](size_t idx) const
        {
            if (idx >= count)
            {
                abort();
            }
            return ptr[idx];
        }
    };

    /// since the std::vector seems to not work in valkey
    template <typename T>
    struct vector
    {
        std::vector<T, allocator<T>> content{};
        vector() = default;

        explicit vector(size_t r)
        {
            resize(r);
        }

        ~vector()
        {
            clear();
        }

        vector(const vector& other) noexcept
        {
            *this = other;
        }

        vector(vector&& other) noexcept
        {
            *this = std::move(other);
        }

        vector& operator=(const vector& other) noexcept
        {
            content = other.content;
            return *this;
        }

        vector& operator=(vector&& other) noexcept
        {
            content = std::move(other.content);
            return *this;
        }

        [[nodiscard]] bool empty() const
        {
            return content.empty();
        }

        [[nodiscard]] size_t capacity() const
        {
            return content.capacity();
        };

        [[nodiscard]] size_t size() const
        {
            return content.size();
        }

        void resize(size_t n)
        {
            content.resize(n);
        }

        bool included(const T* it) const
        {
            return it >= begin() && it <= end();
        }

        size_t index(const T* it) const
        {
            return it - begin();
        }

        void erase(ptrdiff_t from, ptrdiff_t to)
        {
            erase(begin() + from, begin() + to);
        }

        void erase(const T* from, const T* to)
        {
            if (!included(from) || !included(to) || from > to) return; // TODO: or abort() ?
            content.erase(from, to);
        }

        void reserve(size_t n)
        {
            content.reserve(n);
        }

        void clear()
        {
            content.clear();
        }

        void push_back(const T& x)
        {
            content.emplace_back(x);
        }

        template <typename... Args>
        void emplace_back(Args&&... arg)
        {
            content.emplace_back(std::forward<Args>(arg)...);
        }

        void emplace_back(const T& x)
        {
            content.emplace_back(x);
        }

        void emplace_back()
        {
            content.emplace_back();
        }
        void emplace_back(const T* data, size_t n)
        {
            for (size_t i = 0; i < n; ++i)
                content.emplace_back(data+i);
        }

        void append(const T* start, const T* end)
        {
            content.insert(content.end(), start, end);
        }

        void append(const vector& other)
        {
            append(other.cbegin(), other.cend());
        }

        T& back()
        {
            if (content.size() == 0)
            {
                throw std::out_of_range("back()");
            }
            return content.back();
        }

        const T& back() const
        {
            if (empty())
            {
                throw std::out_of_range("back()");
            }
            return content.back();
        }

        void pop_back()
        {
            if (!empty())
            {
                content.pop_back();
            }
        }

        T& operator [](size_t ix)
        {
            return at(ix);
        }

        const T& operator [](size_t ix) const
        {
            return at(ix);
        }

        T& at(size_t ix)
        {
            if (ix >= size())
            {
                throw std::out_of_range("at()");
            }
            return content[ix];
        }

        const T& at(size_t ix) const
        {
            if (ix >= size())
            {
                throw std::out_of_range("at()");
            }
            return content[ix];
        }

        T* data()
        {
            if (empty())
            {
                throw std::out_of_range("data()");
            }
            return content.data();
        }

        const T* data() const
        {
            if (empty())
            {
                throw std::out_of_range("data()");
            }
            return content.data();
        }

        T* begin()
        {
            if (empty())
            {
                throw std::out_of_range("begin()");
            }
            return content.begin();
        }

        const T* cbegin() const
        {
            if (empty())
            {
                throw std::out_of_range("cbegin()");
            }
            return content.begin();
        }

        const T* begin() const
        {
            if (empty())
            {
                throw std::out_of_range("begin()");
            }
            return &*(content.begin());
        }

        T* end()
        {
            return content.end();
        }

        const T* end() const
        {
            return begin() + size();
        }

        const T* cend() const
        {
            return begin() + size();
        }
    };
}
#endif //SASTAM_H
