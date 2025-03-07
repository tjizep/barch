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
#include <vector>

namespace heap {
    uint64_t get_physical_memory_bytes();
    double get_physical_memory_ratio();
    extern std::atomic<uint64_t> allocated;
    void* allocate(size_t size);

    template<typename T>
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

        // A converting copy constructor:
        template<class U> explicit allocator(const allocator<U>&) noexcept {}
        template<class U> bool operator==(const allocator<U>&) const noexcept
        {
            return true;
        }
        template<class U> bool operator!=(const allocator<U>&) const noexcept
        {
            return false;
        }
        T* allocate(const size_t n) const;
        void deallocate(T* const p, size_t) const noexcept;
    };

    template <class T>
    T* heap::allocator<T>::allocate(const size_t n) const
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

    template<class T>
    void heap::allocator<T>::deallocate(T * const p, size_t n) const noexcept
    {
        heap::free(p,n * sizeof(T));
    }

    template<typename T>
    struct buffer
    {
        T* ptr{nullptr};
        size_t count{0};
        buffer() noexcept = default;

        buffer(buffer&& other) noexcept
        {
            *this = std::move( other );
        }

        explicit buffer(size_t size) : ptr(nullptr), count(size)
        {
            if (size)
            {
                ptr = (T*)allocate(byte_size());
            }
        }

        buffer(const buffer& other) {
            *this = other;
        };

        buffer& operator=( buffer&& other) noexcept {
            /// TODO: should we release here if things are not empty
            release();
            ptr = other.ptr;
            count = other.count;
            other.clear();
            return *this;
        }
        buffer& operator=(const buffer& other)
        {
            if(this == &other) return *this;
            this->release();
            if(other.empty())
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
            if(!src) return;
            if(start + cnt > size())
            {
                abort();
            }
            ::memcpy(ptr + start, src, cnt * sizeof(T));
        }
        void emplace(size_t cnt, const T* src)
        {
            return emplace(0, cnt, src);
        }
        int compare(const buffer& other) const
        {
            return partial_compare(other.count,other);
        }
        int partial_compare(size_t part, const buffer& other) const
        {

            if (this == &other) return 0;
            if (ptr == nullptr && other.ptr) return -1;
            if (ptr && other.ptr == nullptr) return 1;

            int r = memcmp(ptr, other.ptr, std::min<size_t>(count * sizeof(T), part * sizeof(T)));
            if(r == 0)
            {
                if(count < part) return -1;
                if(count > part) return 1;
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
            safe_iterator(T* ptr_, size_t count, size_t offset) : ptr(ptr_),count(count),offset(offset) {};
            operator T*()
            {   return ptr + offset;
            }
            operator const T*() const
            {   return ptr + offset;
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
            safe_iterator& operator++() { ++offset; return *this; }
            safe_iterator& operator--() { --offset; return *this; }
            safe_iterator operator++(int) { safe_iterator tmp = *this; ++tmp.offset; return tmp; }
            safe_iterator operator--(int) { safe_iterator tmp = *this; --tmp.offset; return tmp; }
            safe_iterator& operator+=(size_t n) { offset += n; return *this; }
            safe_iterator& operator-=(size_t n) { offset -= n; return *this; }
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
            return ptr+size();
        }
        const T* end() const
        {
            return ptr+size();
        }

        [[nodiscard]] const T* cbegin() const
        {
            return ptr;
        }
        [[nodiscard]] const T* cend() const
        {
            return ptr+size();
        }
        [[nodiscard]] const T* data() const
        {
            return ptr;
        }

        T* move()
        {

            if(count && !ptr)
            {
                abort();
            }
            T* tmp = ptr;

            clear();
            return tmp;
        }
        void clear()
        {
            if(count && !ptr)
            {
                abort();
            }

            ptr = nullptr;
            count = 0;
        }
        void release()
        {
            if(count && !ptr)
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
        {   if(idx >= count)
            {
                abort();
            }
            return ptr[idx];
        }

        const T& operator[](size_t idx) const
        {
            if(idx >= count)
            {
                abort();
            }
            return ptr[idx];
        }
    };
    /// since the std::vector seems to not work in valkey
    template<typename  T>
    struct vector
    {
        heap::buffer<T> content{};
        size_t count{0};
        vector() = default;
        explicit vector(size_t r)
        {
            reserve(r);
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
            resize(other.size());
            std::copy(other.begin(), other.end(), begin());
            return *this;
        }
        vector& operator=(vector&& other) noexcept
        {
            count = other.count;
            content = std::move(other.content);
            other.count = 0;
            return *this;
        }
        [[nodiscard]] bool empty() const
        {
            return count == 0;
        }
        [[nodiscard]] size_t capacity() const
        {
            return content.size();
        };
        [[nodiscard]] size_t size() const
        {
            return count;
        }
        void resize(size_t n)
        {
            if(count > content.size())
            {
                abort();
            }
            if(capacity() < n)
            {
                reserve(n*2);
            }

            for(auto t = count; t < n; ++t)
            {
                new (&content[t]) T();
            }

            for(auto t = n; t < count; ++t)
            {
                content[t].~T();
            }

            count = n;
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

            vector other(end() - to);
            for(auto s = to; s < end(); ++s)
            {
                other.push_back(*s); // copy the remainder
            }
            resize(size() - (end() - from));
            for (auto &it: other)
            {
                push_back(std::move(it));
            }
        }
        void reserve(size_t n)
        {
            if (count > content.size())
            {
                abort();
            }
            if (capacity() < n)
            {
                heap::buffer<T> other(n);
                for(size_t t = 0; t < count; ++t)
                {
                    new (&other.ptr[t]) T();
                    other.ptr[t] = std::move(content[t]);
                    content[t].~T();
                }
                content = std::move(other);
            }
        }
        void clear()
        {
            for(size_t t = 0; t < count; ++t)
            {
                content[t].~T();
            }
            count = 0;
            content.release();
        }
        void push_back(const T& x)
        {
            auto b = count;
            resize(count + 1);
            content[b] = x;
        }
        template<typename... Args>
        void emplace_back(Args && ...arg)
        {
            auto b = count;
            resize(b + 1);
            content[b] = T(std::forward<Args>(arg)...);
        }
        void emplace_back(const T& x)
        {
            auto b = count;
            resize(b + 1);
            content[b] = x;
        }
        void emplace_back()
        {
            auto b = count;
            resize(b + 1);
        }
        void append(const T* start, const T* end)
        {

            for(auto it = start; it < end; ++it)
            {
                push_back(*it);
            }
        }
        void append(const vector& other)
        {
            reserve(size() + other.size());
            append(other.cbegin(), other.cend());

        }
        T& back()
        {
            return content[count-1];
        }
        const T& back() const
        {
            if(empty())
            {
                abort();
            }
            return content[count-1];
        }
        void pop_back()
        {
            if(count > 0)
            {
                resize(count - 1);
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
            if(ix >= count)
            {
                abort();
            }
            return content[ix];
        }

        const T& at(size_t ix) const
        {
            if(ix >= count)
            {
                abort();
            }
            return content[ix];
        }

        T* data()
        {
            return content.data();
        }
        const T* data() const
        {
            return content.data();
        }
        T* begin()
        {
            return content.begin();
        }
        const T* cbegin() const
        {
            return content.begin();
        }
        const T* begin() const
        {
            return content.begin();
        }
        T* end()
        {
            if(count > capacity())
            {
                abort();
            }
            return content.begin() + count;
        }
        const T* end() const
        {
            if(count > capacity())
            {
                abort();
            }
            return content.begin() + count;
        }
        const T* cend() const
        {
            if(count > capacity())
            {
                abort();
            }
            return content.begin() + count;
        }
    };
}
#endif //SASTAM_H
