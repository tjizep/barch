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
    extern std::atomic<uint64_t> allocated;
    void* allocate(size_t size);

    template<typename T>
    T* allocate(size_t count)
    {
        return static_cast<T*>(allocate(count * sizeof(T)));
    }
    void free(void* ptr, size_t size);
    template<typename T>
    void free(T* ptr)
    {
        free(ptr, sizeof(T));
    }
    void check_ptr(void* ptr, size_t size);

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

        explicit buffer(size_t size) : ptr(size ? allocate<T>(size) : nullptr), count(size)
        {

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
            ptr = allocate<T>(other.size());
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
        {   return ptr[idx];
        }

        const T& operator[](size_t idx) const
        {
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
        ~vector()
        {
            clear();
        }
        vector(const vector& other) noexcept
        {
            *this = other;
        }
        vector& operator=(const vector& other) noexcept
        {
            resize(other.size());
            std::copy(other.begin(), other.end(), begin());
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
                    //new (&other.ptr[t]) T();
                    other.ptr[t] = std::move(content[t]);
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
        T& back()
        {
            return content[count-1];
        }
        const T& back() const
        {
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
    };
}
#endif //SASTAM_H
