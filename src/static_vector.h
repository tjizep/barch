//
// Created by linuxlite on 1/30/25.
//

#ifndef STATIC_VECTOR_H
#define STATIC_VECTOR_H
#include <algorithm>
#include <array>
template <typename T, int SIZE>
class static_vector {
  private:
    std::array<T, SIZE> data;
    size_t nsize;
  public:
    typedef T value_type;
    static_vector() = default;

    size_t size() const {
      return nsize;
    }
    bool empty() const { return nsize == 0; }
    void clear() {
      for(auto i = begin(); i != end(); ++i) {
        *i = T();
      }
      nsize = 0;
    }
    void reserve(size_t n) {}
    void shrink_to_fit() {}
    void push_back(const T& t) {
      data[nsize++] = t;
    }
    void pop_back() { if(nsize) --nsize; }
    T& back() {return data[nsize - 1];}
    const T& back() const {return data[nsize - 1];}
    T& operator[](size_t i) {return data[i];}
    const T& operator[](size_t i) const {return data[i];}
    struct iterator {
      T* b;
      iterator(const T* begin) : b(begin) {}
      iterator() : b(nullptr) {}
      iterator(T* begin, T* end) : b(begin) {}
      iterator& operator++(int) {
        ++b;
        return *this;
      }
      iterator operator++() {
        iterator t = *this;
        ++t;
        return t;
      }
      iterator& operator--(int) {
        --b;
        return *this;
      }
      iterator operator--() {
        iterator t = *this;
        --t;
        return t;
      }
      bool operator==(const iterator& other) const {
        return b == other.b;
      }
      bool operator!=(const iterator& other) const {
        return b != other.b;
      }
      T& operator*() {
        return *b;
      }
      const T& operator*() const {
        return *b;
      }
      T* operator->() {
        return b;
      }

    };
    struct const_iterator {
      const T* b;
      const_iterator(const T* begin) : b(begin) {}
      const_iterator() : b(nullptr){}
      const iterator& operator++(int) {
        ++b;
        return *this;
      }
      const iterator operator++() {
        iterator t = *this;
        ++t;
        return t;
      }
      const iterator& operator--(int) {
        --b;
        return *this;
      }
      const iterator operator--() {
        iterator t = *this;
        --t;
        return t;
      }
      bool operator==(const iterator& other) const {
        return b == other.b;
      }
      bool operator!=(const iterator& other) const {
        return b != other.b;
      }
      const T& operator*() const {
        return *b;
      }
      const T* operator->() const {
        return b;
      }
    };
    struct reverse_iterator {
      T* b;
      reverse_iterator(const T* begin) : b(begin) {}
      reverse_iterator() : b(nullptr) {}
      reverse_iterator(T* begin) : b(begin) {}
      reverse_iterator& operator++(int) {
        --b;
        return *this;
      }
    };
    struct const_reverse_iterator {
      const T* b;
      const_reverse_iterator(const T* begin) : b(begin) {}
      const_reverse_iterator() : b(nullptr) {}
      const_reverse_iterator& operator++(int) {
        --b;
        return *this;
      }
      const_reverse_iterator operator++() {
        const_reverse_iterator t = *this;
        ++t;
        return t;
      }
    };
    reverse_iterator rbegin() const {
      return reverse_iterator(--end());
    }
    reverse_iterator rend() const {
      return reverse_iterator(--begin());
    }
    const_reverse_iterator crbegin() const {
      return const_reverse_iterator(end());
    }
    const_reverse_iterator crend() const {
      return const_reverse_iterator(--begin());
    }
    iterator begin() {return iterator(data.data());}
    iterator end() {return iterator(data.data() + nsize);}
    const_iterator begin() const {return const_iterator(data.data());}
    const_iterator end() const {return const_iterator(data.data() + nsize);}
};
#endif //STATIC_VECTOR_H
