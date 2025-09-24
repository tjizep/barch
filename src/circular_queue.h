//
// Created by teejip on 9/22/25.
//

#ifndef BARCH_CIRCULAR_QUEUE_H
#define BARCH_CIRCULAR_QUEUE_H
#include <mutex>

#include "sastam.h"
template<typename T, int InitialCapacity = 1000>
class circular_queue {
public:
    // index pointers and data array
    mutable std::mutex mutex_{};
    size_t front{};
    size_t rear{};
    size_t enqueues = 0;
    size_t dequeues = 0;
    heap::vector<T> arr{InitialCapacity};
    explicit circular_queue(size_t init_cap) : arr(init_cap == 0 ? InitialCapacity :init_cap) {}
    circular_queue() = default;
    circular_queue(const circular_queue&) = default;
    circular_queue& operator=(const circular_queue&) = default;

    bool empty() const {
        return  (front == rear);
    }

    bool full() const {
        return ((rear + 1) % arr.size() == front);
    }
    void simple_enqueue(const T &item) {
        ++enqueues;
        arr[rear] = item;
        rear = (rear + 1) % arr.size();
    }
    void enqueue(const T& val) {
        std::unique_lock lock(mutex_);
        if (arr.empty()) {
            arr.resize(InitialCapacity);
        }
        if (full()) {
            heap::vector<T> old;
            old.swap(arr);
            arr.resize(old.size() * 2);
            size_t old_front = front;
            size_t old_rear = rear;
            enqueues = 0;
            dequeues = 0;
            front = 0;
            rear = 0;
            while (old_front != old_rear) {
                simple_enqueue(old[old_front]);
                old_front = (old_front + 1) % old.size();
            }
            if (full() || rear - front != enqueues) {
                art::std_err("resize failed");
            }
        }
       simple_enqueue(val);
    }
    std::mutex& mutex() {
        return mutex_;
    }
    bool try_dequeue(T& item) {
        std::unique_lock lock(mutex_);
        return try_dequeue_unlocked(item);
    }
    // function has to be called only when the user
    // has manually engaged the mutex
    bool try_dequeue_unlocked(T& item) {
        if (empty()) {
            return false;
        }
        ++dequeues;
        item = arr[front];
        front = (front + 1) % arr.size();
        return true;
    }
    bool clear_if_empty() {
        std::unique_lock lock(mutex_);
        if (!empty()) {
            return false;
        }
        front = 0;
        rear = 0;
        enqueues = 0;
        dequeues = 0;
        arr = std::move(heap::vector<T>());
        return true;
    }
    // function has to be called only when the user
    // has manually engaged the mutex
    template<typename FT>
    size_t try_dequeue_all(FT&& cbc, size_t max = 0) {
        if (empty()) {
            return 0;
        }
        size_t count = 0;
        std::unique_lock lock(mutex_);
        T item;
        while (try_dequeue_unlocked(item)) {
            cbc(item);
            ++count;
            if (count == max) {
                break;
            }
        }

        return count;
    }
    size_t try_dequeue_bulk(T* items, size_t max) {
        if (empty() || max == 0) {
            return 0;
        }
        size_t count = 0;
        std::unique_lock lock(mutex_);
        while (try_dequeue_unlocked(items[count])) {
            ++count;
            if (count == max) {
                break;
            }
        }

        return count;
    }

    bool peek(T& item) {
        std::unique_lock lock(mutex_);
        if (empty()) {
            return false;
        }
        item = arr[(front + 1) % arr.size()];
        return true;
    }

    size_t capacity() const {
        std::unique_lock lock(mutex_);
        return arr.size();
    }

};
#endif //BARCH_CIRCULAR_QUEUE_H