//
// Created by teejip on 9/22/25.
//

#ifndef BARCH_CIRCULAR_QUEUE_H
#define BARCH_CIRCULAR_QUEUE_H
#include <mutex>

#include "sastam.h"
template<typename T, int InitialCapacity = 16000>
class circular_queue {
public:
    // index pointers and data array
    mutable std::mutex mutex_{};
    size_t front{};
    size_t rear{};
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

    void enqueue(const T& val) {
        std::unique_lock lock(mutex_);
        if (full()) {
            arr.resize(arr.size() * 2);
        }
        arr[rear] = val;
        rear = (rear + 1) % arr.size();

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
        item = arr[front];
        front = (front + 1) % arr.size();
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