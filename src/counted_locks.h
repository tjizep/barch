//
// Created by teejip on 1/31/26.
//

#ifndef BARCH_COUNTED_LOCKS_H
#define BARCH_COUNTED_LOCKS_H
#include "sastam.h"

template<typename Mutex>
struct counted_unique_latch {
    Mutex &m;

    counted_unique_latch(const counted_unique_latch&) = delete;
    counted_unique_latch(counted_unique_latch&&) = delete;
    counted_unique_latch& operator=(const counted_unique_latch&) = delete;
    counted_unique_latch& operator=(counted_unique_latch&&) = delete;

    counted_unique_latch(Mutex& mut) : m(mut) {
        m.lock();
        ++statistics::write_locks_active;
    }
    ~counted_unique_latch() {
        m.unlock();
        --statistics::write_locks_active;
    }
};
template<typename Mutex>
struct counted_shared_latch {
    Mutex &m;
    counted_shared_latch(const counted_shared_latch&) = delete;
    counted_shared_latch(counted_shared_latch&&) = delete;
    counted_shared_latch& operator=(const counted_shared_latch&) = delete;
    counted_shared_latch& operator=(counted_shared_latch&&) = delete;

    counted_shared_latch(Mutex& mut) : m(mut) {
        m.lock_shared();
        ++statistics::read_locks_active;
    }
    ~counted_shared_latch() {
        m.unlock_shared();
        --statistics::read_locks_active;
    }
};

typedef counted_unique_latch<heap::shared_mutex> unique_latch;
typedef counted_shared_latch<heap::shared_mutex> shared_latch;

#endif //BARCH_COUNTED_LOCKS_H