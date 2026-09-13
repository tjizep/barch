//
// Created by teejip on 1/31/26.
//

#ifndef BARCH_COUNTED_LOCKS_H
#define BARCH_COUNTED_LOCKS_H
#include "sastam.h"
#include <chrono>

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

typedef counted_unique_latch<barch::latch_t> unique_latch;
typedef counted_shared_latch<barch::latch_t> shared_latch;

// opportunistic write: skip if the latch is busy rather than stalling
// maintenance behind user traffic. operator bool is true when we own it.
template<typename Mutex>
struct counted_try_unique_latch {
    Mutex &m;
    bool owned = false;

    counted_try_unique_latch(const counted_try_unique_latch&) = delete;
    counted_try_unique_latch(counted_try_unique_latch&&) = delete;
    counted_try_unique_latch& operator=(const counted_try_unique_latch&) = delete;
    counted_try_unique_latch& operator=(counted_try_unique_latch&&) = delete;

    template<typename Rep, typename Period>
    counted_try_unique_latch(Mutex& mut, const std::chrono::duration<Rep, Period>& to) : m(mut) {
        owned = m.try_lock_for(to);
        if (owned)
            ++statistics::write_locks_active;
    }
    explicit operator bool() const noexcept { return owned; }
    ~counted_try_unique_latch() {
        if (owned) {
            m.unlock();
            --statistics::write_locks_active;
        }
    }
};
typedef counted_try_unique_latch<barch::latch_t> try_unique_latch;

/*
 * Read now, maybe write later. Takes the upgradable hold, which lets other
 * readers carry on and keeps writers out, so the value being looked at cannot
 * be replaced while it is being worked on. `upgrade()` then takes the write
 * side for the swap; without it the hold is dropped having written nothing.
 *
 * This is for the background compressor (TODO 300): zstd runs while readers
 * continue, and the write latch is taken only for the moment it takes to put
 * the shorter value back. `try` because it is maintenance work - if the latch
 * is busy with user traffic, come back next tick rather than queue behind it.
 *
 * operator bool is true when we own the upgradable hold.
 */
template<typename Mutex>
struct counted_try_upgradable_latch {
    Mutex &m;
    bool owned = false;
    bool upgraded = false;

    counted_try_upgradable_latch(const counted_try_upgradable_latch&) = delete;
    counted_try_upgradable_latch(counted_try_upgradable_latch&&) = delete;
    counted_try_upgradable_latch& operator=(const counted_try_upgradable_latch&) = delete;
    counted_try_upgradable_latch& operator=(counted_try_upgradable_latch&&) = delete;

    template<typename Rep, typename Period>
    counted_try_upgradable_latch(Mutex& mut, const std::chrono::duration<Rep, Period>& to) : m(mut) {
        owned = m.try_lock_upgradable_for(to);
        if (owned)
            ++statistics::read_locks_active;
    }
    explicit operator bool() const noexcept { return owned; }

    /** take the write side. false means someone else still has the data open. */
    template<typename Rep, typename Period>
    bool upgrade(const std::chrono::duration<Rep, Period>& to) {
        if (!owned || upgraded) return upgraded;
        upgraded = m.try_upgrade_to_write_for(to);
        if (upgraded) {
            --statistics::read_locks_active;
            ++statistics::write_locks_active;
        }
        return upgraded;
    }

    ~counted_try_upgradable_latch() {
        if (!owned) return;
        if (upgraded) {
            m.unlock();
            --statistics::write_locks_active;
        } else {
            m.unlock_upgrade_without_writing();
            --statistics::read_locks_active;
        }
    }
};
typedef counted_try_upgradable_latch<barch::latch_t> try_upgradable_latch;

#endif //BARCH_COUNTED_LOCKS_H