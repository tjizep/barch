//
// Created by test on 4/2/26.
//

#ifndef BARCH_RH_SHARED_LOCK_H
#define BARCH_RH_SHARED_LOCK_H
#include <atomic>
#include <array>
#include <bitset>
#include <shared_mutex>
#include <stdexcept>

#include "logger.h"
// a shared lock for read heavy work loads (like caches)


namespace rh_shared {
    enum {
        max_threads = 64,
        inf_lock_time = 1000000000
    };
    typedef std::array<uint8_t, rh_shared::max_threads> thread_set;
    // every thread that wants to use a lock must initilize first and once only
    void init_thread();
    void release_thread();

    extern thread_local int64_t thread_id;
    class shared_mutex {
    private:
        // align guard values on cache lines to avoid crosstalk - although from tests the effect seems small - will probably be more pronounced if on multiple core complexes
         struct guard { //alignas(64)
            std::atomic<int32_t> can;
        };
        std::array<guard, max_threads> guards{};  // this is a large variable
        std::shared_timed_mutex mutt;           // damn dirty dawg
    public:
        shared_mutex() = default;
        shared_mutex(const shared_mutex&) = delete;
        shared_mutex& operator=(const shared_mutex&) = delete;
        // the reader enters - the chat
        void lock_shared() ;
        bool try_lock_shared_for(decltype(std::chrono::milliseconds(10)) millis);
        // the writer enters the chat
        bool try_lock_for(decltype(std::chrono::milliseconds(10)) millis) ;
        bool try_lock();
        void lock() ;

        // writer leaves the chat
        void unlock() ;

        // reader leaves the chat
        void unlock_shared() ;
    };
}



#endif //BARCH_RH_SHARED_LOCK_H
