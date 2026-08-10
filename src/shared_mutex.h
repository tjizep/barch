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

#include "lzr_log.h"

/**
 * A shared lock meant for read heavy work loads, and not used by anything.
 *
 * The shards latch on `std::shared_timed_mutex`; this was written to replace that and the
 * replacement never happened, so the whole of `rh_shared` is reachable only from its own
 * translation unit. Left compiled in it was not free - it carried a registry of live
 * threads in a file scope static that outlived nothing in particular, which made it a
 * plausible suspect for a stall it turned out to have no part in (TODO 41), and it costs
 * a `std::array<guard, 64>` of thought every time someone reads the header looking for
 * how locking actually works here.
 *
 * So it is behind `_EXPERIMENTAL_` rather than deleted. Define it to build the thing and
 * work on it; leave it undefined and none of this exists.
 */
#ifdef _EXPERIMENTAL_

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

#endif //_EXPERIMENTAL_

#endif //BARCH_RH_SHARED_LOCK_H
