//
// Created by test on 4/2/26.
//

#include "shared_mutex.h"

#include <barrier>
#include <random>
#include <thread>
#include <unordered_set>
#include <vector>

#include "logger.h"
#include "sastam.h"
#include "time_conversion.h"
constexpr bool use_legacy = false;
constexpr bool use_logging = false;
struct rh_state {
    std::mutex mt;
    rh_shared::thread_set active{};
    std::unordered_set<int64_t> released;
    std::atomic<int64_t> initializer = -1;

    std::mutex &get_mutex() {
        return mt;
    }

    rh_shared::thread_set &get_active() {
        return active;
    }

    std::unordered_set<int64_t>& get_released() {
        return released;
    }

    std::atomic<int64_t>& get_initializer() {
        return initializer;
    }
};

static rh_state s;

thread_local int64_t rh_shared::thread_id = -1;

void rh_shared::init_thread() {
    if (use_legacy) {
        return;
    }
    if (rh_shared::thread_id > -1) {
        abort_with ("thread already initialized");
    }
    std::lock_guard ini(s.get_mutex() );
    if (!s.get_released().empty()) {
        rh_shared::thread_id = *s.get_released().begin();
        if (use_logging)
            barch::std_log("reusing thread id",thread_id);
        s.get_active()[thread_id] = true;
        s.get_released().erase(rh_shared::thread_id);
        return;
    }
    rh_shared::thread_id = ++s.get_initializer();
    s.get_active()[thread_id] = true;
}

void rh_shared::release_thread() {
    if (use_legacy) {
        return;
    }
    if (thread_id < 0) return;

    std::lock_guard ini(s.get_mutex() );
    if (use_logging)
        barch::std_log("releasing thread id",thread_id);
    s.get_active()[thread_id] = false;
    s.get_released().insert(rh_shared::thread_id);
    rh_shared::thread_id = -1;
}

void rh_shared::shared_mutex::lock_shared() {
    try_lock_shared_for(std::chrono::milliseconds(inf_lock_time) );
}

bool rh_shared::shared_mutex::try_lock_shared_for(decltype(std::chrono::milliseconds(10)) millis) {
    if (use_legacy) {
        return mutt.try_lock_shared_for(millis);
    }
    auto tid  = thread_id;

    if (tid  < 0) {
        init_thread();
        lock();
        unlock();
        tid = thread_id;
        if (use_logging)
            barch::std_log("finished init (shared)",thread_id);
    }
    if (tid  < 0) {
        abort_with("thread not initialized");
    }
    if (tid > max_threads-1) {
        abort_with("too many threads");
    }
    // todo: maybe check for multiple entries
    int32_t test = 0;
    if (use_logging)
        barch::std_log("lock shared",tid,thread_id);
    // try to set guard to 1
    while (!guards[tid].can.compare_exchange_strong(test, 1)) {

        //if (!mutt.try_lock_shared_for(millis)) {
        //    return false;
        //}// on read heavy workloads this part will be rarely executed
        //mutt.unlock_shared();
        test = 0;
    }
    return true;
}


// the writer enters the chat
bool rh_shared::shared_mutex::try_lock_for(decltype(std::chrono::milliseconds(10)) millis) {
    if (use_legacy) {
        return mutt.try_lock_for(millis);
    }
    // this will be the fun part
    auto tid  = thread_id;

    if (tid  < 0) {
        init_thread();
        lock();
        unlock();
        if (use_logging)
            barch::std_log("finished init (unique)",thread_id);
        tid  = thread_id;
    }
    if (tid  < 0) {
        abort_with("thread not initialized");
    }
    if (tid > max_threads-1) {
        abort_with("too many threads");
    }
    // now for the fun part
    // another writer will block this
    if (!mutt.try_lock_for(millis)) {
        return false;
    }
    if (use_logging)
        barch::std_log("lock unique",tid,thread_id);
    thread_set threads_entered{};
    const thread_set& active_threads = s.get_active(); // it's small
    uint32_t spins = 0;
    for (;;) { // loop until all threads have been excluded
        size_t threads_reading = 0;

        for (auto t = 0; t <= s.get_initializer().load(); ++t) {
            if (active_threads[t]) {
                if (!threads_entered[t]) {
                    int32_t test = 0; // can only acquire when its zero
                    if (!guards[t].can.compare_exchange_strong(test, 1)) {
                        ++threads_reading;
                    }else {
                        threads_entered[t] = true;
                    }
                }
            }
        }
        ++spins;
        if (spins>=1000) {
            if (use_logging)
                barch::std_log("spins",spins,thread_id);
            //std::this_thread::sleep_for(std::chrono::microseconds(250));
        }
        if (!threads_reading) break;
    }

    return true;

}
bool rh_shared::shared_mutex::try_lock() {
    return try_lock_for(std::chrono::milliseconds(0));
}
void rh_shared::shared_mutex::lock() {
    try_lock_for(std::chrono::milliseconds(inf_lock_time));
}

// writer leaves the chat
void rh_shared::shared_mutex::unlock() {
    if (use_legacy) {
        mutt.unlock();
        return;
    }
    const thread_set& active_threads = s.get_active(); // it's small

    for (auto t = 0; t < max_threads; ++t) {
        if (active_threads[t]) { // TODO: if active threads where added since lock was called then theres a problem
            if (use_logging)
                barch::std_log("unique reset",t);
            guards[t].can = 0;
        }
    }
    auto tid  = thread_id;
    if (use_logging)
        barch::std_log("unlock unique",tid,thread_id);
    if (guards[tid].can.load() != 0) {
        abort_with("writer not unlocked");
    }
    mutt.unlock();
}

// reader leaves the chat
void rh_shared::shared_mutex::unlock_shared() {
    if (use_legacy) {
        mutt.unlock_shared();
        return;
    }
    auto at  = thread_id;

    if (at  < 0) {
        abort_with("thread not initialized");
    }
    if (at > max_threads-1) {
        abort_with("too many threads");
    }
    if (guards[at].can.load() != 1) {
        abort_with("reader not locked");
    }
    // todo: compare_exchange to check for correct state of 1
    if (use_logging)
        barch::std_log("ulock shared",at,thread_id);
    guards[at].can = 0; // actually not there anymore

}
#if 0
static int test() {

    std::vector<std::thread> threads;
    enum {
        max_iters = 10000000,
        test_threads = 4,
        write_level = 2
    };
    std::barrier sync_point(test_threads);
    uint64_t write_var = 0;

    std::atomic<uint64_t> read_var = 0;
    std::atomic<uint64_t> count_var = 0;
    std::unordered_set<int64_t> test_resource;
    rh_shared::shared_mutex lock;
    //std::shared_mutex lock;
    auto start = now();
    for (int i = 0; i < test_threads; ++i) {
        threads.emplace_back([&,i]() -> void {
            rh_shared::init_thread();
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<uint64_t> dist(0, 9);
            sync_point.arrive_and_wait();
            barch::std_log("starting rh test thread",i);
            uint64_t local_read = 0,local_count = 0;
            auto tid = rh_shared::thread_id;
            for (int test = 0; test < max_iters; ++test) {
                uint64_t r = dist(gen);
                if (r < write_level) {
                    lock.lock();
                    ++write_var;
                    test_resource.insert(test);
                    lock.unlock();
                }else {
                    lock.lock_shared();
                    ++local_read;//
                    local_count += test_resource.count(test);
                    lock.unlock_shared();
                }
            }
            barch::std_log("ending rh thread",i,tid);
            read_var += local_read;
            count_var += local_count;
            rh_shared::release_thread();
        });
    }
    for (auto& t : threads) if (t.joinable()) t.join();
    barch::std_log("time:",millis(start),"ms");
    barch::std_log("read var", read_var.load());
    barch::std_log("count var", count_var.load());
    barch::std_log("write var",write_var);
    barch::std_log("test resource size",test_resource.size());
    barch::std_log("total var", write_var+read_var.load());


    return 0;
}

static int tested = test();
#endif