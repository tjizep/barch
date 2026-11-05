#include "pool.h"
#include "configuration.h"
#include "lzr_log.h"
#include "statistics.h"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace barch {
namespace foreign {

namespace {
struct pool {
    std::mutex mu;
    std::condition_variable cv;
    std::queue<std::function<void()>> jobs;
    std::vector<std::thread> workers;
    std::once_flag started;
};

// process-lifetime: workers wait on cv, so these must not be destroyed
pool& state() {
    static pool* p = new pool;
    return *p;
}

pool& overtime_state() {
    static pool* p = new pool;
    return *p;
}

void worker(pool* s) {
    for (;;) {
        std::function<void()> job;
        {
            std::unique_lock lk(s->mu);
            s->cv.wait(lk, [&] { return !s->jobs.empty(); });
            job = std::move(s->jobs.front());
            s->jobs.pop();
        }
        if (job) {
            try {
                job();
            } catch (const std::exception& e) {
                barch::err({"foreign worker", e.what()});
            }
        }
    }
}

void start_pool(pool& s, uint64_t threads) {
    for (uint64_t i = 0; i < threads; ++i)
        s.workers.emplace_back(worker, &s);
    for (auto& t : s.workers)
        t.detach();
}

void put(pool& s, uint64_t threads, std::function<void()> job) {
    std::call_once(s.started, [&] { start_pool(s, threads); });
    {
        std::lock_guard lk(s.mu);
        s.jobs.push(std::move(job));
    }
    s.cv.notify_one();
}

std::atomic<uint64_t> in_overtime{0};
}

void enqueue(std::function<void()> job) {
    put(state(), 4, std::move(job));
}

// the width is read once, when the first call goes into overtime
void enqueue_overtime(std::function<void()> job) {
    put(overtime_state(), barch::get_function_overtime_threads(), std::move(job));
}

overtime_slot::~overtime_slot() {
    --in_overtime;
    --statistics::function_overtime_running;
}

overtime_ptr enter_overtime() {
    const uint64_t most = barch::get_function_overtime_max();
    if (!most)
        return nullptr;
    uint64_t now = in_overtime.load();
    do {
        if (now >= most) {
            ++statistics::function_overtime_full;
            return nullptr;
        }
    } while (!in_overtime.compare_exchange_weak(now, now + 1));
    ++statistics::function_overtimes;
    ++statistics::function_overtime_running;
    return std::make_shared<overtime_slot>();
}

}
}
