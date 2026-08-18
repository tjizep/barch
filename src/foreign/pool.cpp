#include "pool.h"
#include "lzr_log.h"

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
};

// process-lifetime: workers wait on cv, so this must not be destroyed
pool& state() {
    static pool* p = new pool;
    return *p;
}

void worker() {
    auto& s = state();
    for (;;) {
        std::function<void()> job;
        {
            std::unique_lock lk(s.mu);
            s.cv.wait(lk, [&] { return !s.jobs.empty(); });
            job = std::move(s.jobs.front());
            s.jobs.pop();
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

void start_pool() {
    auto& s = state();
    for (int i = 0; i < 4; ++i)
        s.workers.emplace_back(worker);
    for (auto& t : s.workers)
        t.detach();
}

std::once_flag started;
}

void enqueue(std::function<void()> job) {
    std::call_once(started, start_pool);
    auto& s = state();
    {
        std::lock_guard lk(s.mu);
        s.jobs.push(std::move(job));
    }
    s.cv.notify_one();
}

}
}
