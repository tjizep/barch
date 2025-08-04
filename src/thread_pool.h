//
// Created by teejip on 8/3/25.
//

#ifndef THREAD_POOL_H
#define THREAD_POOL_H
#include <thread>
#include <atomic>
#include "sastam.h"
struct thread_pool {
    heap::vector<std::thread> pool{};
    bool started = false;
    std::atomic<size_t> stopped{};
    thread_pool(size_t size) {
        pool.resize(size);
    }
    thread_pool() {
        pool.resize(std::max<size_t>(4, std::thread::hardware_concurrency()));
    }
    size_t size() const {
        return pool.size();
    }
    template<typename TF>
    void start(TF&& tf) {
        stop();
        size_t tid = 0;
        stopped = 0;
        for (auto& t : pool) {
            t = std::thread([this,tid,tf]() -> void {
                tf(tid);
                ++stopped;
            });
            ++tid;
        }
        started = true;
    }
    ~thread_pool() {
        stop();
    }
    void stop() {
        if (!started) {
            return;
        }
        for (auto& t : pool) {
            if (t.joinable())
                t.join();
            t = {};
        }
        if (stopped < pool.size()) {
            art::std_err("not all threads have stopped ",(size_t)stopped,"of",pool.size());
        }else {
            art::std_err("all threads have stopped ",(size_t)stopped,"of",pool.size());
        }
        stopped = 0;
        started = false;
    }
};
#endif //THREAD_POOL_H
