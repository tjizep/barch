//
// Created by teejip on 8/3/25.
//

#ifndef THREAD_POOL_H
#define THREAD_POOL_H
#include <thread>
#include <atomic>
#include "sastam.h"
struct thread_pool {
    bool min_threads = false;
    heap::vector<std::thread> pool{};
    bool started = false;
    std::atomic<size_t> stopped{};
    std::atomic<size_t> running{};
    explicit thread_pool(size_t size) {
        pool.resize(size);
    }
    thread_pool() {
        if (min_threads) {
            pool.resize(1);
            return;
        }
        pool.resize(std::max<size_t>(4, std::thread::hardware_concurrency()));
    }
    explicit thread_pool(double factor) {
        if (min_threads) {
            pool.resize(1);
            return;
        }
        double cores = std::thread::hardware_concurrency();
        pool.resize(std::max<size_t>(4, (size_t)(cores*factor)));
    }
    explicit thread_pool(int threads) {
        if (min_threads) {
            pool.resize(1);
            return;
        }
        pool.resize(threads);
    }
    [[nodiscard]] size_t size() const {
        return pool.size();
    }
    template<typename TF>
    void start(TF&& tf) {
        stop();
        size_t tid = 0;
        stopped = 0;
        running = 0;
        for (auto& t : pool) {
            t = std::thread([this,tid,tf]() -> void {
                ++running;
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
    size_t active() const {
        return running - stopped;
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
            barch::std_err("not all threads have stopped ",(size_t)stopped,"of",pool.size());
        }else {
            barch::std_err("all threads have stopped ",(size_t)stopped,"of",pool.size());
        }
        stopped = 0;
        started = false;
    }
};
#endif //THREAD_POOL_H
