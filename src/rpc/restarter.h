//
// Created by test on 2/12/26.
//

#ifndef BARCH_RESTARTER_H
#define BARCH_RESTARTER_H

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <thread>

/**
 * Moves the listener off the thread that asked. START and CONFIG SET run on an io
 * thread of the listener they move, and stopping a listener joins its io threads.
 *
 * One worker runs the requests in order. A request only queues, so the io thread
 * goes back to asio and the stop can join it. A request used to join the previous
 * restart thread instead. A second START that came in while the first was
 * restarting then waited on a thread that was waiting on it, and SIGTERM hung
 * behind both - TODO 856.
 *
 * In order, not just the last: each restart stops TCP and TLS and starts one
 * listener, so a unix START then a TLS START leaves both up, as one at a time did.
 */
struct restarter {
    /**
     * Set once the process is going away, so a restart already on its way does not
     * start a listener into a teardown. That is where `failed to start server
     * std::bad_alloc` on a refused start-up came from: the thread got as far as
     * `server::start` while everything under it was being destroyed. See TODO 241.
     */
    std::atomic<bool> shutting_down{false};

    void shutdown() {
        {
            std::lock_guard l(mut);
            shutting_down.store(true);
            pending.clear();
        }
        wake.notify_all();
        std::thread w;
        {
            std::lock_guard l(mut);
            w = std::move(worker);
        }
        if (w.joinable())
            w.join();
    }

    void asynch_restart(std::string interface, int port, bool ssl) {
        post({std::move(interface), port, ssl});
    }
    void asynch_stop() {
        post({{}, 0, false});
    }
    /** empty when it is listening, else why not - TODO 441 */
    std::string inline_restart(std::string interface, int port, bool ssl) {
        barch::server::stop();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (!interface.empty() || port > 100)
            return barch::server::start(interface,port, ssl);
        return {};
    }
    ~restarter() {
        shutdown();
    }

private:
    struct request {
        std::string interface;
        int port;
        bool ssl;
    };
    std::mutex mut;
    std::condition_variable wake;
    std::deque<request> pending;
    std::thread worker;

    void post(request r) {
        std::lock_guard l(mut);
        if (shutting_down.load())
            return;
        pending.push_back(std::move(r));
        // started on first use, so a Valkey module that never restarts has no thread
        if (!worker.joinable())
            worker = std::thread([this] { run(); });
        wake.notify_one();
    }

    void run() {
        for (;;) {
            request r;
            {
                std::unique_lock l(mut);
                wake.wait(l, [this] { return shutting_down.load() || !pending.empty(); });
                if (shutting_down.load())
                    return;
                r = std::move(pending.front());
                pending.pop_front();
            }
            try {
                barch::server::stop();
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                // asked again after the sleep: a shutdown can begin while this is
                // waiting, and starting then is the case this guards
                if (shutting_down.load())
                    return;
                if (!r.interface.empty() || r.port > 100)
                    barch::server::start(r.interface, r.port, r.ssl);
            }catch (std::exception &e) {
                barch::err({"could not restart server",e.what()});
            }
        }
    }
};

#endif //BARCH_RESTARTER_H
