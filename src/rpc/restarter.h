//
// Created by test on 2/12/26.
//

#ifndef BARCH_RESTARTER_H
#define BARCH_RESTARTER_H

#include <atomic>

struct restarter {
    std::thread restart_thread;
    /**
     * Set once the process is going away, so a restart already on its way does not
     * start a listener into a teardown. That is where `failed to start server
     * std::bad_alloc` on a refused start-up came from: the thread got as far as
     * `server::start` while everything under it was being destroyed. See TODO 241.
     */
    std::atomic<bool> shutting_down{false};

    void shutdown() {
        shutting_down.store(true);
        if (restart_thread.joinable())
            restart_thread.join();
    }

    void asynch_restart(std::string interface, int port, bool ssl) {
        if (shutting_down.load())
            return;
        if (restart_thread.joinable()) {
            restart_thread.join();
        }
        restart_thread = std::thread([this, interface, port, ssl]() {

            try {
                barch::server::stop();
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                // asked again after the sleep: a shutdown can begin while this is
                // waiting, and starting then is the case this guards
                if (shutting_down.load())
                    return;
                if (!interface.empty() || port > 100)
                    barch::server::start(interface,port, ssl);
            }catch (std::exception &e) {
                barch::err({"could not restart server",e.what()});
            }
        });
    }
    void asynch_stop() {
        if (restart_thread.joinable()) {
            restart_thread.join();
        }
        restart_thread = std::thread( []{
            try {
                barch::server::stop();
            }catch (std::exception &e) {
                barch::err({"could not restart server",e.what()});
            }
        });
    }
    void inline_restart(std::string interface, int port, bool ssl) {
        barch::server::stop();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (!interface.empty() || port > 100)
            barch::server::start(interface,port, ssl);
    }
    ~restarter() {
        if (restart_thread.joinable()) {
            restart_thread.join();
        }
    }
};

#endif //BARCH_RESTARTER_H