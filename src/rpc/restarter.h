//
// Created by test on 2/12/26.
//

#ifndef BARCH_RESTARTER_H
#define BARCH_RESTARTER_H

struct restarter {
    std::thread restart_thread;
    void asynch_restart(std::string interface, int port, bool ssl) {
        if (restart_thread.joinable()) {
            restart_thread.join();
        }
        restart_thread = std::thread([interface, port, ssl]() {

            try {
                barch::server::stop();
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                if (!interface.empty() || port > 100)
                    barch::server::start(interface,port, ssl);
            }catch (std::exception &e) {
                barch::std_err("could not restart server",e.what());
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