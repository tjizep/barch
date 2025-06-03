//
// Created by teejip on 5/22/25.
//

#include "server.h"
#include <sys/unistd.h>
#include "logger.h"


#include <utility>
#include "module.h"
#include "statistics.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Weffc++"

#include <asio/io_context.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/signal_set.hpp>
#include <asio/post.hpp>
#include <asio/dispatch.hpp>
#include <asio/write.hpp>

#include <cstdio>

#include "asio/io_service.hpp"
#pragma GCC diagnostic pop

enum {
    cmd_ping = 1,
    cmd_stream = 2,
};
enum {
    opt_max_workers = 8,
    opt_read_timout = 60000

};
using asio::ip::tcp;



namespace barch {
    template<typename T>
    void push_size_t(heap::vector<uint8_t>& buffer,T s) {
        uint8_t tb[sizeof(T)];
        T size = htonl(s);
        memcpy(tb, &size, sizeof(T));
        buffer.insert(buffer.end(), tb, tb + sizeof(T));
    }
    template<typename TBF>
    void timeout(TBF&& check, int max_to = 1000) {
        int to = max_to;
        while (check() && to > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            --to;
        }
    }
    void push_value(heap::vector<uint8_t>& buffer, art::value_type v) {
        push_size_t<uint32_t>(buffer, v.size);
        buffer.insert(buffer.end(), v.bytes, v.bytes + v.size);
    }
    host_id get_host_id() {
        return {"localhost", getpid() % 10000000000000000000ULL};
    }

    struct server_context {
        std::thread server_thread{};
        asio::io_service io{};
        tcp::acceptor acc;
        std::string interface;
        uint_least16_t port;

        void stop() {
            io.stop();
            if (server_thread.joinable())
                server_thread.join();
            server_thread = {};
            port = 0;
        }
        void start_accept() {
            try {


                acc.async_accept([this](asio::error_code error, tcp::socket endpoint) {
                    if (error) {
                        art::std_err("accept error",error.message(),error.value());
                        return; // this happens if there are not threads
                    }
                    process_data(endpoint);
                    start_accept();
                });

            }catch (std::exception& e) {
                art::std_err("failed to start/run replication server", interface, port, e.what());
            }
        }
        void process_data( tcp::socket& endpoint) {
            int cmd = 0;
            stream_write_ctr = 0;
            stream_read_ctr = 0;
            auto stream = tcp::iostream{};
            stream.socket() = std::move(endpoint);
            art::std_log("process data");
            readp(stream,cmd);
            switch (cmd) {
                case cmd_ping:
                    try {
                        writep(stream,rpc_server_version);
                    }catch (std::exception& e) {
                        art::std_err("error",e.what());
                    }

                    break;
                case cmd_stream:
                    try {
                        uint32_t shard = 0;
                        readp(stream,shard);
                        if (shard < art::get_shard_count().size()) {
                            get_art(shard)->send(stream);
                            stream.flush();
                        }else {
                            art::std_err("invalid shard", shard);
                        }

                    }catch (std::exception& e) {
                        art::std_err("failed to stream shard", e.what());
                    }
                    break;
                default:
                    art::std_err("unknown command", cmd);
            }
        }
        server_context(std::string interface, uint_least16_t port)
        :   acc(io, tcp::endpoint(tcp::v4(), port))
        ,   interface(interface)
        ,   port(port){
            start_accept();
            server_thread = std::thread([this]() {
                art::std_log("server started on", this->interface,this->port);
                io.run();
                art::std_log("server stopped");
            });
        }
    };
    std::shared_ptr<server_context>  srv = nullptr;
    void server::start(std::string interface, uint_least16_t port) {
        if (srv) srv->stop();
        srv = std::make_shared<server_context>(interface, port);
    }

    void server::stop() {
        if (srv) srv->stop();
    }
    struct module_stopper {
        module_stopper() {

        }
        ~module_stopper() {
            server::stop();
        }
    };
    static module_stopper stopper;
    namespace repl {


        client::~client() {
            connected = false;
            if (tpoll.joinable())
                tpoll.join();
        };

        void client::connect(std::string host, int port, size_t shard, bool add_as_sink) {
            this->host = std::move(host);
            this->port = port;
            this->shard = shard;

            connected = true;
            tpoll = std::thread([this,add_as_sink]() {
                heap::vector<uint8_t> to_send;
                to_send.reserve(rpc_max_buffer);
                try {
                    asio::io_service io;
                    asio::ip::tcp::endpoint remote(asio::ip::address_v4::from_string(this->host),this->port);
                    auto conn = (tcp::socket{io});
                    while (connected) {
                        if (!conn.is_open()) {
                            connected = false;
                        }

                        {
                            auto t = get_art(this->shard);
                            std::lock_guard lock(t->latch);
                            to_send.swap(this->buffer);
                        }

                        if (!to_send.empty()) {
                            //to_send.clear();
                        }

                        std::this_thread::sleep_for(std::chrono::milliseconds(art::get_maintenance_poll_delay()));

                    }
                }catch (std::exception& e) {
                    this->connected = false;
                    art::std_err("failed to connect to remote server", this->host, this->port);
                }
            });
        }


        bool client::insert(art::value_type key, art::value_type value) {

            if (!connected) {
                ++statistics::repl::instructions_failed;
                return false;
            }

            if (buffer.size() > rpc_max_buffer) {
                ++statistics::repl::instructions_failed;
                return false;
            }

            buffer.push_back('i');
            push_value(buffer, key);
            push_value(buffer, value);
            return true;
        }

        bool client::remove(art::value_type key) {
            if (!connected) {
                ++statistics::repl::instructions_failed;
                return false;
            }
            if (buffer.size() > rpc_max_buffer) {
                ++statistics::repl::instructions_failed;
                return false;
            }
            buffer.push_back('r');
            push_value(buffer, key);
            return true;
        }
        [[nodiscard]] bool client::begin_transaction() const {
            try {
            }catch (std::exception& e) {
                art::std_err("failed to begin transaction", e.what());
                return false;
            }
            return true;
        }
        bool client::load(size_t shard) {
            try {
                if (!ping()) {
                    return false;
                }
                art::std_log("load shard",shard);
                tcp::iostream stream(host, std::to_string(this->port));
                if (!stream) {
                    art::std_err("failed to connect to remote server", host, this->port);
                    return false;
                }
                int cmd = cmd_stream;
                uint32_t s = shard;
                stream_write_ctr = 0;
                stream_read_ctr = 0;
                writep(stream,cmd);
                writep(stream, s);
                if (!get_art(shard)->retrieve(stream)) {
                    art::std_err("failed to retrieve shard", shard);
                    return false;
                }
                stream.close();
                return true;
            }catch (std::exception& e) {
                art::std_err("failed to load shard", shard, e.what());
                return false;
            }
        }
        [[nodiscard]] bool client::commit_transaction() const {
            try {
            } catch (std::exception& e) {

                art::std_err("failed to commit transaction", e.what());
                return false;
            }
            return true;
        }
        bool client::ping() const {
            try {
                tcp::iostream stream(host, std::to_string(this->port ));
                if (!stream) {
                    art::std_err("failed to connect to remote server", host, this->port);
                    return false;
                }
                int cmd = cmd_ping;
                writep(stream,cmd);
                int ping_result = 0;
                readp(stream, ping_result);
                if (ping_result != rpc_server_version) {
                    art::std_err("failed to ping remote server", host, port, "ping returned",ping_result);
                    return false;
                }
                stream.close();
            }catch (std::exception &e) {
                art::std_err("failed to ping remote server", host, port, e.what());
                return false;
            }
            return true;
        }
    }
}