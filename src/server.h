//
// Created by teejip on 5/22/25.
//

#ifndef SERVER_H
#define SERVER_H
#include <cstdint>
#include "value_type.h"
#include <thread>
#include <utility>

#include "key_options.h"

enum {
    rpc_client_context_size = 128,
    rpc_server_version = 21,
    rpc_server_version_min = 21,
    rpc_server_version_max = 21,
    rpc_client_max_wait_default_ms = 30000
};
namespace barch {

    typedef std::pair<std::string, size_t> host_id;
    host_id get_host_id();
    namespace server {
        void start(const std::string& interface, uint_least16_t port);
        void stop();
    };

    namespace repl {
        struct repl_dest {
            std::string host {};
            int port {};
            size_t shard {};
            repl_dest(std::string host, int port, size_t shard) : host(std::move(host)), port(port), shard(shard) {}
            repl_dest() = default;
            repl_dest(const repl_dest&) = default;
            repl_dest(repl_dest&&) = default;
            repl_dest& operator=(const repl_dest&) = default;
        };
        struct client : repl_dest {
            std::atomic<uint32_t> messages = 0;
            heap::vector<uint8_t> buffer{};
            heap::vector<repl_dest> destinations{};
            std::thread tpoll{};
            std::mutex latch{};

            bool connected = false;
            client() = default;
            client(std::string host, int port, size_t shard) : repl_dest(std::move(host), port, shard) {}
            ~client();
            void stop();
            [[nodiscard]] bool begin_transaction() const ;
            [[nodiscard]] bool commit_transaction() const ;
            bool load(size_t shard);
            [[nodiscard]] bool ping() const;
            // dese function should already latched by the shard calling them
            void add_destination(std::string host, int port, size_t shard);
            bool insert(const art::key_options& options, art::value_type key, art::value_type value);
            bool remove(art::value_type key);
        };
    }
}

#endif //SERVER_H
