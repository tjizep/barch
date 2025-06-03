//
// Created by teejip on 5/22/25.
//

#ifndef SERVER_H
#define SERVER_H
#include <cstdint>
#include "value_type.h"
#include <thread>
#include <utility>
enum {
    rpc_client_context_size = 128,
    rpc_max_buffer = 32768,
    rpc_server_version = 21
};
namespace barch {

    typedef std::pair<std::string, size_t> host_id;
    host_id get_host_id();
    namespace server {
        void start(std::string interface, uint_least16_t port);
        void stop();
    };

    namespace repl {
        struct client {
            heap::vector<uint8_t> buffer{};
            std::thread tpoll{};
            bool connected = false;
            std::string host {};
            int port {};
            size_t shard {};
            client() = default;
            client(std::string host, int port, size_t shard) : host(std::move(host)), port(port), shard(shard) {}
            ~client();
            [[nodiscard]] bool begin_transaction() const ;
            [[nodiscard]] bool commit_transaction() const ;
            bool load(size_t shard);
            [[nodiscard]] bool ping() const;
            void connect(std::string host, int port, size_t shard, bool add_as_sink);
            bool insert(art::value_type key, art::value_type value);
            bool remove(art::value_type key);
        };
    }
}

#endif //SERVER_H
