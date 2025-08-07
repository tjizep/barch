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
#include "variable.h"

enum {
    rpc_server_version = 21,
    rpc_server_version_min = 21,
    rpc_server_version_max = 21,
    rpc_client_max_wait_default_ms = 30000,
    rpc_io_buffer_size = 1024 * 4,
    rpc_max_param_buffer_size = 1024 * 1024 * 10,
};
namespace barch {

    typedef std::pair<std::string, size_t> host_id;
    host_id get_host_id();
    namespace server {
        void start(const std::string& interface, uint_least16_t port);
        void stop();
    };

    namespace repl {
        class rpc {
        public:
            virtual ~rpc() = default;
            virtual int call(int& callr, heap::vector<Variable>& result, const std::vector<std::string_view>& params) = 0;
            virtual std::error_code net_error() const = 0;
        };
        std::shared_ptr<barch::repl::rpc> create(const std::string& host, int port);

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
            heap::vector<repl_dest> sources{};
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
            // dese functions should already be latched by the shard calling them
            void add_destination(std::string host, int port);
            bool add_source(std::string host, int port);
            bool call(const std::string& name, const std::vector<std::string_view>& params, const std::string& host, int port);
            bool insert(const art::key_options& options, art::value_type key, art::value_type value);
            bool remove(art::value_type key);
            /**
             * finds a key in the tree
             * @param t the tree that receives the key if it does not exist
             * @param key the key which we want to retrieve
             * @return the node of the added key
             */
            bool find_insert(art::value_type key);
        private:
            void send_art_fun(std::iostream& stream,  const heap::vector<uint8_t>& to_send);
        };
        struct route {
            std::string ip{};
            int64_t port{};
        };
        void clear_route(size_t shard);
        void set_route(size_t shard, const route& destination);
        route get_route(size_t shard);
    }
}

#endif //SERVER_H
