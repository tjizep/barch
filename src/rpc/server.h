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
#include "source.h"
#include "asio_includes.h"
namespace barch {

    typedef std::pair<std::string, size_t> host_id;
    host_id get_host_id();
    namespace server {
        extern void start(const std::string &interface, uint_least16_t port, bool ssl);
        extern void stop();
    };
    namespace repl {
        struct call_result {
            int call_error{};
            int net_error{};
            [[nodiscard]] bool ok() const {
                return call_error == 0 && net_error == 0;
            }
        };
        class rpc {
        public:
            virtual ~rpc() = default;
            virtual call_result call(heap::vector<Variable>& result, const std::vector<std::string>& params) = 0;

            virtual call_result call(heap::vector<Variable>& result, const heap::vector<std::string>& params) = 0;

            virtual call_result call(heap::vector<Variable>& result, const std::vector<std::string_view>& params) = 0;

            virtual call_result call(heap::vector<Variable>& result, const heap::vector<art::value_type>& params) = 0;

            virtual call_result call(heap::vector<Variable>& result, const arg_t& params) = 0;

            virtual call_result asynch_call(heap::vector<Variable>& result, const heap::vector<art::value_type>& params) = 0;
            [[nodiscard]] virtual std::error_code net_error() const = 0;
        };
        std::shared_ptr<rpc> create(const std::string& host, int port);

        struct repl_dest {
            std::string host {};
            std::string name {};
            int port {};
            size_t shard {};
            repl_dest(std::string host, int port, size_t shard) : host(std::move(host)), port(port), shard(shard) {}
            repl_dest() = default;
            repl_dest(const repl_dest&) = default;
            repl_dest(repl_dest&&) = default;
            repl_dest& operator=(const repl_dest&) = default;
        };
        std::shared_ptr<source> create_source(const std::string& host, const std::string& port, size_t shard);
        struct client : repl_dest {
            std::atomic<uint32_t> messages = 0;
            heap::vector<uint8_t> buffer{};
            heap::vector<repl_dest> destinations{};
            heap::vector<std::shared_ptr<source>> sources{};
            std::mutex latch{};
            std::shared_ptr<rpc> dest{};
            bool connected = false;
            client() = default;
            client(std::string host, int port, size_t shard) : repl_dest(std::move(host), port, shard) {}
            ~client();
            void poll();
            void stop();
            [[nodiscard]] bool begin_transaction() const ;
            [[nodiscard]] bool commit_transaction() const ;
            bool load(size_t shard);
            [[nodiscard]] bool ping() const;
            // dees functions should already be latched by the shard calling them
            void add_destination(std::string host, int port);
            bool add_source(std::string host, int port);
            bool insert(heap::shared_mutex& latch, const art::key_options& options, art::value_type key, art::value_type value);
            bool remove(heap::shared_mutex& latch, art::value_type key);
            /**
             * finds a key in the tree
             * @param key the key which we want to retrieve
             * @return the node of the added key
             */
            bool find_insert(art::value_type key);
        private:
            //void send_art_fun(std::iostream& stream,  const heap::vector<uint8_t>& to_send);
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
