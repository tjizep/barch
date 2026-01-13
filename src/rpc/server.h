//
// Created by teejip on 5/22/25.
//

#ifndef SERVER_H
#define SERVER_H
#include <cstdint>
#include "value_type.h"
#include <thread>
#include <utility>

#include "../art/key_options.h"
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
        void publish(const std::string& host, int port);
        bool has_destinations();
        void call(const std::vector<std::string>& params);
        void distribute();
        void stop_repl();
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


        struct temp_client: repl_dest {
            temp_client() = default;
            temp_client(std::string host, int port, size_t shard) : repl_dest(std::move(host), port, shard) {}
            ~temp_client();
            bool load(const std::string& name, size_t shard);
            [[nodiscard]] bool ping() const;

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
