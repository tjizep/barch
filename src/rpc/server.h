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

struct caller;

namespace barch { class key_space; }
namespace barch {

    typedef std::pair<std::string, size_t> host_id;
    host_id get_host_id();
    namespace server {
        /** listen on `interface`:`port`; empty when it is listening, else why not - TODO 441 */
        extern std::string start(const std::string &interface, uint_least16_t port, bool ssl);
        extern void stop();
        /**
         * push one CLIENT INFO style line per open session, as CLIENT LIST. The session
         * vectors live in here, so the walk does too - a caller never sees a session.
         */
        extern void list_clients(caller& call);
        /**
         * The worker io_context of whichever listener is up, or nullptr when none is.
         *
         * This is the same context a session posts an asynchronous batch to, so work
         * queued here runs on the worker pool and never on a service thread. An
         * io_context is thread safe, so the pointer can be used from any thread; what
         * it must not outlive is the server, which is why nothing holds it across a
         * server::stop().
         */
        extern asio::io_context* worker_io();
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
            /**
             * Every shard of the remote space with the same name as `ks`, as shard
             * files beside `ks`'s own - TODO 480. `user` and `secret` log in on
             * the other side, which needs read rights on the space. Nothing is
             * installed: on false, whatever arrived has been dropped.
             */
            bool receive_space(const std::shared_ptr<key_space>& ks, const std::string& user,
                               const std::string& secret, std::string& err);
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
