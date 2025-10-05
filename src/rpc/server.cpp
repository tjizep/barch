//
// Created by teejip on 5/22/25.
//

#include "server.h"
#include "logger.h"


#include <utility>
#include "module.h"
#include "statistics.h"

#include "asio_includes.h"
#include <cstdio>
#include "barch_apis.h"
#include "swig_api.h"
#include "rpc_caller.h"
#include "vk_caller.h"
#include "redis_parser.h"
#include "thread_pool.h"
//#include "uring_context.h"
#include "queue_server.h"
//#include "uring_resp_session.h"
#include "asio_resp_session.h"
#include "rpc/barch_session.h"

namespace barch {
    typedef asio::executor_work_guard<asio::io_context::executor_type> exec_guard;
    struct asio_work_unit {
        asio::io_context io{};
        exec_guard guard;
        asio_work_unit() : guard(asio::make_work_guard(io)){
        }
        ~asio_work_unit() {
            guard.reset();
        }
        void run() {
            io.run();
        }
        void stop() {
            io.stop();
            guard.reset();
        }
    };
    struct server_context {
        thread_pool pool{};
        //thread_pool resp_pool{2};
        thread_pool asio_resp_pool{};
        asio::io_context io{};
        std::vector<std::shared_ptr<asio_work_unit>> asio_resp_ios{};
        //std::vector<std::shared_ptr<work_unit>> io_resp{resp_pool.size()};

        tcp::acceptor acc;
        std::string interface;
        uint_least16_t port;
        std::atomic<size_t> threads_started = 0;
        std::atomic<size_t> resp_distributor{};
        std::atomic<size_t> asio_resp_distributor{};
#if 0
        std::shared_ptr<work_unit> get_unit() {
            size_t r = resp_distributor++ % io_resp.size();
            return io_resp[r];
        }
#endif
        std::shared_ptr<asio_work_unit> get_asio_unit() {
            size_t r = asio_resp_distributor++ % asio_resp_ios.size();
            return asio_resp_ios[r];
        }

        void stop() {
            ::stop_queue_server();
            try {
                acc.close();
            }catch (std::exception& ) {}
            for (auto &proc: asio_resp_ios) {
                try {
                    proc->stop();
                }catch (std::exception& e) {
                    art::std_err("failed to stop resp io service", e.what());
                }

            }
            try {
                io.stop();

            }catch (std::exception& e) {
                art::std_err("failed to stop io service", e.what());
            }
#if 0
            for (auto &proc: io_resp) {
                try{
                    proc->stop();
                }catch (std::exception& e) {
                    art::std_err("failed to stop resp io service",  e.what());
                }
            }
            resp_pool.stop();
#endif
            pool.stop();

            asio_resp_pool.stop();
            port = 0;
        }
        void start_accept() {
            try {

                acc.async_accept([this](asio::error_code error, tcp::socket endpoint) {
                    if (error) {
                        art::std_err("accept error",error.message(),error.value());
                        return; // this happens if there are not threads
                    }
                    {
                        net_stat stat;
                        process_data(endpoint);
                    }
                    start_accept();
                });

            }catch (std::exception& e) {
                art::std_err("failed to start/run replication server", interface, port, e.what());
            }
        }


        void start() {}
        void process_data(tcp::socket& endpoint) {
            try {
                char cs[1] ;
                //readp(stream, cs);
                endpoint.read_some(asio::buffer(cs,1));
                stream_read_ctr += 1;
                if (cs[0]) {
                    auto unit = this->get_asio_unit();
                    tcp::socket socket (unit->io);
                    socket.assign(tcp::v4(),endpoint.release());
                    auto session = std::make_shared<resp_session>(std::move(socket),cs[0]);
                    session->start();
                    return;
                }
                uint32_t cmd = 0;
                endpoint.read_some(asio::buffer(&cmd, sizeof(cmd)));
                if (cmd == cmd_barch_call) {
                    std::make_shared<barch_session>(std::move(endpoint))->start();
                    return;
                }
                heap::vector<uint8_t> buffer{};
                art::key_spec spec;

                tcp::iostream stream(std::move(endpoint));
                if (stream.fail())
                    return;
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
                            return;
                        }
                        break;
                    case cmd_barch_call:
                        try{
                        }catch (std::exception& e) {
                            art::std_err("failed to make barch call", e.what());
                            return;
                        }
                        break;
                    case cmd_art_fun:
                        try {
                            uint32_t buffers_size = 0;
                            uint32_t shard = 0;
                            uint32_t count = 0;
                            readp(stream,shard);
                            readp(stream,count);
                            readp(stream,buffers_size);
                            if (buffers_size == 0) {
                                art::std_err("invalid buffer size", buffers_size);
                                return;
                            }

                            buffer.resize(buffers_size);
                            readp(stream, buffer.data(), buffers_size);
                            auto t = get_art(shard);
                            write_lock release(t->latch);
                            process_art_fun_cmd(t, stream, buffer);
                            //art::std_log("cmd apply changes ",shard, "[",buffers_size,"] bytes","keys",count,"actual",actual,"total",(long long)statistics::repl::key_add_recv);
                        }catch (std::exception& e) {
                            art::std_err("failed to apply changes", e.what());
                            return;
                        }
                        break;
                    default:
                        art::std_err("unknown command", cmd);
                        return;
                }
            }catch (std::exception& e) {
                art::std_err("failed to read command", e.what());
                return;
            }

        }
        server_context(std::string interface, uint_least16_t port)
        :   acc(io, tcp::endpoint(tcp::v4(), port))
        ,   interface(interface)
        ,   port(port){

            start_accept();
            pool.start([this](size_t tid) -> void{
                io.dispatch([this,tid]() {
                    art::std_log("TCP connections accepted on", this->interface,this->port,"using thread",tid);
                });
                io.run();
                art::std_log("server stopped on", this->interface,this->port,"using thread",tid);
            });
            std::atomic<size_t> started = 0;
            art::std_log("resp pool size",asio_resp_pool.size());
            asio_resp_ios.resize(asio_resp_pool.size());
            asio_resp_pool.start([this, &started](size_t tid) -> void {
                ++started;
                asio_resp_ios[tid] = std::make_shared<asio_work_unit>();
                asio_resp_ios[tid]->run();
            });
            while (started != asio_resp_pool.size()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    };
    std::shared_ptr<server_context>  srv = nullptr;
    void server::start(const std::string& interface, uint_least16_t port) {
        if (srv) srv->stop();
        try {
            if (port == 0) return;
            srv = std::make_shared<server_context>(interface, port);
        }catch (std::exception& e) {
            art::std_err("failed to start server", e.what());
        }

    }

    void server::stop() {
        if (srv) srv->stop();
    }
    struct module_stopper {
        module_stopper() = default;
        ~module_stopper() {
            server::stop();
        }
    };
    static module_stopper stopper;
    namespace repl {
        class rpc_impl : public rpc {
        private:
            std::mutex latch{};
            std::string host;
            int port;
            asio::io_context ioc{};
            tcp::resolver resolver{ioc};
            tcp::socket s;
            size_t requests_in_stream = 0;
            std::error_code error{};
            heap::vector<uint8_t> replies{};
            vector_stream stream{};
            heap::vector<uint8_t> to_send{};
        public:
            rpc_impl(const std::string& host, int port)
            : host(host), port(port), s(ioc) {

            }
            [[nodiscard]] std::error_code net_error() const override {
                return error;
            }
            virtual ~rpc_impl() = default;
            void run(std::chrono::steady_clock::duration timeout) {
                run_to(ioc, s, timeout);
            }

            template<typename SockT,typename BufT>
            size_t write(SockT& sock, const BufT& buf) {
                size_t r = 0;
                asio::async_write(sock, buf,[&](const std::error_code& result_error,
                std::size_t result_n)
                {
                    r += result_n;
                    stream_write_ctr += result_n;
                    error = result_error;
                });
                run(art::get_rpc_write_to_s());
                if (error) {
                    ++statistics::repl::request_errors;
                    throw_exception<std::runtime_error>("failed to write");
                };
                return r;
            }

            template<typename SockT,typename BufT>
            size_t read(SockT& sock, BufT buf) {
                size_t r = 0;
                asio::async_read(sock, buf,[&](const std::error_code& result_error,
                std::size_t result_n)
                {
                    r += result_n;
                    stream_read_ctr += result_n;
                    error = result_error;
                });
                run(art::get_rpc_read_to_s());
                if (error) {
                    ++statistics::repl::request_errors;
                    throw_exception<std::runtime_error>("failed to read");
                };
                return r;
            }
            template<typename ResultT,typename ParamT>
            call_result tcall(ResultT& result, const ParamT& params) {

                std::lock_guard lock(latch);
                to_send.clear();
                call_result r;
                if (params.empty()) {
                    return call_result{-1,0};
                }
                try {

                    if (!s.is_open()) {
                        stream.clear();

                        auto resolution = resolver.resolve(host,std::to_string(port));
                        asio::async_connect(s, resolution,[this](const std::error_code& ec, tcp::endpoint unused(endpoint)) {
                            if (!ec) {
                                uint32_t cmd = cmd_barch_call;
                                writep(stream,uint8_t{0x00});
                                writep(stream, cmd);
                            }
                            error = ec;
                        });
                        run(art::get_rpc_connect_to_s());
                        if (error) {
                            throw_exception<std::runtime_error>("failed to connect");
                        };
                    }

                    for (auto p: params) {
                        push_value(to_send, art::value_type{p});
                    }

                    uint32_t calls = 1;
                    uint32_t buffers_size = to_send.size();
                    writep(stream, calls);
                    writep(stream, buffers_size);
                    writep(stream, to_send.data(), to_send.size());
                    net_stat stat;
                    write(s,asio::buffer(stream.buf.data(), stream.buf.size()));
                    read(s,asio::buffer(&r.call_error,sizeof(r.call_error)));
                    read(s,asio::buffer(&buffers_size,sizeof(buffers_size)));
                    replies.resize(buffers_size);
                    size_t reply_length = read(s,asio::buffer(replies));
                    if (reply_length != buffers_size) {
                        art::std_err(reply_length,"!=",buffers_size);
                        throw_exception<std::length_error>("invalid reply length");
                    }
                    for (size_t i = 0; i < buffers_size; i++) {
                        auto v = get_variable(i, replies);
                        result.emplace_back(v.first);
                        i = v.second;
                    }
                    stream.clear();
                }catch (std::exception& e) {
                    art::std_err("call failed [", e.what(),"] to",host,port,"because [",error.message(),error.value(),"]");
                    stream.clear();
                    return {-1,-1};
                }

                return r;
            }

            call_result call(heap::vector<Variable>& result, const heap::vector<art::value_type>& params) override {
                return tcall(result, params);
            }
            call_result asynch_call(heap::vector<Variable>& result, const heap::vector<art::value_type>& params) override {
                return tcall(result, params);
            }

            call_result call(heap::vector<Variable>& result, const std::vector<std::string_view>& params) override {
                return tcall(result, params);
            }
        };
        std::shared_ptr<barch::repl::rpc> create(const std::string& host, int port) {
            return std::make_shared<rpc_impl>(host, port);
        }

        client::~client() {
            connected = false;
            if (tpoll.joinable())
                tpoll.join();
        };

        void client::stop() {
            connected = false;
            if (tpoll.joinable())
                tpoll.join();
        }

        void client::add_destination(std::string host, int port) {
            auto dest = repl_dest{std::move(host), port, shard};
            {
                std::lock_guard lock(this->latch);
                for (auto& d : destinations) {
                    if (d.host == dest.host && d.port == dest.port) {
                        art::std_err("destination already added", d.host, d.port);
                        return;
                    }
                }
                destinations.emplace_back(dest);
            }
            std::lock_guard lock(this->latch);
            if (!connected) {
                connected = true;
                tpoll = std::thread([this]() {
                heap::vector<uint8_t> to_send;
                to_send.reserve(art::get_rpc_max_buffer());
                try {
                    asio::io_context io;
                    tcp::resolver resolver{io};
                    heap::vector<repl_dest> dests;
                    vector_stream stream;
                    uint32_t message_count = 0;
                    while (connected) {

                        {
                            std::lock_guard lock(this->latch);
                            to_send.swap(this->buffer);
                            message_count = this->messages;
                            this->buffer.clear();
                            this->messages = 0;
                            dests = destinations;
                        }
                        if (!to_send.empty()) {
                            for (auto& dest : dests) {
                                try {
                                    stream.clear();
                                    std::error_code error{};
                                    tcp::socket s = tcp::socket(io);
                                    auto resolution = resolver.resolve(dest.host,std::to_string(dest.port));
                                    asio::async_connect(s, resolution,[&](const std::error_code& ec, tcp::endpoint unused(endpoint)) {
                                        if (!ec) {

                                            uint32_t cmd = cmd_art_fun;
                                            uint32_t buffers_size = to_send.size();
                                            uint32_t sh = this->shard;
                                            writep(stream,uint8_t{0x00});
                                            writep(stream, cmd);
                                            writep(stream, sh);
                                            writep(stream, message_count);
                                            if (buffers_size == 0) {
                                                art::std_err("invalid buffer size", buffers_size);
                                                return;
                                            }
                                            writep(stream, buffers_size);
                                            writep(stream, to_send.data(), to_send.size());
                                            async_write_to( s, asio::buffer(stream.buf.data(), stream.buf.size()), error);
                                        }else {
                                            error = ec;
                                        }

                                    });
                                    if (!run_to(io, s, art::get_rpc_connect_to_s())) {
                                        throw_exception<std::runtime_error>("failed to connect to server");
                                    }
                                    if (error) {
                                        continue;
                                    }

#if 0
                                    tcp::iostream stream(dest.host, std::to_string(dest.port));
                                    if (stream.fail()) {
                                        continue;
                                    }
#endif
                                    //art::std_log("sent", buffers_size, "bytes to", dest.host, dest.port, "total sent",total_messages,"still queued",(uint32_t)this->messages,"iq",(long long)statistics::repl::insert_requests);

                                }catch (std::exception& e) {
                                    art::std_err("failed to write to stream", e.what(),"to",dest.host,dest.port);
                                }
                            }
                            to_send.clear();
                        }
                        if (this->messages == 0) // send asap
                            std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    }
                }catch (std::exception& e) {
                    this->connected = false;
                    art::std_err("failed to connect to remote server", this->host, this->port,e.what());
                }
            });
            }

        }

        bool client::add_source(std::string host, int port) {
            auto src = repl_dest{std::move(host), port, shard};
            {
                std::lock_guard lock(this->latch);
                for (auto& d : sources) {
                    if (d.host == src.host && d.port == src.port) {
                        art::std_err("source already added", d.host, d.port);
                        return false;
                    }
                }
                sources.emplace_back(src);
            }
            return true;
        }

        bool client::rpc_insert(std::shared_mutex& latch, const art::key_options& options, art::value_type key, art::value_type value){
            latch.unlock();
            thread_local heap::vector<Variable> result;
            thread_local heap::vector<art::value_type> params;
            params = {"RPC_INSERT",
                    std::to_string(shard),
                    std::to_string(options.flags),
                    std::to_string(options.get_expiry()),
                    key.to_string(),value.to_string()};
            size_t ss = 0;
            if (!host.empty()) {
                result.clear();
                auto cr = caller()->asynch_call(result, params);
                if (cr.ok()) ++ss;
                else ++statistics::repl::instructions_failed;
            }

            for (auto &d: destinations) {
                result.clear();
                auto cr = d.caller()->asynch_call(result, params);
                if (cr.ok()) ++ss;
                else ++statistics::repl::instructions_failed;
            }
            if (ss != 0)
                ++statistics::repl::insert_requests;

            latch.lock();
            return  ss > 0;

        }
        bool client::insert(std::shared_mutex& latch, const art::key_options& options, art::value_type key, art::value_type value) {
            if (!connected) {
                if (!destinations.empty()) ++statistics::repl::instructions_failed;
                return destinations.empty();
            }
            bool ok = time_wait(art::get_rpc_max_client_wait_ms(), [this,&latch]() {
                bool r = buffer.size() < art::get_rpc_max_buffer();
                if (!r) {
                    latch.unlock();
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    latch.lock();
                }
                r = buffer.size() < art::get_rpc_max_buffer();
                return r;
            });

            if (!ok) {
                ++statistics::repl::instructions_failed;
                art::std_err("replication buffer size exceeded", buffer.size());
                return false;
            }
            std::lock_guard lock(this->latch);
            buffer.push_back('i');
            push_options(buffer, options);
            push_value(buffer, key);
            push_value(buffer, value);
            ++statistics::repl::insert_requests;
            ++messages;
            return true;

        }

        bool client::remove(std::shared_mutex& latch, art::value_type key) {
            if (!connected) {
                if (!destinations.empty()) ++statistics::repl::instructions_failed;
                return destinations.empty();
            }
            bool ok = time_wait(art::get_rpc_max_client_wait_ms(), [this, &latch]() {
                bool r = buffer.size() < art::get_rpc_max_buffer();
                if (!r) {
                    latch.unlock();
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    latch.lock();
                }
                r = buffer.size() < art::get_rpc_max_buffer();
                return r;
            });
            if (!ok) {
                ++statistics::repl::instructions_failed;
                art::std_err("replication buffer size exceeded", buffer.size());
                return false;
            }

            std::lock_guard lock(this->latch);
            buffer.push_back('r');
            push_value(buffer, key);
            ++statistics::repl::remove_requests;
            ++messages;
            return true;
        }
        void client::send_art_fun(std::iostream& stream, const heap::vector<uint8_t>& to_send) {
            uint32_t cmd = cmd_art_fun;
            uint32_t buffers_size = to_send.size();
            uint32_t sh = this->shard;
            writep(stream,uint8_t{0x00});
            writep(stream, cmd);
            writep(stream, sh);
            writep(stream, messages);
            writep(stream, buffers_size);
            writep(stream, to_send.data(), to_send.size());
            stream.flush();

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
                uint32_t cmd = cmd_stream;
                uint32_t s = shard;
                net_stat stats;
                writep(stream,uint8_t{0x00});
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
        bool client::find_insert(art::value_type key) {
            if (sources.empty()) return false;
            bool added = false;
            heap::vector<uint8_t> to_send, rbuff;
            to_send.push_back('f');// <-- to find a key
            push_value(to_send, key);
            // todo: r we choosing a random 1
            for (auto& source : sources) {
                try {

                    net_stat stat;
                    tcp::iostream stream;

                    stream.connect(source.host, std::to_string(source.port));
                    if (stream.fail()) {
                        art::std_err("failed to connect to remote server", host, this->port);
                        continue;
                    }

                    send_art_fun(stream, to_send);
                    if (!recv_buffer(stream, rbuff)) {
                        continue;
                    }
                    auto t = get_art(this->shard);
                    //storage_release release(t); // the latch should be called outside
                    t->last_leaf_added = nullptr;
                    auto r = process_art_fun_cmd(t, stream, rbuff);
                    if (r.add_applied > 0) {
                        added = true;
                        break; // don't call the other servers
                    }
                }catch (std::exception& e) {
                    art::std_err("failed to write to stream", e.what(),"to",source.host,source.port);
                    ++statistics::repl::request_errors;
                }
                ++statistics::repl::find_requests;
            }
            ++messages;
            return added;
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
                net_stat stats;
                uint32_t cmd = cmd_ping;
                writep(stream,uint8_t{0x00});
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
        static heap::vector<route> routes;
        void set_route(size_t shard, const route& destination) {
            if (routes.empty()) routes.resize(art::get_shard_count().size());
            if (shard >= routes.size()) {
                art::std_err("invalid shard",shard, routes.size());
                return;
            }
            routes[shard] = destination;
        }
        void clear_route(size_t shard) {
            if (routes.empty()) routes.resize(art::get_shard_count().size());
            if (shard >= routes.size()) {
                art::std_err("invalid shard",shard, routes.size());
                return;
            }
            routes[shard] = {};
        }
        route get_route(size_t shard) {
            if (routes.empty()) routes.resize(art::get_shard_count().size());
            if (shard >= routes.size()) {
                art::std_err("invalid shard",shard, routes.size());
                return {};
            }
            return routes[shard];
        }
    }
}

extern "C"{
    int RPC_GET(caller& call, const arg_t& argv) {
        if (argv.size() != 2)
            return call.wrong_arity();
        auto key = argv[1];
        auto t = get_art(key);
        read_lock rl(t);
        auto node = t->search(key);
        if (node.null()) return call.null();
        return call.vt(node.cl()->get_value());

    }
    int RPC_INSERT(caller& call, const arg_t& argv) {
        ++statistics::repl::key_add_recv;
        if (argv.size() != 6)
            return call.wrong_arity();
        size_t shard = conversion::as_variable(argv[1]).i();
        if (shard >= art::get_shard_count().size())
            return call.error("invalid shard");
        auto fc = [&](const art::node_ptr &) -> void {};
        uint8_t flags = conversion::as_variable(argv[2]).i();
        uint64_t expiry = conversion::as_variable(argv[3]).i();
        auto key = argv[5];
        auto value = argv[6];
        auto t = get_art(shard);
        art::key_options options;
        options.flags = flags;
        options.set_expiry(expiry);
        write_lock l(t->latch);
        t->opt_rpc_insert(options, key, value, true, fc);
        ++statistics::repl::key_add_recv_applied;

        return 0;
    }
    int RPC_FUN(caller& call, const arg_t& argv) {
        if (argv.size() != 2)
            return call.wrong_arity();
        try {
            vector_stream stream{argv[1]};
            uint32_t buffers_size = 0;
            uint32_t shard = 0;
            uint32_t count = 0;
            readp(stream,shard);
            readp(stream,count);
            readp(stream,buffers_size);
            if (buffers_size == 0) {
                art::std_err("invalid buffer size", buffers_size);
                return call.error("invalid buffer size");
            }

            heap::vector<uint8_t> buffer;
            buffer.resize(buffers_size);
            readp(stream, buffer.data(), buffers_size);
            auto t = get_art(shard);
            write_lock release(t->latch);
            barch::process_art_fun_cmd(t, stream, buffer);
            //art::std_log("cmd apply changes ",shard, "[",buffers_size,"] bytes","keys",count,"actual",actual,"total",(long long)statistics::repl::key_add_recv);
        }catch (std::exception& e) {
            art::std_err("failed to apply changes", e.what());
            return call.error("failed to apply changes");
        }
        return 0;
    }
    int ADDROUTE(caller& call, const arg_t& argv) {
        if (argv.size() != 4)
            return call.wrong_arity();
        size_t shard = conversion::as_variable(argv[1]).i();
        if (shard >= art::get_shard_count().size())
            return call.error("invalid shard");
        auto host = conversion::as_variable(argv[2]).s();
        auto port = conversion::as_variable(argv[3]).i();
        if (port <= 0 || port >= 65536)
            return call.error("invalid port");
        if (host.empty())
            return call.error("no host");
        barch::repl::set_route(shard, {host,port});
        return call.simple(host.c_str());
    }
    int ROUTE(caller& call, const arg_t& argv) {
        if (argv.size() != 2)
            return call.wrong_arity();
        size_t shard = atoi(argv[1].chars());
        if (shard >= art::get_shard_count().size())
            return call.error("invalid shard");
        auto route = barch::repl::get_route(shard);
        call.start_array();
        call.simple(route.ip.c_str());
        call.long_long(route.port);
        call.end_array(0);
//
        return 0;
    }
    int REMROUTE(caller& call, const arg_t& argv) {
        if (argv.size() != 2)
            return call.wrong_arity();
        size_t shard = atoi(argv[1].chars());
        if (shard >= art::get_shard_count().size())
            return call.error("invalid shard");
        auto route = barch::repl::get_route(shard);
        barch::repl::clear_route(shard);
        call.start_array();
        call.simple(route.ip.c_str());
        call.long_long(route.port);
        call.end_array(0);
        return 0;
    }
}
