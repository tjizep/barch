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
#include "asio_resp_session.h"
#include "rpc/barch_session.h"
#include "repl_session.h"
//#include "uring_resp_session.h"
#include "rpc/constants.h"

namespace barch {
    std::atomic<uint64_t> client_id = 0;
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
    static std::recursive_mutex& srv_mut() {
        static std::recursive_mutex srv_get{};
        return srv_get;
    }
    struct server_context {
        bool started = false;
        std::atomic<size_t> num_started = 0;

        thread_pool pool{(double)tcp_accept_pool_factor/100.0f};
        thread_pool asio_resp_pool{(double)resp_pool_factor/100.0f};
        thread_pool uring_resp_pool{(double)resp_pool_factor/100.0f};
        thread_pool work_pool{asynch_proccess_workers};

        asio::io_context io{};
        asio::io_context workers{};
        exec_guard worker_guard {asio::make_work_guard(workers)};
        std::vector<std::shared_ptr<asio_work_unit>> asio_resp_ios{};
        //std::vector<std::shared_ptr<uring_work_unit>> uring_resp_ios{};

        tcp::acceptor accept;
        asio::ssl::context ssl_context;
        std::string interface;
        uint_least16_t port;
        std::atomic<size_t> threads_started = 0;
        std::atomic<size_t> resp_distributor{};
        std::atomic<size_t> asio_resp_distributor{};
        //std::atomic<size_t> uring_resp_distributor{};
        bool use_ssl = false;

        std::shared_ptr<asio_work_unit> get_asio_unit() {
            size_t r = (asio_resp_distributor % asio_resp_ios.size());
            asio_resp_distributor++;
            return asio_resp_ios[r];
        }

        void stop() {
            if ( num_started == 0) {
                barch::std_err("server not started");
                return;
            }
            try {
                accept.close();
            }catch (std::exception& ) {}
            for (auto &proc: asio_resp_ios) {
                try {
                    proc->stop();
                }catch (std::exception& e) {
                    barch::std_err("failed to stop resp io service", e.what());
                }

            }
            try {
                io.stop();

            }catch (std::exception& e) {
                barch::std_err("failed to stop io service", e.what());
            }

            try {
                workers.stop();

            }catch (std::exception& e) {
                barch::std_err("failed to workers service", e.what());
            }
            work_pool.stop();
            pool.stop();

            asio_resp_pool.stop();
            port = 0;
            started = false;
        }
        void start_accept() {
            try {

                accept.async_accept([this](asio::error_code error, tcp::socket endpoint) {
                    if (error) {
                        barch::std_err("accept error",error.message(),error.value());
                        return; // this happens if there are no threads
                    }
                    {
                        net_stat stat;
                        if (use_ssl) {
                            auto ssl = ssl_stream(std::move(endpoint), ssl_context);
                            auto session = std::make_shared<resp_session<ssl_stream>>(std::move(ssl),workers);
                            session->start_ssl();
                        }else {
                            process_data(endpoint);
                        }
                    }
                    start_accept();
                });

            }catch (std::exception& e) {
                barch::std_err("failed to start/run replication server", interface, port, e.what());
            }
        }


        void start() {}
        bool use_uring = false;
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
                        auto fd = socket.lowest_layer().native_handle();
                        int quickack = 1;
                        setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &quickack, sizeof(quickack));
                        int flag = 1;
                        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
                        auto session = std::make_shared<resp_session<tcp::socket>>(std::move(socket),workers, cs[0]);
                        session->start();

                    return;
                }
                uint32_t cmd = 0;
                endpoint.read_some(asio::buffer(&cmd, sizeof(cmd)));
                if (cmd == cmd_barch_call) {
                    std::make_shared<barch_session>(std::move(endpoint))->start();
                    return;
                }
                if (cmd == cmd_art_fun) {
                    std::make_shared<repl_session>(std::move(endpoint),1+sizeof(cmd))->start();
                    return;
                }
                heap::vector<uint8_t> buffer{};
                barch::key_spec spec;

                tcp::iostream stream(std::move(endpoint));
                if (stream.fail())
                    return;
                switch (cmd) {
                    case cmd_ping:
                        try {
                            writep(stream,rpc_server_version);
                        }catch (std::exception& e) {
                            barch::std_err("error",e.what());
                        }

                        break;
                    case cmd_stream:
                        try {
                            uint32_t shard = 0;
                            readp(stream,shard);
                            // TODO: add named keyspace support
                            auto ks = get_default_ks();
                            if (shard < barch::get_shard_count().size()) {
                                ks->get(shard)->send(stream);
                                stream.flush();
                            }else {
                                barch::std_err("invalid shard", shard);
                            }

                        }catch (std::exception& e) {
                            barch::std_err("failed to stream shard", e.what());
                            return;
                        }
                        break;
                    default:
                        barch::std_err("unknown command", cmd);
                        return;
                }
            }catch (std::exception& e) {
                barch::std_err("failed to read command", e.what());
                return;
            }

        }
        std::string get_password() const
        {
            return "test";
        }

        server_context(std::string interface, uint_least16_t port, bool ssl)
        :   accept(io, tcp::endpoint(tcp::v4(), port))
        ,   ssl_context(asio::ssl::context::tlsv13)
        ,   interface(interface)
        ,   port(port)
        ,   use_ssl(ssl) {

            if (use_ssl) {
                ssl_context.set_options(
                asio::ssl::context::default_workarounds
                | asio::ssl::context::no_tlsv1_1
                | asio::ssl::context::single_dh_use);
                ssl_context.set_password_callback(std::bind(&server_context::get_password, this));
                ssl_context.use_certificate_chain_file(get_tls_pem_certificate_chain_file());
                ssl_context.use_private_key_file(get_tls_private_key_file(), asio::ssl::context::pem);
                ssl_context.use_tmp_dh_file(get_tls_tmp_dh_file());
            }

            start_accept();
            pool.start([this](size_t tid) -> void{

                asio::dispatch(io ,[this,tid]() {
                    barch::std_log(use_ssl ? "TLS/SSL":"TCP","connections accepted on", this->interface,this->port,"using thread",tid);
                });
                io.run();
                barch::std_log("server stopped on", this->interface,this->port,"using thread",tid);
            });
            work_pool.start([this](size_t tid) -> void{
                workers.run();
                barch::std_log("worker stopped using thread",tid);
            });
            num_started = 0;
            barch::std_log("resp pool size",asio_resp_pool.size());
            asio_resp_ios.resize(asio_resp_pool.size());
            //uring_resp_ios.resize(uring_resp_pool.size());
            asio_resp_pool.start([this](size_t tid) -> void {
                ++num_started;
                asio_resp_ios[tid] = std::make_shared<asio_work_unit>();
                asio_resp_ios[tid]->run();
            });
            while (num_started != asio_resp_pool.size()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            num_started = 0;
            while (num_started != asio_resp_pool.size()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }

            //asio_resp_pool.pin_threads();
            started = true;
        }
        ~server_context() {
            std::lock_guard f(srv_mut());
            stop();
        }
    };


    static std::shared_ptr<server_context>& get_srv() {
        static std::shared_ptr<server_context> srv = nullptr;
        return srv;
    }
    static std::shared_ptr<server_context>& get_srv_ssl() {
        static std::shared_ptr<server_context> srv = nullptr;
        return srv;
    }
    void handle_start(const std::string& interface, uint_least16_t port, bool ssl, std::shared_ptr<server_context>& s) {
        s = nullptr;
        try {
            if (port == 0) return;
            s = std::make_shared<server_context>(interface, port, ssl);
        }catch (std::exception& e) {
            barch::std_err("failed to start server", e.what());
        }

    }
    void handle_stop(std::shared_ptr<server_context>& s) {

        s = nullptr;
    }
    void server::start(const std::string& interface, uint_least16_t port, bool ssl) {
        std::unique_lock l(srv_mut());
        if (ssl) {
            handle_start(interface, port, true, get_srv_ssl());
        }else {
            handle_start(interface, port, false, get_srv());
        }
    }

    void server::stop() {
        std::unique_lock l(srv_mut());
        handle_stop(get_srv());
        handle_stop(get_srv_ssl());
    }
    struct module_stopper {
        module_stopper() = default;
        ~module_stopper() {
            server::stop();
        }
    };
    //static module_stopper _stopper;
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
                run(barch::get_rpc_write_to_s());
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
                run(barch::get_rpc_read_to_s());
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
                    net_stat stat;
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
                        run(barch::get_rpc_connect_to_s());
                        if (error) {
                            throw_exception<std::runtime_error>("failed to connect");
                        };
                    }

                    for (auto p: params) {
                        push_value(to_send, value_type{p});
                    }

                    uint32_t calls = 1;
                    uint32_t buffers_size = to_send.size();
                    writep(stream, calls);
                    writep(stream, buffers_size);
                    writep(stream, to_send.data(), to_send.size());

                    write(s,asio::buffer(stream.buf.data(), stream.buf.size()));
                    read(s,asio::buffer(&r.call_error,sizeof(r.call_error)));
                    read(s,asio::buffer(&buffers_size,sizeof(buffers_size)));
                    replies.resize(buffers_size);
                    size_t reply_length = read(s,asio::buffer(replies));
                    if (reply_length != buffers_size) {
                        barch::std_err(reply_length,"!=",buffers_size);
                        throw_exception<std::length_error>("invalid reply length");
                    }
                    for (size_t i = 0; i < buffers_size; i++) {
                        auto v = get_variable(i, replies);
                        result.emplace_back(v.first);
                        i = v.second;
                    }
                    stream.clear();
                }catch (std::exception& e) {
                    barch::std_err("call failed [", e.what(),"] to",host,port,"because [",error.message(),error.value(),"]");
                    stream.clear();
                    return {-1,-1};
                }

                return r;
            }
            call_result call(heap::vector<Variable>& result, const heap::vector<std::string>& params) override {
                return tcall(result, params);
            }
            call_result call(heap::vector<Variable>& result, const heap::vector<value_type>& params) override {
                return tcall(result, params);
            }
            call_result call(heap::vector<Variable>& result, const arg_t& params) override {
                return tcall(result, params);
            }
            call_result asynch_call(heap::vector<Variable>& result, const heap::vector<value_type>& params) override {
                return tcall(result, params);
            }

            call_result call(heap::vector<Variable>& result, const std::vector<std::string_view>& params) override {
                return tcall(result, params);
            }
            call_result call(heap::vector<Variable>& result, const std::vector<std::string>& params) override {
                return tcall(result, params);
            }
        };
        std::shared_ptr<barch::repl::rpc> create(const std::string& host, int port) {
            return std::make_shared<rpc_impl>(host, port);
        }

        client::~client() {
            connected = false;
        };

        void client::stop() {
            connected = false;
        }
        void client::poll() {
            if (!connected) return;

            thread_local heap::vector<uint8_t> to_send;
            thread_local art_fun sender;
            thread_local heap::vector<repl_dest> dests;
            to_send.reserve(barch::get_rpc_max_buffer());
            try {

                {
                    std::lock_guard lock(this->latch);
                    to_send.swap(this->buffer);
                    sender.message_count = this->messages; // detect missing
                    this->buffer.clear();
                    this->messages = 0;
                    dests = destinations;
                }

                if (!to_send.empty()) {
                    for (auto& dest : dests) {
                        // statistics are updated
                        sender.send(this->shard, this->name, dest.host, dest.port, to_send);
                    }
                    to_send.clear();
                }

            }catch (std::exception& e) {
                this->connected = false;
                barch::std_err("failed to connect to remote server", this->host, this->port,e.what());
            }
        }
        void client::add_destination(std::string host, int port) {
            auto dest = repl_dest{std::move(host), port, shard};
            {
                std::lock_guard lock(this->latch);
                for (auto& d : destinations) {
                    if (d.host == dest.host && d.port == dest.port) {
                        barch::std_err("destination already added", d.host, d.port);
                        return;
                    }
                }
                destinations.emplace_back(dest);
            }
            std::lock_guard lock(this->latch);
            if (!connected) {
                connected = true;
            }
        }

        bool client::add_source(std::string host, int port) {
            auto src = create_source(std::move(host), std::to_string(port), shard);
            {
                std::lock_guard lock(this->latch);
                for (auto& d : sources) {
                    if (d->host == src->host && d->port == src->port) {
                        barch::std_err("source already added", d->host, d->port);
                        return false;
                    }
                }
                sources.emplace_back(src);
            }
            return true;
        }

        bool client::insert(heap::shared_mutex& latch, const barch::key_options& options, value_type key, value_type value) {
            if (!connected) {
                if (!destinations.empty()) ++statistics::repl::instructions_failed;
                return destinations.empty();
            }
            bool ok = time_wait(barch::get_rpc_max_client_wait_ms(), [this,&latch]() {
                bool r = buffer.size() < barch::get_rpc_max_buffer();
                if (!r) {
                    latch.unlock();
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    latch.lock();
                }
                r = buffer.size() < barch::get_rpc_max_buffer();
                return r;
            });

            if (!ok) {
                ++statistics::repl::instructions_failed;
                barch::std_err("replication buffer size exceeded", buffer.size());
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

        bool client::remove(heap::shared_mutex& latch, value_type key) {
            if (!connected) {
                if (!destinations.empty()) ++statistics::repl::instructions_failed;
                return destinations.empty();
            }
            bool ok = time_wait(barch::get_rpc_max_client_wait_ms(), [this, &latch]() {
                bool r = buffer.size() < barch::get_rpc_max_buffer();
                if (!r) {
                    latch.unlock();
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    latch.lock();
                }
                r = buffer.size() < barch::get_rpc_max_buffer();
                return r;
            });
            if (!ok) {
                ++statistics::repl::instructions_failed;
                barch::std_err("replication buffer size exceeded", buffer.size());
                return false;
            }

            std::lock_guard lock(this->latch);
            buffer.push_back('r');
            push_value(buffer, key);
            ++statistics::repl::remove_requests;
            ++messages;
            return true;
        }

        template<typename Stream>
        auto get_value() {
            return cmd_art_fun;
        }

        template<typename Stream>
        void send_art_fun(Stream& stream, const std::string& uname, const heap::vector<uint8_t>& to_send, uint32_t messages, size_t shard) {
            std::string name = barch::ks_undecorate(uname);
            uint32_t cmd = get_value<Stream>();
            uint32_t buffers_size = to_send.size();
            uint32_t sh = shard;
            writep(stream, uint8_t{0x00});
            writep(stream, cmd);
            writep(stream, sh);
            writep(stream, messages);
            writep(stream, (uint32_t)name.size());
            writep(stream, buffers_size);
            // the header must always be the same size

            writep(stream, name.data(), name.size());
            writep(stream, to_send.data(), to_send.size());
            stream.flush();

        }
        std::shared_ptr<source> create_source(const std::string& host, const std::string& port, size_t shard) {
            std::shared_ptr src = std::make_shared<sock_fun>();
            src->host = host;
            src->port = port;
            src->shard = shard;
            return src;
        }
        [[nodiscard]] bool client::begin_transaction() const {
            try {
            }catch (std::exception& e) {
                barch::std_err("failed to begin transaction", e.what());
                return false;
            }
            return true;
        }
        bool client::load(size_t shard) {
            try {
                if (!ping()) {
                    return false;
                }
                barch::std_log("load shard",shard);
                tcp::iostream stream(host, std::to_string(this->port));
                if (!stream) {
                    barch::std_err("failed to connect to remote server", host, this->port);
                    return false;
                }
                uint32_t cmd = cmd_stream;
                uint32_t s = shard;
                net_stat stat;
                writep(stream,uint8_t{0x00});
                writep(stream,cmd);
                writep(stream, s);
                auto ks = get_keyspace(ks_undecorate(this->name));
                if (!ks->get(shard)->retrieve(stream)) {
                    barch::std_err("failed to retrieve shard", shard);
                    return false;
                }
                stream.close();

                return true;
            }catch (std::exception& e) {
                barch::std_err("failed to load shard", shard, e.what());
                return false;
            }
        }
        bool client::find_insert(value_type key) {
            if (sources.empty()) return false;
            bool added = false;
            if (debug_repl == 1) {
                std_log("looking for key");
                log_encoded_key(key);
            }
            heap::vector<uint8_t> to_send, rbuff;
            to_send.push_back('f');// <-- to find a key
            push_value(to_send, key);
            // todo: r we choosing a random 1
            for (auto& source : sources) {
                try {

                    net_stat stat;

                    if (!source->is_open())
                        source->connect(source->host, source->port);

                    if (source->fail()) {
                        barch::std_err("failed to connect to remote server", host, this->port);
                        continue;
                    }
                    //
                    send_art_fun(*source, this->name, to_send, messages, shard);
                    if (!recv_buffer(*source, rbuff)) {
                        continue;
                    }
                    // TODO: check if this name exists
                    auto ks = get_keyspace(ks_undecorate(name));
                    // we are in a lock here (from the caller)
                    auto r = process_art_fun_cmd(ks, 0, *source, rbuff, false);
                    if (r.add_applied > 0) {
                        added = true;
                        break; // don't call the other servers - paxos would have us get a quorum
                    }
                }catch (std::exception& e) {
                    source->close();
                    barch::std_err("failed to write to stream", e.what(),"to",source->host,source->port);
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

                barch::std_err("failed to commit transaction", e.what());
                return false;
            }
            return true;
        }
        bool client::ping() const {
            try {
                tcp::iostream stream(host, std::to_string(this->port ));
                if (!stream) {
                    barch::std_err("failed to connect to remote server", host, this->port);
                    return false;
                }
                net_stat stats;
                uint32_t cmd = cmd_ping;
                writep(stream,uint8_t{0x00});
                writep(stream,cmd);
                int ping_result = 0;
                readp(stream, ping_result);
                if (ping_result != rpc_server_version) {
                    barch::std_err("failed to ping remote server", host, port, "ping returned",ping_result);
                    return false;
                }
                stream.close();
            }catch (std::exception &e) {
                barch::std_err("failed to ping remote server", host, port, e.what());
                return false;
            }
            return true;
        }
        static heap::vector<route> routes;
        void set_route(size_t shard, const route& destination) {
            if (routes.empty()) routes.resize(barch::get_shard_count().size());
            if (shard >= routes.size()) {
                barch::std_err("invalid shard",shard, routes.size());
                return;
            }
            routes[shard] = destination;
        }
        void clear_route(size_t shard) {
            if (routes.empty()) routes.resize(barch::get_shard_count().size());
            if (shard >= routes.size()) {
                barch::std_err("invalid shard",shard, routes.size());
                return;
            }
            routes[shard] = {};
        }
        route get_route(size_t shard) {
            if (routes.empty()) routes.resize(barch::get_shard_count().size());
            if (shard >= routes.size()) {
                barch::std_err("invalid shard",shard, routes.size());
                return {};
            }
            return routes[shard];
        }
    }
}

extern "C"{
    int ADDROUTE(caller& call, const arg_t& argv) {
        if (argv.size() != 4)
            return call.wrong_arity();
        Variable shard = argv[1];
        if (shard.ui() >= barch::get_shard_count().size())
            return call.push_error("invalid shard");
        Variable host = argv[2];
        Variable port = argv[3];
        if (port.i() <= 0 || port.i() >= 65536)
            return call.push_error("invalid port");
        if (host.s().empty())
            return call.push_error("no host");
        barch::repl::set_route(shard.i(), {host.s(),port.i()});
        return call.push_simple(host.s());
    }
    int ROUTE(caller& call, const arg_t& argv) {
        if (argv.size() != 2)
            return call.wrong_arity();
        size_t shard = atoi(argv[1].chars());
        if (shard >= barch::get_shard_count().size())
            return call.push_error("invalid shard");
        auto route = barch::repl::get_route(shard);
        call.start_array();
        call.push_simple(route.ip.c_str());
        call.push_ll(route.port);
        call.end_array(0);
//
        return 0;
    }
    int REMROUTE(caller& call, const arg_t& argv) {
        if (argv.size() != 2)
            return call.wrong_arity();
        size_t shard = atoi(argv[1].chars());
        if (shard >= barch::get_shard_count().size())
            return call.push_error("invalid shard");
        auto route = barch::repl::get_route(shard);
        barch::repl::clear_route(shard);
        call.start_array();
        call.push_simple(route.ip.c_str());
        call.push_ll(route.port);
        call.end_array(0);
        return 0;
    }
}
