//
// Created by teejip on 5/22/25.
//

#include "server.h"
#include "lzr_log.h"


#include <utility>
#include <deque>
#include <chrono>
#include "module.h"
#include "statistics.h"

#include "asio_includes.h"
#include <cstdio>
#include "barch_apis.h"
#include "swig_api.h"
//#include "rpc_caller.h"
#include "vk_caller.h"
#include "redis_parser.h"
#include "thread_pool.h"
#include "asio_resp_session.h"
#include "rpc/barch_session.h"
//#include "uring_resp_session.h"
#include "rpc/constants.h"
#include "cron.h"
#include "queue_service.h"
#include "auth_api.h"
#include "sharded_store.h"

namespace {
    /** a string on the binary protocol: u32 length, then the bytes - TODO 480 */
    void put_string(std::ostream& out, const std::string& v) {
        const uint32_t n = (uint32_t) v.size();
        out.write((const char*) &n, sizeof(n));
        out.write(v.data(), (std::streamsize) v.size());
    }
    bool get_string(std::istream& in, std::string& v, uint32_t largest = 1u << 20) {
        uint32_t n = 0;
        in.read((char*) &n, sizeof(n));
        if (!in || n > largest)
            return false;
        v.resize(n);
        in.read(v.data(), n);
        return (bool) in;
    }
    void put_u32(std::ostream& out, uint32_t v) {
        out.write((const char*) &v, sizeof(v));
    }
    bool get_u32(std::istream& in, uint32_t& v) {
        in.read((char*) &v, sizeof(v));
        return (bool) in;
    }

    /*
     * The sending side of RETRIEVE - TODO 480.
     *
     *   in:  space name, user, secret
     *   out: u32 status, 0 for ok; otherwise the reason as a string, and nothing
     *        more. Then u32 shard count, then each shard as send_frozen writes it.
     *
     * The login is the same one a RESP client makes, and it needs read rights on
     * the space, so this hands out nothing a client couldn't read anyway. Every
     * shard is frozen at one moment, the way a save does it, so the other side
     * gets one consistent space while writes here carry on. Every shard is let
     * go again whatever happens to the connection: one left frozen would hold up
     * every save and BEGIN after it.
     */
    void stream_space(std::iostream& stream) {
        std::string name, user, secret, why;
        if (!get_string(stream, name) || !get_string(stream, user) || !get_string(stream, secret)) {
            barch::err({"RETRIEVE: could not read the request"});
            return;
        }
        heap::vector<bool> acl;
        barch::key_space_ptr ks;
        if (!authenticate_user(user, secret, acl)) {
            why = "could not log in as " + (user.empty() ? std::string("default") : user);
        } else if (!barch::keyspace_exists(name)) {
            why = "no key space called " + name;
        } else {
            ks = barch::get_keyspace(name);
            const auto overrides = barch::read_space_overrides(user.empty() ? "default" : user);
            if (auto o = overrides.find(ks->get_canonical_name()); o != overrides.end())
                acl = barch::apply_overrides(acl, o->second);
            const auto read = get_category_map().at("read");
            if (read >= acl.size() || !acl[read])
                why = "no read rights on " + name;
        }
        // only built once there's a space: it throws on none, and that would drop
        // the connection before the reason got to the other side
        if (why.empty() && !barch::sharded_store(ks).freeze_space(nullptr))
            why = "the key space is in a transaction";
        if (!why.empty()) {
            put_u32(stream, 1);
            put_string(stream, why);
            stream.flush();
            barch::err({"RETRIEVE refused:", why});
            return;
        }
        const auto shards = barch::sharded_store(ks).shards();
        bool ok = true;
        size_t handed = 0;      // shards whose freeze this has let go
        try {
            put_u32(stream, 0);
            put_u32(stream, (uint32_t) shards.size());
            for (; handed < shards.size(); ) {
                const auto& shard = shards[handed];
                // once the stream has failed the rest are only let go
                const bool sent = ok && shard->send_frozen(&stream);
                if (!ok)
                    shard->send_frozen(nullptr);
                ++handed;
                ok = sent;
            }
            stream.flush();
        } catch (const std::exception& e) {
            barch::err({"RETRIEVE: sending", name, "failed:", e.what()});
            ok = false;
        }
        // only the ones not yet let go: one that was may be frozen by a save now,
        // and letting go of that would merge pages the save is still reading
        for (; handed < shards.size(); ++handed)
            shards[handed]->send_frozen(nullptr);
        if (!ok)
            barch::err({"RETRIEVE: could not send all of", name});
    }
}

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
    template<typename Proto>
    struct server_context {
        bool started = false;
        std::atomic<size_t> num_started = 0;

        thread_pool pool{(double)tcp_accept_pool_factor/100.0f};
        thread_pool asio_resp_pool{(double)resp_pool_factor/100.0f};
        thread_pool work_pool{asynch_proccess_workers};

        /*
         * Declared before io and workers, so it is destroyed after both.
         *
         * A session's socket belongs to one of these units, and its destructor
         * reaches into that unit's io_context services. A handler queued on
         * `workers` can be the last thing holding a session - an asynchronous batch
         * that never ran - and that handler is only destroyed when `workers` is.
         * With the units declared after workers they went first, and releasing the
         * session then ran a socket destructor against a context that no longer
         * existed. Whatever can hold a session has to die before the contexts its
         * sockets live in. See TODO 196.
         */
        std::vector<std::shared_ptr<asio_work_unit>> asio_resp_ios{};
        //std::vector<std::shared_ptr<uring_work_unit>> uring_resp_ios{};

        asio::io_context io{};
        asio::io_context workers{};
        exec_guard worker_guard {asio::make_work_guard(workers)};

        Proto::acceptor accept;
        asio::ssl::context ssl_context;
        //std::string interface;
        //uint_least16_t port;

        std::string description;
        std::atomic<size_t> threads_started = 0;
        std::atomic<size_t> resp_distributor{};
        std::atomic<size_t> asio_resp_distributor{};
        std::mutex session_latch;
        bool use_ssl = false;
        const bool use_uring = false;
        typedef asio::local::stream_protocol uds;
        std::vector<std::shared_ptr<resp_session<tcp::socket>>> tcp_sessions;
        std::vector<std::shared_ptr<resp_session<uds::socket>>> uds_sessions;
        moodycamel::LightweightSemaphore collector_control{};
        moodycamel::LightweightSemaphore collector_exit{};
        heap::unordered_set<size_t> open_pos_tcp;
        heap::unordered_set<size_t> open_pos_uds;
        std::thread session_collector;
        // must be called in mutext
        template<typename Sock_T>
        void register_(
            const std::shared_ptr<resp_session<Sock_T>>& session, // the session which must be registered
            heap::unordered_set<size_t>& open, //open positions
            std::vector<std::shared_ptr<resp_session<Sock_T>>> &sessions // array of sessions to register into
            ) {
            std::lock_guard lock(session_latch);

            if (open.empty()) {
                sessions.push_back(session);
            }else {
                size_t at = *open.begin();
                open.erase(at);
                sessions.at(at) = session;
            }
        }
        void register_session(const std::shared_ptr<resp_session<tcp::socket>>& session) {
            register_(session, open_pos_tcp, tcp_sessions);
        }
        void register_session(const std::shared_ptr<resp_session<uds::socket>>& session) {
            register_(session,open_pos_uds, uds_sessions);
        }
        /*
         * asio_resp_ios is filled in the constructor before any thread that could
         * call this exists, and is not touched again until stop(), so reading it
         * here needs no lock. See TODO 197.
         */
        std::shared_ptr<asio_work_unit> get_asio_unit() {
            // one fetch_add, not a load and a separate increment: two accept
            // threads reading the old value before either stored the new one both
            // took the same slot, which is not what round robin is for
            size_t r = asio_resp_distributor.fetch_add(1, std::memory_order_relaxed);
            return asio_resp_ios[r % asio_resp_ios.size()];
        }
        template<typename Sock_T>
        void collect_sessions(heap::unordered_set<size_t>& open_pos, std::vector<std::shared_ptr<resp_session<Sock_T>>> &sessions) {
            std::lock_guard lock(session_latch); // TODO: this can block new connections
            size_t pos = 0;
            for (auto &s : sessions) {
                if (s) {

                    auto fd =  s->socket_.lowest_layer().native_handle();
                    char buffer[8];
                    if (recv(fd, buffer, 1, MSG_PEEK | MSG_DONTWAIT) == 0) {
                        if (open_pos.contains(pos)) {
                            err({"position already taken - possible memory leak",pos});
                        }
                        open_pos.insert(pos);
                        s = nullptr;
                    }
                }
                ++pos;
            }


        }
        /**
         * append a CLIENT INFO style line for each open session in `sessions`.
         * Called on a copy, never on the live vector - see append_client_lines.
         */
        template<typename Sock_T>
        static void append_lines(std::string& out,
                                 const std::vector<std::shared_ptr<resp_session<Sock_T>>>& sessions) {
            for (const auto& s : sessions) {
                if (!s) continue; // a slot the collector has already swept
                try {
                    if (!s->socket_.lowest_layer().is_open()) continue;
                    out += s->get_info(s->socket_);
                } catch (std::exception&) {
                    // a peer can go away between the is_open test and reading its
                    // endpoint, which asio reports by throwing. that connection is
                    // leaving anyway, so drop its line rather than fail the command
                }
            }
        }

        /**
         * one line per open session, in the format CLIENT INFO already produces.
         *
         * The session vectors are appended to by the accept path and nulled out by the
         * collector, both under session_latch, so they are copied under the latch and
         * walked outside it - holding it across the walk would block new connections
         * for as long as the reply takes to build. Copying a vector of shared_ptr also
         * keeps every session alive for the duration of the walk, which matters
         * because building a line reads the socket.
         */
        void append_client_lines(std::string& out) {
            std::vector<std::shared_ptr<resp_session<tcp::socket>>> tcp_copy;
            std::vector<std::shared_ptr<resp_session<uds::socket>>> uds_copy;
            {
                std::lock_guard lock(session_latch);
                tcp_copy = tcp_sessions;
                uds_copy = uds_sessions;
            }
            append_lines(out, tcp_copy);
            append_lines(out, uds_copy);
        }

        void start_session_collector() {

            session_collector = std::thread([this]() {
                log({"starting session collector thread"});
                while (!this->collector_control.wait((int64_t)get_maintenance_poll_delay()*1000ll)) {
                    collect_sessions<tcp::socket>(open_pos_tcp,tcp_sessions);
                    collect_sessions<uds::socket>(open_pos_uds,uds_sessions);
                }
                log({"ending session collector thread"});
                collector_exit.signal(1);
            });
        }

        void stop() {
            if ( num_started == 0) {
                barch::err({"server not started"});
                return;
            }
            try {
                accept.close();
            }catch (std::exception& ) {}
            for (auto &proc: asio_resp_ios) {
                try {
                    proc->stop();
                }catch (std::exception& e) {
                    barch::err({"failed to stop resp io service", e.what()});
                }

            }
            try {
                io.stop();

            }catch (std::exception& e) {
                barch::err({"failed to stop io service", e.what()});
            }

            try {
                workers.stop();

            }catch (std::exception& e) {
                barch::err({"failed to workers service", e.what()});
            }

            work_pool.stop();
            pool.stop();

            asio_resp_pool.stop();
            collector_control.signal(1);
            collector_exit.wait();
            if (session_collector.joinable())
                session_collector.join();

            /*
             * Let the sessions go here, with every io_context still alive and the
             * threads that touch them already joined.
             *
             * A session owns a socket belonging to one of the asio_resp_ios
             * contexts, so its destructor reaches into that context's services. If
             * it is still held when ~server_context starts destroying members, that
             * destructor runs against a context that is already going away. The
             * collector is joined just above, so nothing is registering any more,
             * and the latch is only for symmetry with the rest of the access.
             * See TODO 196.
             */
            {
                std::lock_guard lock(session_latch);
                tcp_sessions.clear();
                uds_sessions.clear();
                open_pos_tcp.clear();
                open_pos_uds.clear();
            }

            //port = 0;
            started = false;
        }
        void handle_ssl(tcp::endpoint &ep) {
            auto ssl = ssl_stream(std::move(ep), ssl_context);
            if (statistics::repl::redis_sessions > get_max_resp_connections()) {
                ++statistics::repl::refused_connections;
                err({"Too many resp sessions/connections",statistics::repl::redis_sessions.load()});
            }else {
                auto session = std::make_shared<resp_session<ssl_stream>>(std::move(ssl),workers);
                session->start_ssl();
            }

        }
        template<typename UnknT>
        void handle_ssl(UnknT&) {

        }
        static void handle_assign(tcp::socket& socket, tcp::socket& endpoint) {
            socket.assign(tcp::v4(),endpoint.release());
            int flag = 1;
            setsockopt(socket.lowest_layer().native_handle(), IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
        }
        static void handle_assign(asio::local::stream_protocol::socket& socket, asio::local::stream_protocol::socket& endpoint) {
            socket.assign(asio::local::stream_protocol(), endpoint.release());
        }

        template<typename UnkProto>
        static void handle_assign(UnkProto::socket& , UnkProto::socket&) {
            err({"cannot assign unknown socket type"});
        }

        void start_accept() {
            try {

                accept.async_accept([this](asio::error_code error, Proto::socket endpoint) {
                    if (error) {
                        ++statistics::repl::accept_errors;
                        barch::err({"accept error",error.message(),error.value()});
                        return; // this happens if there are no threads
                    }
                    {
                        net_stat stat;
                        if (use_ssl) {
                            handle_ssl(endpoint);
                        }else {
                            process_data(endpoint);
                        }
                    }
                    start_accept();
                });

            }catch (std::exception& e) {
                barch::err({"failed to start/run replication server", e.what()});
            }
        }

        void start() {}

        void process_data(Proto::socket& endpoint) {
            try {
                char cs[1] ;
                //readp(stream, cs);
                endpoint.read_some(asio::buffer(cs,1));
                stream_read_ctr += 1;
                if (cs[0]) {
                    if (statistics::repl::redis_sessions > get_max_resp_connections()) {
                        ++statistics::repl::refused_connections;
                        err({"Too many resp sessions/connections",statistics::repl::redis_sessions.load()});
                        return;
                    }
                    auto unit = this->get_asio_unit();
                    typename Proto::socket socket (unit->io);
                    handle_assign(socket, endpoint);
                    auto session = std::make_shared<resp_session<typename Proto::socket>>(std::move(socket),workers, cs[0]);
                    register_session(session);

                    session->start();

                    return;
                }
                uint32_t cmd = 0;
                endpoint.read_some(asio::buffer(&cmd, sizeof(cmd)));
                if (cmd == cmd_barch_call) {
                    std::make_shared<barch_session<Proto>>(std::move(endpoint))->start();
                    return;
                }
                if (cmd == cmd_art_fun) {
                    barch::err({"command not implemented"});
                    return;
                }
                heap::vector<uint8_t> buffer{};
                barch::key_spec spec;
                typename Proto::iostream stream(std::move(endpoint));
                if (stream.fail())
                    return;
                switch (cmd) {
                    case cmd_ping:
                        try {
                            writep(stream,rpc_server_version);
                        }catch (std::exception& e) {
                            barch::err({"error",e.what()});
                        }

                        break;
                    case cmd_stream_space:
                        stream_space(stream);
                        break;
                    default:
                        barch::err({"unknown command", cmd});
                        return;
                }
            }catch (std::exception& e) {
                barch::err({"failed to read command", e.what()});
                return;
            }

        }
#if 0
        std::string get_password() const
        {
            return "test";
        }
#endif

        server_context(Proto::endpoint ep, bool ssl)
        :   accept(io, ep)
        ,   ssl_context(asio::ssl::context::tlsv13)
        ,   use_ssl(ssl) {

            start_session_collector();
            if (use_ssl) {
                ssl_context.set_options(
                asio::ssl::context::default_workarounds
                | asio::ssl::context::no_tlsv1_1
                | asio::ssl::context::single_dh_use);
#if 0
                ssl_context.set_password_callback(std::bind(&server_context::get_password, this));
#endif
                ssl_context.use_certificate_chain_file(get_tls_pem_certificate_chain_file());
                ssl_context.use_private_key_file(get_tls_private_key_file(), asio::ssl::context::pem);
                ssl_context.use_tmp_dh_file(get_tls_tmp_dh_file());
            }

            /*
             * Everything a connection needs is built here, on this thread, before
             * any thread that reads it exists.
             *
             * The accept threads used to be started first, while asio_resp_ios was
             * still empty, and a connection landing in that window reached
             * get_asio_unit(). Three ways for that to end, all of them bad: before
             * the resize the vector is empty and `% asio_resp_ios.size()` is a
             * divide by zero; during the resize the read walks a buffer that is
             * being reallocated; after it, the slot is an empty shared_ptr and
             * process_data dereferences it as `unit->io`. The barrier below did not
             * help, because `++num_started` was counted before the slot it counts
             * was written, so it could fall through with slots still empty.
             *
             * Filling the vector before creating any reader takes the whole
             * question away - constructing a thread happens-after everything the
             * constructing thread did, so get_asio_unit needs no synchronisation of
             * its own. See TODO 197.
             */
            num_started = 0;
            barch::log({"resp pool size",asio_resp_pool.size()});
            asio_resp_ios.resize(asio_resp_pool.size());
            for (auto &unit : asio_resp_ios) {
                unit = std::make_shared<asio_work_unit>();
            }
            // run() never returns, so the count has to be taken before it
            asio_resp_pool.start([this](size_t tid) -> void {
                ++num_started;
                asio_resp_ios[tid]->run();
            });
            while (num_started != asio_resp_pool.size()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }

            // workers before accepts: a session posts its asynchronous batches there
            work_pool.start([this](size_t tid) -> void{
                workers.run();
                barch::log({"worker stopped using thread",tid});
            });

            /*
             * Accepting comes last. start_accept() only arms the handler on `io`;
             * nothing completes until a pool thread calls io.run(), so this is the
             * line that opens the server to traffic and everything above it is
             * ready by the time it runs.
             */
            start_accept();
            /*
             * addr and prot_name are worked out here and captured by value. `ep`
             * is a by-value constructor parameter, so it lives on the stack frame
             * of whichever thread called the constructor - capturing it by
             * reference left the accept threads reading that frame long after it
             * had returned and been reused by the next call on that thread. See
             * TODO 214.
             */
            auto accept_addr = address_off(ep);
            auto accept_proto = proto_name(ep);
            pool.start([this,addr = accept_addr,prot_name = accept_proto](size_t tid) -> void{
                asio::dispatch(io ,[this,tid,addr,prot_name]() {
                    log({use_ssl ? "TLS/SSL":prot_name,"connections accepted on",addr,"using thread",tid});
                });
                io.run();
                log({prot_name,"server stopped on", addr,"using thread",tid});
            });

            started = true;
        }
        ~server_context() {
            std::lock_guard f(srv_mut());
            stop();
        }
    };


    static std::shared_ptr<server_context<tcp>>& get_srv() {
        static std::shared_ptr<server_context<tcp>> srv = nullptr;
        return srv;
    }

    static std::shared_ptr<server_context<tcp>>& get_srv_ssl() {
        static std::shared_ptr<server_context<tcp>> srv = nullptr;
        return srv;
    }

    static std::shared_ptr<server_context<asio::local::stream_protocol>>& get_srv_unix() {
        static std::shared_ptr<server_context<asio::local::stream_protocol>> srv = nullptr;
        return srv;
    }
    template<typename Proto>
    std::string handle_start(typename Proto::endpoint ep, bool ssl, std::shared_ptr<server_context<Proto>>& s) {
        s = nullptr;
        try {
            barch::set_configuration_value("static_bloom_filter", barch::get_static_bloom_filter() ? "on":"off");
            s = std::make_shared<server_context<Proto>>(ep, ssl);
        }catch (std::exception& e) {
            barch::err({"failed to start server", e.what()});
            // handed back rather than swallowed: barchd used to log this and then
            // "listening" and run on with no listener at all - TODO 441
            return e.what();
        }
        return {};
    }

    /*
     * The address to listen on - TODO 442. The endpoint used to be built from the
     * port alone, so --bind and START's host were logged and then ignored, and every
     * server listened on 0.0.0.0. Empty still means every interface; "localhost" is
     * the loopback address, since make_address doesn't resolve names.
     */
    static asio::ip::address listen_address(const std::string& interface) {
        if (interface.empty() || interface == "*")
            return asio::ip::address_v4::any();
        if (interface == "localhost")
            return asio::ip::address_v4::loopback();
        asio::error_code ec;
        auto a = asio::ip::make_address(interface, ec);
        if (ec)
            throw_exception<std::invalid_argument>(
                ("'" + interface + "' is not an address to listen on").c_str());
        return a;
    }
    void handle_stop(std::shared_ptr<server_context<tcp>>& s) {

        s = nullptr;
    }
    /*
     * Whichever listener is up, preferring plain tcp because that is the one barchd
     * builds. There is one worker context per server_context, not one per process, so
     * a caller that wants "the server's workers" has to be told which server that is,
     * and this is that answer.
     */
    asio::io_context* server::worker_io() {
        std::unique_lock l(srv_mut());
        if (auto s = get_srv()) return &s->workers;
        if (auto s = get_srv_ssl()) return &s->workers;
        if (auto s = get_srv_unix()) return &s->workers;
        return nullptr;
    }

    std::string server::start(const std::string& interface, uint_least16_t port, bool ssl) {
        std::unique_lock l(srv_mut());
        // the scheduler holds a timer on a worker context, and handle_start below
        // destroys whatever context is there before building the new one. The
        // queue consumer holds one too - TODO 366
        barch::cron::stop();
        barch::mq::stop();
        std::string failed;
        try {
            if (port == 0) {
                ::unlink(interface.c_str());
                asio::local::stream_protocol::endpoint ep(interface);
                failed = handle_start(ep, false, get_srv_unix());
            }else if (ssl) {
                auto ep = tcp::endpoint(listen_address(interface), port);
                failed = handle_start(ep, true, get_srv_ssl());
            }else {
                auto ep = tcp::endpoint(listen_address(interface), port);
                failed = handle_start(ep, false, get_srv());
            }
        } catch (std::exception& e) {
            barch::err({"failed to start server", e.what()});
            failed = e.what();
        }
        /*
         * Cron is armed here rather than where barchd calls cron::start(), because a
         * schedule needs a worker context to run on and that context only exists once
         * a listener does. cron::start() before this point is a no-op that leaves the
         * jobs on disk untouched, which is the accepted compromise in TODO 271: a
         * process with no server runs no schedules.
         */
        barch::cron::start();
        barch::mq::start();
        return failed;
    }

    void server::stop() {

        std::unique_lock l(srv_mut());
        // before the contexts go: both timers live in one of them
        barch::cron::stop();
        barch::mq::stop();
        handle_stop(get_srv());
        handle_stop(get_srv_ssl());
    }

    void server::list_clients(caller& call) {
        std::string out;
        {
            // srv_mut guards the server pointers themselves; each context takes its own
            // session latch while it copies
            std::unique_lock l(srv_mut());
            if (auto s = get_srv()) s->append_client_lines(out);
            if (auto s = get_srv_ssl()) s->append_client_lines(out);
            if (auto s = get_srv_unix()) s->append_client_lines(out);
        }
        // redis answers CLIENT LIST with one bulk string, the lines separated by
        // newlines, not with an array. get_info already terminates each line
        call.push_string(out);
    }
    struct module_stopper {
        module_stopper() = default;
        ~module_stopper() {
            repl::stop_repl();
            server::stop();
        }
    };
    //static module_stopper _stopper;
    namespace repl {
        template<typename Proto>
        class rpc_impl : public rpc {
        private:
            std::mutex latch{};
            std::string host;
            int port;
            asio::io_context ioc{};

            Proto::socket s;
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
            template<typename OC>
            void do_connect(tcp::socket & sock, OC&& once_connected) {
                tcp::resolver resolver{ioc};
                auto resolution = resolver.resolve(host,std::to_string(port));
                asio::async_connect(sock, resolution, once_connected);

            }
            template<typename OC>
            void do_connect(asio::local::stream_protocol::socket & sock, OC&& once_connected) {
                typename Proto::endpoint ep(host);
                try {
                    sock.connect(ep);
                    std::error_code ec{};
                    once_connected(ec,ep);
                }catch (std::exception& e) {
                    barch::err({"error connecting to[",host,"]",e.what()});
                }

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
                        auto once_connected = [this](const std::error_code& ec, typename Proto::endpoint unused(ep)) {
                            if (!ec) {
                                uint32_t cmd = cmd_barch_call;
                                writep(stream,uint8_t{0x00});
                                writep(stream, cmd);
                            }
                            error = ec;
                        };
                        do_connect(s, once_connected);
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
                        barch::err({reply_length,"!=",buffers_size});
                        throw_exception<std::length_error>("invalid reply length");
                    }
                    // get_variable answers with the offset of the *next* variable, so
                    // the loop must not advance i as well. It used to be a for loop with
                    // an i++, which stepped one byte past each value after the first -
                    // so the second variable onwards was decoded from a type byte taken
                    // out of the middle of the preceding payload. The first element of a
                    // reply was right and everything after it came back as whatever that
                    // stray byte happened to mean, usually a bool or a double. Only a
                    // reply with more than one value could show it, which is why every
                    // remote binding that answers with an array was wrong and nothing
                    // noticed. See TODO 44
                    size_t i = 0;
                    while (i < buffers_size) {
                        auto v = get_variable(i, replies);
                        result.emplace_back(v.first);
                        if (v.second <= i) {
                            throw_exception<std::runtime_error>("reply decode made no progress");
                        }
                        i = v.second;
                    }
                    stream.clear();
                }catch (std::exception& e) {
                    barch::err({"call failed [", e.what(),"] to",host,port,"because [",error.message(),error.value(),"]"});
                    stream.clear();
                    /*
                     * Closed, so the next call connects again - TODO 485. Only a
                     * timeout used to close it (run_to). A peer that went away
                     * answers a read with EOF or a reset, which left the socket
                     * open, so `is_open()` skipped the reconnect and every call
                     * after it failed on the dead connection, for good. A socket
                     * a call failed on can't be trusted anyway: a reply may still
                     * be on its way and would be read as the next call's.
                     */
                    std::error_code ignored;
                    s.close(ignored);
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
            if (port == 0) {
                return std::make_shared<rpc_impl<asio::local::stream_protocol>>(host, port);
            }
            return std::make_shared<rpc_impl<tcp>>(host, port);
        }
        /**
         * The writes the embedded module publishes, and where they go.
         *
         * Every key space's maintenance thread calls distribute(), so it has to
         * be safe for several at once - and it wasn't, in three ways (TODO 485):
         *
         *   - two threads could each take a batch and send them side by side, so
         *     an older SET could land last. Now one thread sends at a time, and
         *     it takes the buffer while it holds that right, so batches go out in
         *     the order they were made. A thread that finds a send under way
         *     leaves it be; the sender picks up anything new next time round;
         *   - a failed call dropped the rest of its batch for that destination.
         *     Now each destination has its own queue, and a write leaves it only
         *     once the destination has answered. After a failure the queue waits
         *     and is tried again, backing off, from the write that failed;
         *   - nothing bounded the buffer. A destination's queue is capped, and
         *     one that falls that far behind is dropped with an error, since it
         *     needs a full resync rather than a backlog.
         *
         * Retrying is at least once, not exactly once: a call whose reply was
         * lost may have been applied, and is sent again. That's harmless for a
         * SET or a delete and not for an APPEND, which is the price of not
         * dropping writes without sequence numbers on the wire.
         */
        struct consumers {
            // a destination this far behind is dropped rather than queued for
            static constexpr size_t max_pending_bytes = 256ull << 20;

            struct destination {
                std::shared_ptr<rpc> link;
                std::deque<std::vector<std::string>> pending{};
                size_t pending_bytes{0};
                uint32_t failures{0};
                std::chrono::steady_clock::time_point retry_at{};
            };

            std::mutex m;               // buffer and destinations
            std::mutex sending;         // one sender at a time, which is what keeps the order
            heap::vector<std::vector<std::string>> buffer;
            heap::string_map<std::shared_ptr<destination>> destinations;
            // read by distribute() outside the mutex that guards the rest of
            // this, and set from another thread on the way out. See TODO 213.
            std::atomic<bool> exit{false};
            consumers() {

            }
            ~consumers() {
                stop();
            }
            static size_t bytes_of(const std::vector<std::string>& params) {
                size_t n = 0;
                for (const auto& p : params)
                    n += p.size() + sizeof(std::string);
                return n;
            }
            void distribute() {
                if (exit) return;
                std::unique_lock send(sending, std::try_to_lock);
                if (!send) return;      // someone else is sending, and will get to these
                heap::vector<std::vector<std::string>> todo;
                heap::vector<std::pair<std::string, std::shared_ptr<destination>>> active;
                {
                    std::lock_guard l(m);
                    if (destinations.empty()) return;
                    todo.swap(buffer);
                    for (const auto& d : destinations)
                        active.emplace_back(d.first, d.second);
                }
                // `pending` is only touched while `sending` is held, so no `m`
                for (auto& [name, dest] : active) {
                    for (const auto& p : todo) {
                        dest->pending.push_back(p);
                        dest->pending_bytes += bytes_of(p);
                    }
                    if (dest->pending_bytes > max_pending_bytes) {
                        statistics::repl::instructions_failed += dest->pending.size();
                        barch::err({"replication to", name, "is", dest->pending_bytes,
                                    "bytes behind, past", max_pending_bytes, "- dropping",
                                    dest->pending.size(), "queued writes. It needs a full resync"});
                        dest->pending.clear();
                        dest->pending_bytes = 0;
                    }
                }
                todo.clear();
                const auto now = std::chrono::steady_clock::now();
                for (auto& [name, dest] : active) {
                    if (now < dest->retry_at) continue;
                    heap::vector<Variable> results{};
                    while (!dest->pending.empty()) {
                        if (exit) return;
                        results.clear();
                        const auto r = dest->link->call(results, dest->pending.front());
                        if (r.net_error) {
                            // keep it, and everything behind it, for the retry
                            ++dest->failures;
                            const auto wait = std::min<int64_t>(5000, 100ll << std::min<uint32_t>(dest->failures, 6));
                            dest->retry_at = std::chrono::steady_clock::now() + std::chrono::milliseconds(wait);
                            if (dest->failures == 1)
                                barch::err({"call to", name, "failed -", dest->pending.size(),
                                            "writes kept for when it answers"});
                            break;
                        }
                        dest->failures = 0;
                        dest->pending_bytes -= std::min(dest->pending_bytes, bytes_of(dest->pending.front()));
                        dest->pending.pop_front();
                    }
                }
                size_t queued = 0;
                for (auto& [name, dest] : active)
                    queued += dest->pending.size();
                statistics::repl::out_queue_size = queued;
            }
            void add(const std::string &host, int port) {
                std::lock_guard l(m);
                std::string addr = host;
                addr += ":";
                addr += std::to_string(port);
                auto d = std::make_shared<destination>();
                d->link = create(host,port);
                destinations[addr] = d;
            }
            void consume(const std::vector<std::string>& params) {
                std::lock_guard l(m);
                if (destinations.empty()) return;
                buffer.push_back(params);
            }
            bool any() {
                std::lock_guard l(m);
                return !destinations.empty();
            }
            void stop() {
                std::lock_guard l(m);
                destinations.clear();
                buffer.clear();
                exit = true;
            }
        };
        consumers& dests() {
            static consumers d;
            return d;
        }
        void publish(const std::string& host, int port) {
            dests().add(host, port);
        }
        bool has_destinations() {
            return dests().any();
        }
        void call(const std::vector<std::string>& params){
            dests().consume(params);
        }
        void distribute() {
            dests().distribute();
        }


        std::shared_ptr<source> create_source(const std::string& host, const std::string& port, size_t shard) {
            std::shared_ptr src = std::make_shared<sock_fun>();
            src->host = host;
            src->port = port;
            src->shard = shard;
            return src;
        }
        temp_client::~temp_client() {

        }

        bool temp_client::receive_space(const std::shared_ptr<key_space>& ks, const std::string& user,
                                        const std::string& secret, std::string& err) {
            const auto shards = ks->get_shards();
            const auto drop = [&]() {
                for (const auto& s : shards)
                    if (s) s->drop_received();
            };
            try {
                if (!ping()) {
                    err = "could not reach " + host + ":" + std::to_string(port);
                    return false;
                }
                tcp::iostream stream(host, std::to_string(this->port));
                if (!stream) {
                    err = "could not connect to " + host + ":" + std::to_string(port);
                    return false;
                }
                const uint8_t binary = 0x00;
                stream.write((const char*) &binary, 1);
                put_u32(stream, cmd_stream_space);
                put_string(stream, ks->get_canonical_name());
                put_string(stream, user);
                put_string(stream, secret);
                stream.flush();
                uint32_t status = 1, count = 0;
                if (!get_u32(stream, status)) {
                    err = "no answer from " + host + ":" + std::to_string(port);
                    return false;
                }
                if (status != 0) {
                    std::string why;
                    get_string(stream, why);
                    err = "the other side refused: " + why;
                    return false;
                }
                if (!get_u32(stream, count)) {
                    err = "the answer ended early";
                    return false;
                }
                if (count != shards.size()) {
                    // shard files only mean something at the count they were written at
                    err = "the other side has " + std::to_string(count) + " shards and this one "
                          + std::to_string(shards.size());
                    return false;
                }
                for (const auto& s : shards) {
                    if (!s || !s->receive_files(stream, err)) {
                        if (err.empty()) err = "a shard is missing here";
                        drop();
                        return false;
                    }
                }
                return true;
            } catch (std::exception& e) {
                err = std::string("retrieving failed: ") + e.what();
                drop();
                return false;
            }
        }

        bool temp_client::ping() const {
            try {
                tcp::iostream stream(host, std::to_string(this->port ));
                if (!stream) {
                    barch::err({"failed to connect to remote server", host, this->port});
                    return false;
                }
                net_stat stats;
                uint32_t cmd = cmd_ping;
                writep(stream,uint8_t{0x00});
                writep(stream,cmd);
                int ping_result = 0;
                readp(stream, ping_result);
                if (ping_result != rpc_server_version) {
                    barch::err({"failed to ping remote server", host, port, "ping returned",ping_result});
                    return false;
                }
                stream.close();
            }catch (std::exception &e) {
                barch::err({"failed to ping remote server", host, port, e.what()});
                return false;
            }
            return true;
        }
        static std::mutex& route_lock() {
            static std::mutex rl;
            return rl;
        }
        static heap::vector<route>& get_routes() {
            static heap::vector<route> routes;
            return routes;
        }
        static void resize_routes(size_t shard) {
            if (get_routes().size() <= shard)
                get_routes().resize(std::max<size_t>(shard + 1, barch::get_shard_count().size()));
        }
        void set_route(size_t shard, const route& destination) {
            std::unique_lock rl(route_lock());
            resize_routes(shard);
            if (shard >= get_routes().size()) {
                barch::err({"invalid shard",shard, get_routes().size()});
                return;
            }
            get_routes()[shard] = destination;
        }
        void clear_route(size_t shard) {
            std::unique_lock rl(route_lock());
            resize_routes(shard);
            if (shard >= get_routes().size()) {
                barch::err({"invalid shard",shard, get_routes().size()});
                return;
            }
            get_routes()[shard] = {};
        }
        route get_route(size_t shard) {
            std::unique_lock rl(route_lock());
            resize_routes(shard);
            return get_routes()[shard];
        }
    }
}

extern "C"{
    int ADDROUTE(caller& call, const arg_t& argv) {
        if (argv.size() != 4)
            return call.wrong_arity();
        Variable shard = argv[1];
        if (shard.ui() >= call.kspace()->get_shard_count())
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
        if (shard >= call.kspace()->get_shard_count())
            return call.push_error("invalid shard");
        auto route = barch::repl::get_route(shard);
        call.start_array();
        call.push_simple(route.ip.c_str());
        call.push_ll(route.port);
        call.end_array();

        return 0;
    }
    int REMROUTE(caller& call, const arg_t& argv) {
        if (argv.size() != 2)
            return call.wrong_arity();
        size_t shard = atoi(argv[1].chars());
        if (shard >= call.kspace()->get_shard_count())
            return call.push_error("invalid shard");
        auto route = barch::repl::get_route(shard);
        barch::repl::clear_route(shard);
        call.start_array();
        call.push_simple(route.ip.c_str());
        call.push_ll(route.port);
        call.end_array();
        return 0;
    }
}
namespace barch {
    namespace repl {
        void stop_repl() {
            dests().stop();
        }
    }
}
