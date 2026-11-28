//
// Created by teejip on 9/7/25.
//

#ifndef BARCH_ASIO_RESP_SESISON_H
#define BARCH_ASIO_RESP_SESISON_H
#include "park_core.h"
#include "stream_core.h"
#include <sys/socket.h>
#include <cerrno>
#include <atomic>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <variant>

#include "abstract_session.h"
#include "test_jitter.h"
#include "replace_file.h"
#include "cluster_hooks.h"
#include "asio_includes.h"
#include "redis_parser.h"
#include "rpc_caller.h"
#include "traffic.h"
#include "netstat.h"
#include "vector_stream.h"
#include "constants.h"
#include "time_conversion.h"
#include "rpc/proto_info.h"
#include "foreign/foreign.h"
#include "function_api.h"
#include "sharded_store.h"
#include "dictionary_compressor.h"
#include "keys.h"
#include "key_hash.h"
#include "key_type.h"
#include "drop_core.h"
namespace barch {
    extern std::atomic<uint64_t> client_id;
    template<typename TSock>
    class resp_session :
        public abstract_session,
        public std::enable_shared_from_this<resp_session<TSock>>
    {
    public:
        //typedef TSock sock_t;
        resp_session(const resp_session&) = delete;
        resp_session& operator=(const resp_session&) = delete;

        template<typename sock_T>
        resp_session(sock_T socket, asio::io_context &workers, asio::io_context &raft_workers)
        : socket_(std::move(socket)), timer( socket_.get_executor()), workers(workers), raft_workers(raft_workers)
        {
            caller.info_fun = [this]() -> std::string {
                return get_info(socket_);
            };
            bind_socket_writer();
            caller.set_context(ctx_resp);
            // INFO transactions names a transaction's session by CLIENT LIST's id - TODO 720
            caller.tx_state->who = "client:" + std::to_string(id);
            // a key lock that someone else holds parks the session - TODO 721
            caller.lock_mode = rpc_caller::lock_wait::park;
            //asio::socket_base::send_buffer_size option(65536); // or larger
            //socket_.set_option(option);

            ++statistics::repl::redis_sessions;
        }
        template<typename sock_T>
        resp_session(sock_T socket, asio::io_context &workers, asio::io_context &raft_workers, char init_char)
            : socket_(std::move(socket)), timer(socket_.get_executor()), workers(workers), raft_workers(raft_workers)
        {
            parser.init(init_char);
            caller.info_fun = [this]() -> std::string {
                return get_info(socket_);
            };
            bind_socket_writer();
            caller.set_context(ctx_resp);
            // INFO transactions names a transaction's session by CLIENT LIST's id - TODO 720
            caller.tx_state->who = "client:" + std::to_string(id);
            // a key lock that someone else holds parks the session - TODO 721
            caller.lock_mode = rpc_caller::lock_wait::park;
            ++statistics::repl::redis_sessions;
        }

        ~resp_session() {
            --statistics::repl::redis_sessions;
            // whatever this session still had counted towards max_memory - TODO 493
            account(omem_send, 0);
            account(omem_stream, 0);
            account(qbuf_bytes, 0);
        }
        void start_ssl() {
            auto self(this->shared_from_this());
            socket_.async_handshake(asio::ssl::stream_base::server,
            [this, self](const std::error_code& error){
                if (!error) {
                    do_read();
                    return;
                }
                /*
                 * The server holds a TLS session too now, so it no longer dies
                 * with this handler. A peer that stays connected after a bad
                 * handshake peeks as open, not 0, and kept its fd. Shut it and
                 * let the collector drop it, as the read paths do - TODO 859.
                 */
                std::error_code ignored;
                socket_.lowest_layer().shutdown(asio::socket_base::shutdown_both, ignored);
                let_go.store(true, std::memory_order_release);
            });
        }
        void start()
        {
            do_read();
        }
        // socket independent function to get info for session
        std::string get_info_l(const std::string& laddress, const std::string& raddress ) const {
            uint64_t seconds = (art::now() - created)/1000;
            const uint64_t omem = omem_send.load(std::memory_order_relaxed)
                                + omem_stream.load(std::memory_order_relaxed);
            const uint32_t oll = oll_send.load(std::memory_order_relaxed)
                               + oll_stream.load(std::memory_order_relaxed);
            std::string r =
                "id="+std::to_string(this->id)+" addr="+raddress+ " "
                "laddr="+laddress+" fd="+"10"+ " "
                "name="+""+" age="+std::to_string(seconds)+" "+
                "idle=0 flags=N capa= db=0 sub=0 psub=0 ssub=0 "+
                "multi=-1 watch=0 qbuf=0 qbuf-free=0 argv-mem=10 multi-mem=0 "+
                // obl is 0 for good: there's no fixed reply buffer here. omem and oll are
                // the send queue and a streamed KEYS reply - TODO 492
                "rbs=1024 rbp=0 obl=0 oll="+std::to_string(oll)+" omem="+std::to_string(omem)+" "+
                "tot-mem="+std::to_string(rpc_io_buffer_size+parser_peak.load(std::memory_order_relaxed)+omem)+" "+
                "events=r cmd=client|info user="+caller.get_user()+" redir=-1 "+
                "resp="+std::to_string(caller.get_protocol())+" lib-name= lib-ver= "+
                // SCAN cursors this connection is holding, and what they cost. An
                // abandoned scan keeps one alive until the connection closes, so a
                // client that leaks them can see it here
                "iters="+std::to_string(caller.iteration_count())+" "+
                "iters-mem="+std::to_string(caller.iteration_memory())+" "+
                "tot-net-in="+ std::to_string(bytes_recv.load(std::memory_order_relaxed))+ " " +
                "tot-net-out=" + std::to_string(bytes_sent.load(std::memory_order_relaxed))+ " " +
                "tot-cmds=" + std::to_string(calls_recv.load(std::memory_order_relaxed)) + "\n";
            return r;
        }

        template<typename  LowestLType>
        std::string get_info_t(const LowestLType& sock) const {


            std::string laddress = local_address_off(sock);
            std::string raddress = remote_address_off(sock);
            return get_info_l(laddress, raddress);
        }

        std::string get_info(const TSock& sock) const {
            // a TLS session's addresses are its TCP socket's
            if constexpr (std::is_same_v<TSock, ssl_stream>)
                return get_info_t(sock.next_layer());
            else
                return get_info_t(sock);
        }

        /**
         * Hand the turn to whoever is next on this key.
         *
         * Called once a waiter has taken what it wanted, because there may be more left
         * for the one behind it - a client that asked for one member out of five leaves
         * four. Only after being served: a waiter that found nothing means the key is
         * empty, and waking the next one would only send it round the same loop.
         */
        void pass_the_turn(const barch::key_space_ptr& space, size_t shard_index,
                           const std::string& key) {
            if (!space) return;
            auto t = space->get(shard_index);
            if (!t) return;
            std::unique_lock lck(t->get_latch());
            t->call_unblock(key);
        }

        void do_block_continue(const std::string& woken_by) override {
            if (caller.has_blocks()) {
                const uint64_t gen = park_gen.load();
                auto self(this->shared_from_this());
                // where to send the turn next, read before erase_blocks clears them.
                // The latch the waker holds keeps them: erase_blocks takes it first
                barch::key_space_ptr woken_space;
                size_t woken_shard = 0;
                for (auto& d : caller.get_blocks()) {
                    if (d.key == woken_by) {
                        woken_space = d.space;
                        woken_shard = d.shard_index;
                        break;
                    }
                }
                // post, not execute: SET/FOREIGN_MISS/finish_fetch call this
                // while the shard write lock is still held. execute can run
                // the waiter on this thread, and reply_after_wait takes the
                // same lock.
                asio::post(this->socket_.get_executor(),
                           [this,self,woken_by,woken_space,woken_shard,gen]() {
                    note_mode();
                    /*
                     * The timer got there first: it answered, or parked again. The
                     * wake this took off the key's list goes on to the next waiter
                     * there instead - TODO 878.
                     */
                    const auto what = park::on_wake(still_parked(gen), waiter_deadline, art::now());
                    if (what == park::wake::pass_on) {
                        pass_the_turn(woken_space, woken_shard, woken_by);
                        return;
                    }
                    /*
                     * The timer and the deadline are this thread's, so they're touched
                     * here and not on the waking thread - TODO 883. A steady_timer
                     * isn't safe to cancel from one thread while another arms it.
                     */
                    timer.cancel();
                    // the fetch landed after the waiter deadline: this GET is still a
                    // timeout. The value stays for the next waiter, who gets the turn
                    if (what == park::wake::time_out) {
                        do_block_to();
                        pass_the_turn(woken_space, woken_shard, woken_by);
                        return;
                    }
                    waiter_deadline = 0;
                    int r = caller.call_blocks();
                    // the waiter looked and there was nothing for it. Stay parked: the
                    // blocks are still on the caller, but call_unblock took this session
                    // out of the key's list when it woke us, so it has to go back in.
                    // Only the timeout answers a waiter that never got anything
                    if (caller.take_block_retry()) {
                        // the timer first, as the first park does: once back on the
                        // list, another thread can wake this session and cancel the
                        // timer, which must not be half set up then - TODO 726
                        start_block_to(true);
                        add_caller_blocks();
                        return;
                    }
                    write_result(caller, stream, r);
                    erase_blocks();
                    pass_the_turn(woken_space, woken_shard, woken_by);
                    // the reply has to be out before anything else starts writing, so
                    // the rest of an interrupted batch waits on this one completing
                    send(stream);
                    when_sent([this, self]() {
                        resume_after_blocks();
                    });
                });
            }
        }

    private:

        /** the category a stored function is gated on, as a vector to compare against */
        static const heap::vector<bool>& function_cats() {
            static heap::vector<bool> cats = [] {
                catmap m;
                m["function"] = true;
                m["data"] = true;
                return cats2vec(m);
            }();
            return cats;
        }

        static bool is_authorized(const heap::vector<bool>& func,const heap::vector<bool>& user) {
            size_t s = std::min<size_t>(user.size(),func.size());
            if (s < func.size()) return false;
#if defined(__GLIBCXX__)
            // a word at a time - TODO 845. A bit at a time was 4% of a GET.
            // libstdc++ (MinGW's too) packs vector<bool> into words from begin()
            const auto* f = func.begin()._M_p;
            const auto* u = user.begin()._M_p;
            constexpr size_t W = sizeof(*f) * 8;
            const size_t full = s / W;
            for (size_t i = 0; i < full; ++i)
                if (f[i] & ~u[i]) return false;
            if (const size_t rest = s % W) {
                const auto mask = (std::remove_cv_t<std::remove_reference_t<decltype(*f)>>) ((1ull << rest) - 1);
                if (f[full] & ~u[full] & mask) return false;
            }
            return true;
#endif
            for (size_t i = 0; i < s; ++i) {
                if (func[i] && !user[i])
                    return false;
            }
            return true;
        }
        template<typename Stream>
        static void write_result(rpc_caller& local_caller, Stream& local_stream, int32_t r) {
            if (r < 0) {
                // a failed call is a call error for monitoring - the rpc_caller
                // catch already bumped exceptions_raised, this is the reply-side
                // half of the same event. See TODO 391.
                ++statistics::repl::request_errors;
                if (!local_caller.errors.empty())
                    redis::rwrite(local_stream, error{local_caller.errors[0]});
                else
                    redis::rwrite(local_stream, error{"null error"});
            } else if (!local_caller.reply_sent) {
                redis::rwrite(local_stream, local_caller.results, local_caller.get_protocol());
            }
        }

        struct asynch_call_context {
            asynch_call_context(const rpc_caller& caller, barch_function f, const std::vector<redis::string_param_t>& params, const std::string &cn )
                : caller(caller), f(std::move(f)), params(params.begin(), params.end()), cn(cn) {
                // off the I/O thread, a leader read may wait for a read index - TODO 785
                this->caller.reads_wait = true;
                this->caller.read_defer = false;
            }
            rpc_caller caller{};
            barch_function f{};
            vector_stream stream{}; // the stream buffer needs to stau alive while the call completes
            std::vector<std::string> params{};
            std::string cn;
            // waits on a Raft commit, so it runs on raft_workers - TODO 626
            bool raft{false};
        };
        typedef std::shared_ptr<asynch_call_context> asynch_call_context_ptr;
        typedef std::shared_ptr<heap::vector<asynch_call_context_ptr>> asynch_batch_ptr;

        enum class read_flight { none, chain, park };
        struct phase_reading { read_flight read{read_flight::none}; };
        struct phase_paused { bool park_read_out{false}; };
        struct phase_parked { bool park_read_out{false}; };
        struct phase_batch {
            bool park_read_out{false};
            asynch_batch_ptr pending_batch{};
            size_t pending_at{0};
        };
        using chain_state = std::variant<phase_reading, phase_paused, phase_parked, phase_batch>;

        [[nodiscard]] bool has_read_out() const {
            return std::visit([](const auto& s) -> bool {
                using T = std::decay_t<decltype(s)>;
                if constexpr (std::is_same_v<T, phase_reading>)
                    return s.read != read_flight::none;
                else
                    return s.park_read_out;
            }, chain);
        }

        [[nodiscard]] bool park_read_out() const {
            return std::visit([](const auto& s) -> bool {
                using T = std::decay_t<decltype(s)>;
                if constexpr (std::is_same_v<T, phase_reading>)
                    return s.read == read_flight::park;
                else
                    return s.park_read_out;
            }, chain);
        }

        [[nodiscard]] read_flight read_in_flight() const {
            return std::visit([](const auto& s) -> read_flight {
                using T = std::decay_t<decltype(s)>;
                if constexpr (std::is_same_v<T, phase_reading>)
                    return s.read;
                else
                    return s.park_read_out ? read_flight::park : read_flight::none;
            }, chain);
        }

        bool clear_socket_read() {
            return std::visit([](auto& s) -> bool {
                using T = std::decay_t<decltype(s)>;
                if constexpr (std::is_same_v<T, phase_reading>) {
                    const bool fp = (s.read == read_flight::park);
                    s.read = read_flight::none;
                    return fp;
                } else {
                    const bool fp = s.park_read_out;
                    s.park_read_out = false;
                    return fp;
                }
            }, chain);
        }

        void mark_read_started(bool for_park) {
            std::visit([for_park](auto& s) {
                using T = std::decay_t<decltype(s)>;
                if constexpr (std::is_same_v<T, phase_reading>) {
                    s.read = for_park ? read_flight::park : read_flight::chain;
                } else {
                    s.park_read_out = true;
                }
            }, chain);
        }

        [[nodiscard]] park::chain snapshot_chain() {
            return std::visit([this](const auto& s) -> park::chain {
                using T = std::decay_t<decltype(s)>;
                if constexpr (std::is_same_v<T, phase_reading>) {
                    return {false, s.read != read_flight::none, s.read == read_flight::park,
                            caller.has_blocks(), false, false};
                } else if constexpr (std::is_same_v<T, phase_paused>) {
                    return {true, s.park_read_out, s.park_read_out,
                            caller.has_blocks(), false, false};
                } else if constexpr (std::is_same_v<T, phase_parked>) {
                    return {false, s.park_read_out, s.park_read_out,
                            caller.has_blocks(), false, false};
                } else {
                    return {false, s.park_read_out, s.park_read_out,
                            caller.has_blocks(), true, s.pending_batch != nullptr};
                }
            }, chain);
        }

        /*
         * Whether a builtin waits on a Raft commit - TODO 626. It used to wait on this
         * session's unit thread, and every other connection on that thread waited
         * with it, whatever space it used. So these go the asynchronous way, on a
         * pool of their own, while the unit thread goes on serving the rest; this
         * connection still waits its turn, so its replies stay in order.
         *
         * A write of data to a space replicated with Raft, and CLUSTER, whose calls
         * between nodes commit to the cluster's group or wait on another node. Not
         * inside MULTI, or EXEC itself: those use the session's own caller, which a
         * call off this thread doesn't have (it runs on a copy).
         */
        /** a read of data in a space replicated with Raft, which may wait for a read index - TODO 785 */
        bool raft_read(const barch_info& fi) {
            if (fi.is_write() || !fi.is_data() || fi.is_asynch)
                return false;
            const auto& ks = caller.kspace();
            return ks && ks->has_raft();
        }

        /** the session's own call may defer a read, and only while it runs - TODO 785 */
        struct read_defer_scope {
            rpc_caller& c;
            explicit read_defer_scope(rpc_caller& c) : c(c) { c.read_defer = true; }
            ~read_defer_scope() { c.read_defer = false; }
        };

        /**
         * Run the call again on the Raft pool, behind whatever this batch has queued:
         * a leader read the lease didn't cover, which waits there for a read index
         * rather than refusing - TODO 785.
         */
        void defer_read(barch_function f, const std::vector<redis::string_param_t>& params,
                        heap::vector<asynch_call_context_ptr>& asynch_calls) {
            auto ctx = std::make_shared<asynch_call_context>(caller, std::move(f), params, prev_cn);
            ctx->raft = true;
            if (!stream.empty())
                ctx->stream = std::move(stream);
            caller.read_wait_on.reset();        // the copy has it
            asynch_calls.push_back(ctx);
        }

        bool waits_on_raft(const barch_info& fi) {
            if (caller.is_buffering() || prev_kind == prev_transaction)
                return false;
            if (prev_kind == prev_cluster)
                return barch::cluster::built();
            if (!fi.is_write() || !fi.is_data())
                return false;
            const auto& ks = caller.kspace();
            return ks && ks->has_raft();
        }

        template<typename Stream>
        void run_params(Stream& ostream, const std::vector<redis::string_param_t>& params,heap::vector<asynch_call_context_ptr> &asynch_calls) {

            barch::test::jitter("command"); // TODO 805
            std::string_view raw = params[0];
            std::string cn;
            std::string fn_space;
            key_space_ptr old_spc;
            bool should_reset_space = false;
            try {
                /*
                 * The space this connection chose went: DROP or UNLOAD - TODO 945. Its
                 * name is another space now, or opens one from the files.
                 */
                if (const auto& cur = caller.kspace();
                    barch::drop_core::follows_name(cur && cur->is_unloaded(), caller.transaction()->open)) {
                    const std::string name = cur->get_canonical_name();
                    caller.set_kspace(barch::get_keyspace(name));
                }
                // memtier and redis-benchmark send GET already uppercased, so the
                // same-command cache is a pointer compare against prev_cn and
                // skips the string + toupper. Lowercase still folds below.
                bool cached = raw.find(':') == std::string_view::npos && raw == prev_cn;
                if (!cached) {
                    cn.assign(raw);
                    auto colon = cn.find_last_of(':');
                    if (colon != std::string::npos && colon < cn.size()-1) {
                        old_spc = caller.kspace();
                        std::string space = cn.substr(0,colon);
                        cn = cn.substr(colon+1);

                        if (!old_spc || old_spc->get_canonical_name() != space) {

                            caller.set_kspace(barch::get_keyspace(space));
                            should_reset_space = true;
                        }
                    }

                    // command names are case insensitive, as they are in redis. The table is
                    // keyed in upper case and the lookup used to be an exact match on
                    // whatever arrived, so `set` and `Set` were unknown commands while `SET`
                    // worked. Every example in redis's own documentation is lower case, and
                    // so is the whole of valkey's test suite, which is how this was found.
                    // a dotted name says where the definition comes from: KS1.PRINT_NAME
                    // is PRINT_NAME as defined in KS1. Split before folding for the same
                    // reason the colon is - the space half keeps its case. A dotted name
                    // never takes the cached path above, because prev_cn holds the folded
                    // function name and the raw never equals it
                    fn_space.clear();
                    auto dot = cn.find('.');
                    if (dot != std::string::npos && dot > 0 && dot + 1 < cn.size()) {
                        fn_space = cn.substr(0, dot);
                        cn = cn.substr(dot + 1);
                    }
                    // Folded here rather than above so the key space in a `space:CMD` prefix
                    // keeps the case it was given - space names are not case insensitive
                    for (auto& ch : cn) {
                        ch = (char) toupper((unsigned char) ch);
                    }
                    if (prev_cn != cn) {
                        ic = barch_functions->find(cn);
                        prev_cn = std::move(cn);
                        prev_kind = (prev_cn == "EXEC" || prev_cn == "MULTI" || prev_cn == "DISCARD")
                                        ? prev_transaction
                                        : prev_cn == "CLUSTER" ? prev_cluster : prev_other;
                    }
                }
                // Authorization is checked per command, not per new command name.
                //
                // It used to sit inside the lookup above, which meant a repeated name
                // was never checked again - harmless while rights were global, and
                // wrong the moment they vary by key space: `KS1:GET` then `KS2:GET`
                // is one name and two answers. See TODO 135.
                //
                // A dotted name is never a builtin. HNSW.SET is the stored function
                // SET in HNSW, running against the current space; HNSW:SET (colon)
                // is the builtin SET in HNSW. Builtins win only when there is no
                // dot. See TODO 160.
                /*
                 * Record it, if recording is on - TODO 316. Here rather than in
                 * the branches below so that a stored function call is recorded
                 * like a builtin, and before authorization so that what is
                 * recorded is what arrived rather than what was allowed. The
                 * space is the one in force after any `space:CMD` prefix was
                 * applied above, which is what a replay has to put back, and the
                 * connection id is what lets a replay put back the concurrency.
                 */
                if (barch::traffic::capturing()) {
                    const auto& spc = caller.kspace();
                    barch::traffic::record(id, spc ? std::string_view(spc->canonical()) : std::string_view(),
                                           params);
                }
                const bool dotted = !fn_space.empty();
                if (dotted || ic == barch_functions->end()) {
                    // a stored function - see TODO 98. Deliberately not cached
                    // alongside `ic`: a name that missed once would otherwise stay
                    // unknown for the life of the session, and SETF followed by a
                    // call on the same connection would fail for a reason that has
                    // nothing to do with the function
                    /*
                     * Two authorizations, not one - TODO 188.
                     *
                     * Calling a stored function at all needs `function_cats()`, as it
                     * always did. A command a resp transport() exposes then carries
                     * its own categories on top, so a read-only one and a writing one
                     * are not the same right. A plain stored function declares none
                     * and is left as it was.
                     */
                    const caller::resolved* fn = nullptr;
                    if (!is_authorized(function_cats(), caller.get_space_acl())) {
                        redis::rwrite(ostream, error{"not authorized"});
                    } else if ((fn = barch::functions::resolve(caller, fn_space, prev_cn))
                               && !fn->cats.empty()
                               && !is_authorized(fn->cats, caller.get_space_acl())) {
                        redis::rwrite(ostream, error{"not authorized"});
                    } else if (fn) {
                        // a function parks rather than running here, so it costs this
                        // thread nothing to start - the script goes on the foreign pool
                        // in slices and the reply is written when it wakes.
                        //
                        // Unless the batch is already asynchronous, in which case this
                        // has to queue behind it like everything else does, or its
                        // reply overtakes the ones in front of it
                        /*
                         * Not sent on to the destinations - TODO 686. TODO 188 sent a call
                         * that declares write and data, from before a function's writes
                         * were records. Since TODO 498 every one of them goes as a record,
                         * and the call itself was an unknown command on the barch port
                         * (whose table has builtins only), which the sender ignores. Had
                         * it run, it would have written everything twice.
                         */
                        if (!asynch_calls.empty()) {
                            auto ctx = std::make_shared<asynch_call_context>(caller, fn->call, params, prev_cn);
                            if (!stream.empty())
                                ctx->stream = std::move(stream);
                            asynch_calls.push_back(ctx);
                        } else {
                            caller.reply_out = &ostream;
                            int32_t r = 0;
                            {
                                read_defer_scope defer(caller);
                                r = caller.call(params, fn->call);
                            }
                            caller.reply_out = nullptr;
                            if (caller.take_read_deferred())
                                defer_read(fn->call, params, asynch_calls);
                            else if (!caller.has_blocks())
                                write_result<Stream>(caller, ostream, r);
                        }
                    } else {
                        redis::rwrite(ostream, error{"unknown command"});
                    }
                } else if (!is_authorized(ic->second.cats_for(params.size() > 1
                                                              ? std::string_view(params[1])
                                                              : std::string_view()),
                                          caller.get_space_acl())) {
                    // no return: a refused `space:CMD` has to put the space back like
                    // any other, or the connection stays in it - TODO 594
                    redis::rwrite(ostream, error{"not authorized"});
                } else {
                    auto &f = ic->second.call;
                    note_command_call(ic->second);
                    /*
                     * Nothing sent from here for a builtin - TODO 503. The shard
                     * records what each write did (TODO 498), so this was the
                     * same write a second time, sent before it ran, failed ones
                     * included. And it went as the client spelled it: `space:SET`
                     * isn't a name the rpc side can look up, so every write to a
                     * named space was refused there and held up the rest.
                     */


                    // once one call is asynch all calls in this batch must be asynch to preserve order
                    const bool raft_wait = waits_on_raft(ic->second);
                    if (park::joins_batch(!asynch_calls.empty(), ic->second.is_asynch || raft_wait,
                                          caller.is_buffering())) {
                        // this is relatively slow so only potentially long-running and expensive calls should be marked as asynch
                        asynch_call_context_ptr ctx = std::make_shared<asynch_call_context>(caller,f,params,prev_cn);
                        // a read behind it may wait for a read index, which the Raft pool is for - TODO 785
                        ctx->raft = raft_wait || raft_read(ic->second);
                        if (!stream.empty())
                            ctx->stream = std::move(stream); // move the current stream - it should be empty after the move
                        asynch_calls.push_back(ctx);

                    }else {

                        // auto current = now(); // remove this for now since it has a measurable impact on performance

                        caller.reply_out = &ostream;
                        int32_t r = 0;
                        {
                            read_defer_scope defer(caller);
                            r = caller.call(params,f);
                        }
                        caller.reply_out = nullptr;
                        if (caller.take_read_deferred())
                            defer_read(f, params, asynch_calls);
                        else if (!caller.has_blocks())
                            write_result<Stream>(caller, ostream, r);

                        //ic->second.total_nanos += nanos(current);

                    }
                }
            }catch (std::exception& e) {
                caller.reply_out = nullptr;
                redis::rwrite(ostream, error{e.what()});
            }
            if (should_reset_space)
                caller.set_kspace(old_spc); // return to old value

        }
        /**
         * Go back to taking requests - TODO 490. Every place that used to call
         * do_read() once it was done with what it had calls this instead.
         *
         * The session used to read and run requests however far its replies had
         * backed up, so a client that pipelined faster than it read grew `queued`
         * without limit - and that memory isn't counted by max_memory. Now, past
         * rpc_output_high_water it stops. The client's requests wait in the kernel
         * and the parser instead, and the send that drains the replies picks up
         * again (see resume_if_drained). A client that pipelines everything before
         * reading anything can't deadlock on this: the replies still go out while
         * it's stopped, and the client's socket takes them.
         *
         * Requests can be left in the parser when consume_available stopped for
         * output or a block. Those run first, through the read handler with nothing
         * new read, so they get the same error handling as a real read.
         */
        void continue_reading() {
            switch (park::continue_reading(output_backlog(), rpc_output_high_water, parse_pending)) {
                case park::next::pause:
                    chain = phase_paused{park_read_out()};
                    return;
                case park::next::run_kept:
                    chain = phase_reading{read_in_flight()};
                    parse_pending = false;
                    do_read(true);
                    return;
                case park::next::read:
                    chain = phase_reading{read_in_flight()};
                    do_read();
                    return;
            }
        }
        /** reply bytes handed to send() and not yet written */
        [[nodiscard]] size_t output_backlog() const {
            return sending.buf.size() + queued.buf.size();
        }
        /**
         * What CLIENT LIST shows as omem and oll - TODO 492. It reads this session
         * from its own thread, and the send queue belongs to the socket's thread, so
         * the queue publishes its size here whenever it changes, and CLIENT LIST only
         * ever reads the atomics.
         */
        void publish_send_queue() {
            account(omem_send, output_backlog());
            oll_send.store((sending.empty() ? 0u : 1u) + (queued.empty() ? 0u : 1u),
                           std::memory_order_relaxed);
        }
        /** requests read and not yet run - after a read, and after they've run */
        void publish_query() {
            account(qbuf_bytes, parser.remaining());
        }
        /**
         * Set one of this session's shares of statistics::connection_buffer_bytes,
         * which max_memory counts - TODO 493. Each share only ever has one writer at a
         * time (the socket's thread, or stream_mut), so the swap and the add can't
         * interleave with another change to it. Unsigned, so taking away is the same
         * add, wrapping.
         */
        static void account(std::atomic<uint64_t>& share, uint64_t now) {
            const uint64_t was = share.exchange(now, std::memory_order_relaxed);
            if (now != was)
                statistics::connection_buffer_bytes.fetch_add(now - was, std::memory_order_relaxed);
        }
        /** the same for the streamed KEYS reply. Holding stream_mut */
        void publish_stream_queue_locked() {
            const uint64_t pending = stream_pending.buf.size();
            account(omem_stream, stream_backlog);
            // what isn't pending is in stream_writing, which only the socket's
            // thread may look at, so it's worked out from the backlog instead
            oll_stream.store((pending ? 1u : 0u) + (stream_backlog > pending ? 1u : 0u),
                             std::memory_order_relaxed);
        }
        /** on the socket's thread, after a write: carry on if continue_reading stopped */
        void resume_if_drained() {
            auto* paused = std::get_if<phase_paused>(&chain);
            if (park::resumes(paused != nullptr, output_backlog(), rpc_output_high_water)) {
                chain = phase_reading{paused->park_read_out ? read_flight::park : read_flight::none};
                continue_reading();
            }
        }
        // the async call context needs to stay alive while calls complete.
        // `buffered` runs what's already in the parser instead of reading more
        void do_read(bool buffered = false, bool for_park = false) {
            /*
             * The self pointer keeps this session alive while the read is
             * outstanding. Every idle connection has one of these pending, so the
             * collector retiring a session - which is exactly what it does when the
             * peer goes away, and exactly when this read completes - would otherwise
             * leave the handler running on freed memory.
             *
             * This was left on a raw `this` in DONE 186 because adding it there
             * produced use-after-free at restart instead: a session held into
             * teardown outlived the io_context its socket belonged to. DONE 188
             * fixed that ordering - asio_resp_ios is declared ahead of io and
             * workers, so the socket contexts are destroyed last - and with that in
             * place this is both safe and free. See TODO 199.
             */
            auto self(this->shared_from_this());
            auto on_read = [this, self](std::error_code ec, std::size_t length, bool from_socket)
            {
                note_mode();
                bool was_for_park = false;
                if (from_socket) {
                    was_for_park = clear_socket_read();
                }
                if (!ec){
                    bytes_recv += length;
                    parser.add_data(data_, length);
                    parser_peak.store(parser.get_max_buffer_size(), std::memory_order_relaxed);
                    publish_query();
                    // a park's read: keep what came for when the block answers - TODO 923
                    switch (park::on_arrival(was_for_park, caller.has_blocks(), parser.remaining(),
                                             barch::get_rpc_query_buffer_limit())) {
                        case park::arrived::keep:
                            ++statistics::parked_wakeups;
                            parse_pending = true;
                            watch_for_disconnect();
                            return;
                        case park::arrived::hold:
                            ++statistics::parked_wakeups;
                            parse_pending = true;
                            return;
                        case park::arrived::drop: {
                            // sent too much while parked - TODO 924. Shutdown and not
                            // close, and let_go for the collector, as with a protocol
                            // error below. No read is out, so nothing else holds it
                            ++statistics::parked_wakeups;
                            ++statistics::query_limit_drops;
                            barch::err({"closing client", std::to_string(id), "- it sent",
                                        std::to_string(parser.remaining()),
                                        "bytes while parked (rpc_query_buffer_limit)"});
                            timer.cancel();
                            erase_blocks();
                            std::error_code ignored;
                            socket_.lowest_layer().shutdown(asio::socket_base::shutdown_both, ignored);
                            let_go.store(true, std::memory_order_release);
                            return;
                        }
                        case park::arrived::run:
                            break;
                    }

                    try {

                        const bool parked = consume_available();
                        publish_query();
                        if (!parked) {
                            send(stream);
                            continue_reading();
                        }

                    }catch (std::exception& e) {
                        /*
                         * A request that can't be parsed - an argument over
                         * redis_max_item_len, say - used to be logged and nothing
                         * else: no reply, no next read, no close, and the client
                         * waited forever. The stream is past the point anything can
                         * be found in it again, so do what redis does with a
                         * protocol error: say so, then close. What earlier requests
                         * in the same read answered goes out first. TODO 428.
                         *
                         * Shutdown and not close - TODO 489. Close sets the handle
                         * to -1 under the session collector, which reads it from its
                         * own thread with no lock of ours. That's a data race, and
                         * after it the collector's peek gets EBADF rather than the 0
                         * it collects on, so the session was never let go. The fd
                         * number could also be handed to the next accept while the
                         * collector still peeks it. The fd is closed when the
                         * session goes.
                         *
                         * The peek alone won't let it go either: the rest of the
                         * oversized argument is still unread, so it peeks as data,
                         * not 0, and no read is coming. let_go tells the collector.
                         */
                        barch::err({"error", e.what()});
                        redis::rwrite(stream, error{std::string("Protocol error: ") + e.what()});
                        send(stream);
                        when_sent([this, self]() {
                            std::error_code ignored;
                            socket_.lowest_layer().shutdown(asio::socket_base::shutdown_both, ignored);
                            let_go.store(true, std::memory_order_release);
                        });
                    }
                }else {
                    if (was_for_park)
                        ++statistics::parked_wakeups;
                    if (caller.has_blocks()) {
                        timer.cancel();
                        erase_blocks();
                    }
                    // a read error is a dead or broken socket - the peer went
                    // away mid-conversation. EOF between requests is the normal
                    // close, however much the client ran first, and is not
                    // counted: it used to be whenever the client had sent
                    // anything at all. See TODO 391 and 440.
                    const auto end = park::on_read_end(ec == asio::error::eof, parser.mid_request());
                    if (end.net_error)
                        ++statistics::repl::net_errors;
                    /*
                     * Nothing reads this socket again. A TLS session that read a bad
                     * record or a close_notify still has its TCP open, so the
                     * collector's peek never read 0, and it stayed while the client
                     * did - TODO 932. Shut it once what is queued is out, and say so.
                     */
                    if (end.shut)
                        when_sent([this, self]() {
                            std::error_code ignored;
                            socket_.lowest_layer().shutdown(asio::socket_base::shutdown_both, ignored);
                            let_go.store(true, std::memory_order_release);
                        });
                    //if (ec.category())
                     //barch::err({ec.message().c_str()});
                }
            };
            if (buffered) {
                // posted rather than called, so a long run of pauses and resumes
                // never builds up a stack
                asio::post(socket_.get_executor(), [on_read]() mutable { on_read({}, 0, false); });
                return;
            }
            // one read at a time. A park's read already out is taken over, and what
            // it brings runs - TODO 923
            if (!for_park && !park::chain_reads(has_read_out())) {
                if (auto* r = std::get_if<phase_reading>(&chain))
                    r->read = read_flight::chain;
                return;
            }
            mark_read_started(for_park);
            socket_.async_read_some(asio::buffer(data_, rpc_io_buffer_size),
                [on_read](std::error_code ec, std::size_t length) mutable { on_read(ec, length, true); });
        }

        /*
         * `again` is a waiter woken with nothing for it, parking once more: it keeps
         * the time it had left - TODO 879. Armed afresh, each such wake gave it its
         * whole timeout again.
         */
        void start_block_to(bool again = false) {
            const uint64_t gen = ++park_gen;
            const auto plan = park::plan_timer(caller.block_to_ms, again, park_deadline, art::now());
            park_deadline = plan.deadline;
            waiter_deadline = plan.deadline;
            if (!plan.armed)
                return;
            timer.cancel();
            timer.expires_after(std::chrono::milliseconds(plan.wait_ms));
            auto self(this->shared_from_this());
            timer.async_wait([this,self,gen](const std::error_code& ec)
                {
                    note_mode();
                    // a wake that came first may have answered it already - TODO 878
                    if (park::timer_answers((bool) ec, still_parked(gen))) {
                        do_block_to();
                    }
                });
        }
        /*
         * Test knob, TODO 879: while the file BARCH_TEST_PARK_HOLD_FILE names exists, a
         * session about to go on its keys' lists waits here, and says so by making
         * <file>.held. So a test lands a push between a pop's look and its
         * registration every time. The file says which thread it parks on: "home",
         * the connection's own, or "away" - TODO 880.
         */
        /*
         * Test knob, TODO 935: while the file BARCH_TEST_BATCH_WRITE_HOLD_FILE names
         * exists, a batch's worker waits here, before it writes its call's reply, and
         * says so by making <file>.held. A test resets the client meanwhile, so that
         * write fails every time.
         */
        void hold_before_batch_write_for_test() {
            static const char* f = [] {
                const char* v = std::getenv("BARCH_TEST_BATCH_WRITE_HOLD_FILE");
                return v && *v ? v : nullptr;
            }();
            std::error_code ec;
            if (!f || !std::filesystem::exists(f, ec))
                return;
            {
                const std::string held = std::string(f) + ".held";
                { std::ofstream out(held + ".new"); out << "away"; }
                barch::replace_file((held + ".new").c_str(), held.c_str());
            }
            while (std::filesystem::exists(f, ec))
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        void hold_before_park_for_test() {
            static const char* f = [] {
                const char* v = std::getenv("BARCH_TEST_PARK_HOLD_FILE");
                return v && *v ? v : nullptr;
            }();
            // exists(), not an open: Windows won't let the test delete a file open
            // here - TODO 918
            std::error_code ec;
            if (!f || !std::filesystem::exists(f, ec))
                return;
            {
                const auto ex = socket_.get_executor();
                const auto* io = ex.template target<asio::io_context::executor_type>();
                // written beside it and renamed, so it's never read half made
                const std::string held = std::string(f) + ".held";
                { std::ofstream out(held + ".new"); out << (io && io->running_in_this_thread() ? "home" : "away"); }
                barch::replace_file((held + ".new").c_str(), held.c_str());
            }
            while (std::filesystem::exists(f, ec))
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        void add_caller_blocks() {
            hold_before_park_for_test();
            caller.transfer_rpc_blocks(this->shared_from_this());
            // anything that parked and then started its own work gets told the waiter
            // exists now, so work that already finished is not left unwoken
            caller.after_blocks_registered();
            for (auto& d : caller.get_blocks()) {
                if (d.space && d.space->has_foreign())
                    barch::foreign::kick(d.space, d.key);
            }
            watch_for_disconnect();
        }
        /**
         * Notice a parked client going away.
         *
         * While parked the read chain is deliberately down - the connection owes the
         * client one reply and must not run anything else first - so nothing was
         * watching the socket, and a client that disconnected mid-block left its
         * registration in the shard's blocked_sessions for good. That is not just a
         * leak: `blocked_clients` never comes back down, and the next wake on that key
         * goes to the dead session, which pops the value and drops it while a live
         * waiter behind it carries on waiting. See TODO 226.
         *
         * So a park keeps one read out. What it brings goes into the parser and runs
         * once the block answers, so a command pipelined behind the blocking one
         * still comes after its reply. The read ending is the client gone, and the
         * read handler drops the blocks.
         *
         * A wait for readable, with a peek, did this before - TODO 919, 923. It left
         * pipelined bytes in the socket, and asio arms a wait with EPOLL_CTL_MOD,
         * which fires at once on what is already there. So it either stopped on the
         * bytes and missed a hang-up behind them, or armed again and spun. A read
         * takes the bytes, so the next one only comes back for something new. It
         * works the same through TLS and on a select reactor.
         *
         * It reads on however much the client sends. A park that stopped would close
         * the TCP window, and the client's FIN would wait behind its unsent data on
         * its side, where nothing here can see it. Past rpc_query_buffer_limit kept,
         * the client is closed instead - TODO 924.
         */
        void watch_for_disconnect() {
            if (!park::watch_now(caller.has_blocks(), has_read_out()))
                return;
            do_read(false, true);
        }
        /**
         * A run of plain GETs at the front of the buffer, as one batch - TODO 851.
         * Each GET used to go through run_params, rpc_caller::call and GET on its
         * own, and paid its hash slot and then its leaf from memory with nothing
         * else in flight. Here each key is routed once, its slot is warmed under a
         * try-lock held for that key alone, and then the searches run and reply in
         * order. Holding all the locks at once, the way MGET does, made SETs sleep
         * in lock_unique (TODO 850).
         *
         * Only where the dispatch would do nothing for a GET: rpc_caller's short
         * path holds (plain_space), no MULTI, no traffic capture, no foreign
         * space, and GET is authorized here. A key GET would refuse, or a miss,
         * ends the batch, and that request runs the usual way. Returns how many
         * requests it answered and took from the parser.
         */
        size_t run_get_batch() {
            enum { most = 32 };
            // every request comes through here, and most are no GET. Ask for two
            // first: clearing all of `raw` for each SET was 4% of a fill - TODO 896
            std::string_view first[2];
            if (parser.peek_get_run(first, 2) < 2)
                return 0;
            std::string_view raw[most];
            const size_t n = parser.peek_get_run(raw, most);
            if (n < 2 || barch::traffic::capturing() || caller.is_buffering()
                || caller.is_collecting_exec() || caller.has_blocks() || !caller.plain_space())
                return 0;
            const auto& ks = caller.kspace();
            if (ks->has_foreign()) return 0;
            if (get_cmd == nullptr) {
                auto i = barch_functions->find("GET");
                if (i == barch_functions->end()) return 0;
                get_cmd = &i->second;
            }
            if (get_cmd->is_asynch
                || !is_authorized(get_cmd->cats_for(std::string_view()), caller.get_space_acl()))
                return 0;
            barch::sharded_store store(ks, barch::sharded_store::borrow);
            conversion::comparable_key converted[most];
            art::value_type keys[most];
            uint64_t kh[most];
            // borrowed: ks holds the space, and a live space keeps its shards - TODO 854
            barch::shard_ref owner[most];
            // encode_key reads the byte after the key, which has to be 0. Argv strings
            // end in one; these views end where the socket buffer has '\r'
            size_t total = 0;
            for (size_t i = 0; i < n; ++i) total += raw[i].size() + 1;
            get_keys.resize(total);
            char* at = get_keys.data();
            for (size_t i = 0; i < n; ++i) {
                memcpy(at, raw[i].data(), raw[i].size());
                at[raw[i].size()] = 0;
                raw[i] = std::string_view{at, raw[i].size()};
                at += raw[i].size() + 1;
            }
            size_t m = 0;
            for (; m < n; ++m) {
                if (key_ok(raw[m].data(), raw[m].size()) != 0) break;
                converted[m] = ks->encode_key(art::value_type{raw[m].data(), raw[m].size()});
                keys[m] = converted[m].get_value();
                kh[m] = barch::key_hash(keys[m].chars(), keys[m].size);
                owner[m] = store.shard_ref_for(keys[m], kh[m]);
                if (!owner[m]) break;
                // a hint: a shard that can't be read at once is left cold
                auto* t = owner[m];
                if (!t->sources() && t->get_latch().try_lock_shared()) {
                    t->prefetch_search(keys[m], 0);
                    t->get_latch().unlock_shared();
                }
            }
            size_t done = 0;
            try {
                for (; done < m; ++done) {
                    const bool found = store.search(keys[done], kh[done], owner[done],
                                                    [&](const art::node_ptr& r) {
                        auto cl = r.const_leaf();
                        auto vt = cl->get_value();
                        if (cl->is_compressed()) {
                            vt = dictionary::decompress(art::space_of(r), vt);
                        }
                        redis::rwrite_bulk(stream, vt.chars(), vt.size);
                    });
                    if (found) continue;
                    // GET's own miss: nil unless a container holds the key - TODO 851
                    if (barch::kind_of_container(store, art::value_type{raw[done].data(), raw[done].size()},
                                                 false, false) != barch::container_kind::none)
                        break;
                    redis::rwrite_null(stream, caller.get_protocol());
                }
            } catch (std::exception&) {
                // nothing was written for it: it runs the usual way and says why
            }
            for (size_t i = 0; i < done; ++i) {
                parser.read_new_request();
                ++calls_recv;
                ++statistics::local_calls;
                note_command_call(*get_cmd);
            }
            return done;
        }
        /**
         * SETs come pipelined. At a run of plain SETs, start the reads of the hash
         * lines each one will write, so their misses overlap instead of coming one
         * at a time under the shard lock. A hint only: nothing changes, and a busy
         * shard is left cold. The keys are routed again when the SETs run. TODO 899
         */
        void prefetch_set_run() {
            enum { most = 16 };
            if (set_prefetched > 0) {
                --set_prefetched;
                return;
            }
            std::string_view raw[most];
            const size_t n = parser.peek_set_run(raw, most);
            if (n < 2 || !caller.plain_space()) return;
            const auto& ks = caller.kspace();
            if (ks->has_foreign()) return;
            barch::sharded_store store(ks, barch::sharded_store::borrow);
            // encode_key reads the byte after the key, which has to be 0
            size_t total = 0;
            for (size_t i = 0; i < n; ++i) total += raw[i].size() + 1;
            set_keys.resize(total);
            char* at = set_keys.data();
            try {
                for (size_t i = 0; i < n; ++i) {
                    memcpy(at, raw[i].data(), raw[i].size());
                    at[raw[i].size()] = 0;
                    std::string_view k{at, raw[i].size()};
                    at += raw[i].size() + 1;
                    if (key_ok(k.data(), k.size()) != 0) break;
                    auto converted = ks->encode_key(art::value_type{k.data(), k.size()});
                    auto key = converted.get_value();
                    auto* t = store.shard_ref_for(key, barch::key_hash(key.chars(), key.size));
                    if (!t) break;
                    if (!t->sources() && t->get_latch().try_lock_shared()) {
                        t->prefetch_search(key, 2);
                        t->get_latch().unlock_shared();
                    }
                }
            } catch (std::exception&) {
                // the SET refuses the same key, and says why
            }
            // this request is the first of them
            set_prefetched = n - 1;
        }
        /** parse already-buffered requests. true if the connection is parked. */
        bool consume_available() {
            stream.clear();
            heap::vector<asynch_call_context_ptr> asynch_calls;
            while (!held.empty() || parser.remaining() > 0) {
                if (!held.empty()) {
                    // held back from the last batch: it goes first, on this caller
                    std::vector<std::string> h = std::move(held);
                    held.clear();
                    std::vector<redis::string_param_t> hp(h.begin(), h.end());
                    run_params(stream, hp, asynch_calls);
                    if (caller.has_blocks())
                        break;
                    continue;
                }
                if (asynch_calls.empty() && run_get_batch() > 0) {
                    if (parser.remaining() > 0
                        && stream.buf.size() + output_backlog() >= rpc_output_high_water) {
                        parse_pending = true;
                        break;
                    }
                    continue;
                }
                prefetch_set_run();
                auto &params = parser.read_new_request();
                if (params.empty())
                    break;
                ++calls_recv;
                if (park::holds(!asynch_calls.empty(),
                                park::changes_connection(rpc_caller::upper_name(params[0])))) {
                    held.assign(params.begin(), params.end());
                    parse_pending = true;   // the batch's end runs it, as kept bytes
                    break;
                }
                run_params(stream, params, asynch_calls);
                if (caller.has_blocks())
                    break;
                // replies have backed up: leave the rest where it is until they
                // drain - TODO 490. continue_reading picks it up
                if (parser.remaining() > 0
                    && stream.buf.size() + output_backlog() >= rpc_output_high_water) {
                    parse_pending = true;
                    break;
                }
            }
            if (!asynch_calls.empty()) {
                auto batch = std::make_shared<heap::vector<asynch_call_context_ptr>>(
                    std::move(asynch_calls));
                // the batch writes the socket itself, from the worker pool, so
                // whatever earlier reads queued has to be out first
                auto self(this->shared_from_this());
                chain = phase_batch{park_read_out(), {}, 0};
                when_sent([this, self, batch]() {
                    run_asynch_batch(batch, 0);
                });
                return true;
            }
            if (caller.has_blocks()) {
                // what ran ahead of the park is answered now, not with the park's
                // answer - TODO 930. The answer is written to an empty stream then.
                // A parked CALLF's job may stream through the same buffer
                chain = phase_parked{park_read_out()};
                {
                    std::lock_guard lk(socket_write_mutex);
                    send(stream);
                }
                start_block_to();
                add_caller_blocks();
                return true;
            }
            return false;
        }
        void erase_blocks() {
            caller.erase_blocks(this->shared_from_this());
            if (auto* p = std::get_if<phase_parked>(&chain))
                chain = phase_reading{p->park_read_out ? read_flight::park : read_flight::none};
        }
        /*
         * The chain's six values make a mode that can happen - TODO 930. Checked as each
         * handler on the socket's thread starts, which is between handlers: inside one
         * the values move one at a time. Counted in INFO clients, and the tests keep it
         * at 0. parkcoretest runs the same rules against a model of the chain.
         */
        void note_mode() {
            const park::chain c = snapshot_chain();
            if (park::mode_of(c) != park::mode::impossible)
                return;
            if (statistics::chain_mode_errors.fetch_add(1) == 0)
                barch::err({"read chain in a mode that can't happen: paused", std::to_string(c.paused),
                            "read_out", std::to_string(c.read_out), "for_park", std::to_string(c.for_park),
                            "parked", std::to_string(c.parked), "in_batch", std::to_string(c.in_batch),
                            "batch_waiting", std::to_string(c.batch_waiting)});
        }
        /*
         * Test knob, TODO 878: with BARCH_TEST_WAKE_AT_BLOCK_TIMEOUT set, a waiter whose
         * time ran out wakes its own keys first, as a wake from another thread landing
         * just then would. So a test meets that race every time.
         */
        void wake_at_timeout_for_test() {
            static const bool on = std::getenv("BARCH_TEST_WAKE_AT_BLOCK_TIMEOUT") != nullptr;
            if (!on)
                return;
            const auto keys = caller.get_blocks();
            for (auto& d : keys)
                pass_the_turn(d.space, d.shard_index, d.key);
        }
        void do_block_to() {
            waiter_deadline = 0;
            wake_at_timeout_for_test();
            // kept past the erase in case the waiter parks again - TODO 745, 874
            auto running = caller.parked_running;
            erase_blocks();
            int r = caller.call_blocks();
            /*
             * A CALLF whose wait ran out: its job still runs, on this session's Luau
             * state. It was told to stop, and the session waits for it to end - TODO
             * 874. Answering now let the next command run on that state from another
             * thread. A retry with no blocks answers, as it did.
             */
            if (caller.take_block_retry() && caller.has_blocks()) {
                caller.parked_running = std::move(running);
                if (!std::holds_alternative<phase_batch>(chain))
                    chain = phase_parked{park_read_out()};
                start_block_to();
                add_caller_blocks();
                return;
            }
            write_result(caller, stream, r);
            auto self(this->shared_from_this());
            send(stream);
            when_sent([this, self]() {
                resume_after_blocks();
            });
        }
        /**
         * KEYS's encoded reply goes out through here, from the worker pool, so it never
         * sits on the result stack. send_socket_tail calls it after the call returns,
         * so the wait below holds no gate - TODO 925. The mutex keeps each write whole.
         *
         * Anything already encoded for this connection (a GET that ran in the same
         * pipeline, before KEYS) has to leave first, or KEYS overtakes it on the wire.
         *
         * This used to be a blocking asio::write, made while art::glob holds its
         * process-wide glob_queue mutex - TODO 488. A client that sent KEYS * and
         * stopped reading never let that write finish, so the worker and the lock
         * stayed taken, and every KEYS on the server queued behind them for good.
         * Now the bytes are queued and go out through async writes on the socket's
         * own thread - see stream_out.
         */
        void bind_socket_writer() {
            caller.write_socket_bytes = [this](const char* data, size_t n) -> bool {
                std::lock_guard lk(socket_write_mutex);
                if (!stream.empty()) {
                    const bool ok = stream_out((const char*) stream.buf.data(), stream.buf.size());
                    stream.clear();
                    if (!ok) return false;
                }
                return stream_out(data, n);
            };
        }

        /**
         * Queue streamed bytes for the socket. Called on a worker, never on the
         * socket's thread, which is the one that has to finish the writes.
         *
         * Most replies fit under rpc_stream_high_water and never wait at all. Past
         * it the worker waits for the client to take some. The limit is on the
         * client making no progress, not on the whole reply: a slow reader that
         * keeps taking bytes can take as long as it likes. One that takes nothing
         * for rpc_client_max_wait_ms is disconnected, and this returns false, which
         * stops the batch. 0 waits for as long as it takes.
         */
        bool stream_out(const char* data, size_t n) {
            std::unique_lock lk(stream_mut);
            if (!stream_wait(lk, rpc_stream_high_water))
                return false;
            if (!n) return true;
            stream_pending.write(data, n);
            stream_backlog += n;
            publish_stream_queue_locked();
            if (stream_core::posts_next(stream_busy)) {
                stream_busy = true;
                auto self(this->shared_from_this());
                asio::post(socket_.get_executor(), [this, self]() { stream_next(); });
            }
            return true;
        }
        /**
         * Wait until everything streamed is on the socket, on the same terms as
         * stream_out. The async batch calls this before it writes the rest of a
         * reply, so the two never have writes out on the socket at once.
         */
        bool stream_flush() {
            std::unique_lock lk(stream_mut);
            return stream_wait(lk, 1);
        }
        /** wait, holding `lk`, until the backlog is under `below`. false if the client is let go */
        bool stream_wait(std::unique_lock<std::mutex>& lk, uint64_t below) {
            /*
             * This may wait on the client for rpc_client_max_wait_ms, or for ever at 0.
             * Anything held here is held that long, and whatever waits on it waits on
             * the client. So it holds only this session's own mutexes: no commit gate
             * and no latch - TODO 925. Counted in INFO clients, and the tests check 0.
             */
            if (barch::commit_gate::held_here() > 0
                || barch::latch_t::held_by_this_thread(nullptr, nullptr, 0) > 0) {
                if (statistics::stream_wait_locked++ == 0)
                    barch::err({"a streamed reply waits on its client holding a gate or a latch"});
            }
            const uint64_t limit_ms = barch::get_rpc_max_client_wait_ms();
            // INFO clients stream_waiting, so a test knows a worker is here - TODO 925
            struct counted {
                bool in = false;
                void enter() { if (!in) { in = true; ++statistics::stream_waiting; } }
                ~counted() { if (in) --statistics::stream_waiting; }
            } waiting;
            while (stream_core::wait_step(stream_failed, stream_backlog, below) == stream_core::on_wait::wait) {
                waiting.enter();
                const uint64_t seen = stream_progress;
                auto moved = [&]() {
                    return stream_failed || stream_backlog < below || stream_progress != seen;
                };
                if (limit_ms == 0) {
                    stream_cv.wait(lk, moved);
                } else if (!stream_cv.wait_for(lk, std::chrono::milliseconds(limit_ms), moved)) {
                    stream_failed = true;
                    lk.unlock();
                    barch::err({"closing client", std::to_string(id), "- it took none of its reply for",
                                std::to_string(limit_ms), "ms (rpc_client_max_wait_ms)"});
                    ++statistics::repl::net_errors;
                    auto self(this->shared_from_this());
                    /*
                     * On the socket's thread, which owns it. A shutdown fails the write
                     * that's out, and its completion clears the rest.
                     *
                     * Shutdown and not close: close sets the handle to -1 under the
                     * session collector, which reads it from its own thread with no
                     * lock of ours. That's a data race (TSan found it), and after it the
                     * collector's peek gets EBADF rather than the 0 it collects on, so
                     * the session would never be let go. The fd is closed when the
                     * session goes.
                     *
                     * The peek alone won't let it go: anything the client sent after
                     * the call that stalled is still unread, so it peeks as data, not
                     * 0, and no read is coming. let_go tells the collector - TODO 496.
                     * The collector only drops its own reference, so a worker call or
                     * a read still out keeps the session alive until it's done.
                     */
                    asio::post(socket_.get_executor(), [this, self]() {
                        std::error_code ignored;
                        socket_.lowest_layer().shutdown(asio::socket_base::shutdown_both, ignored);
#ifdef _WIN32
                        // a shutdown fails a blocked write on linux. On windows the
                        // overlapped send that's out just stays out, waiting on a peer
                        // that isn't reading, and with it the session. Cancel it, which
                        // completes it with an error the way the shutdown does - TODO 599
                        socket_.lowest_layer().cancel(ignored);
#endif
                        let_go.store(true, std::memory_order_release);
                    });
                    lk.lock();
                    return false;
                }
            }
            return !stream_failed;
        }
        /**
         * On the socket's thread: put what's queued on the socket, one write at a
         * time. write_some rather than write, so every piece the client takes counts
         * as progress - a whole-buffer write only completes at the end, and a slow
         * reader that's keeping up would look stalled.
         */
        void stream_next() {
            {
                std::lock_guard lk(stream_mut);
                switch (stream_core::next(stream_failed, stream_written >= stream_writing.buf.size(),
                                          stream_pending.empty())) {
                    case stream_core::on_next::stop:
                        stream_pending.clear();
                        stream_writing.clear();
                        stream_written = 0;
                        stream_backlog = 0;
                        stream_busy = false;
                        publish_stream_queue_locked();
                        stream_cv.notify_all();
                        return;
                    case stream_core::on_next::swap:
                        // swapped rather than copied; only this thread touches stream_writing
                        std::swap(stream_writing.buf, stream_pending.buf);
                        std::swap(stream_writing.pos, stream_pending.pos);
                        stream_pending.clear();
                        stream_written = 0;
                        publish_stream_queue_locked();
                        break;
                    case stream_core::on_next::write:
                        break;
                }
            }
            auto self(this->shared_from_this());
            size_t piece = stream_writing.buf.size() - stream_written;
#ifdef _WIN32
            /*
             * Linux takes what fits in the socket's buffer and completes the
             * write short. Windows checks its buffer limit only before taking a
             * send, and then takes all of it however big, completing at once: one
             * 16MB KEYS reply "went out" in a single write to a client that read
             * none of it, nothing was ever left waiting, and stream_wait never
             * saw a stall. Pieces no bigger than asio::async_write uses give
             * Windows the same backpressure Linux has - TODO 599
             */
            piece = std::min<size_t>(piece, 64 * 1024);
#endif
            socket_.async_write_some(
                asio::buffer(stream_writing.buf.data() + stream_written, piece),
                [this, self](std::error_code ec, std::size_t length) {
                    stream_written += length;
                    if (ec) {
                        stream_writing.clear();
                        stream_written = 0;
                    }
                    stream_core::after_write w;
                    {
                        std::lock_guard lk(stream_mut);
                        w = stream_core::written(bool(ec), stream_failed, stream_backlog, length);
                        if (w.count_error)
                            ++statistics::repl::net_errors;
                        stream_failed = w.failed;
                        stream_backlog = w.backlog;
                        if (w.progress)
                            ++stream_progress;
                        publish_stream_queue_locked();
                        stream_cv.notify_all();
                    }
                    if (w.let_go) {
                        /*
                         * The client is gone, or reset. The worker gives up when it
                         * sees the stream failed, and the read chain is down for its
                         * batch, so no read sees the error. A reset socket with input
                         * still unread peeks as that input, not 0, so the collector
                         * kept the session and its fd for good. Shut it and let it go,
                         * as stream_wait does - TODO 943. This is the socket's thread.
                         */
                        std::error_code ignored;
                        socket_.lowest_layer().shutdown(asio::socket_base::shutdown_both, ignored);
                        let_go.store(true, std::memory_order_release);
                    }
                    if (!ec) {
                        net_stat stat;
                        stream_write_ctr += length;
                        bytes_sent += length;
                    }
                    stream_next();
                });
        }

        /**
         * Put a reply buffer on the socket, keeping the order it was sent in. `out`
         * is left empty and can be filled again straight away.
         *
         * Only one async_write is out at a time, and the bytes it's sending belong
         * to it until it completes. What arrives meanwhile waits in `queued` and
         * goes out next. The read handler used to async_write `stream` and go
         * straight back to reading, and the next read cleared and refilled that
         * same buffer while the write was still going. Once a client fell behind
         * and the write went out in pieces, the later pieces came from the new
         * replies, and a big pipeline of GETs got a broken RESP stream. TODO 475.
         *
         * Reading carries on while this drains, the way redis keeps reading while
         * its output buffer grows: a client that sends its whole pipeline before
         * reading anything would otherwise wait on the server while the server
         * waits on it.
         *
         * Runs on the socket's thread, as do the completions, so nothing here is
         * locked.
         */
        void send(vector_stream& out) {
            if (out.empty()) return;
            if (write_busy) {
                queued.write((const char*) out.buf.data(), out.buf.size());
                out.clear();
                publish_send_queue();
                return;
            }
            // swapped rather than copied, so both buffers keep their capacity
            std::swap(sending.buf, out.buf);
            std::swap(sending.pos, out.pos);
            out.clear();
            start_send();
        }
        /** run `then` once everything passed to send() is on the socket */
        void when_sent(std::function<void()> then) {
            if (!write_busy) {
                then();
                return;
            }
            after_sent.push_back(std::move(then));
        }
        void start_send() {
            write_busy = true;
            publish_send_queue();
            auto self(this->shared_from_this()); // see the note in do_read
            asio::async_write(socket_, asio::buffer(sending.buf),
                [this, self](std::error_code ec, std::size_t length){
                    note_mode();
                    sending.clear();
                    if (!ec){
                        net_stat stat;
                        stream_write_ctr += length;
                        bytes_sent += length;
                        if (!queued.empty()) {
                            std::swap(sending.buf, queued.buf);
                            std::swap(sending.pos, queued.pos);
                            queued.clear();
                            start_send();
                            resume_if_drained();
                            return;
                        }
                    }else {
                        // the socket is gone; what's queued can't go anywhere either
                        ++statistics::repl::net_errors;
                        queued.clear();
                    }
                    write_busy = false;
                    publish_send_queue();
                    auto waiting = std::move(after_sent);
                    after_sent.clear();
                    for (auto& then : waiting)
                        then();
                    // after the waiters: one of them may have started work of its own,
                    // and then read_paused is false and this does nothing
                    resume_if_drained();
                });
        }
        /**
         * Run one batch of asynchronous calls, one at a time and in the order they were
         * read. run_params has already made every call after the first asynchronous one
         * asynchronous too, so this order is the request order and has to be kept.
         *
         * Each call runs on the worker pool, which is the point of the exercise - a long
         * KEYS must not sit on a service thread. Only the socket work is serialised: the
         * next call does not start until the previous reply has been written, so there is
         * never more than one async_write outstanding on this socket, and reading only
         * resumes once the batch is done.
         */
        void run_asynch_batch(asynch_batch_ptr batch, size_t at) {
            if (at >= batch->size()) {
                // the one place the chain is resumed for an asynchronous batch. This
                // can be on a worker (write_then runs its continuation inline when
                // there's nothing to write), and continue_reading looks at the send
                // queue, which belongs to the socket's thread - TODO 490
                auto self(this->shared_from_this());
                asio::post(socket_.get_executor(), [this, self]() {
                    note_mode();
                    chain = phase_reading{park_read_out() ? read_flight::park : read_flight::none};
                    continue_reading();
                });
                return;
            }
            // the read chain is down for the length of a batch, so its self pointer
            // is not holding this session up - the sessions vector was, and the
            // collector can null that while this lambda is streaming to the socket.
            // That is the use-after-free TSan reports. See TODO 196.
            auto self(this->shared_from_this());
            asio::post((*batch)[at]->raft ? raft_workers : workers, [this, self, batch, at]() {
                auto ctx = (*batch)[at];
                // this copy runs here, on the pool, so it may stream - TODO 488
                ctx->caller.stream_to_socket = true;
                // replies encoded before this call (a sync GET in the same
                // pipeline) live on ctx->stream. they have to hit the socket
                // before KEYS writes, or the client sees KEYS first.
                if (!ctx->stream.empty()) {
                    std::lock_guard lk(socket_write_mutex);
                    const bool ok = stream_out((const char*) ctx->stream.buf.data(), ctx->stream.buf.size());
                    ctx->stream.clear();
                    // the client was let go - see stream_wait
                    if (park::batch_after_write(ok, false) == park::after_write::stop) return;
                }
                auto fn = barch_functions->find(ctx->cn); // not `ic`: that is a member
                if (fn != barch_functions->end()) {
                    auto current = now();
                    int32_t r = ctx->caller.call(ctx->params, fn->second.call);
                    // refused at a key's shard, where it couldn't wait: once more,
                    // checking that key's group first, where it can - TODO 785
                    if (ctx->caller.take_read_deferred())
                        r = ctx->caller.call(ctx->params, fn->second.call);
                    // a call that registered a block has no answer yet, exactly as on
                    // the synchronous path - the reply is written when it resolves
                    if (!ctx->caller.has_blocks())
                        write_result<vector_stream>(ctx->caller, ctx->stream, r);
                    note_command_nanos(fn->second, nanos(current));
                } else if (ctx->f) {
                    // a stored function: it is not in the table, so the context carries
                    // the only handle to it
                    int32_t r = ctx->caller.call(ctx->params, ctx->f);
                    if (ctx->caller.take_read_deferred())
                        r = ctx->caller.call(ctx->params, ctx->f);
                    if (!ctx->caller.has_blocks())
                        write_result<vector_stream>(ctx->caller, ctx->stream, r);
                }
                bool blocked = ctx->caller.has_blocks();
                // what the call encoded for the socket, sent now that it holds nothing -
                // TODO 925. Sent from inside the call, a stalled client held the space's
                // tx_gate, and every COMMITTX there waited on it
                if (park::batch_after_write(ctx->caller.send_socket_tail(), false) == park::after_write::stop)
                    return;             // the client was let go - see stream_wait
                // whatever the call streamed has to be on the socket before write_then
                // starts a write of its own. A client that stalled was let go, and the
                // rest of the batch doesn't run: its replies have nowhere to go, and the
                // commands in it are not ones a disconnected client is still owed
                if (park::batch_after_write(stream_flush(), false) == park::after_write::stop)
                    return;
                hold_before_batch_write_for_test();
                // an unknown command leaves ctx->stream holding whatever synchronous
                // output was carried into it, so it is written either way. Anything
                // ahead of a blocking command in the batch belongs on the wire now,
                // since its replies come before the one being waited for.
                write_then(ctx, [this, self, batch, at, blocked](bool written) {
                    const auto next = park::batch_after_write(written, blocked);
                    if (next == park::after_write::stop)
                        return;         // the client is gone, and write_then let it go
                    if (next == park::after_write::park) {
                        /*
                         * On the socket's thread, as a park from the read handler is -
                         * TODO 880. write_then runs this inline when there's nothing to
                         * write, which is here, on a worker. Parking from it put the
                         * session on its keys' lists with the worker still in
                         * add_caller_blocks, and a wake from then on answered on the
                         * socket's thread: two threads on one caller and one socket.
                         */
                        asio::post(socket_.get_executor(), [this, self, batch, at]() {
                            suspend_for_blocks(batch, at);
                        });
                    } else {
                        run_asynch_batch(batch, at + 1);
                    }
                });
            });
        }
        /**
         * A batch that hits a blocking command stops there. The blocks are moved onto
         * the session's caller, which is the one that answers when they resolve, and the
         * read chain is left suspended - the same as a blocking command that arrives on
         * its own. Where the batch had got to is remembered so the rest of it can run
         * once the block is done, because the replies still owe the client their order.
         */
        void suspend_for_blocks(asynch_batch_ptr batch, size_t at) {
            caller.adopt_blocks((*batch)[at]->caller);
            chain = phase_batch{park_read_out(), std::move(batch), at};
            start_block_to();
            add_caller_blocks();
            // deliberately no do_read(): the chain stays down until the block answers
        }
        /**
         * carry on after a block has been answered and its reply written, either with
         * the rest of the batch that was interrupted or by reading again
         */
        void resume_after_blocks() {
            auto* b = std::get_if<phase_batch>(&chain);
            if (park::after_block(b && b->pending_batch != nullptr) == park::after_answer::rest_of_batch) {
                auto batch = std::move(b->pending_batch);
                auto at = b->pending_at;
                b->pending_batch.reset();
                run_asynch_batch(batch, at + 1);
                return;
            }
            // whatever was pipelined behind the blocking command runs now, through
            // the read handler so a bad request gets its protocol error - TODO 490
            parse_pending = true;
            continue_reading();
        }
        /**
         * write a ctx, then carry on. The continuation runs on the completion, so the
         * caller can be sure this write is finished before it starts another. It is
         * told whether the write worked.
         */
        void write_then(asynch_call_context_ptr ctx, const std::function<void(bool)>& then) {
            if (ctx->stream.empty()) {
                then(true);
                return;
            }
            auto self(this->shared_from_this()); // see TODO 196
            asio::async_write(socket_, asio::buffer(ctx->stream.buf),
                [this, self, ctx, then](std::error_code ec, std::size_t length){
                    if (!ec){
                        net_stat stat;
                        stream_write_ctr += length;
                        bytes_sent += length;
                    } else {
                        ++statistics::repl::net_errors;
                        /*
                         * The client is gone, or reset - TODO 935. The read chain is
                         * down for the batch, so no read sees it, and a reset socket
                         * with input unread peeks as that input. Shut it and let it go,
                         * as stream_next does. This is the socket's thread.
                         */
                        std::error_code ignored;
                        socket_.lowest_layer().shutdown(asio::socket_base::shutdown_both, ignored);
                        let_go.store(true, std::memory_order_release);
                    }
                    then(!ec);
                });
        }
        // write a ctx and preserve its lifetime
        void do_write(asynch_call_context_ptr ctx) {
            if (ctx->stream.empty()) return;

            auto self(this->shared_from_this()); // see TODO 196
            asio::async_write(socket_, asio::buffer(ctx->stream.buf),
                [this, self, ctx](std::error_code ec, std::size_t length){
                    if (!ec){
                        net_stat stat;
                        stream_write_ctr += length;
                        bytes_sent += length;
                    }else {
                        ++statistics::repl::net_errors;
                        //art::err({"error", ec.message(), ec.value()});
                    }
                });
        }
    public:
        TSock socket_;
        // set once the session has shut its socket down for good. The collector
        // frees it on this alone, without the peek: a shut down socket with unread
        // input peeks as that input, not as 0, and nothing reads it any more. The
        // collector only drops its reference, so anything still out keeps the
        // session alive - TODO 489, and stream_wait sets it too - TODO 496
        std::atomic<bool> let_go{false};
        /**
         * The server is stopping and the socket's thread has stopped with it, so
         * nothing queued will go out. A worker waiting for it would wait out the
         * whole rpc_client_max_wait_ms while the stop joins it - TODO 538.
         */
        void abandon_stream() {
            {
                std::lock_guard lk(stream_mut);
                stream_failed = true;
            }
            stream_cv.notify_all();
        }
    private:
        char data_[rpc_io_buffer_size];
        redis::redis_parser parser{};
        rpc_caller caller{};
        vector_stream stream{};
        // the write queue - see send()
        vector_stream sending{};
        vector_stream queued{};
        bool write_busy{false};
        // read/park/batch state machine - replaces the six separate flags (TODO 984)
        chain_state chain{phase_reading{}};
        // complete requests may be waiting in the parser, to run before reading more
        bool parse_pending{false};
        std::vector<std::function<void()>> after_sent{};
        std::mutex socket_write_mutex{};
        // the streamed reply - see stream_out. stream_mut guards everything but
        // stream_writing and stream_written, which only the socket's thread touches
        std::mutex stream_mut{};
        std::condition_variable stream_cv{};
        vector_stream stream_pending{};
        vector_stream stream_writing{};
        size_t stream_written{0};
        uint64_t stream_backlog{0};         // queued or being written, not yet taken
        uint64_t stream_progress{0};        // bumped on every piece the client takes
        bool stream_busy{false};            // a write is out, or about to be
        bool stream_failed{false};          // the client was let go; nothing more streams
        // the queues' sizes, for CLIENT LIST from another thread - see publish_send_queue
        std::atomic<uint64_t> omem_send{0};
        std::atomic<uint32_t> oll_send{0};
        std::atomic<uint64_t> omem_stream{0};
        std::atomic<uint32_t> oll_stream{0};
        // requests waiting in the parser, counted towards max_memory - TODO 493
        std::atomic<uint64_t> qbuf_bytes{0};
        // a command that changes the connection, read while a batch was open. It runs
        // first once the batch is done - TODO 931
        std::vector<std::string> held{};
        std::string prev_cn{};
        /**
         * what waits_on_raft asks of prev_cn, set when prev_cn changes. It compared
         * prev_cn with four names on every call - TODO 845
         */
        enum { prev_other, prev_transaction, prev_cluster } prev_kind = prev_other;
        function_map::iterator ic{};
        // GET's entry, for run_get_batch - TODO 851
        barch_info* get_cmd{nullptr};
        heap::vector<char> get_keys{};
        // SET keys for prefetch_set_run, and how many requests it has warmed ahead
        heap::vector<char> set_keys{};
        size_t set_prefetched{0};
        uint64_t id = ++client_id;
        // atomic because CLIENT LIST reads them from another thread while this
        // session is busy - TODO 492, where TSan first caught it. As is the parser's
        // largest buffer, published by the read handler
        std::atomic<uint64_t> bytes_recv{0};
        std::atomic<uint64_t> bytes_sent{0};
        std::atomic<uint64_t> calls_recv{0};
        std::atomic<uint64_t> parser_peak{0};
        uint64_t created = art::now();

        // millisecond waiter. time_t_timer only ticks once a second, so a
        // 200ms FOREIGN timeout never beat a 500ms fetch.
        asio::steady_timer timer;
        int64_t waiter_deadline{0};
        // when this park's time runs out, kept through a park again - TODO 879
        int64_t park_deadline{0};
        /*
         * Which park this is, one more each time start_block_to starts one - TODO 878.
         * The timer's handler and a wake's continuation each carry the one they were
         * made for, and do nothing once it's gone: answered, or parked again. Without
         * it, a timeout and a wake that met both ran the command and both answered.
         * Read by a wake on another thread, under the shard latch the park took.
         */
        std::atomic<uint64_t> park_gen{0};
        [[nodiscard]] bool still_parked(uint64_t gen) {
            return park::still_parked(gen, park_gen.load(), caller.has_blocks());
        }
        asio::io_context& workers;
        // where calls that wait on a Raft commit run - TODO 626
        asio::io_context& raft_workers;
        std::shared_ptr<function_map> barch_functions = functions_by_name(); // take a snapshot

    };
}
#endif //BARCH_ASIO_RESP_SESISON_H