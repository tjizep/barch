//
// Created by teejip on 9/7/25.
//

#ifndef BARCH_ASIO_RESP_SESISON_H
#define BARCH_ASIO_RESP_SESISON_H
#include "socket_peek.h"
#include <sys/socket.h>
#include <cerrno>
#include <atomic>
#include <cctype>
#include <condition_variable>
#include <mutex>
#include <utility>

#include "abstract_session.h"
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
        resp_session(sock_T socket, asio::io_context &workers)
        : socket_(std::move(socket)), timer( socket_.get_executor()), workers(workers)
        {
            caller.info_fun = [this]() -> std::string {
                return get_info(socket_);
            };
            bind_socket_writer();
            caller.set_context(ctx_resp);
            //asio::socket_base::send_buffer_size option(65536); // or larger
            //socket_.set_option(option);

            ++statistics::repl::redis_sessions;
        }
        template<typename sock_T>
        resp_session(sock_T socket, asio::io_context &workers, char init_char)
            : socket_(std::move(socket)), timer(socket_.get_executor()), workers(workers)
        {
            parser.init(init_char);
            caller.info_fun = [this]() -> std::string {
                return get_info(socket_);
            };
            bind_socket_writer();
            caller.set_context(ctx_resp);
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
                }
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
           return get_info_t( sock);
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
                timer.cancel();
                auto self(this->shared_from_this());
                // the fetch landed after the waiter deadline: this GET is
                // still a timeout. the value is already stored for the next one.
                if (waiter_deadline && art::now() >= waiter_deadline) {
                    asio::post(this->socket_.get_executor(), [this,self]() {
                        do_block_to();
                    });
                    return;
                }
                // post, not execute: SET/FOREIGN_MISS/finish_fetch call this
                // while the shard write lock is still held. execute can run
                // the waiter on this thread, and reply_after_wait takes the
                // same lock.
                waiter_deadline = 0;
                // where to send the turn next, read before erase_blocks clears them
                barch::key_space_ptr woken_space;
                size_t woken_shard = 0;
                for (auto& d : caller.get_blocks()) {
                    if (d.key == woken_by) {
                        woken_space = d.space;
                        woken_shard = d.shard_index;
                        break;
                    }
                }
                asio::post(this->socket_.get_executor(),
                           [this,self,woken_by,woken_space,woken_shard]() {
                    int r = caller.call_blocks();
                    // the waiter looked and there was nothing for it. Stay parked: the
                    // blocks are still on the caller, but call_unblock took this session
                    // out of the key's list when it woke us, so it has to go back in.
                    // Only the timeout answers a waiter that never got anything
                    if (caller.take_block_retry()) {
                        add_caller_blocks();
                        start_block_to();
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

        void do_callback_into_socket_context(vector_stream& local_stream) {
            send(local_stream);
            continue_reading();
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
                : caller(caller), f(std::move(f)), params(params.begin(), params.end()), cn(cn) {}
            rpc_caller caller{};
            barch_function f{};
            vector_stream stream{}; // the stream buffer needs to stau alive while the call completes
            std::vector<std::string> params{};
            std::string cn;
        };
        typedef std::shared_ptr<asynch_call_context> asynch_call_context_ptr;

        template<typename Stream>
        void run_params(Stream& ostream, const std::vector<redis::string_param_t>& params,heap::vector<asynch_call_context_ptr> &asynch_calls) {

            std::string_view raw = params[0];
            std::string cn;
            std::string fn_space;
            key_space_ptr old_spc;
            bool should_reset_space = false;
            try {
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
                        // a stored function could not say it writes before, so nothing
                        // it did was ever replicated. One that declares write and data
                        // now goes on to the destinations like any builtin - TODO 188
                        if (fn->is_write && fn->is_data && barch::repl::has_destinations()) {
                            std::vector<std::string> owned(params.begin(), params.end());
                            repl::call(owned);
                        }
                        if (!asynch_calls.empty()) {
                            auto ctx = std::make_shared<asynch_call_context>(caller, fn->call, params, prev_cn);
                            if (!stream.empty())
                                ctx->stream = std::move(stream);
                            asynch_calls.push_back(ctx);
                        } else {
                            caller.reply_out = &ostream;
                            int32_t r = caller.call(params, fn->call);
                            caller.reply_out = nullptr;
                            if (!caller.has_blocks())
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
                    if (ic->second.is_asynch || !asynch_calls.empty()) {
                        // this is relatively slow so only potentially long-running and expensive calls should be marked as asynch
                        if (!stream.empty()) {
                            asynch_call_context_ptr ctx = std::make_shared<asynch_call_context>(caller,f,params,prev_cn);
                            ctx->stream = std::move(stream); // move the current stream - it should be empty after the move
                            asynch_calls.push_back(ctx);
                        }else {
                            asynch_calls.emplace_back(std::make_shared<asynch_call_context>(caller,f,params,prev_cn));
                        }

                    }else {

                        // auto current = now(); // remove this for now since it has a measurable impact on performance

                        caller.reply_out = &ostream;
                        int32_t r = caller.call(params,f);
                        caller.reply_out = nullptr;
                        if (!caller.has_blocks())
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
            if (output_backlog() >= rpc_output_high_water) {
                read_paused = true;
                return;
            }
            if (parse_pending) {
                parse_pending = false;
                do_read(true);
                return;
            }
            do_read();
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
            if (read_paused && output_backlog() < rpc_output_high_water / 2) {
                read_paused = false;
                continue_reading();
            }
        }
        // the async call context needs to stay alive while calls complete.
        // `buffered` runs what's already in the parser instead of reading more
        void do_read(bool buffered = false) {
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
            auto on_read = [this, self](std::error_code ec, std::size_t length)
            {

                if (!ec){
                    bytes_recv += length;
                    parser.add_data(data_, length);
                    parser_peak.store(parser.get_max_buffer_size(), std::memory_order_relaxed);
                    publish_query();

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
                    if (caller.has_blocks())
                        erase_blocks();
                    // a read error is a dead or broken socket - the peer went
                    // away mid-conversation. EOF between requests is the normal
                    // close, however much the client ran first, and is not
                    // counted: it used to be whenever the client had sent
                    // anything at all. See TODO 391 and 440.
                    if (ec != asio::error::eof || parser.mid_request())
                        ++statistics::repl::net_errors;
                    //if (ec.category())
                     //barch::err({ec.message().c_str()});
                }
            };
            if (buffered) {
                // posted rather than called, so a long run of pauses and resumes
                // never builds up a stack
                asio::post(socket_.get_executor(), [on_read]() mutable { on_read({}, 0); });
                return;
            }
            socket_.async_read_some(asio::buffer(data_, rpc_io_buffer_size), std::move(on_read));
        }

        void start_block_to() {
            if (caller.block_to_ms == 0 || caller.block_to_ms >= (uint64_t) std::numeric_limits<int64_t>::max()) {
                waiter_deadline = 0;
                return;
            }
            timer.cancel();
            waiter_deadline = art::now() + static_cast<int64_t>(caller.block_to_ms);
            timer.expires_after(std::chrono::milliseconds(caller.block_to_ms));
            auto self(this->shared_from_this());
            timer.async_wait([this,self](const std::error_code& ec)
                {
                    if (!ec) {
                        do_block_to();
                    }
                });
        }
        void add_caller_blocks() {
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
         * async_wait, not a read: it reports the socket readable without taking any
         * bytes, so a command pipelined behind the blocking one is still in the buffer
         * for consume_available() to find when the block answers. Readable with
         * nothing to peek is the peer having closed.
         */
        void watch_for_disconnect() {
            if (watching_disconnect || !caller.has_blocks())
                return;
            watching_disconnect = true;
            auto self(this->shared_from_this());
            socket_.lowest_layer().async_wait(
                asio::socket_base::wait_read,
                [this, self](const std::error_code& ec) {
                    watching_disconnect = false;
                    if (ec)
                        return;                       // cancelled, or the socket is gone
                    if (!caller.has_blocks())
                        return;                       // answered while this was armed
                    // through the fd: lowest_layer() is a basic_socket, which has no
                    // receive of its own, and a peek is the whole point here
                    auto n = peek_now(socket_.lowest_layer().native_handle());
                    if (n > 0)
                        return;                       // pipelined bytes, not a disconnect
                    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
                        watch_for_disconnect();       // readable but nothing there yet
                        return;
                    }
                    // gone. Drop the blocks the way the read error path does and let
                    // the session go with them - holding the last reference here means
                    // it dies when this handler returns
                    timer.cancel();
                    erase_blocks();
                });
        }
        /** parse already-buffered requests. true if the connection is parked. */
        bool consume_available() {
            stream.clear();
            heap::vector<asynch_call_context_ptr> asynch_calls;
            while (parser.remaining() > 0) {
                auto &params = parser.read_new_request();
                if (params.empty())
                    break;
                ++calls_recv;
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
                when_sent([this, self, batch]() {
                    run_asynch_batch(batch, 0);
                });
                return true;
            }
            if (caller.has_blocks()) {
                start_block_to();
                add_caller_blocks();
                return true;
            }
            return false;
        }
        void erase_blocks() {
            caller.erase_blocks(this->shared_from_this());

        }
        void do_block_to() {
            waiter_deadline = 0;
            erase_blocks();
            int r = caller.call_blocks();
            write_result(caller, stream, r);
            auto self(this->shared_from_this());
            send(stream);
            when_sent([this, self]() {
                resume_after_blocks();
            });
        }
        /**
         * KEYS writes each encoded item through here, from the worker pool, so the
         * reply never sits on the result stack. The mutex is the glob workers: they
         * call in together and each item has to go out whole.
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
         * makes KEYS stop and let go of glob_queue. 0 waits for as long as it takes.
         */
        bool stream_out(const char* data, size_t n) {
            std::unique_lock lk(stream_mut);
            if (!stream_wait(lk, rpc_stream_high_water))
                return false;
            if (!n) return true;
            stream_pending.write(data, n);
            stream_backlog += n;
            publish_stream_queue_locked();
            if (!stream_busy) {
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
            const uint64_t limit_ms = barch::get_rpc_max_client_wait_ms();
            while (!stream_failed && stream_backlog >= below) {
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
            if (stream_written >= stream_writing.buf.size()) {
                std::lock_guard lk(stream_mut);
                if (stream_pending.empty() || stream_failed) {
                    stream_pending.clear();
                    stream_writing.clear();
                    stream_written = 0;
                    stream_backlog = 0;
                    stream_busy = false;
                    publish_stream_queue_locked();
                    stream_cv.notify_all();
                    return;
                }
                // swapped rather than copied; only this thread touches stream_writing
                std::swap(stream_writing.buf, stream_pending.buf);
                std::swap(stream_writing.pos, stream_pending.pos);
                stream_pending.clear();
                stream_written = 0;
                publish_stream_queue_locked();
            }
            auto self(this->shared_from_this());
            socket_.async_write_some(
                asio::buffer(stream_writing.buf.data() + stream_written,
                             stream_writing.buf.size() - stream_written),
                [this, self](std::error_code ec, std::size_t length) {
                    stream_written += length;
                    if (ec) {
                        stream_writing.clear();
                        stream_written = 0;
                    }
                    {
                        std::lock_guard lk(stream_mut);
                        if (ec) {
                            if (!stream_failed)
                                ++statistics::repl::net_errors;
                            stream_failed = true;
                        } else {
                            stream_backlog -= std::min<uint64_t>(stream_backlog, length);
                            ++stream_progress;
                        }
                        publish_stream_queue_locked();
                        stream_cv.notify_all();
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
        typedef std::shared_ptr<heap::vector<asynch_call_context_ptr>> asynch_batch_ptr;
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
                asio::post(socket_.get_executor(), [this, self]() { continue_reading(); });
                return;
            }
            // the read chain is down for the length of a batch, so its self pointer
            // is not holding this session up - the sessions vector was, and the
            // collector can null that while this lambda is streaming to the socket.
            // That is the use-after-free TSan reports. See TODO 196.
            auto self(this->shared_from_this());
            asio::post(workers, [this, self, batch, at]() {
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
                    if (!ok) return;        // the client was let go - see stream_wait
                }
                auto fn = barch_functions->find(ctx->cn); // not `ic`: that is a member
                if (fn != barch_functions->end()) {
                    auto current = now();
                    int32_t r = ctx->caller.call(ctx->params, fn->second.call);
                    // a call that registered a block has no answer yet, exactly as on
                    // the synchronous path - the reply is written when it resolves
                    if (!ctx->caller.has_blocks())
                        write_result<vector_stream>(ctx->caller, ctx->stream, r);
                    note_command_nanos(fn->second, nanos(current));
                } else if (ctx->f) {
                    // a stored function: it is not in the table, so the context carries
                    // the only handle to it
                    int32_t r = ctx->caller.call(ctx->params, ctx->f);
                    if (!ctx->caller.has_blocks())
                        write_result<vector_stream>(ctx->caller, ctx->stream, r);
                }
                bool blocked = ctx->caller.has_blocks();
                // whatever the call streamed has to be on the socket before write_then
                // starts a write of its own. A client that stalled was let go, and the
                // rest of the batch doesn't run: its replies have nowhere to go, and the
                // commands in it are not ones a disconnected client is still owed
                if (!stream_flush())
                    return;
                // an unknown command leaves ctx->stream holding whatever synchronous
                // output was carried into it, so it is written either way. Anything
                // ahead of a blocking command in the batch belongs on the wire now,
                // since its replies come before the one being waited for.
                write_then(ctx, [this, batch, at, blocked]() {
                    if (blocked) {
                        suspend_for_blocks(batch, at);
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
            pending_batch = batch;
            pending_at = at;
            start_block_to();
            add_caller_blocks();
            // deliberately no do_read(): the chain stays down until the block answers
        }
        /**
         * carry on after a block has been answered and its reply written, either with
         * the rest of the batch that was interrupted or by reading again
         */
        void resume_after_blocks() {
            if (pending_batch) {
                auto batch = pending_batch;
                auto at = pending_at;
                pending_batch.reset();
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
         * caller can be sure this write is finished before it starts another.
         */
        void write_then(asynch_call_context_ptr ctx, const std::function<void()>& then) {
            if (ctx->stream.empty()) {
                then();
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
                    }
                    then();
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
        // stopped taking requests until the replies drain - see continue_reading
        bool read_paused{false};
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
        // an asynchronous batch that stopped on a blocking command, and how far it got.
        // Set while the chain is suspended and cleared as it is picked back up.
        asynch_batch_ptr pending_batch{};
        size_t pending_at{0};
        // one disconnect watch at a time while parked - see watch_for_disconnect
        bool watching_disconnect{false};
        std::string prev_cn{};
        function_map::iterator ic{};
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
        asio::io_context& workers;
        std::shared_ptr<function_map> barch_functions = functions_by_name(); // take a snapshot

    };
}
#endif //BARCH_ASIO_RESP_SESISON_H