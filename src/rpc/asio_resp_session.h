//
// Created by teejip on 9/7/25.
//

#ifndef BARCH_ASIO_RESP_SESISON_H
#define BARCH_ASIO_RESP_SESISON_H
#include <cctype>
#include <mutex>
#include <utility>

#include "abstract_session.h"
#include "asio_includes.h"
#include "redis_parser.h"
#include "rpc_caller.h"
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
            std::string r =
                "id="+std::to_string(this->id)+" addr="+raddress+ " "
                "laddr="+laddress+" fd="+"10"+ " "
                "name="+""+" age="+std::to_string(seconds)+" "+
                "idle=0 flags=N capa= db=0 sub=0 psub=0 ssub=0 "+
                "multi=-1 watch=0 qbuf=0 qbuf-free=0 argv-mem=10 multi-mem=0 "+
                "rbs=1024 rbp=0 obl=0 oll=0 omem=0 tot-mem="+std::to_string(rpc_io_buffer_size+parser.get_max_buffer_size())+" "+
                "events=r cmd=client|info user="+caller.get_user()+" redir=-1 "+
                "resp="+std::to_string(caller.get_protocol())+" lib-name= lib-ver= "+
                // SCAN cursors this connection is holding, and what they cost. An
                // abandoned scan keeps one alive until the connection closes, so a
                // client that leaks them can see it here
                "iters="+std::to_string(caller.iteration_count())+" "+
                "iters-mem="+std::to_string(caller.iteration_memory())+" "+
                "tot-net-in="+ std::to_string(bytes_recv)+ " " +
                "tot-net-out=" + std::to_string(bytes_sent)+ " " +
                "tot-cmds=" + std::to_string(calls_recv) + "\n";
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
                    auto out = std::make_shared<vector_stream>(std::move(stream));
                    stream.clear();
                    write_then(out, [this, self]() {
                        resume_after_blocks();
                    });
                });
            }
        }

        void do_callback_into_socket_context(vector_stream& local_stream) {
            do_write(local_stream);
            do_read();
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
                } else if (!is_authorized(ic->second.cats, caller.get_space_acl())) {
                    redis::rwrite(ostream, error{"not authorized"});
                    return;
                } else {
                    auto &f = ic->second.call;
                    ++ic->second.calls;
                    if (ic->second.is_write() && ic->second.is_data()
                        && barch::repl::has_destinations()) {
                        std::vector<std::string> owned(params.begin(), params.end());
                        repl::call(owned);
                    }


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
        // the async call context needs to stay alive while calls complete
        void do_read() {
            socket_.async_read_some(asio::buffer(data_, rpc_io_buffer_size),
                [this](std::error_code ec, std::size_t length)// NOTE: the self shared pointers can cause noticeable cpu usage so we keep the session afloat elsewhere
            {

                if (!ec){
                    bytes_recv += length;
                    parser.add_data(data_, length);

                    try {

                        if (!consume_available()) {
                            do_write(stream);
                            do_read();
                        }

                    }catch (std::exception& e) {
                        barch::err({"error", e.what()});
                    }
                }else {
                    if (caller.has_blocks())
                        erase_blocks();
                    //if (ec.category())
                     //barch::err({ec.message().c_str()});
                }
            });
        }

        void start_block_to() {
            if (caller.block_to_ms == 0 || caller.block_to_ms >= std::numeric_limits<long>::max()) {
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
            }
            if (!asynch_calls.empty()) {
                auto batch = std::make_shared<heap::vector<asynch_call_context_ptr>>(
                    std::move(asynch_calls));
                run_asynch_batch(batch, 0);
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
            auto out = std::make_shared<vector_stream>(std::move(stream));
            stream.clear();
            write_then(out, [this, self]() {
                resume_after_blocks();
            });
        }
        /**
         * KEYS writes each encoded item through here. asio::write puts the
         * bytes on the socket now, which is what keeps the reply off the
         * result stack. The mutex is the glob workers: they call in together
         * and one write must finish before the next starts.
         *
         * Anything already encoded for this connection (a GET that ran in
         * the same pipeline, before KEYS) has to leave first, or KEYS
         * overtakes it on the wire.
         */
        bool write_socket_now(const char* data, size_t n) {
            if (!n) return true;
            std::error_code ec;
            asio::write(socket_, asio::buffer(data, n), ec);
            if (ec) return false;
            net_stat stat;
            stream_write_ctr += n;
            bytes_sent += n;
            return true;
        }
        void drain_stream(vector_stream& s) {
            if (s.empty()) return;
            write_socket_now((const char*) s.buf.data(), s.buf.size());
            s.clear();
        }
        void bind_socket_writer() {
            caller.write_socket_bytes = [this](const char* data, size_t n) -> bool {
                std::lock_guard lk(socket_write_mutex);
                drain_stream(stream);
                return write_socket_now(data, n);
            };
        }

        void do_write(const vector_stream& local_stream) {

            if (local_stream.empty()) return;

            asio::async_write(socket_, asio::buffer(local_stream.buf),
                [this](std::error_code ec, std::size_t length){ // NOTE: the self shared pointers can cause noticeable cpu usage so we keep the session afloat elsewhere
                    if (!ec){
                        net_stat stat;
                        stream_write_ctr += length;
                        bytes_sent += length;
                    }else {
                        //art::err({"error", ec.message(), ec.value()});
                    }
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
                do_read(); // the one place the chain is resumed for an asynchronous batch
                return;
            }
            asio::post(workers, [this, batch, at]() { // NOTE: the self shared pointers can cause noticeable cpu usage so we keep the session afloat elsewhere
                auto ctx = (*batch)[at];
                // replies encoded before this call (a sync GET in the same
                // pipeline) live on ctx->stream. they have to hit the socket
                // before KEYS writes, or the client sees KEYS first.
                if (!ctx->stream.empty()) {
                    std::lock_guard lk(socket_write_mutex);
                    drain_stream(ctx->stream);
                }
                auto fn = barch_functions->find(ctx->cn); // not `ic`: that is a member
                if (fn != barch_functions->end()) {
                    auto current = now();
                    int32_t r = ctx->caller.call(ctx->params, fn->second.call);
                    // a call that registered a block has no answer yet, exactly as on
                    // the synchronous path - the reply is written when it resolves
                    if (!ctx->caller.has_blocks())
                        write_result<vector_stream>(ctx->caller, ctx->stream, r);
                    fn->second.total_nanos += nanos(current);
                } else if (ctx->f) {
                    // a stored function: it is not in the table, so the context carries
                    // the only handle to it
                    int32_t r = ctx->caller.call(ctx->params, ctx->f);
                    if (!ctx->caller.has_blocks())
                        write_result<vector_stream>(ctx->caller, ctx->stream, r);
                }
                bool blocked = ctx->caller.has_blocks();
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
            if (!consume_available()) {
                do_write(stream);
                do_read();
            }
        }
        /**
         * write a standalone buffer and carry on once it is out, keeping it alive in the
         * meantime. Used where a reply has to be on the wire before the next call starts.
         */
        void write_then(std::shared_ptr<vector_stream> out, const std::function<void()>& then) {
            if (out->empty()) {
                then();
                return;
            }
            asio::async_write(socket_, asio::buffer(out->buf),
                [this, out, then](std::error_code ec, std::size_t length){
                    if (!ec){
                        net_stat stat;
                        stream_write_ctr += length;
                        bytes_sent += length;
                    }
                    then();
                });
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
            asio::async_write(socket_, asio::buffer(ctx->stream.buf),
                [this, ctx, then](std::error_code ec, std::size_t length){
                    if (!ec){
                        net_stat stat;
                        stream_write_ctr += length;
                        bytes_sent += length;
                    }
                    then();
                });
        }
        // write a ctx and preserve its lifetime
        void do_write(asynch_call_context_ptr ctx) {
            if (ctx->stream.empty()) return;

            asio::async_write(socket_, asio::buffer(ctx->stream.buf),
                [this, ctx](std::error_code ec, std::size_t length){ // NOTE: the self shared pointers can cause noticeable cpu usage so we keep the session afloat elsewhere
                    if (!ec){
                        net_stat stat;
                        stream_write_ctr += length;
                        bytes_sent += length;
                    }else {
                        //art::err({"error", ec.message(), ec.value()});
                    }
                });
        }
    public:
        TSock socket_;
    private:
        char data_[rpc_io_buffer_size];
        redis::redis_parser parser{};
        rpc_caller caller{};
        vector_stream stream{};
        std::mutex socket_write_mutex{};
        // an asynchronous batch that stopped on a blocking command, and how far it got.
        // Set while the chain is suspended and cleared as it is picked back up.
        asynch_batch_ptr pending_batch{};
        size_t pending_at{0};
        std::string prev_cn{};
        function_map::iterator ic{};
        uint64_t id = ++client_id;
        uint64_t bytes_recv = 0;
        uint64_t bytes_sent = 0;
        uint64_t calls_recv = 0;
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