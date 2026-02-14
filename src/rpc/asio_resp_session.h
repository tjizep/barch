//
// Created by teejip on 9/7/25.
//

#ifndef BARCH_ASIO_RESP_SESISON_H
#define BARCH_ASIO_RESP_SESISON_H
#include <utility>

#include "abstract_session.h"
#include "asio_includes.h"
#include "redis_parser.h"
#include "rpc_caller.h"
#include "netstat.h"
#include "vector_stream.h"
#include "constants.h"
#include "rpc/proto_info.h"
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
            //asio::socket_base::send_buffer_size option(65536); // or larger
            //socket_.set_option(option);

            ++statistics::repl::redis_sessions;
        }
        template<typename sock_T>
        resp_session(sock_T socket, asio::io_context &workers, char init_char)
            : socket_(std::move(socket)), timer( socket_.get_executor()), workers(workers)
        {
            parser.init(init_char);
            caller.info_fun = [this]() -> std::string {
                return get_info(socket_);
            };
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
                "$id="+std::to_string(this->id)+" addr="+raddress+ " "
                "laddr="+laddress+" fd="+"10"+ " "
                "name="+""+" age="+std::to_string(seconds)+" "+
                "idle=0 flags=N capa= db=0 sub=0 psub=0 ssub=0 "+
                "multi=-1 watch=0 qbuf=0 qbuf-free=0 argv-mem=10 multi-mem=0 "+
                "rbs=1024 rbp=0 obl=0 oll=0 omem=0 tot-mem="+std::to_string(rpc_io_buffer_size+parser.get_max_buffer_size())+" "+
                "events=r cmd=client|info user="+caller.get_user()+" redir=-1 "+
                "resp=2 lib-name= lib-ver= "+
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

        void do_block_continue() override {
            if (caller.has_blocks()) {
                auto self(this->shared_from_this());
                this->socket_.get_executor().execute([this,self]() {
                    caller.call_blocks();
                    write_result(caller, stream, 0);
                    erase_blocks();
                    do_write(stream);
                    do_read();

                });
            }
        }

        void do_callback_into_socket_context(vector_stream& local_stream) {
            do_write(local_stream);
            do_read();
        }
    private:

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
            } else {
                redis::rwrite(local_stream, local_caller.results);
            }
        }

        struct call_context {
            call_context(const rpc_caller& caller, barch_function f,std::vector<redis::string_param_t> params ): caller(caller), f(std::move(f)), params(std::move(params)) {}
            rpc_caller caller{};
            barch_function f{};
            vector_stream stream{}; // the stream buffer needs to stau alive while the call completes
            std::vector<redis::string_param_t> params{};
        };
        typedef std::shared_ptr<call_context> call_context_ptr;

        template<typename Stream>
        void run_params(Stream& ostream, const std::vector<redis::string_param_t>& params,heap::vector<call_context_ptr> &asynch_calls) {
            std::string cn{ params[0]};

            auto colon = cn.find_last_of(':');
            auto old_spc = caller.kspace();
            bool should_reset_space = false;
            try {
                if (colon != std::string::npos && colon < cn.size()-1) {
                    std::string space = cn.substr(0,colon);
                    cn = cn.substr(colon+1);

                    if (!old_spc || old_spc->get_canonical_name() != space) {

                        caller.set_kspace(barch::get_keyspace(space));
                        should_reset_space = true;
                    }
                }

                auto bf = barch_functions; // take a snapshot
                if (prev_cn != cn) {
                    ic = bf->find(cn);
                    prev_cn = cn;
                    if (ic != bf->end() &&
                        !is_authorized(ic->second.cats,caller.get_acl())) {
                        redis::rwrite(ostream, error{"not authorized"});
                        return ;
                    }
                }
                if (ic == bf->end()) {
                    redis::rwrite(ostream, error{"unknown command"});
                } else {
                    // TODO: ic->second.is_asynch
                    auto &f = ic->second.call;
                    ++ic->second.calls;
                    if (ic->second.is_write() && ic->second.is_data()) {
                        repl::call(params);
                    }
                    // once one call is asynch all calls in this batch must be asynch to preserve order
                    if (ic->second.is_asynch || !asynch_calls.empty()) {
                        // this is relatively slow so only potentially long-running and expensive calls should be marked as asynch
                        if (!stream.empty()) {
                            call_context_ptr ctx = std::make_shared<call_context>(caller,f,params);
                            ctx->stream = std::move(stream); // move the current stream - it should be empty after the move
                            asynch_calls.push_back(ctx);
                        }else {
                            asynch_calls.emplace_back(std::make_shared<call_context>(caller,f,params));
                        }

                    }else {
                        int32_t r = caller.call(params,f);
                        if (!caller.has_blocks())
                            write_result<Stream>(caller, ostream, r);
                    }
                }
            }catch (std::exception& e) {
                redis::rwrite(ostream, error{e.what()});
            }
            if (should_reset_space)
                caller.set_kspace(old_spc); // return to old value

        }
        // the async call context needs to stay alive while calls complete
        void do_read() {
            auto self(this->shared_from_this());
            socket_.async_read_some(asio::buffer(data_, rpc_io_buffer_size),
                [this, self](std::error_code ec, std::size_t length)
            {

                if (!ec){
                    bytes_recv += length;
                    parser.add_data(data_, length);

                    try {

                        stream.clear();
                        heap::vector<call_context_ptr> asynch_calls;
                        // collect the call results into the "stream" buffer
                        while (parser.remaining() > 0) {
                            auto &params = parser.read_new_request();
                            if (!params.empty()) {
                                ++calls_recv;
                                run_params(stream, params, asynch_calls);
                            }else {
                                break;
                            }
                        }
                        for (auto ctx: asynch_calls) {
                            // all data will be kept alive while "workers" are busy
                            asio::post(workers,[this, self, ctx]() {
                                int32_t r = ctx->caller.call(ctx->params,ctx->f);
                                write_result<vector_stream>(ctx->caller, ctx->stream, r);
                                do_write(ctx);
                                do_read();
                            });
                        }
                        if (caller.has_blocks()) {
                            start_block_to();
                            add_caller_blocks();
                        }else{
                            do_write(stream);
                            do_read();
                        }

                    }catch (std::exception& e) {
                        barch::std_err("error", e.what());
                    }
                }else {
                    //if (ec.category())
                     //barch::std_err(ec.message().c_str());
                }
            });
        }

        void start_block_to() {
            if (caller.block_to_ms == 0 || caller.block_to_ms >= std::numeric_limits<long>::max()) {
                return;
            }
            timer.cancel();
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
        }
        void erase_blocks() {
            caller.erase_blocks(this->shared_from_this());

        }
        void do_block_to() {
            erase_blocks();
            caller.call_blocks();
            write_result(caller, stream, 0);
            do_write(stream);
            do_read();
        }
        void do_write(const vector_stream& local_stream) {
            auto self(this->shared_from_this());
            if (local_stream.empty()) return;

            asio::async_write(socket_, asio::buffer(local_stream.buf),
                [this, self](std::error_code ec, std::size_t length){
                    if (!ec){
                        net_stat stat;
                        stream_write_ctr += length;
                        bytes_sent += length;
                    }else {
                        //art::std_err("error", ec.message(), ec.value());
                    }
                });
        }
        // write a ctx and preserve its lifetime
        void do_write(call_context_ptr ctx) {
            auto self(this->shared_from_this());
            if (ctx->stream.empty()) return;

            asio::async_write(socket_, asio::buffer(ctx->stream.buf),
                [this, self, ctx](std::error_code ec, std::size_t length){
                    if (!ec){
                        net_stat stat;
                        stream_write_ctr += length;
                        bytes_sent += length;
                    }else {
                        //art::std_err("error", ec.message(), ec.value());
                    }
                });
        }

        TSock socket_;
        char data_[rpc_io_buffer_size];
        redis::redis_parser parser{};
        rpc_caller caller{};
        vector_stream stream{};
        std::string prev_cn{};
        function_map::iterator ic{};
        uint64_t id = ++client_id;
        uint64_t bytes_recv = 0;
        uint64_t bytes_sent = 0;
        uint64_t calls_recv = 0;
        uint64_t created = art::now();

        time_t_timer timer;
        asio::io_context& workers;

    };
}
#endif //BARCH_ASIO_RESP_SESISON_H