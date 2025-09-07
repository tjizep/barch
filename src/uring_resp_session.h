//
// Created by teejip on 9/6/25.
//

#ifndef BARCH_URING_RESP_SESSION_H
#define BARCH_URING_RESP_SESSION_H
#include "barch_apis.h"
#include "redis_parser.h"
#include "rpc_caller.h"
#include "uring_context.h"
#include "barch_functions.h"
namespace barch {
    struct work_unit {
        uring_context ur{};

        void run(size_t tid) {
            ur.start(tid);
        }
        void stop() {
            ur.stop();
        }
    };

    class uring_resp_session : public std::enable_shared_from_this<uring_resp_session>
    {
    public:
        uring_resp_session(const uring_resp_session&) = delete;
        uring_resp_session& operator=(const uring_resp_session&) = delete;
        uring_resp_session(tcp::socket socket, std::shared_ptr<work_unit> work, char init_char)
          : socket_(std::move(socket)), work(work)
        {
            caller.set_context(ctx_resp);
            parser.init(init_char);
            ++statistics::repl::redis_sessions;
        }

        ~uring_resp_session() {
            --statistics::repl::redis_sessions;
            if (opt_debug_uring == 1)
                art::std_log("redis session closed",(uint64_t)statistics::repl::redis_sessions);
        }

        void start()
        {
            do_read();
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
    private:
        void run_params(vector_stream& in_stream, const std::vector<std::string_view>& params) {
            if (!params.empty()) {
                //redis::rwrite(stream, OK);
                //return;
            }
            if (prev_cn != params[0]) {
                prev_cn = params[0];
                ic = barch_functions.find(prev_cn);
                if (ic != barch_functions.end() &&
                    !is_authorized(ic->second.cats,caller.get_acl())) {
                    redis::rwrite(in_stream, error{"not authorized"});
                    return;
                    }
            }

            if (ic == barch_functions.end()) {
                redis::rwrite(in_stream, error{"unknown command"});
            } else {

                auto &f = ic->second.call;
                ++ic->second.calls;

                int32_t r = caller.call(params,f);
                if (r < 0) {
                    if (!caller.errors.empty())
                        redis::rwrite(in_stream, error{caller.errors[0]});
                    else
                        redis::rwrite(in_stream, error{"null error"});
                } else {
                    redis::rwrite(in_stream, caller.results);
                }
            }
        }
        void do_parse() {
            while (parser.remaining() > 0) {
                auto &params = parser.read_new_request();
                if (!params.empty()) {
                    run_params(stream, params);
                }else {
                    break;
                }
            }

            do_write(stream);
            do_read();

        }
        void do_read() {
            auto self = shared_from_this();
            work->ur.read(socket_, rreq,{data_, rpc_io_buffer_size}, [this,self](art::value_type v) {
                if (v.size == 0) return;
                if (!socket_.is_open()) return;
                parser.add_data(v.chars(), v.size);
                try {
                    stream.clear();
                    do_parse();
                }catch (std::exception& e) {
                    art::std_err("error", e.what());
                }
            });
        }
        // write statistics are already updated (by vector_stream)
        void do_write(vector_stream& in_stream) {
            if (in_stream.empty()) return;
            if (!socket_.is_open()) return;

            size_t bcount = in_stream.tellg();
            auto self = shared_from_this();
            work->ur.write(socket_, wreq,{in_stream.buf.data(),bcount}, [this,self](art::value_type) {

            });
        }
        tcp::socket socket_;
        std::shared_ptr<work_unit> work{};
        char data_[rpc_io_buffer_size]{};
        redis::redis_parser parser{};
        rpc_caller caller{};
        vector_stream stream{};
        std::string prev_cn{};
        Variable OK = "OK";
        function_map::iterator ic{};
        request rreq{};
        request wreq{};
    };
}
#endif //BARCH_URING_RESP_SESSION_H