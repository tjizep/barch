//
// Created by teejip on 9/7/25.
//

#ifndef BARCH_ASIO_RESP_SESISON_H
#define BARCH_ASIO_RESP_SESISON_H
#include "asio_includes.h"
#include "netstat.h"
namespace barch {
    class resp_session : public std::enable_shared_from_this<resp_session>
        {
        public:
            resp_session(const resp_session&) = delete;
            resp_session& operator=(const resp_session&) = delete;
            resp_session(tcp::socket socket,char init_char)
              : socket_(std::move(socket))
            {
                parser.init(init_char);
                ++statistics::repl::redis_sessions;
            }
            ~resp_session() {
                --statistics::repl::redis_sessions;
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
            template<typename PT>
            void run_params(vector_stream& stream, const PT& params) {
                std::string cn{ params[0]};
                auto bf = barch_functions; // take a snapshot
                if (prev_cn != cn) {
                    ic = bf->find(cn);
                    prev_cn = cn;
                    if (ic != bf->end() &&
                        !is_authorized(ic->second.cats,caller.get_acl())) {
                        redis::rwrite(stream, error{"not authorized"});
                        return;
                    }
                }
                if (ic == bf->end()) {
                    redis::rwrite(stream, error{"unknown command"});
                } else {

                    auto &f = ic->second.call;
                    ++ic->second.calls;

                    int32_t r = caller.call(params,f);
                    if (r < 0) {
                        if (!caller.errors.empty())
                            redis::rwrite(stream, error{caller.errors[0]});
                        else
                            redis::rwrite(stream, error{"null error"});
                    } else {
                        redis::rwrite(stream, caller.results);
                    }
                }
            }
            void do_read()
            {
                    auto self(shared_from_this());
                    socket_.async_read_some(asio::buffer(data_, rpc_io_buffer_size),
                        [this, self](std::error_code ec, std::size_t length)
                    {

                        if (!ec){

                            parser.add_data(data_, length);
                            try {

                                stream.clear();
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
                            }catch (std::exception& e) {
                                art::std_err("error", e.what());
                            }
                        }
                    });

            }

            void do_write(vector_stream& stream) {
                auto self(shared_from_this());
                if (stream.empty()) return;

                asio::async_write(socket_, asio::buffer(stream.buf),
                    [this, self](std::error_code ec, std::size_t length){
                        if (!ec){
                                net_stat stat;
                                stream_write_ctr += length;
                        }else {
                            //art::std_err("error", ec.message(), ec.value());
                        }
                    });
            }
            tcp::socket socket_;
            char data_[rpc_io_buffer_size];
            redis::redis_parser parser{};
            rpc_caller caller{};
            vector_stream stream{};
            std::string prev_cn{};
            function_map::iterator ic{};
        };

}
#endif //BARCH_ASIO_RESP_SESISON_H