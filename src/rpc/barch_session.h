//
// Created by teejip on 9/17/25.
//

#ifndef BARCH_BARCH_SESSION_H
#define BARCH_BARCH_SESSION_H
#include "barch_parser.h"
#include "statistics.h"
#include "vector_stream.h"
#include "asio_includes.h"
#include <memory>

namespace barch {
    class barch_session : public std::enable_shared_from_this<barch_session>
    {
    public:
        barch_session(const barch_session&) = delete;
        barch_session& operator=(const barch_session&) = delete;
        explicit barch_session(tcp::socket socket)
          : socket_(std::move(socket))
        {
            ++statistics::repl::redis_sessions;
        }
        ~barch_session() {
            --statistics::repl::redis_sessions;
        }

        void start()
        {
            do_read();
        }

    private:
        void do_read()
        {
            auto self(shared_from_this());
            socket_.async_read_some(asio::buffer(data_, rpc_io_buffer_size),
                [this, self](std::error_code ec, std::size_t length)
            {

                if (!ec){
                    stream_read_ctr += length;
                    parser.add_data(data_, length);
                    try {
                        vector_stream out;
                        parser.process(out);
                        if (out.tellg() > 0) {
                            do_write(out);
                            do_read();
                        }else {
                            do_read();
                        }
                    }catch (std::exception& e) {
                        art::std_err("error", e.what());
                    }
                }else {
                    //art::std_err("error", ec.message(), ec.value());
                }
            });
        }

        void do_write(const vector_stream& stream) {
            auto self(this->shared_from_this());

            asio::async_write(socket_, asio::buffer(stream.buf),
                [this, self](std::error_code ec, std::size_t /*length*/){
                    if (!ec){
                        do_read();
                    }
                });
        }
        tcp::socket socket_;
        uint8_t data_[rpc_io_buffer_size]{};
        barch_parser parser{};
    };
}
#endif //BARCH_BARCH_SESSION_H