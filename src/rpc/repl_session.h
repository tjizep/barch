//
// Created by teejip on 10/7/25.
//

#ifndef BARCH_REPL_SESSION_H
#define BARCH_REPL_SESSION_H
#include "barch_parser.h"
#include "statistics.h"
#include "vector_stream.h"
#include "asio_includes.h"
#include <memory>

#include "module.h"

namespace barch {
    // a session for art replication alone
    //
    class repl_session : public std::enable_shared_from_this<repl_session>
    {
    public:
        enum {
            header_size = 1 + sizeof(uint32_t)*4
        };
        repl_session(const repl_session&) = delete;
        repl_session& operator=(const repl_session&) = delete;
        repl_session(tcp::socket socket, uint32_t bytes_alread_read)
        :   socket_(std::move(socket))
        ,   bytes_already_read(bytes_alread_read)
        {
            ++statistics::repl::art_sessions;
        }
        ~repl_session() {
            --statistics::repl::art_sessions;
        }

        void start()
        {
            do_read_header();
        }

    private:
        void do_read_data() {
            if (stream.empty()) {
                do_read_header();
                return;
            }
            stream.seek(0);
            auto self(shared_from_this());
            asio::async_read(socket_, asio::buffer(stream.buf.data(), stream.buf.size()),
                [this, self](std::error_code ec, std::size_t unused(length)) {
                    if (!ec) {
                        auto t = get_art(shard);
                        ostream.clear();
                        // self calls will deadlock here
                        write_lock release(t->get_latch());
                        process_art_fun_cmd(t, ostream, stream.buf);
                        do_write(ostream);
                    }else {
                        // clients can disconnect for all sorts of reasons
                    }

                });
        }
        void do_read_header()
        {

            if (bytes_already_read > 1 + sizeof(uint32_t)) {
                barch::std_err("invalid header offset",bytes_already_read);
                return;
            }
            stream.buf.resize(header_size - bytes_already_read);
            uint32_t old_bytes_already_read = bytes_already_read;
            bytes_already_read = 0;// reset asap
            // TODO: if the data sent by the client is too small then it will block
            // so we need some kind of timeout maybe
            auto self(shared_from_this());
            asio::async_read(socket_, asio::buffer(stream.buf.data(), stream.buf.size()),
                [this, self, old_bytes_already_read](std::error_code ec, std::size_t unused(length)) {
                    if (!ec) {
                        try {
                            uint32_t buffers_size = 0;
                            char start;
                            uint32_t cmd = 0;
                            shard = 0;
                            uint32_t count = 0;
                            stream.seek(0);
                            if (old_bytes_already_read == 0) {
                                readp(stream,start);
                                if (start != 0) {
                                    barch::std_err("repl: invalid repl start byte",start);
                                    return;
                                }
                                readp(stream,cmd);
                                if (cmd != cmd_art_fun) {
                                    barch::std_err("repl: invalid repl command",cmd);
                                    return;
                                }
                            }
                            readp(stream,shard);
                            readp(stream,count);
                            readp(stream,buffers_size);
                            if (buffers_size == 0) {
                                barch::std_err("repl: invalid buffer size", buffers_size);
                                return;
                            }
                            stream.buf.resize(buffers_size);
                            do_read_data();
                            //art::std_log("cmd apply changes ",shard, "[",buffers_size,"] bytes","keys",count,"actual",actual,"total",(long long)statistics::repl::key_add_recv);
                        }catch (std::exception& e) {
                            barch::std_err("repl: failed to apply changes due to unexpected error", e.what());
                            return;
                        }
                    }else if (ec.value() != 2){
                        barch::std_err("repl: socket error receiving data",ec.message(),ec.value());
                    }
                });
        }

        void do_write(const vector_stream& s) {
            auto self(this->shared_from_this());
            if (s.empty()) {
                do_read_header();
                return;
            }
            asio::async_write(socket_, asio::buffer(s.buf),
                [this, self](std::error_code ec, std::size_t /*length*/){
                    if (!ec){
                        do_read_header();
                    }
                });
        }
        tcp::socket socket_;
        uint32_t shard{};
        vector_stream stream{};
        vector_stream ostream{};
        uint32_t bytes_already_read{};
    };
}
#endif //BARCH_REPL_SESSION_H