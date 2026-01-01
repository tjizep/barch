
//
// Created by teejip on 9/6/25.
//

#ifndef BARCH_URING_RESP_SESSION_H
#define BARCH_URING_RESP_SESSION_H
#include "barch_apis.h"
#include "redis_parser.h"
#include "rpc_caller.h"
#include "uring_context.h"
#include "rpc/barch_functions.h"
#include "abstract_session.h"
#include <sys/socket.h>
#include <netinet/tcp.h>

namespace barch {

    struct uring_work_unit {
        uring_context ur{};

        void run(size_t tid) {
            ur.start(tid);
        }
        void stop() {
            ur.stop();
        }
    };

    class uring_resp_session :
        public abstract_session,
        public std::enable_shared_from_this<uring_resp_session>
    {
    public:
        // Session configuration
        struct session_config {
            static constexpr size_t READ_BUFFER_SIZE = rpc_io_buffer_size;
            static constexpr size_t WRITE_BUFFER_RESERVE = 4096;
            static constexpr bool USE_TCP_NODELAY = false;
            static constexpr int SOCKET_SNDBUF_SIZE = 65536;
            static constexpr int SOCKET_RCVBUF_SIZE = 65536;
        };

        uring_resp_session(const uring_resp_session&) = delete;
        uring_resp_session& operator=(const uring_resp_session&) = delete;

        uring_resp_session(tcp::socket socket, std::shared_ptr<uring_work_unit> work, char init_char)
            : socket_(std::move(socket))
            , work_(std::move(work))
            , id_(++client_id)
            , created_(art::now())
        {
            caller.set_context(ctx_resp);
            caller.info_fun = [this]() -> std::string {
                return get_info();
            };
            parser.init(init_char);

            // Configure socket for optimal performance
            configure_socket();

            // Pre-allocate write buffer
            stream.buf.reserve(session_config::WRITE_BUFFER_RESERVE);

            ++statistics::repl::redis_sessions;
        }

        ~uring_resp_session() override {
            --statistics::repl::redis_sessions;
            if constexpr (uring_config::DEBUG)
                barch::std_log("uring session closed, active:", (uint64_t)statistics::repl::redis_sessions);
        }

        void start() {
            do_read();
        }

        // For blocking operations support
        void do_block_continue() override {
            if (caller.has_blocks()) {
                caller.call_blocks();
                write_result(caller, stream, 0);
                erase_blocks();
                do_write();
                do_read();
            }
        }

        std::string get_info() const {
            try {
                auto rmote = socket_.remote_endpoint();
                auto lcal = socket_.local_endpoint();

                std::string laddress = lcal.address().to_string() + ":" + std::to_string(lcal.port());
                std::string raddress = rmote.address().to_string() + ":" + std::to_string(rmote.port());

                uint64_t seconds = (art::now() - created_) / 1000;
                return "$id=" + std::to_string(id_) + " addr=" + raddress + " "
                    "laddr=" + laddress + " fd=" + std::to_string(0) + " " //socket_.native_handle()
                    "name= age=" + std::to_string(seconds) + " "
                    "idle=0 flags=N db=0 sub=0 psub=0 ssub=0 "
                    "multi=-1 watch=0 qbuf=0 qbuf-free=0 "
                    "tot-net-in=" + std::to_string(bytes_recv_) + " "
                    "tot-net-out=" + std::to_string(bytes_sent_) + " "
                    "tot-cmds=" + std::to_string(calls_recv_) + " "
                    "user=" + caller.get_user() + "\n";
            } catch (...) {
                return "$id=" + std::to_string(id_) + " (disconnected)\n";
            }
        }

    private:
        void configure_socket() {
            int fd = socket_.native_handle();

            // TCP_NODELAY - disable Nagle's algorithm for lower latency
            if constexpr (session_config::USE_TCP_NODELAY) {
                int flag = 1;
                setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
            }

            // Increase socket buffer sizes
            int sndbuf = session_config::SOCKET_SNDBUF_SIZE;
            int rcvbuf = session_config::SOCKET_RCVBUF_SIZE;
            setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
            setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

            // TCP_QUICKACK - send ACKs immediately (Linux specific)
#ifdef TCP_QUICKACK
            int quickack = 1;
            setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &quickack, sizeof(quickack));
#endif
        }

        static bool is_authorized(const heap::vector<bool>& func, const heap::vector<bool>& user) {
            size_t s = std::min<size_t>(user.size(), func.size());
            if (s < func.size()) return false;
            for (size_t i = 0; i < s; ++i) {
                if (func[i] && !user[i])
                    return false;
            }
            return true;
        }

        void write_result(rpc_caller& local_caller, vector_stream& local_stream, int32_t r) {
            if (r < 0) {
                if (!local_caller.errors.empty())
                    redis::rwrite(local_stream, error{local_caller.errors[0]});
                else
                    redis::rwrite(local_stream, error{"null error"});
            } else {
                redis::rwrite(local_stream, local_caller.results);
            }
        }

        void run_params(vector_stream& out_stream, const std::vector<std::string>& params) {
            if (params.empty()) return;

            auto bf = barch_functions; // take a snapshot

            // Cache command lookup
            if (prev_cn_ != params[0]) {
                prev_cn_ = params[0];
                ic_ = bf->find(prev_cn_);

                if (ic_ != bf->end() && !is_authorized(ic_->second.cats, caller.get_acl())) {
                    redis::rwrite(out_stream, error{"not authorized"});
                    return;
                }
            }

            if (ic_ == bf->end()) {
                redis::rwrite(out_stream, error{"unknown command"});
            } else {
                auto& f = ic_->second.call;
                ++ic_->second.calls;
                ++calls_recv_;

                int32_t r = caller.call(params, f);
                if (!caller.has_blocks()) {
                    write_result(caller, out_stream, r);
                }
            }
        }

        void do_parse() {
            while (parser.remaining() > 0) {
                auto& params = parser.read_new_request();
                if (!params.empty()) {
                    run_params(stream, params);
                } else {
                    break;
                }
            }

            if (caller.has_blocks()) {
                add_caller_blocks();
            } else {
                do_write();
                do_read();
            }
        }

        void do_read() {
            if (!socket_.is_open()) return;

            auto self = this->shared_from_this();
            work_->ur.read(socket_, rreq_, {data_, session_config::READ_BUFFER_SIZE},
                [this, self](art::value_type v) {
                    if (v.size == 0 || !socket_.is_open()) return;

                    bytes_recv_ += v.size;
                    parser.add_data(v.chars(), v.size);

                    try {
                        stream.clear();
                        do_parse();
                    } catch (std::exception& e) {
                        barch::std_err("uring session error:", e.what());
                    }
                });
        }

        void do_write() {
            if (stream.empty() || !socket_.is_open()) return;

            size_t bcount = stream.tellg();
            bytes_sent_ += bcount;

            auto self = shared_from_this();
            work_->ur.write(socket_, wreq_, {stream.buf.data(), bcount},
                [this, self](art::value_type v) {
                    // Write completed
                    if constexpr (uring_config::DEBUG) {
                        if (v.size > 0) {
                            barch::std_log("write completed:", v.size, "bytes");
                        }
                    }
                });
        }

        void add_caller_blocks() {
            caller.transfer_rpc_blocks(
                std::static_pointer_cast<abstract_session>(shared_from_this()));
        }

        void erase_blocks() {
            caller.erase_blocks(
                std::static_pointer_cast<abstract_session>(shared_from_this()));
        }

        // Socket and I/O
        tcp::socket socket_;
        std::shared_ptr<uring_work_unit> work_;
        char data_[session_config::READ_BUFFER_SIZE]{};

        // Protocol handling
        redis::redis_parser parser{};
        rpc_caller caller{};
        vector_stream stream{};

        // Command caching
        std::string prev_cn_{};
        function_map::iterator ic_{};

        // io_uring requests (reused per session)
        request rreq_{};
        request wreq_{};

        // Statistics
        uint64_t id_;
        uint64_t bytes_recv_{0};
        uint64_t bytes_sent_{0};
        uint64_t calls_recv_{0};
        uint64_t created_;
    };

    // Factory function to create sessions
    inline std::shared_ptr<uring_resp_session> make_uring_session(
        tcp::socket socket,
        std::shared_ptr<uring_work_unit> work,
        char init_char = '*')
    {
        return std::make_shared<uring_resp_session>(std::move(socket), std::move(work), init_char);
    }

}  // namespace barch

#endif //BARCH_URING_RESP_SESSION_H