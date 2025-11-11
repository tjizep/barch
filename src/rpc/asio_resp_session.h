//
// Created by teejip on 9/7/25.
//

#ifndef BARCH_ASIO_RESP_SESISON_H
#define BARCH_ASIO_RESP_SESISON_H
#include "abstract_session.h"
#include "asio_includes.h"
#include "redis_parser.h"
#include "rpc_caller.h"

#include "vector_stream.h"
#include "server.h"
#include "constants.h"
namespace barch {
    extern std::atomic<uint64_t> client_id;

    class resp_session : public abstract_session, public std::enable_shared_from_this<resp_session>
    {
    public:
        resp_session(const resp_session&) = delete;
        resp_session& operator=(const resp_session&) = delete;
        resp_session(tcp::socket socket,char init_char);
        ~resp_session() override;

        void start();
        //static bool is_authorized(const heap::vector<bool>& func,const heap::vector<bool>& user) ;
        std::string get_info() const ;
        void do_block_continue() override;
    private:
        void write_result(int32_t r);
        void run_params(vector_stream& stream, const std::vector<std::string_view>& params) ;
        void do_read();
        void start_block_to();
        void add_caller_blocks();
        void erase_blocks() ;

        void do_block_to() ;
        void do_write(vector_stream& stream);
        tcp::socket socket_;
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
    };
    typedef std::shared_ptr<resp_session> resp_session_ptr;
}
#endif //BARCH_ASIO_RESP_SESISON_H