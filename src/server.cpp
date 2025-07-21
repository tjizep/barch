//
// Created by teejip on 5/22/25.
//

#include "server.h"
#include <sys/unistd.h>
#include "logger.h"


#include <utility>
#include "module.h"
#include "statistics.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Weffc++"

#include <asio/io_context.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/signal_set.hpp>
#include <asio/post.hpp>
#include <asio/dispatch.hpp>
#include <asio/write.hpp>
#include <asio.hpp>
#include "asio/io_service.hpp"
#pragma GCC diagnostic pop
#include <cstdio>
#include "barch_apis.h"
#include "swig_api.h"
#include "swig_caller.h"
#include "vk_caller.h"
#include "redis_parser.h"
enum {
    cmd_ping = 1,
    cmd_stream = 2,
    cmd_art_fun = 3,
    cmd_barch_call = 4
};
enum {
    opt_max_workers = 8,
    opt_read_timout = 60000
};

using asio::ip::tcp;
using asio::local::stream_protocol;

namespace barch {
    static auto barch_functions = functions_by_name();
    template<typename T>
    bool time_wait(int64_t millis, T&& fwait ) {
        if (fwait()) {
            return true;
        }
        auto start = std::chrono::steady_clock::now();
        while (true) {
            if (fwait()) {
                return true;
            }
            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count() > millis) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    struct net_stat {
        uint64_t saved_writes = stream_write_ctr;
        uint64_t saved_reads = stream_read_ctr;
        net_stat() {
        }
        ~net_stat() {
            statistics::repl::bytes_recv += stream_read_ctr - saved_reads;
            statistics::repl::bytes_sent += stream_write_ctr - saved_writes;
        }
    };

    inline double ntohl(double x) {
        return __builtin_bswap32(x);
    }
    template<typename T>
    void push_size_t(heap::vector<uint8_t>& buffer,T s) {
        uint8_t tb[sizeof(T)];
        T size = htonl(s);
        memcpy(tb, &size, sizeof(T));
        buffer.insert(buffer.end(), tb, tb + sizeof(T));
    }
    template<typename T>
    T get_size_t(size_t at, const heap::vector<uint8_t>& buffer) {
        if (sizeof(T) > buffer.size()-at) {
            throw_exception<std::runtime_error>("invalid size");
        }
        T size = 0;
        memcpy(&size, buffer.data()+at, sizeof(T));
        return ntohl(size);
    }
    template<typename TBF>
    void timeout(TBF&& check, int max_to = 1000) {
        int to = max_to;
        while (check() && to > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            --to;
        }
    }
    void push_options(heap::vector<uint8_t>& buffer, const art::key_options& options) {
        buffer.push_back(options.flags);
        if (options.has_expiry())
            push_size_t<uint64_t>(buffer, options.get_expiry());
    }
    std::pair<art::key_options,size_t> get_options(size_t at, const heap::vector<uint8_t>& buffer) {
        if (buffer.size() <= at+sizeof(uint64_t) + 1) {
            throw_exception<std::runtime_error>("invalid size");
        }
        art::key_options r;
        r.flags = buffer[at];
        if (r.has_expiry()) {
            r.set_expiry(get_size_t<uint64_t>(at+1, buffer));
            return {r, at+sizeof(uint64_t)+1};
        }
        return {r, at+1};
    }
    std::pair<art::value_type,size_t> get_value(size_t at, const heap::vector<uint8_t>& buffer) {
        auto size = get_size_t<uint32_t>(at, buffer);
        if (buffer.size()+sizeof(size)+at + 1< size) {
            throw_exception<std::runtime_error>("invalid size");
        }
        art::value_type r = {buffer.data() + at + sizeof(uint32_t),size};
        return {r, at+sizeof(size)+size + 1};
    }

    void push_value(heap::vector<uint8_t>& buffer, art::value_type v) {
        push_size_t<uint32_t>(buffer, v.size);
        buffer.insert(buffer.end(), v.bytes, v.bytes + v.size);
        buffer.push_back(0x00);
    }
    void push_value(heap::vector<uint8_t>& buffer, const std::string& v) {
        push_value(buffer, art::value_type{v});
    }
    void push_value(heap::vector<uint8_t>& buffer, const Variable& v) {
        uint8_t i = v.index();
        if (i >= 255) {
            throw_exception<std::runtime_error>("invalid index");
        }
        buffer.push_back(i);
        switch (i) {
            case var_bool:
                buffer.push_back(*std::get_if<bool>(&v) ? 1 : 0);
                break;
            case var_int64:
                push_size_t<uint64_t>(buffer, *std::get_if<int64_t>(&v));
                break;
            case var_double:
                push_size_t<double>(buffer, *std::get_if<double>(&v));
                break;
            case var_string:
                push_value(buffer, *std::get_if<std::string>(&v));
                break;
            case var_null:
                break;
            default:
                break;
        }

    }

    std::pair<Variable,size_t> get_variable(size_t at, const heap::vector<uint8_t>& buffer) {
        std::pair<art::value_type,size_t> vt;
        auto bsize = buffer.size();
        if (at >= bsize) {
            throw_exception<std::runtime_error>("invalid at");
        }
        uint8_t i = buffer[at];
        if (i >= 255) {
            throw_exception<std::runtime_error>("invalid index");
        }
        ++at;
        switch (i) {
            case var_bool:
                if (at + 1 > bsize) {
                    throw_exception<std::runtime_error>("invalid at");
                }
                return { buffer[at] == 1,at+1} ;
            case var_int64:
                if (at + sizeof(int64_t) > bsize) {
                    throw_exception<std::runtime_error>("invalid at");
                }
                return  {get_size_t<int64_t>(at, buffer),at+sizeof(int64_t)};
            case var_double:
                if (at + sizeof(double) > bsize) {
                    throw_exception<std::runtime_error>("invalid at");
                }
                return { get_size_t<double>(at, buffer), at+sizeof(double)};
            case var_string:
                vt = get_value(at, buffer);
                return { std::string(vt.first.chars(), vt.first.size),vt.second};
            case var_null:
                return { nullptr,at};
            default:
                break;
        }
        throw_exception<std::runtime_error>("invalid index");
        return {};
    }

    bool recv_buffer(std::iostream& stream, heap::vector<uint8_t>& buffer) {
        uint32_t buffers_size = 0;
        readp(stream,buffers_size);
        if (buffers_size == 0) {
            return false;
        }
        buffer.clear();
        buffer.resize(buffers_size);
        readp(stream, buffer.data(), buffer.size());
        return true;
    }
    void send_art_fun(uint32_t shard, uint32_t messages, std::iostream& stream, heap::vector<uint8_t>& to_send) {
        uint32_t cmd = cmd_art_fun;
        uint32_t buffers_size = to_send.size();
        uint32_t sh = shard;
        writep(stream, cmd);
        writep(stream, sh);
        writep(stream, messages);
        writep(stream, buffers_size);
        writep(stream, to_send.data(), to_send.size());
        stream.flush();

    }

    host_id get_host_id() {
        return {"localhost", getpid() % 10000000000000000000ULL};
    }
    struct af_result {
        size_t add_called {};
        size_t add_applied {};
        size_t remove_called {};
        size_t remove_applied {};
        size_t find_called {};
        size_t find_applied {};
        bool error{false};
    };
    /**
     * process a cmd_art_fun buffer
     * Note: no latching in this function all latches are set outside
     * @param t
     * @param buffer buffer containing commands and data
     * @param buffers_size size of the buffer (from 0)
     * @param stream for writing reply data
     * @return true if no errors occurred
     */

    af_result process_art_fun_cmd(art::tree* t, tcp::iostream& stream, heap::vector<uint8_t>& buffer) {
        af_result r;
        heap::vector<uint8_t> tosend;
        uint32_t actual = 0;
        art::node_ptr found;
        uint32_t buffers_size = buffer.size();
        bool flush_buffers = false;
        for (size_t i = 0; i < buffers_size;) {
            char cmd = buffer[i];
            switch (cmd) {
                case 'c': {

                }
                case 'i': {
                        auto options = get_options(i+1, buffer);
                        auto key = get_value(options.second, buffer);
                        auto value = get_value(key.second, buffer);
                        if (statistics::logical_allocated > art::get_max_module_memory()) {
                            // do not add data if memory limit is reached
                            statistics::oom_avoided_inserts++;
                            r.error = true;
                            return r;
                        } else {
                            art_insert(t, options.first, key.first, value.first,true,
                                [&r](const art::node_ptr &){
                                    ++statistics::repl::key_add_recv_applied;
                                    ++r.add_applied;
                            });

                        }
                        ++statistics::repl::key_add_recv;
                        ++actual;
                        ++r.add_called;
                        i = value.second;
                    }
                    break;
                    case 'r': {
                        auto key = get_value(i+1, buffer);
                        art_delete(t, key.first,[&r](const art::node_ptr &) {
                            ++statistics::repl::key_rem_recv_applied;
                            ++r.remove_applied;
                        });
                        i = key.second;
                        ++statistics::repl::key_rem_recv;
                        ++actual;
                        ++r.remove_called;
                    }
                    break;
                case 'f': {
                        auto key = get_value(i+1, buffer);
                        found = art::find(t, key.first);
                        if (found.is_leaf) {
                            auto l = found.const_leaf();
                            tosend.push_back('i');
                            push_options(tosend, *l);
                            push_value(tosend, l->get_key());
                            push_value(tosend, l->get_value());
                            ++r.find_applied;
                        }
                        i = key.second;
                        ++statistics::repl::key_find_recv;
                        ++actual;
                        ++r.find_called;
                        flush_buffers = true;
                    }
                    break;
                case 'a': {
                    auto lkey = get_value(i+1, buffer);
                    auto ukey = get_value(lkey.second, buffer);
                    ++r.find_called;
                    art::iterator ai(t, lkey.first);
                    while (ai.ok()) {
                        auto k = ai.key();
                        if (k >= lkey.first && k <= ukey.first) {
                            tosend.push_back('i');
                            auto l = found.const_leaf();
                            push_options(tosend, *l);
                            push_value(tosend, l->get_key());
                            push_value(tosend, l->get_value());
                            ++r.find_applied;
                        } else {
                            break;
                        }
                        ai.next();
                    }
                    flush_buffers = true;
                    i = ukey.second;
                    ++statistics::repl::key_find_recv;
                    ++actual;
                }
                break;
                default:
                    art::std_err("unknown command", cmd);
                    r.error = true;
                    return r;
            }
        }
        if (flush_buffers) {
            auto s = (uint32_t)tosend.size();
            writep(stream,s);
            writep(stream,tosend.data(),tosend.size());
            stream.flush();
        }
        return r;
    }
    struct server_context {

        std::thread server_thread[rpc_io_thread_count]{};
        asio::io_service io{rpc_io_thread_count};
        tcp::acceptor acc;
        std::string interface;
        uint_least16_t port;
        std::atomic<size_t> threads_started = 0;

        void stop() {
            try {
                acc.close();
            }catch (std::exception& e) {}
            try {
                io.stop();
            }catch (std::exception& e) {
                art::std_err("failed to stop io service", e.what());
            }

            for (int it = 0; it < rpc_io_thread_count; ++it) {

                if (server_thread[it].joinable())
                    server_thread[it].join();
                server_thread[it] = {};
            }
            port = 0;
        }
        void start_accept() {
            try {

                acc.async_accept([this](asio::error_code error, tcp::socket endpoint) {
                    if (error) {
                        art::std_err("accept error",error.message(),error.value());
                        return; // this happens if there are not threads
                    }
                    {
                        net_stat stat;
                        process_data(endpoint);
                    }
                    start_accept();
                });

            }catch (std::exception& e) {
                art::std_err("failed to start/run replication server", interface, port, e.what());
            }
        }
        class barch_parser {
        public:
            enum {
                header_size = 4
            };
            enum {
                barch_wait_for_header = 1,
                barch_wait_for_buffer_size,
                barch_wait_for_buffer,
                barch_parse_params
            };
            barch_parser() = default;
            ~barch_parser() = default;
            vector_stream in{};
            int state = barch_wait_for_header;
            uint32_t calls = 0;
            uint32_t c = 0;
            uint32_t buffers_size = 0;
            uint32_t replies_size = 0;

            swig_caller caller{};
            heap::vector<uint8_t> replies;
            heap::vector<uint8_t> buffer;
            void clear() {
                state = barch_wait_for_header;
                calls = 0;
                buffers_size = 0;
                replies_size = 0;
                c = 0;
                replies.clear();
                buffer.clear();
                in.clear();
            }
            size_t remaining() const {
                if (in.pos > in.buf.size()) {
                    art::std_err("invalid buffer size", in.buf.size());
                    return 0;
                }
                return in.buf.size() - in.pos;
            }
            /**
             *
             * @return true if someing needs to be written
             */
            bool process(vector_stream& out) {
                while (remaining() > 0) {
                    switch (state) {
                        case barch_wait_for_header:
                            if (remaining() >= header_size) {
                                readp(in,calls);
                                if (1 != calls) {
                                    return false; // nothing to do - wait for more data
                                }
                                c = 0;
                                buffers_size = 0;
                                state = barch_wait_for_buffer_size;
                            }
                            break;
                        case barch_wait_for_buffer_size:
                            if (c < calls) {
                                if (remaining() < sizeof(buffers_size)) {
                                    return false; // nothing to do - wait for more data
                                }
                                readp(in,buffers_size);
                                if (buffers_size == 0) {
                                    art::std_err("invalid buffer size", buffers_size);
                                    clear();
                                    return false;
                                }
                                buffer.resize(buffers_size);
                                state = barch_wait_for_buffer;
                            }else {
                                state = barch_wait_for_header;
                                if (remaining()  >= in.buf.size())
                                    return false;
                            }
                            break;
                        case barch_wait_for_buffer:{
                            // wait for input buffer to reach a target
                            if (remaining() < buffers_size) {
                                return false; // nothing to do - wait for more data
                            }
                            readp(in, buffer.data(), buffers_size);
                            int32_t r = 0;
                            if (buffers_size > rpc_max_param_buffer_size) {
                                r = -1;
                                replies.clear();
                                push_value(replies, error{"parameter buffer too large"});
                            }else {
                                // TODO: max buffer size check
                                std::vector<std::string_view> params;
                                for (size_t i = 0; i < buffers_size;) {
                                    auto vp = get_value(i, buffer);
                                    params.push_back({vp.first.chars(), vp.first.size});
                                    i = vp.second;
                                }
                                std::string cn = std::string{params[0]};
                                auto ic = barch_functions.find(cn);
                                if (ic == barch_functions.end()) {
                                    art::std_err("invalid call", cn);
                                    writep(out, replies_size);
                                    clear();
                                    return true;
                                }
                                auto f = ic->second.call;
                                ++ic->second.calls;
                                r = caller.call(params,f);
                                replies.clear();
                                for (auto &v: caller.results) {
                                    push_value(replies,v);
                                }
                            }

                            replies_size = replies.size();
                            writep(out, r);
                            writep(out, replies_size);
                            writep(out, replies.data(), replies_size);
                            ++c;
                            if (c < calls) {
                                state = barch_wait_for_buffer;
                            }else {
                                state = barch_wait_for_header;
                                buffers_size = 0;
                            }

                            if (remaining() <= 0) {
                                in.clear();
                                return true;
                            }
                        }
                    }
                }
                return false; // consumed all data but no action could be taken
            }
            void add_data(const uint8_t *data, size_t length) {
                in.buf.insert(in.buf.end(),data,data+length);
            }
        };

        class resp_session : public std::enable_shared_from_this<resp_session>
        {
        public:
            resp_session(tcp::socket socket, char init_char)
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

        private:
            void run_params(vector_stream& stream, const heap::vector<std::string>& params) {
                const std::string &cn = params[0];
                auto ic = barch_functions.find(cn);
                if (ic == barch_functions.end()) {
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
                                vector_stream stream;

                                while (parser.remaining() > 0) {
                                    auto &params = parser.read_new_request();
                                    if (!params.empty()) {
                                        run_params(stream, params);
                                    }else {
                                        break;
                                    }
                                }
                                if (!stream.empty()) {
                                    net_stat stat;
                                    do_write(stream);
                                }else {
                                    try {
                                        do_read();
                                    }catch (std::exception& e) {
                                        art::std_err("error", e.what());
                                    }

                                }

                            }catch (std::exception& e) {
                                art::std_err("error", e.what());
                            }
                        }
                    });

            }

            void do_write(const vector_stream& stream) {
                auto self(shared_from_this());

                asio::async_write(socket_, asio::buffer(stream.buf),
                    [this, self](std::error_code ec, std::size_t /*length*/){
                        if (!ec){
                            try {
                                do_read();
                            }catch (std::exception& e) {
                                art::std_err("error", e.what());
                            }

                        }else {
                            //art::std_err("error", ec.message(), ec.value());
                        }
                    });
            }

            tcp::socket socket_;
            char data_[rpc_io_buffer_size];
            redis::redis_parser parser{};
            swig_caller caller{};
        };

        class barch_session : public std::enable_shared_from_this<barch_session>
        {
        public:
            barch_session(tcp::socket socket)
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
                        parser.add_data(data_, length);
                        try {
                            vector_stream out;
                            parser.process(out);
                            if (out.tellg() > 0) {
                                do_write(out);
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
                auto self(shared_from_this());

                asio::async_write(socket_, asio::buffer(stream.buf),
                    [this, self](std::error_code ec, std::size_t /*length*/){
                        if (!ec){
                            do_read();
                        }
                    });
            }

            tcp::socket socket_;
            uint8_t data_[rpc_io_buffer_size];
            barch_parser parser{};
        };

        void process_data(tcp::socket& endpoint) {
            try {
                char cs[1] ;
                //readp(stream, cs);
                endpoint.read_some(asio::buffer(cs));
                if (cs[0]) {
                    std::make_shared<resp_session>(std::move(endpoint),cs[0])->start();
                    return;
                }
                uint32_t cmd = 0;
                endpoint.read_some(asio::buffer(&cmd, sizeof(cmd)));
                if (cmd == cmd_barch_call) {
                    std::make_shared<barch_session>(std::move(endpoint))->start();
                    return;
                }
                heap::vector<uint8_t> buffer{};
                art::key_spec spec;

                tcp::iostream stream(std::move(endpoint));
                if (stream.fail())
                    return;
                switch (cmd) {
                    case cmd_ping:
                        try {
                            writep(stream,rpc_server_version);
                        }catch (std::exception& e) {
                            art::std_err("error",e.what());
                        }

                        break;
                    case cmd_stream:
                        try {
                            uint32_t shard = 0;
                            readp(stream,shard);
                            if (shard < art::get_shard_count().size()) {
                                get_art(shard)->send(stream);
                                stream.flush();
                            }else {
                                art::std_err("invalid shard", shard);
                            }

                        }catch (std::exception& e) {
                            art::std_err("failed to stream shard", e.what());
                            return;
                        }
                        break;
                    case cmd_barch_call:
                        try{
                        }catch (std::exception& e) {
                            art::std_err("failed to make barch call", e.what());
                            return;
                        }
                        break;
                    case cmd_art_fun:
                        try {
                            uint32_t buffers_size = 0;
                            uint32_t shard = 0;
                            uint32_t count = 0;
                            readp(stream,shard);
                            readp(stream,count);
                            readp(stream,buffers_size);
                            if (buffers_size == 0) {
                                art::std_err("invalid buffer size", buffers_size);
                                return;
                            }

                            buffer.resize(buffers_size);
                            readp(stream, buffer.data(), buffers_size);
                            auto t = get_art(shard);
                            storage_release release(t->latch);
                            process_art_fun_cmd(t, stream, buffer);
                            //art::std_log("cmd apply changes ",shard, "[",buffers_size,"] bytes","keys",count,"actual",actual,"total",(long long)statistics::repl::key_add_recv);
                        }catch (std::exception& e) {
                            art::std_err("failed to apply changes", e.what());
                            return;
                        }
                        break;
                    default:
                        art::std_err("unknown command", cmd);
                        return;
                }
            }catch (std::exception& e) {
                art::std_err("failed to read command", e.what());
                return;
            }

        }
        server_context(std::string interface, uint_least16_t port)
        :   acc(io, tcp::endpoint(tcp::v4(), port))
        ,   interface(interface)
        ,   port(port){
            start_accept();

            for (int it = 0; it < rpc_io_thread_count; ++it) {
                server_thread[it] = std::thread([this,it]() {
                    art::std_log("server started on", this->interface,this->port,"using thread",it);
                    try {
                        io.run();
                    }catch (std::exception& e) {
                        art::std_err("failed to run server", e.what());
                    }

                    art::std_log("server stopped on thread",it);
                });
            }
        }
    };
    std::shared_ptr<server_context>  srv = nullptr;
    void server::start(const std::string& interface, uint_least16_t port) {
        if (srv) srv->stop();
        try {
            srv = std::make_shared<server_context>(interface, port);
        }catch (std::exception& e) {
            art::std_err("failed to start server", e.what());
        }

    }

    void server::stop() {
        if (srv) srv->stop();
    }
    struct module_stopper {
        module_stopper() = default;
        ~module_stopper() {
            server::stop();
        }
    };
    static module_stopper stopper;
    namespace repl {
        class rpc_impl : public rpc{
        private:
            heap::vector<uint8_t> to_send{};
            std::string host;
            int port;
            asio::io_context ioc{};
            tcp::socket s;

            heap::vector<uint8_t> replies{};
            vector_stream stream{};
            std::error_code error{};
        public:
            rpc_impl(const std::string& host, int port)
            : host(host), port(port), s(ioc) {

            }
            std::error_code net_error() const {
                return error;
            }
            virtual ~rpc_impl() {
            }
            void run(std::chrono::steady_clock::duration timeout) {
                ioc.restart();
                ioc.run_for(timeout);
                if (!ioc.stopped()) {
                    s.close();
                    ioc.run();
                }
            }

            template<typename SockT,typename BufT>
            size_t write(SockT& sock, const BufT& buf) {
                size_t r = 0;
                asio::async_write(sock, buf,[&](const std::error_code& result_error,
                std::size_t result_n)
                {
                    r += result_n;
                    stream_write_ctr += result_n;
                    error = result_error;
                });
                run(art::get_rpc_write_to_s());
                if (error) {
                    ++statistics::repl::request_errors;
                    throw_exception<std::runtime_error>("failed to write");
                };
                return r;
            }

            template<typename SockT,typename BufT>
            size_t read(SockT& sock, BufT buf) {
                size_t r = 0;
                asio::async_read(sock, buf,[&](const std::error_code& result_error,
                std::size_t result_n)
                {
                    r += result_n;
                    stream_read_ctr += result_n;
                    error = result_error;
                });
                run(art::get_rpc_read_to_s());
                if (error) {
                    ++statistics::repl::request_errors;
                    throw_exception<std::runtime_error>("failed to read");
                };
                return r;
            }

            int call(int& callr, heap::vector<Variable>& result, const std::vector<std::string_view>& params) {
                to_send.clear();
                if (params.empty()) {
                    return -1;
                }
                try {
                    stream.clear();

                    if (!s.is_open()) {
                        tcp::resolver resolver(ioc);
                        auto resolution = resolver.resolve(host,std::to_string(port));
                        asio::async_connect(s, resolution,[this](const std::error_code& ec, tcp::endpoint unused(endpoint)) {
                            if (!ec) {
                                uint32_t cmd = cmd_barch_call;
                                writep(stream,uint8_t{0x00});
                                writep(stream, cmd);
                            }
                            error = ec;
                        });
                        run(art::get_rpc_connect_to_s());
                        if (error) {
                            throw_exception<std::runtime_error>("failed to connect");
                        };
                    }

                    for (auto p: params) {
                        push_value(to_send, art::value_type{p});
                    }

                    uint32_t calls = 1;
                    uint32_t buffers_size = to_send.size();
                    writep(stream, calls);
                    writep(stream, buffers_size);
                    writep(stream, to_send.data(), to_send.size());
                    net_stat stat;
                    write(s,asio::buffer(stream.buf.data(), stream.buf.size()));

                    read(s,asio::buffer(&callr,sizeof(callr)));
                    read(s,asio::buffer(&buffers_size,sizeof(buffers_size)));
                    replies.resize(buffers_size);
                    size_t reply_length = read(s,asio::buffer(replies));
                    if (reply_length != buffers_size) {
                        art::std_err(reply_length,"!=",buffers_size);
                    }
                    for (size_t i = 0; i < buffers_size; i++) {
                        auto v = get_variable(i, replies);
                        result.emplace_back(v.first);
                        i = v.second;
                    }
                }catch (std::exception& e) {
                    art::std_err("call failed [", e.what(),"] to",host,port,"because [",error.message(),error.value(),"]");
                    return -1;
                }
                return 0;
            }
        };
        std::shared_ptr<barch::repl::rpc> create(const std::string& host, int port) {
            return std::make_shared<rpc_impl>(host, port);
        }

        client::~client() {
            connected = false;
            if (tpoll.joinable())
                tpoll.join();
        };

        void client::stop() {
            connected = false;
            if (tpoll.joinable())
                tpoll.join();
        }

        void client::add_destination(std::string host, int port) {
            auto dest = repl_dest{std::move(host), port, shard};
            {
                std::lock_guard lock(this->latch);
                for (auto& d : destinations) {
                    if (d.host == dest.host && d.port == dest.port) {
                        art::std_err("destination already added", d.host, d.port);
                        return;
                    }
                }
                destinations.emplace_back(dest);
            }
            std::lock_guard lock(this->latch);
            if (!connected) {
                connected = true;
                tpoll = std::thread([this]() {
                heap::vector<uint8_t> to_send;
                to_send.reserve(art::get_rpc_max_buffer());
                try {
                    asio::io_service io;
                    heap::vector<repl_dest> dests;
                    uint32_t messages = 0;
                    int64_t total_messages = 0;
                    while (connected) {

                        {
                            std::lock_guard lock(this->latch);
                            to_send.swap(this->buffer);
                            messages = this->messages;
                            total_messages += messages;
                            this->buffer.clear();
                            this->messages = 0;
                            dests = destinations;
                        }
                        if (!to_send.empty()) {
                            for (auto& dest : dests) {
                                try {
                                    net_stat stat;
                                    tcp::iostream stream(dest.host, std::to_string(dest.port));
                                    if (stream.fail()) {
                                        continue;
                                    }
                                    uint32_t cmd = cmd_art_fun;
                                    uint32_t buffers_size = to_send.size();
                                    uint32_t sh = this->shard;
                                    writep(stream,uint8_t{0x00});
                                    writep(stream, cmd);
                                    writep(stream, sh);
                                    writep(stream, messages);
                                    if (buffers_size == 0) {
                                        art::std_err("invalid buffer size", buffers_size);
                                        return;
                                    }
                                    writep(stream, buffers_size);
                                    writep(stream, to_send.data(), to_send.size());
                                    stream.flush();
                                    stream.close();
                                    //art::std_log("sent", buffers_size, "bytes to", dest.host, dest.port, "total sent",total_messages,"still queued",(uint32_t)this->messages,"iq",(long long)statistics::repl::insert_requests);
                                }catch (std::exception& e) {
                                    art::std_err("failed to write to stream", e.what(),"to",dest.host,dest.port);
                                }
                            }
                            to_send.clear();
                        }
                        if (this->messages == 0) // send asap
                            std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    }
                }catch (std::exception& e) {
                    this->connected = false;
                    art::std_err("failed to connect to remote server", this->host, this->port,e.what());
                }
            });
            }

        }

        bool client::add_source(std::string host, int port) {
            auto src = repl_dest{std::move(host), port, shard};
            {
                std::lock_guard lock(this->latch);
                for (auto& d : sources) {
                    if (d.host == src.host && d.port == src.port) {
                        art::std_err("source already added", d.host, d.port);
                        return false;
                    }
                }
                sources.emplace_back(src);
            }
            return true;
        }
        bool client::insert(const art::key_options& options, art::value_type key, art::value_type value) {

            if (!connected) {
                if (!destinations.empty()) ++statistics::repl::instructions_failed;
                return destinations.empty();
            }
            bool ok = time_wait(art::get_rpc_max_client_wait_ms(), [this]() {
                return buffer.size() < art::get_rpc_max_buffer();
            });

            if (!ok) {
                ++statistics::repl::instructions_failed;
                art::std_err("replication buffer size exceeded", buffer.size());
                return false;
            }
            std::lock_guard lock(this->latch);
            buffer.push_back('i');
            push_options(buffer, options);
            push_value(buffer, key);
            push_value(buffer, value);
            ++statistics::repl::insert_requests;
            ++messages;
            return true;
        }

        bool client::remove(art::value_type key) {
            if (!connected) {
                if (!destinations.empty()) ++statistics::repl::instructions_failed;
                return destinations.empty();
            }
            bool ok = time_wait(art::get_rpc_max_client_wait_ms(), [this]() {
                return buffer.size() < art::get_rpc_max_buffer();
            });
            if (!ok) {
                ++statistics::repl::instructions_failed;
                art::std_err("replication buffer size exceeded", buffer.size());
                return false;
            }

            std::lock_guard lock(this->latch);
            buffer.push_back('r');
            push_value(buffer, key);
            ++statistics::repl::remove_requests;
            ++messages;
            return true;
        }
        void client::send_art_fun(std::iostream& stream, const heap::vector<uint8_t>& to_send) {
            uint32_t cmd = cmd_art_fun;
            uint32_t buffers_size = to_send.size();
            uint32_t sh = this->shard;
            writep(stream,uint8_t{0x00});
            writep(stream, cmd);
            writep(stream, sh);
            writep(stream, messages);
            writep(stream, buffers_size);
            writep(stream, to_send.data(), to_send.size());
            stream.flush();

        }
        [[nodiscard]] bool client::begin_transaction() const {
            try {
            }catch (std::exception& e) {
                art::std_err("failed to begin transaction", e.what());
                return false;
            }
            return true;
        }
        bool client::load(size_t shard) {
            try {
                if (!ping()) {
                    return false;
                }
                art::std_log("load shard",shard);
                tcp::iostream stream(host, std::to_string(this->port));
                if (!stream) {
                    art::std_err("failed to connect to remote server", host, this->port);
                    return false;
                }
                uint32_t cmd = cmd_stream;
                uint32_t s = shard;
                net_stat stats;
                writep(stream,uint8_t{0x00});
                writep(stream,cmd);
                writep(stream, s);
                if (!get_art(shard)->retrieve(stream)) {
                    art::std_err("failed to retrieve shard", shard);
                    return false;
                }
                stream.close();

                return true;
            }catch (std::exception& e) {
                art::std_err("failed to load shard", shard, e.what());
                return false;
            }
        }
        bool client::find_insert(art::value_type key) {
            if (sources.empty()) return false;
            bool added = false;
            heap::vector<uint8_t> to_send, rbuff;
            to_send.push_back('f');// <-- to find a key
            push_value(to_send, key);
            // todo: r we choosing a random 1
            for (auto& source : sources) {
                try {

                    net_stat stat;
                    tcp::iostream stream;
                    stream.connect(source.host, std::to_string(source.port));
                    if (stream.fail()) {
                        art::std_err("failed to connect to remote server", host, this->port);
                        continue;
                    }

                    send_art_fun(stream, to_send);
                    if (!recv_buffer(stream, rbuff)) {
                        continue;
                    }
                    auto t = get_art(this->shard);
                    //storage_release release(t->latch); // the latch should be called outside
                    t->last_leaf_added = nullptr;
                    auto r = process_art_fun_cmd(t, stream, rbuff);
                    if (r.add_applied > 0) {
                        added = true;
                        break; // don't call the other servers
                    }
                }catch (std::exception& e) {
                    art::std_err("failed to write to stream", e.what(),"to",source.host,source.port);
                    ++statistics::repl::request_errors;
                }
                ++statistics::repl::find_requests;
            }
            ++messages;
            return added;
        }

        [[nodiscard]] bool client::commit_transaction() const {
            try {
            } catch (std::exception& e) {

                art::std_err("failed to commit transaction", e.what());
                return false;
            }
            return true;
        }
        bool client::ping() const {
            try {
                tcp::iostream stream(host, std::to_string(this->port ));
                if (!stream) {
                    art::std_err("failed to connect to remote server", host, this->port);
                    return false;
                }
                net_stat stats;
                uint32_t cmd = cmd_ping;
                writep(stream,uint8_t{0x00});
                writep(stream,cmd);
                int ping_result = 0;
                readp(stream, ping_result);
                if (ping_result != rpc_server_version) {
                    art::std_err("failed to ping remote server", host, port, "ping returned",ping_result);
                    return false;
                }
                stream.close();
            }catch (std::exception &e) {
                art::std_err("failed to ping remote server", host, port, e.what());
                return false;
            }
            return true;
        }
        static heap::vector<route> routes;
        void set_route(size_t shard, const route& destination) {
            if (routes.empty()) routes.resize(art::get_shard_count().size());
            if (shard >= routes.size()) {
                art::std_err("invalid shard",shard, routes.size());
                return;
            }
            routes[shard] = destination;
        }
        void clear_route(size_t shard) {
            if (routes.empty()) routes.resize(art::get_shard_count().size());
            if (shard >= routes.size()) {
                art::std_err("invalid shard",shard, routes.size());
                return;
            }
            routes[shard] = {};
        }
        route get_route(size_t shard) {
            if (routes.empty()) routes.resize(art::get_shard_count().size());
            if (shard >= routes.size()) {
                art::std_err("invalid shard",shard, routes.size());
                return {};
            }
            return routes[shard];
        }
    }
}

extern "C"{
    int ADDROUTE(caller& call, const arg_t& argv) {
        if (argv.size() != 4)
            return call.wrong_arity();
        size_t shard = conversion::as_variable(argv[1].chars()).i();
        if (shard >= art::get_shard_count().size())
            return call.error("invalid shard");
        auto host = conversion::as_variable(argv[2]).s();
        auto port = conversion::as_variable(argv[3].chars()).i();
        if (port <= 0 || port >= 65536)
            return call.error("invalid port");
        if (host.empty())
            return call.error("no host");
        barch::repl::set_route(shard, {host,port});
        return call.simple(host.c_str());
    }
    int ROUTE(caller& call, const arg_t& argv) {
        if (argv.size() != 2)
            return call.wrong_arity();
        size_t shard = atoi(argv[1].chars());
        if (shard >= art::get_shard_count().size())
            return call.error("invalid shard");
        auto route = barch::repl::get_route(shard);
        call.start_array();
        call.simple(route.ip.c_str());
        call.long_long(route.port);
        call.end_array(0);
        return 0;
    }
    int REMROUTE(caller& call, const arg_t& argv) {
        if (argv.size() != 2)
            return call.wrong_arity();
        size_t shard = atoi(argv[1].chars());
        if (shard >= art::get_shard_count().size())
            return call.error("invalid shard");
        auto route = barch::repl::get_route(shard);
        barch::repl::clear_route(shard);
        call.start_array();
        call.simple(route.ip.c_str());
        call.long_long(route.port);
        call.end_array(0);
        return 0;
    }
}
