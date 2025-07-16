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
        std::thread server_thread{};
        asio::io_service io{16};
        tcp::acceptor acc;
        std::string interface;
        uint_least16_t port;

        void stop() {
            try {
                acc.close();
            }catch (...){}

            io.stop();
            if (server_thread.joinable())
                server_thread.join();
            server_thread = {};
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


        class redis_session : public std::enable_shared_from_this<redis_session>
        {
        public:
            redis_session(tcp::socket socket, char init_char)
              : socket_(std::move(socket))
            {
                parser.init(init_char);
            }

            void start()
            {
                do_read();
            }

        private:
            void do_read()
            {
                auto self(shared_from_this());
                socket_.async_read_some(asio::buffer(data_, max_length),
                    [this, self](std::error_code ec, std::size_t length)
                {

                    if (!ec){

                        parser.add_data(data_, length);
                        auto &params = parser.read_new_request();
                        if (!params.empty()) {
                            do_write(params);
                        }

                    }
                });
            }

            void do_write(const std::vector<std::string>& params) {
                auto self(shared_from_this());

                stream.clear();
                art::std_log(params[0] );

                if (params[0] == "COMMAND") {
                    if (params[1] == "DOCS") {
                        std::vector<Variable> results;

                        for (auto& p: barch_functions) {
                            results.emplace_back(p.first);
                            results.emplace_back("function");
                        }
                        redis::rwrite(stream, results);
                    }else {
                        redis::rwrite(stream, error{"unknown command"});
                    }

                }else {
                    std::string cn = std::string{params[0]};
                    auto ic = barch_functions.find(cn);
                    if (ic == barch_functions.end()) {
                        redis::rwrite(stream, error{"unknown command"});
                    }else {
                        auto f = ic->second;
                        int32_t r = caller.call(params,f);
                        if (r < 0) {
                            redis::rwrite(stream, error{"command failed"});
                        } else {
                            redis::rwrite(stream, caller.results);
                        }
                    }
                }
                asio::async_write(socket_, asio::buffer(stream.buf),
                    [this, self](std::error_code ec, std::size_t /*length*/){
                        if (!ec){
                            do_read();
                        }
                    });
            }

            tcp::socket socket_;
            enum { max_length = 4096 };
            char data_[max_length];
            redis::redis_parser parser{};
            swig_caller caller{};
            vector_stream stream{};
        };

        void process_data(tcp::socket& endpoint) {
            tcp::iostream stream ;
            heap::vector<uint8_t> buffer{};
            uint32_t cmd = 0;
            stream_write_ctr = 0;
            stream_read_ctr = 0;
            art::key_spec spec;
            try {
                char cs[1] ;
                //readp(stream, cs);
                endpoint.read_some(asio::buffer(cs));
                if (cs[0]) {
                    //auto state = std::make_shared<redis_async_shared>(stream);
                    //state->redis_start(cs);
                    std::make_shared<redis_session>(std::move(endpoint),cs[0])->start();

                    return;
                }
                tcp::iostream stream(std::move(endpoint));
                if (stream.fail())
                    return;
                readp(stream,cmd);

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
                        try {
                            swig_caller caller;
                            heap::vector<uint8_t> replies;
                            uint32_t calls = 0;
                            readp(stream,calls);
                            for (uint32_t c = 0; c < calls; ++c) {
                                uint32_t buffers_size = 0;
                                uint32_t replies_size = 0;
                                readp(stream,buffers_size);
                                if (buffers_size == 0) {
                                    art::std_err("invalid buffer size", buffers_size);
                                    return;
                                }

                                buffer.resize(buffers_size);
                                readp(stream, buffer.data(), buffers_size);
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
                                    writep(stream, replies_size);
                                    stream.flush();
                                    return;
                                }
                                auto f = ic->second;
                                int32_t r = caller.call(params,f);
                                replies.clear();

                                for (auto &v: caller.results) {
                                    push_value(replies,v);
                                }
                                replies_size = replies.size();
                                writep(stream, r);
                                writep(stream, replies_size);
                                writep(stream, replies.data(), replies_size);
                                stream.flush();
                            }
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
            server_thread = std::thread([this]() {
                art::std_log("server started on", this->interface,this->port);
                io.run();
                art::std_log("server stopped");
            });
        }
    };
    std::shared_ptr<server_context>  srv = nullptr;
    void server::start(const std::string& interface, uint_least16_t port) {
        if (srv) srv->stop();
        srv = std::make_shared<server_context>(interface, port);
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
        public:
            rpc_impl(const std::string& host, int port) : host(host), port(port) {
                //if (stream.fail())
                //    art::std_err("failed to connect to remote server",host,port);
            }
            virtual ~rpc_impl() {
                try {
                    //stream.close();
                }catch (std::exception& e) {
                    art::std_err(e.what());
                }
            }
            int call(std::vector<Variable>& result, const std::vector<std::string_view>& params) {
                to_send.clear();
                int32_t r = 0;
                if (params.empty()) {
                    return -1;
                }
                try {

                    //art::std_log("calling rpc",params[0],"on",host,port);
                    net_stat stat;
                    tcp::iostream stream(host, std::to_string(port));
                    if (!stream) {
                        art::std_err("failed to connect to remote server");
                        return -1;
                    }
                    for (auto p: params) {
                        push_value(to_send, art::value_type{p});
                    }
                    uint32_t cmd = cmd_barch_call;
                    uint32_t buffers_size = to_send.size();
                    writep(stream,uint8_t{0x00});
                    writep(stream, cmd);
                    writep(stream, 1);
                    writep(stream, buffers_size);
                    writep(stream, to_send.data(), to_send.size());
                    heap::vector<uint8_t> replies;
                    readp(stream, r);
                    recv_buffer(stream, replies);

                    for (size_t i = 0; i < replies.size(); i++) {
                        auto v = get_variable(i, replies);
                        result.emplace_back(v.first);
                        i = v.second;
                    }

                }catch (std::exception& e) {
                    art::std_err("failed to write to stream", e.what(),"to",host,port);
                    return -1;
                }
                return r;
            }
        };
        std::shared_ptr<barch::repl::rpc> create(const std::string& host, int port) {
            return std::make_shared<rpc_impl>(host, port);
        }
        int call(std::vector<Variable>& result, const std::vector<std::string_view>& params, const std::string& host, int port) {
            heap::vector<uint8_t> to_send;
            int32_t r = 0;
            if (params.empty()) {
                return -1;
            }
            try {

                //art::std_log("calling rpc",params[0],"on",host,port);
                net_stat stat;
                tcp::iostream stream(host, std::to_string(port));
                if (!stream) {
                    art::std_err("failed to connect to remote server", host, port);
                    return -1;
                }
                for (auto p: params) {
                    push_value(to_send, art::value_type{p});
                }
                uint32_t cmd = cmd_barch_call;
                uint32_t buffers_size = to_send.size();
                writep(stream,uint8_t{0x00});
                writep(stream, cmd);
                writep(stream, buffers_size);
                writep(stream, to_send.data(), to_send.size());
                heap::vector<uint8_t> replies;
                readp(stream, r);
                recv_buffer(stream, replies);

                for (size_t i = 0; i < replies.size(); i++) {
                    auto v = get_variable(i, replies);
                    result.emplace_back(v.first);
                    i = v.second;
                }

            }catch (std::exception& e) {
                art::std_err("failed to write to stream", e.what(),"to",host,port);
                return -1;
            }
            return r;
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
    }
}