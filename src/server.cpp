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

#include <cstdio>

#include "asio/io_service.hpp"
#pragma GCC diagnostic pop

enum {
    cmd_ping = 1,
    cmd_stream = 2,
    cmd_art_fun = 3
};
enum {
    opt_max_workers = 8,
    opt_read_timout = 60000

};
using asio::ip::tcp;

namespace barch {
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
    bool recv_buffer(std::iostream& stream, heap::vector<uint8_t>& buffer) {
        uint32_t buffers_size = 0;
        readp(stream,buffers_size);
        if (buffers_size == 0) {
            return false;
        }
        buffer.resize(buffers_size);
        readp(stream, buffer.data(), buffer.size());
        return true;
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
        buffer.insert(buffer.end(), options.flags, 1);
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
    void push_value(heap::vector<uint8_t>& buffer, art::value_type v) {
        push_size_t<uint32_t>(buffer, v.size);
        buffer.insert(buffer.end(), v.bytes, v.bytes + v.size);
    }
    void buffer_insert
    (   heap::vector<uint8_t>& buffer
    ,   art::key_options options
    ,   art::value_type key
    ,   art::value_type value
    ) {
        buffer.push_back('i');
        push_options(buffer, options);
        push_value(buffer, key);
        push_value(buffer, value);
        ++statistics::repl::insert_requests;
    }

    void push_leaf_insert(heap::vector<uint8_t>& buffer, const art::leaf* leaf) {
        buffer_insert(buffer, *leaf, leaf->get_key(), leaf->get_value());
    }

    std::pair<art::value_type,size_t> get_value(size_t at, const heap::vector<uint8_t>& buffer) {
        auto size = get_size_t<uint32_t>(at, buffer);
        if (buffer.size()+sizeof(size)+at < size) {
            throw_exception<std::runtime_error>("invalid size");
        }
        art::value_type r = {buffer.data() + at + sizeof(uint32_t),size};
        return {r, at+sizeof(size)+size};
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
    af_result process_art_fun(art::tree* t, heap::vector<uint8_t>& buffer, uint32_t buffers_size, std::iostream& stream) {
        af_result r;
        heap::vector<uint8_t> tosend;
        art::node_ptr found ;
        uint32_t actual = 0;
        bool returning = false;
        for (size_t i = 0; i < buffers_size;) {
            char cmd = buffer[i];
            switch (cmd) {
                case 'i': {
                        auto options = get_options(i+1, buffer);
                        auto key = get_value(options.second, buffer);
                        auto value = get_value(key.second, buffer);

                        if (statistics::logical_allocated > art::get_max_module_memory()) {
                            // do not add data if memory limit is reached
                            ++statistics::oom_avoided_inserts;
                        } else {
                            art_insert(t, options.first, key.first, value.first,true,
                                [&r](const art::node_ptr &){
                                    ++statistics::repl::key_add_recv_applied;
                                    ++r.add_applied;
                            });

                        }
                        ++statistics::repl::key_add_recv;
                        ++r.add_called;
                        ++actual;
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
                        ++r.remove_called;
                        ++actual;
                    }
                    break;
                case 'f': {
                        returning = true;
                        auto key = get_value(i+1, buffer);
                        found = art::find(t, key.first);
                        if (found.is_leaf) {
                            ++r.find_applied;
                            auto leaf = found.const_leaf();
                            push_leaf_insert(tosend, leaf);
                        }
                        i = key.second;
                        ++statistics::repl::key_find_recv;
                        ++r.find_called;
                        ++actual;
                    }
                    break;
                case 'a': {
                    returning = true;
                    auto lkey = get_value(i+1, buffer);
                    auto ukey = get_value(lkey.second, buffer);
                    art::iterator ai(t, lkey.first);
                    while (ai.ok()) {
                        auto k = ai.l();
                        if (k->get_key() >= lkey.first && k->get_key() <= ukey.first) {
                            ++r.find_applied;
                            push_leaf_insert(tosend, found.const_leaf());
                        }else {
                            break;
                        }
                        ai.next();
                    }
                    i = ukey.second;
                    ++statistics::repl::key_find_recv;
                    ++r.find_called;
                    ++actual;
                }
                break;
                default:
                    art::std_err("unknown command", cmd);
                    r.error = true;
                    return r;
                    break;
            }
        }
        if (returning) {
            auto s = (uint32_t)tosend.size();
            writep(stream, s);
            writep(stream, tosend.data(), tosend.size());
            stream.flush();
        }
        return r;
    }
    host_id get_host_id() {
        return {"localhost", getpid() % 10000000000000000000ULL};
    }

    struct server_context {
        std::thread server_thread{};
        asio::io_service io{};
        tcp::acceptor acc;
        std::string interface;
        uint_least16_t port;
        heap::vector<uint8_t> buffer{};

        void stop() {
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

        void process_data(tcp::socket& endpoint) {
            auto stream = tcp::iostream{};
            stream.socket() = std::move(endpoint);
            int cmd = 0;
            stream_write_ctr = 0;
            stream_read_ctr = 0;
            art::key_spec spec;

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
                    }
                    break;
                case cmd_art_fun:
                    try {
                        uint32_t buffers_size = 0;
                        uint32_t shard = 0;
                        uint32_t count = 0;
                        readp(stream,shard);
                        readp(stream,count);
                        if (!recv_buffer(stream,buffer)) {
                            art::std_err("failed to read buffer");
                            break;
                        }
                        auto t = get_art(shard);
                        storage_release release(t->latch);
                        process_art_fun(t, buffer, buffers_size,stream);

                        //art::std_log("cmd apply changes ",shard, "[",buffers_size,"] bytes","keys",count,"actual",actual,"total",(long long)statistics::repl::key_add_recv);
                    }catch (std::exception& e) {
                        art::std_err("failed to apply changes", e.what());
                    }
                    break;
                default:
                    art::std_err("unknown command", cmd);
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

        bool client::add_source(std::string host, int port, size_t shard) {
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
        bool client::add_destination(std::string host, int port, size_t shard) {
            auto dest = repl_dest{std::move(host), port, shard};
            {
                std::lock_guard lock(this->latch);
                for (auto& d : destinations) {
                    if (d.host == dest.host && d.port == dest.port) {
                        art::std_err("destination already added", d.host, d.port);
                        return false;
                    }
                }
                destinations.emplace_back(dest);
            }
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
                                    tcp::iostream stream(dest.host, std::to_string(dest.port)); //,art::get_rpc_connect_to_s()
                                    if (stream.fail()) {
                                        ++statistics::repl::request_errors;
                                        continue;
                                    }
                                    send_art_fun(stream, to_send);
                                    stream.close();
                                    //art::std_log("sent", buffers_size, "bytes to", dest.host, dest.port, "total sent",total_messages,"still queued",(uint32_t)this->messages,"iq",(long long)statistics::repl::insert_requests);
                                }catch (std::exception& e) {
                                    art::std_err("failed to write to stream", e.what(),"to",dest.host,dest.port);
                                    ++statistics::repl::request_errors;
                                }
                            }
                            to_send.clear();
                        }
                        if (this->messages == 0) // send asap
                            std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    }
                }catch (std::exception& e) {
                    this->connected = false;
                    art::std_err("failed client send thread", this->host, this->port,e.what());

                }
            });

            }
            return true;
        }
        void client::send_art_fun(std::iostream& stream, heap::vector<uint8_t>& to_send) {
            int cmd = cmd_art_fun;
            uint32_t buffers_size = to_send.size();
            uint32_t sh = this->shard;
            writep(stream, cmd);
            writep(stream, sh);
            writep(stream, messages);
            writep(stream, buffers_size);
            writep(stream, to_send.data(), to_send.size());
            stream.flush();

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
            buffer_insert(buffer, options, key, value);
            ++messages;
            return true;
        }
        bool client::find_insert(art::value_type key) {
            bool added = false;
            heap::vector<uint8_t> to_send, rbuff;
            to_send.push_back('f');// <-- to find a key
            push_value(to_send, key);
            // todo: r we choosing a random 1
            for (auto& source : sources) {
                try {

                    net_stat stat;
                    tcp::iostream stream;
                    stream.expires_from_now(art::get_rpc_connect_to_s());
                    stream.connect(source.host, std::to_string(source.port));
                    if (stream.fail()) {
                        art::std_err("failed to connect to remote server", host, this->port);
                        continue;
                    }

                    send_art_fun(stream, rbuff);
                    if (!recv_buffer(stream, rbuff)) {
                        art::std_err("failed to read buffer");
                        ++statistics::repl::request_errors;
                        continue;
                    }
                    auto t = get_art(this->shard);
                    //storage_release release(t->latch); // the latch should be called outside
                    t->last_leaf_added = nullptr;
                    auto r = process_art_fun(t, rbuff, buffer.size(),stream);
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

            //std::lock_guard lock(this->latch);
            buffer.push_back('r');
            push_value(buffer, key);
            ++statistics::repl::remove_requests;
            ++messages;
            return true;
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
                int cmd = cmd_stream;
                uint32_t s = shard;
                net_stat stats;
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
                int cmd = cmd_ping;
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