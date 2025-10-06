//
// Created by teejip on 9/17/25.
//

#ifndef BARCH_BARCH_RPC_H
#define BARCH_BARCH_RPC_H
#include "art.h"

enum {
    cmd_ping = 1,
    cmd_stream = 2,
    cmd_art_fun = 3,
    cmd_barch_call = 4
};

enum {
    opt_read_timout = 60000
};


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
    inline void push_options(heap::vector<uint8_t>& buffer, const art::key_options& options) {
        buffer.push_back(options.flags);
        if (options.has_expiry())
            push_size_t<uint64_t>(buffer, options.get_expiry());
    }
    inline std::pair<art::key_options,size_t> get_options(size_t at, const heap::vector<uint8_t>& buffer) {
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
    inline std::pair<art::value_type,size_t> get_value(size_t at, const heap::vector<uint8_t>& buffer) {
        auto size = get_size_t<uint32_t>(at, buffer);
        if (buffer.size()+sizeof(size)+at + 1< size) {
            throw_exception<std::runtime_error>("invalid size");
        }
        art::value_type r = {buffer.data() + at + sizeof(uint32_t),size};
        return {r, at+sizeof(size)+size + 1};
    }

    inline void push_value(heap::vector<uint8_t>& buffer, art::value_type v) {
        push_size_t<uint32_t>(buffer, v.size);
        buffer.insert(buffer.end(), v.bytes, v.bytes + v.size);
        buffer.push_back(0x00);
    }
    inline void push_value(heap::vector<uint8_t>& buffer, const std::string& v) {
        push_value(buffer, art::value_type{v});
    }
    inline void push_value(heap::vector<uint8_t>& buffer, const Variable& v) {
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
                push_size_t<int64_t>(buffer, *std::get_if<int64_t>(&v));
                break;
            case var_uint64:
                push_size_t<uint64_t>(buffer, *std::get_if<uint64_t>(&v));
                break;
            case var_double:
                push_size_t<double>(buffer, *std::get_if<double>(&v));
                break;
            case var_string:
                push_value(buffer, *std::get_if<std::string>(&v));
                break;
            default:
                break;
        }

    }

    inline std::pair<Variable,size_t> get_variable(size_t at, const heap::vector<uint8_t>& buffer) {
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
            case var_uint64:
                if (at + sizeof(uint64_t) > bsize) {
                    throw_exception<std::runtime_error>("invalid at");
                }
                return  {get_size_t<uint64_t>(at, buffer),at+sizeof(int64_t)};
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

    inline bool recv_buffer(std::iostream& stream, heap::vector<uint8_t>& buffer) {
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
    inline void send_art_fun(uint32_t shard, uint32_t messages, std::iostream& stream, heap::vector<uint8_t>& to_send) {
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

    inline host_id get_host_id() {
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
     * @param stream for writing reply data
     * @return true if no errors occurred
     */

    template<typename StreamT>
     af_result process_art_fun_cmd(art::tree* t, StreamT& stream, heap::vector<uint8_t>& buffer) {
        af_result r;
        heap::vector<uint8_t> tosend;
        art::node_ptr found;
        uint32_t buffers_size = buffer.size();
        bool flush_buffers = false;
        for (size_t i = 0; i < buffers_size;) {
            char cmd = (char)buffer[i];
            switch (cmd) {
                case 'c': {

                }
                case 'i': {
                    auto options = get_options(i+1, buffer);
                    auto key = get_value(options.second, buffer);
                    auto value = get_value(key.second, buffer);
                    if (get_total_memory() > art::get_max_module_memory()) {
                        // do not add data if the memory limit is reached
                        ++statistics::oom_avoided_inserts;
                        r.error = true;
                        return r;
                    } else {
                        auto fc = [&r](const art::node_ptr &){
                            ++statistics::repl::key_add_recv_applied;
                            ++r.add_applied;};
                        if (t->opt_ordered_keys) {
                            art_insert(t, options.first, key.first, value.first,true,fc);
                        }else
                        {
                            t->hash_insert(options.first, key.first, value.first,true,fc);
                        }


                    }
                    ++statistics::repl::key_add_recv;
                    ++r.add_called;
                    i = value.second;
                }
                    break;
                case 'r': {
                    auto key = get_value(i+1, buffer);
                    if (t->opt_ordered_keys) {
                        art_delete(t, key.first,[&r](const art::node_ptr &) {
                            ++statistics::repl::key_rem_recv_applied;
                            ++r.remove_applied;
                        });
                    }else {
                        if (t->remove_leaf_from_uset(key.first)) {
                            ++statistics::repl::key_rem_recv_applied;
                            ++r.remove_applied;
                        }
                    }


                    i = key.second;
                    ++statistics::repl::key_rem_recv;
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
    template<typename SockT>
    bool run_to(asio::io_context& ioc, SockT& s, std::chrono::steady_clock::duration timeout) {
        ioc.restart();
        ioc.run_for(timeout);
        if (!ioc.stopped()) {
            s.close();
            ioc.run();
            return false;
        }
        return true;
    }
    template<typename SockT,typename BufT>
    void async_write_to(SockT& sock, const BufT& buf, std::error_code& error) {
        asio::async_write(sock, buf,[&](const std::error_code& result_error,
        std::size_t result_n)
        {
            error = result_error;
            if (error) {
                ++statistics::repl::request_errors;
            }else {
                net_stat stat;
                stream_write_ctr += result_n;
            }
        });
    }
}
#endif //BARCH_BARCH_RPC_H