//
// Created by teejip on 8/12/25.
//

#include "info_api.h"
#include "caller.h"
#include "conversion.h"
#include "module.h"
#include "time_conversion.h"
#include "asio/detail/chrono.hpp"

auto start_time = std::chrono::high_resolution_clock::now();
auto now() {
    return std::chrono::high_resolution_clock::now();
}
template <typename T>
std::string tos(const T& in) {
    return std::to_string(in);
}
extern "C"{
int INFO(caller& call, const arg_t& argv) {
    if (argv.size() == 3 && argv[1] == "SHARD") {
        uint64_t shard = 0;

        if (argv[2].starts_with("#") && argv[2].size > 1) {
            if (!conversion::to_ui64(argv[2].sub(1), shard)) {
                shard = get_shard(argv[2]);
            }
            if (shard >= barch::get_shard_count().size()) {
                return call.error("shard number out of range");
            }
        }else {
            shard = get_shard(argv[2]);
        }
        auto s = get_art(shard);
        std::string order = s->opt_ordered_keys ? "ordered" : "unordered";
        std::string index = s->opt_ordered_keys ? "ART" : "HASH";
        std::string response =
        "# Shard\n\n"
        "number:"+tos(shard)+"\n"
        "index_physical:"+index+"\n"
        "index_logical:"+order+"\n"
        "size:"+tos(shard_size(s))+"\n"
        "bytes_allocated:"+tos(s->get_leaves().get_bytes_allocated() + s->get_nodes().get_bytes_allocated()) + "\n"
        "virtual_allocated:"+tos(s->get_leaves().get_allocated() + s->get_nodes().get_allocated()) + "\n";

        call.vt(response);
        return 0;
    }
    if (argv.size() == 2 && argv[1] == "SERVER") {
        auto n = now();
        std::string port = std::to_string(barch::get_server_port());
        std::string os = "Linux x86_64";
        std::string response =
        "# Server\n\n"
        "redis_version:8.2.0\n"
        "redis_git_sha1:00000000\n"
        "redis_git_dirty:1\n"
        "redis_build_id:00000000000000\n"
        "redis_mode:standalone\n"
        "os:"+os+"\n"
        "arch_bits:64\n"
        "monotonic_clock:POSIX clock_gettime\n"
        "multiplexing_api:epoll+io_uring\n"
        "atomicvar_api:c11-builtin\n"
        "gcc_version:12.2.0\n"
        "process_id:1\n"
        "process_supervised:no\n"
        "run_id:0\n"
        "tcp_port:"+port+"\n"
        "server_time_usec:"+tos(micros(n,start_time))+"\n"
        "uptime_in_seconds:"+tos(secs(n,start_time))+"\n"
        "uptime_in_days:"+tos(days(n,start_time))+"\n"
        "hz:10\n"
        "configured_hz:10\n"
        "lru_clock:0\n"
        "executable:_barch.so\n"
        "config_file:_barch.so\n"
        "io_threads_active:"+tos(std::thread::hardware_concurrency())+"\n"
        "listener0:name=tcp,bind=*,bind=-::*,port="+port+"\n";

        call.vt(response);
        return 0;
    }
    return call.error("not implemented");
}
}