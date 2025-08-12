//
// Created by teejip on 8/12/25.
//

#include "info_api.h"
#include "caller.h"
#include "time_convertsion.h"
#include "asio/detail/chrono.hpp"

auto start_time = std::chrono::high_resolution_clock::now();
auto now() {
    return std::chrono::high_resolution_clock::now();
}
extern "C"{
int INFO(caller& call, const arg_t& argv) {
    if (argv.size() == 2 && argv[1] == "SERVER") {
        auto n = now();
        std::string response =
        "# Server\n\n"
        "redis_version:8.2.0\n"
        "redis_git_sha1:00000000\n"
        "redis_git_dirty:1\n"
        "redis_build_id:1a867ddf2a9d6677\n"
        "redis_mode:standalone\n"
        "os:Linux 6.6.93+ x86_64\n"
        "arch_bits:64\n"
        "monotonic_clock:POSIX clock_gettime\n"
        "multiplexing_api:epoll+io_uring\n"
        "atomicvar_api:c11-builtin\n"
        "gcc_version:12.2.0\n"
        "process_id:1\n"
        "process_supervised:no\n"
        "run_id:744d7c7f8cff5fdfed2dc41dfefb5530767cb664\n"
        "tcp_port:14000\n"
        "server_time_usec:"+std::to_string(micros(n,start_time)) +"\n"
        "uptime_in_seconds:"+std::to_string(secs(n,start_time))+"\n"
        "uptime_in_days:"+std::to_string(days(n,start_time))+"\n"
        "hz:10\n"
        "configured_hz:10\n"
        "lru_clock:10162822\n"
        "executable:/data/redis-server\n"
        "config_file:/etc/redis/redis.conf\n"
        "io_threads_active:0\n"
        "listener0:name=tcp,bind=*,bind=-::*,port=14000";
        call.vt(response);
        return 0;
    }
    return call.error("not implemented");
}
}