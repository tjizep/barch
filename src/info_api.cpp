//
// Created by teejip on 8/12/25.
//

#include "info_api.h"

#include "barch_apis.h"
#include "caller.h"
#include "conversion.h"
#include "module.h"
#include "shard.h"
#include "time_conversion.h"
#include "asio/detail/chrono.hpp"
#include "version.h"
auto start_time = std::chrono::high_resolution_clock::now();

template <typename T>
std::string tos(const T& in) {
    return std::to_string(in);
}
static double roundn(double value, int n) {
    double p10 = std::pow(10.0, n);
    return std::round(value * p10) / p10;
}
extern "C"{
int INFO(caller& call, const arg_t& argv) {
    if (argv.size() == 3 && argv[1] == "SHARD") {
        uint64_t shard = 0;
        auto ks = call.kspace();
        if (argv[2].starts_with("#") && argv[2].size > 1) {
            if (!conversion::to_ui64(argv[2].sub(1), shard)) {
                shard = ks->get_shard_index(argv[2]);
            }
            if (shard >= ks->get_shard_count()) {
                return call.push_error("shard number out of range");
            }
        }else {
            shard = ks->get_shard_index(argv[2]);
        }
        auto s = ks->get(shard);
        std::string order = s->opt_ordered_keys ? "ordered" : "unordered";
        std::string index = s->opt_ordered_keys ? "ART" : "HASH";
        std::string response =
        "# Shard\n\n"
        "number:"+tos(shard)+"\n"
        "index_physical:"+index+"\n"
        "index_logical:"+order+"\n"
        "size:"+tos(s->get_size())+"\n"
        "bytes_allocated:"+tos(s->get_ap().get_leaves().get_bytes_allocated() + s->get_ap().get_nodes().get_bytes_allocated()) + "\n"
        "virtual_allocated:"+tos(s->get_ap().get_leaves().get_allocated() + s->get_ap().get_nodes().get_allocated()) + "\n";

        call.push_vt(response);
        return 0;
    }
    std::string text;
    auto lower = [](std::string &text, const std::string& s) -> std::string {
        text = s;
        std::transform(text.begin(), text.end(), text.begin(), ::tolower);
        return text;
    };
    if ((argv.size() == 2 || argv.size() == 3) && lower(text, argv[1].to_string()) == "commandstats") {

        auto functions = functions_by_name();
        std::string result = "";
        auto make_line = [&result, lower](const function_map::value_type& f) {
            std::string text;
            if (f.second.calls > 0) { // for verbosity AND div-zero
                auto micros = (double)f.second.total_nanos/1000.0f;
                std::string line = "cmdstat_";
                line += lower(text, f.first);
                line += ":";
                line += "calls=";
                line += std::to_string(f.second.calls);
                line += ",";
                line += "usec=";
                line += conversion::as_variable(roundn(micros,4)).s();
                line += ",";
                line += "avg_usec=";
                line += conversion::as_variable(roundn(roundn(micros/f.second.calls,4), 4)).s();
                line += "\n";
                result += line;
            }
        };
        if (argv.size() == 3) {
            auto f = functions->find(argv[2].to_string());
            if (f == functions->end()) {
                return call.push_error("function not found");
            }else {
                make_line(*f);
            }
        }else {
            for (auto f : *functions) {
                make_line(f);
            }
        }
        return call.push_vt(result);
    }
    if (argv.size() == 2 && argv[1] == "SERVER") {
        auto n = now();
        std::string port = std::to_string(barch::get_server_port());
        std::string os = "Linux x86_64";
        std::string response =
        "# Server\n\n"
        "redis_version:"
        BARCH_PROJECT_VERSION
        "\n"
        "redis_git_sha1:"
        BARCH_GIT_COMMIT_HASH
        "\n"
        "barch_version:"
        BARCH_PROJECT_VERSION
        "\n"
        "barch_git_sha1:"
        BARCH_GIT_COMMIT_HASH
        "\n"
        "barch_build_type:"
        BARCH_BUILD_TYPE
        "\n"
        "redis_git_dirty:1\n"
        "redis_build_id:0\n"
        "redis_mode:library\n"
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
        "executable:_barch.so or liblbarch.so\n"
        "config_file:NONE/RESP\n"
        "io_threads_active:"+tos(std::thread::hardware_concurrency())+"\n"
        "listener0:name=tcp,bind=*,bind=-::*,port="+port+"\n";

        call.push_vt(response);
        return 0;
    }
    return call.push_error("not implemented");
}
}