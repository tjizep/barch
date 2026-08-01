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
#include "configuration.h"
#include "key_space.h"
#include "sastam.h"
#include "statistics.h"
#include <fstream>
#include <unistd.h>
auto start_time = std::chrono::high_resolution_clock::now();

template <typename T>
std::string tos(const T& in) {
    return std::to_string(in);
}
static double roundn(double value, int n) {
    double p10 = std::pow(10.0, n);
    return std::round(value * p10) / p10;
}
/**
 * resident set size of this process. the module is linux only (see the SERVER section) so
 * /proc/self/statm is always available - 0 is returned if it isn't
 */
static uint64_t get_rss_bytes() {
    std::ifstream statm("/proc/self/statm");
    uint64_t total_pages = 0, resident_pages = 0;
    if (!(statm >> total_pages >> resident_pages)) {
        return 0;
    }
    return resident_pages * (uint64_t) sysconf(_SC_PAGESIZE);
}
/**
 * format bytes the way redis does it in bytesToHuman: 0B, 1008.02K, 15.62G etc.
 */
static std::string human(uint64_t bytes) {
    static const char* units[] = {"B", "K", "M", "G", "T", "P"};
    if (bytes < 1024) {
        return tos(bytes) + "B";
    }
    double d = (double) bytes;
    size_t unit = 0;
    while (d >= 1024.0 && unit + 1 < sizeof(units) / sizeof(units[0])) {
        d /= 1024.0;
        ++unit;
    }
    char buffer[64];
    snprintf(buffer, sizeof(buffer), "%.2f%s", d, units[unit]);
    return buffer;
}
/**
 * redis renders the ratios and percentages in the memory section with two decimals
 */
static std::string fixed2(double value) {
    char buffer[64];
    snprintf(buffer, sizeof(buffer), "%.2f", value);
    return buffer;
}
static std::string ratio(uint64_t numerator, uint64_t denominator) {
    if (denominator == 0) return fixed2(0.0);
    return fixed2((double) numerator / (double) denominator);
}
static std::string perc(uint64_t numerator, uint64_t denominator) {
    if (denominator == 0) return fixed2(0.0) + "%";
    return fixed2(100.0 * (double) numerator / (double) denominator) + "%";
}
/**
 * a difference that may legitimately go negative (rss below the allocator total for instance)
 */
static std::string diff(uint64_t a, uint64_t b) {
    return tos((int64_t) a - (int64_t) b);
}
static std::atomic<uint64_t> memory_peak{0};
static uint64_t update_memory_peak(uint64_t used) {
    uint64_t peak = memory_peak.load(std::memory_order_relaxed);
    while (used > peak && !memory_peak.compare_exchange_weak(peak, used, std::memory_order_relaxed)) {
        // peak is reloaded by compare_exchange_weak
    }
    return std::max(peak, used);
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
    if (argv.size() == 2 && lower(text, argv[1].to_string()) == "memory") {
        art_statistics as = barch::get_statistics();
        // the arenas know the difference between what the data actually occupies (logical/virtual)
        // and what has been committed for it (physical) - leaves hold the dataset, nodes the index
        uint64_t leaf_logical = 0, node_logical = 0;
        uint64_t leaf_physical = 0, node_physical = 0;
        uint64_t free_list_bytes = 0, pages = 0, keys = 0, shards = 0;
        barch::all_shards([&](const barch::shard_ptr& s) {
            auto& ap = s->get_ap();
            leaf_logical += ap.get_leaves().get_allocated();
            node_logical += ap.get_nodes().get_allocated();
            leaf_physical += ap.get_leaves().get_bytes_allocated();
            node_physical += ap.get_nodes().get_bytes_allocated();
            free_list_bytes += ap.get_leaves().get_bytes_in_free_list() + ap.get_nodes().get_bytes_in_free_list();
            pages += ap.get_leaves().get_page_count() + ap.get_nodes().get_page_count();
            keys += s->get_size();
            ++shards;
        });

        uint64_t used = get_total_memory();
        uint64_t peak = update_memory_peak(used);
        uint64_t rss = get_rss_bytes();
        uint64_t vmm = heap::vmm_allocated;
        uint64_t startup = std::min<uint64_t>(get_startup_memory(), used);
        // dataset is what the keys and values occupy, everything else is overhead
        uint64_t dataset = leaf_logical;
        uint64_t overhead = used > dataset ? used - dataset : 0;
        uint64_t net = used > startup ? used - startup : 0;
        // heap::allocated already counts the mapped vmm arena so used memory is address space
        // reserved, not paged in - rss is what the process actually holds. the arena free lists
        // are barch's equivalent of allocator pages that are held but unused, and anything mapped
        // and not resident is the closest thing barch has to jemalloc's muzzy pages
        uint64_t allocator_allocated = used;
        uint64_t allocator_active = used + free_list_bytes;
        uint64_t allocator_resident = rss;
        uint64_t allocator_muzzy = used > rss ? used - rss : 0;
        uint64_t maxmemory = barch::get_max_module_memory();
        if (maxmemory == std::numeric_limits<uint64_t>::max()) {
            maxmemory = 0; // redis reports an unbounded limit as 0
        }
        std::string policy = barch::get_eviction_policy();
        if (policy == "none") {
            policy = "noeviction";
        }
        std::string allocator = barch::get_use_vmm_memory() ? "barch-vmm" : "barch-heap";

        std::string response =
        "# Memory\n\n"
        "used_memory:"+tos(used)+"\n"
        "used_memory_human:"+human(used)+"\n"
        "used_memory_rss:"+tos(rss)+"\n"
        "used_memory_rss_human:"+human(rss)+"\n"
        "used_memory_peak:"+tos(peak)+"\n"
        "used_memory_peak_human:"+human(peak)+"\n"
        "used_memory_peak_perc:"+perc(used, peak)+"\n"
        "used_memory_overhead:"+tos(overhead)+"\n"
        "used_memory_startup:"+tos(startup)+"\n"
        "used_memory_dataset:"+tos(dataset)+"\n"
        "used_memory_dataset_perc:"+perc(dataset, net)+"\n"
        "allocator_allocated:"+tos(allocator_allocated)+"\n"
        "allocator_active:"+tos(allocator_active)+"\n"
        "allocator_resident:"+tos(allocator_resident)+"\n"
        "allocator_muzzy:"+tos(allocator_muzzy)+"\n"
        "allocator_frag_ratio:"+ratio(allocator_active, allocator_allocated)+"\n"
        "allocator_frag_bytes:"+diff(allocator_active, allocator_allocated)+"\n"
        "allocator_rss_ratio:"+ratio(allocator_resident, allocator_active)+"\n"
        "allocator_rss_bytes:"+diff(allocator_resident, allocator_active)+"\n"
        "rss_overhead_ratio:"+ratio(rss, allocator_resident)+"\n"
        "rss_overhead_bytes:"+diff(rss, allocator_resident)+"\n"
        "total_system_memory:"+tos(heap::get_physical_memory_bytes())+"\n"
        "total_system_memory_human:"+human(heap::get_physical_memory_bytes())+"\n"
        "used_memory_lua:0\n"
        "used_memory_vm_eval:0\n"
        "used_memory_lua_human:0B\n"
        "used_memory_scripts_eval:0\n"
        "number_of_cached_scripts:0\n"
        "number_of_functions:"+tos(functions_by_name()->size())+"\n"
        "number_of_libraries:0\n"
        "used_memory_vm_functions:0\n"
        "used_memory_vm_total:0\n"
        "used_memory_vm_total_human:0B\n"
        "used_memory_functions:0\n"
        "used_memory_scripts:0\n"
        "used_memory_scripts_human:0B\n"
        "maxmemory:"+tos(maxmemory)+"\n"
        "maxmemory_human:"+human(maxmemory)+"\n"
        "maxmemory_policy:"+policy+"\n"
        "mem_fragmentation_ratio:"+ratio(rss, used)+"\n"
        "mem_fragmentation_bytes:"+diff(rss, used)+"\n"
        "mem_not_counted_for_evict:0\n"
        "mem_replication_backlog:0\n"
        "mem_total_replication_buffers:0\n"
        "mem_clients_slaves:0\n"
        "mem_clients_normal:0\n"
        "mem_cluster_links:0\n"
        "mem_aof_buffer:0\n"
        "mem_allocator:"+allocator+"\n"
        "mem_overhead_db_hashtable_lut:"+tos(node_logical)+"\n"
        "mem_overhead_db_hashtable_rehashing:0\n"
        "active_defrag_running:"+tos(barch::get_active_defrag() ? 1 : 0)+"\n"
        "lazyfree_pending_objects:0\n"
        "lazyfreed_objects:"+tos(as.keys_evicted)+"\n"
        // barch specific memory detail - the numbers redis has no equivalent for
        "barch_keys:"+tos(keys)+"\n"
        "barch_shards:"+tos(shards)+"\n"
        "barch_pages:"+tos(pages)+"\n"
        "barch_vmm_bytes_allocated:"+tos(vmm)+"\n"
        "barch_leaf_bytes_logical:"+tos(leaf_logical)+"\n"
        "barch_leaf_bytes_physical:"+tos(leaf_physical)+"\n"
        "barch_interior_bytes_logical:"+tos(node_logical)+"\n"
        "barch_interior_bytes_physical:"+tos(node_physical)+"\n"
        "barch_bytes_in_free_lists:"+tos(free_list_bytes)+"\n"
        "barch_value_bytes_compressed:"+tos(as.value_bytes_compressed)+"\n"
        "barch_leaf_nodes:"+tos(as.leaf_nodes)+"\n"
        "barch_size_4_nodes:"+tos(as.node4_nodes)+"\n"
        "barch_size_16_nodes:"+tos(as.node16_nodes)+"\n"
        "barch_size_48_nodes:"+tos(as.node48_nodes)+"\n"
        "barch_size_256_nodes:"+tos(as.node256_nodes)+"\n"
        "barch_pages_evicted:"+tos(as.pages_evicted)+"\n"
        "barch_keys_evicted:"+tos(as.keys_evicted)+"\n"
        "barch_pages_defragged:"+tos(as.pages_defragged)+"\n"
        "barch_vmm_pages_defragged:"+tos(as.vmm_pages_defragged)+"\n"
        "barch_vmm_pages_popped:"+tos(as.vmm_pages_popped)+"\n"
        "barch_oom_avoided_inserts:"+tos(as.oom_avoided_inserts)+"\n"
        "barch_vacuum_count:"+tos(as.vacuums_performed)+"\n"
        "barch_last_vacuum_time:"+tos(as.last_vacuum_time)+"\n";

        call.push_vt(response);
        return 0;
    }
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

/* the info commands as a RESP client sees them */
void register_info_api(function_map& r) {
    r["INFO"] = {::INFO,{"read","stats"}};
}
