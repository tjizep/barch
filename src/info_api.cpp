//
// Created by teejip on 8/12/25.
//

#include "counted_locks.h"
#include "info_api.h"
#include "module.h"
#include "vk_caller.h"
#include "sharded_store.h"
#include "keyspace_locks.h"
#include "swig_api.h"

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
// taken once, the first time INFO has a real number, so a later write cannot move it.
// min() with used is so we never claim more startup than the process holds; doing that
// on every INFO made the figure track used as the first inserts grew the hybrid hash.
static std::atomic<uint64_t> frozen_startup{0};
static uint64_t freeze_startup_memory(uint64_t used) {
    uint64_t frozen = frozen_startup.load(std::memory_order_relaxed);
    if (frozen != 0) {
        return frozen;
    }
    uint64_t v = std::min<uint64_t>(get_startup_memory(), used);
    if (v == 0) {
        return 0;
    }
    uint64_t expected = 0;
    if (frozen_startup.compare_exchange_strong(expected, v, std::memory_order_relaxed)) {
        return v;
    }
    return expected;
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
        std::string index = s->opt_ordered_keys ? (s->hybrid_active() ? "ART+HASH" : "ART") : "HASH";
        std::string response =
        "# Shard\n\n"
        "number:"+tos(shard)+"\n"
        "index_physical:"+index+"\n"
        "index_logical:"+order+"\n"
        // how a key reaches this shard rather than how it is held once here. A key
        // space setting, reported per shard because this is where somebody looks
        "sharding:"+std::string(ks->opt_range_sharded ? "range" : "hash")+"\n"
        "size:"+tos(s->get_size())+"\n"
        "bytes_allocated:"+tos(s->get_ap().get_leaves().get_bytes_allocated() + s->get_ap().get_nodes().get_bytes_allocated()) + "\n"
        "virtual_allocated:"+tos(s->get_ap().get_leaves().get_allocated() + s->get_ap().get_nodes().get_allocated()) + "\n"
        "foreign_flights:"+tos(static_cast<const barch::shard*>(s.get())->flights.size())+"\n";

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
            // see TODO 204 - all_shards calls back holding nothing
            shared_latch release(s->get_latch());
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
        uint64_t startup = freeze_startup_memory(used);
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
        // what the Luau VMs hold - the per session function states, the foreign fill
        // states and the scratch one SETF compiles against. See TODO 151
        "used_memory_luau:"+tos(statistics::luau_bytes)+"\n"
        "used_memory_luau_human:"+human(statistics::luau_bytes)+"\n"
        "luau_states:"+tos(statistics::luau_states)+"\n"
        "luau_functions_compiled:"+tos(statistics::luau_functions)+"\n"
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
            // one read each: these are written by every session thread, so the
            // line is built from a snapshot rather than re-reading a moving value
            const uint64_t calls = command_calls(f.second);
            if (calls > 0) { // for verbosity AND div-zero
                auto micros = (double)command_nanos(f.second)/1000.0f;
                std::string line = "cmdstat_";
                line += lower(text, f.first);
                line += ":";
                line += "calls=";
                line += std::to_string(calls);
                line += ",";
                line += "usec=";
                line += conversion::as_variable(roundn(micros,4)).s();
                line += ",";
                line += "avg_usec=";
                line += conversion::as_variable(roundn(roundn(micros/calls,4), 4)).s();
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
    // only what barch actually knows. redis's Clients section carries sixteen fields and
    // inventing the other fifteen would make INFO a worse source than no INFO at all -
    // blocked_clients is the one the valkey suite reads, through wait_for_blocked_client,
    // and it is the one that is real here. See DONE 120
    if (argv.size() == 2 && lower(text, argv[1].to_string()) == "clients") {
        std::string response =
        "# Clients\n\n"
        "blocked_clients:"+tos(statistics::blocked_clients.load())+"\n";
        call.push_vt(response);
        return 0;
    }
    if (argv.size() == 2 && lower(text, argv[1].to_string()) == "foreign") {
        uint64_t inflight = 0;
        barch::all_spaces([&](const std::string&, const barch::key_space_ptr& ks) {
            inflight += ks->foreign_inflight.load();
        });
        std::string response =
        "# Foreign\n\n"
        "foreign_queries:"+tos(statistics::foreign_queries.load())+"\n"
        "foreign_misses:"+tos(statistics::foreign_misses.load())+"\n"
        "foreign_errors:"+tos(statistics::foreign_errors.load())+"\n"
        "foreign_waiters:"+tos(statistics::foreign_waiters.load())+"\n"
        "foreign_coalesced:"+tos(statistics::foreign_coalesced.load())+"\n"
        "foreign_overloaded:"+tos(statistics::foreign_overloaded.load())+"\n"
        "foreign_cancelled:"+tos(statistics::foreign_cancelled.load())+"\n"
        "foreign_inflight:"+tos(inflight)+"\n";
        call.push_vt(response);
        return 0;
    }
    return call.push_error("not implemented");
}
}

extern "C" {
static auto startTime = std::chrono::high_resolution_clock::now();

int STATS(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();
    art_statistics as = barch::get_statistics();

    call.start_array();
    call.push_values({"heap_bytes_allocated", get_total_memory()});
    call.push_values({"vmm_bytes_allocated", heap::vmm_allocated});
    call.push_values({"value_bytes_compressed",as.value_bytes_compressed});
    call.push_values({ "last_vacuum_time", as.last_vacuum_time});
    call.push_values({ "vacuum_count", as.vacuums_performed});
    call.push_values({ "bytes_addressable", as.bytes_allocated});
    call.push_values({ "interior_bytes_addressable", as.bytes_interior});
    call.push_values({ "leaf_nodes", as.leaf_nodes});
    call.push_values({ "size_4_nodes", as.node4_nodes});
    call.push_values({ "size_16_nodes", as.node16_nodes});
    call.push_values({ "size_48_nodes", as.node48_nodes});
    call.push_values({ "size_256_nodes", as.node256_nodes});
    call.push_values({ "size_256_occupancy", as.node256_occupants});
    call.push_values({ "leaf_nodes_replaced", as.leaf_nodes_replaced});
    call.push_values({ "pages_evicted", as.pages_evicted});
    call.push_values({ "keys_evicted", as.keys_evicted});
    call.push_values({ "pages_defragged", as.pages_defragged});
    call.push_values({ "vmm_pages_defragged", as.vmm_pages_defragged});
    call.push_values({ "vmm_pages_popped", as.vmm_pages_popped});
    call.push_values({ "read_locks_active", as.read_locks_active});
    call.push_values({ "write_locks_active", as.write_locks_active});
    call.push_values({ "exceptions_raised", as.exceptions_raised});
    call.push_values({ "maintenance_cycles", as.maintenance_cycles});
    call.push_values({ "shards", as.shards});
    call.push_values({ "local_calls", as.local_calls});
    call.push_values({ "max_spin", as.max_spin});
    call.push_values({"logical_allocated", as.logical_allocated});
    call.push_values({"bytes_in_free_lists", as.bytes_in_free_lists});
    call.push_values({"oom_avoided_inserts", as.oom_avoided_inserts});
    call.push_values({"keys_found", as.keys_found});
    call.push_values({"foreign_queries", statistics::foreign_queries.load()});
    call.push_values({"foreign_misses", statistics::foreign_misses.load()});
    call.push_values({"foreign_errors", statistics::foreign_errors.load()});
    call.push_values({"foreign_waiters", statistics::foreign_waiters.load()});
    call.push_values({"foreign_coalesced", statistics::foreign_coalesced.load()});
    call.push_values({"foreign_overloaded", statistics::foreign_overloaded.load()});
    call.push_values({"foreign_cancelled", statistics::foreign_cancelled.load()});
    call.push_values({"foreign_slow", statistics::foreign_slow.load()});
    call.end_array();
    return 0;
}
/* B.STATISTICS
 *
 * get memory statistics. */
int cmd_STATS(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, STATS);
}
int OPS(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();

    art_ops_statistics as = barch::get_ops_statistics();
    call.start_array();
    call.push_values({"delete_ops", as.delete_ops});
    call.push_values({"retrieve_ops", as.get_ops});
    call.push_values({"insert_ops", as.insert_ops});
    call.push_values({"iterations", as.iter_ops});
    call.push_values({"range_iterations", as.iter_range_ops});
    call.push_values({"lower_bound_ops", as.lb_ops});
    call.push_values({"maximum_ops", as.max_ops});
    call.push_values({"minimum_ops", as.min_ops});
    call.push_values({"range_ops", as.range_ops});
    call.push_values({"set_ops", as.set_ops});
    call.push_values({"size_ops", as.size_ops});
    call.push_values({"foreign_queries", statistics::foreign_queries.load()});
    call.push_values({"foreign_misses", statistics::foreign_misses.load()});
    call.push_values({"foreign_errors", statistics::foreign_errors.load()});
    call.push_values({"foreign_waiters", statistics::foreign_waiters.load()});
    call.push_values({"foreign_coalesced", statistics::foreign_coalesced.load()});
    call.push_values({"foreign_overloaded", statistics::foreign_overloaded.load()});
    call.push_values({"foreign_cancelled", statistics::foreign_cancelled.load()});
    call.push_values({"foreign_slow", statistics::foreign_slow.load()});
    call.end_array();
    return 0;
}
/* B.OPS
 *
 * get data structure ops. */
int cmd_OPS(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, OPS);
}
int HEAPBYTES(caller& call, const arg_t& argv) {
    //compressed_release release;
    if (argv.size() != 1)
        return call.wrong_arity();;
    auto vbytes = 0ll;
    barch::sharded_store store(call.kspace());
    // as SIZE: allocator byte counts are reads
    store.each_shard_read([&](const barch::shard_ptr& s) {
        vbytes += s->get_ap().get_nodes().get_bytes_allocated() + s->get_ap().get_leaves().get_bytes_allocated();
    });
    return call.push_ll( (int64_t) heap::allocated + vbytes);
}
int cmd_HEAPBYTES(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HEAPBYTES);
}
int cmd_VACUUM(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc) {

    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);
    size_t result = 0;
    return ValkeyModule_ReplyWithLongLong(ctx, (int64_t) result);
}
int cmd_MILLIS(ValkeyModuleCtx *ctx, ValkeyModuleString **, int) {
    auto t = std::chrono::high_resolution_clock::now();
    const auto d = std::chrono::duration_cast<std::chrono::milliseconds>(t - startTime);
    return ValkeyModule_ReplyWithLongLong(ctx, d.count());
}
}

int add_info_api(ValkeyModuleCtx *ctx) {
    if (ValkeyModule_CreateCommand(ctx, NAME(STATS), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(OPS), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(MILLIS), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(VACUUM), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(HEAPBYTES), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    return VALKEYMODULE_OK;
}

/* what a RESP client can ask about the server itself. HEAPBYTES, VACUUM and MILLIS are
 * registered with the valkey module above but not here - they have never been reachable
 * over RESP, and that is left as it was. */
void register_info_api(function_map& r) {
    r["INFO"] = {::INFO,{"read","stats"}};
    r["STATS"] = {::STATS,{"read","stats"}};
    r["OPS"] = {::OPS,{"read","stats"}};
}
