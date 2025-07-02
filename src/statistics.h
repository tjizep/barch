#pragma once
#include <atomic>
#include <exception>
#include "logger.h"
namespace statistics {
    /**
     * size stats
     */
    extern std::atomic<uint64_t> n4_nodes;
    extern std::atomic<uint64_t> n16_nodes;
    extern std::atomic<uint64_t> n48_nodes;
    extern std::atomic<uint64_t> n256_nodes;
    extern std::atomic<uint64_t> node256_occupants;
    extern std::atomic<uint64_t> leaf_nodes;
    extern std::atomic<uint64_t> page_bytes_compressed;
    extern std::atomic<uint64_t> max_page_bytes_uncompressed;
    extern std::atomic<uint64_t> page_bytes_uncompressed;
    extern std::atomic<uint64_t> pages_uncompressed;
    extern std::atomic<uint64_t> pages_compressed;
    extern std::atomic<uint64_t> pages_evicted;
    extern std::atomic<uint64_t> keys_evicted;
    extern std::atomic<uint64_t> pages_defragged;
    extern std::atomic<uint64_t> exceptions_raised;
    extern std::atomic<uint64_t> max_leaf_size;
    extern std::atomic<uint64_t> logical_allocated;
    extern std::atomic<uint64_t> oom_avoided_inserts;
    /**
    * internal stats
    */
    extern std::atomic<uint64_t> vacuums_performed;
    extern std::atomic<uint64_t> last_vacuum_time;
    extern std::atomic<uint64_t> leaf_nodes_replaced;
    extern std::atomic<uint64_t> maintenance_cycles;
    extern std::atomic<uint64_t> shards;
    extern std::atomic<uint64_t> local_calls;
    /**
     * ops stats
     */
    extern std::atomic<uint64_t> delete_ops;
    extern std::atomic<uint64_t> set_ops;
    extern std::atomic<uint64_t> iter_ops;
    extern std::atomic<uint64_t> iter_start_ops;
    extern std::atomic<uint64_t> iter_range_ops;
    extern std::atomic<uint64_t> range_ops;
    extern std::atomic<uint64_t> get_ops;
    extern std::atomic<uint64_t> lb_ops;
    extern std::atomic<uint64_t> size_ops;
    extern std::atomic<uint64_t> insert_ops;
    extern std::atomic<uint64_t> min_ops;
    extern std::atomic<uint64_t> max_ops;
    extern std::atomic<uint64_t> incr_ops;
    extern std::atomic<uint64_t> decr_ops;
    extern std::atomic<uint64_t> update_ops;
    /**
     * replication + network stats
     */
    namespace repl {
        extern std::atomic<uint64_t> push_connections_open;
        extern std::atomic<uint64_t> key_add_recv;
        extern std::atomic<uint64_t> key_add_recv_applied;
        extern std::atomic<uint64_t> key_rem_recv;
        extern std::atomic<uint64_t> key_find_recv;
        extern std::atomic<uint64_t> key_rem_recv_applied;
        extern std::atomic<uint64_t> bytes_recv;
        extern std::atomic<uint64_t> bytes_sent;
        extern std::atomic<uint64_t> out_queue_size;
        extern std::atomic<uint64_t> instructions_failed;
        extern std::atomic<uint64_t> insert_requests;
        extern std::atomic<uint64_t> remove_requests;
        extern std::atomic<uint64_t> find_requests;
        extern std::atomic<uint64_t> request_errors;
    }
}

template<typename Ext>
static void throw_exception(const char *name) {
    ++statistics::exceptions_raised;
    art::std_err("exception", name);
    throw Ext(name);
}
