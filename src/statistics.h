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
    extern std::atomic<uint64_t> value_bytes_compressed;
    extern std::atomic<uint64_t> pages_evicted;
    extern std::atomic<uint64_t> keys_evicted;
    extern std::atomic<uint64_t> pages_defragged;
    extern std::atomic<uint64_t> vmm_pages_defragged;
    extern std::atomic<uint64_t> vmm_pages_popped;
    extern std::atomic<uint64_t> exceptions_raised;
    extern std::atomic<uint64_t> max_leaf_size;
    extern std::atomic<uint64_t> oom_avoided_inserts;
    extern std::atomic<uint64_t> keys_found;
    extern std::atomic<uint64_t> new_keys_added;
    extern std::atomic<uint64_t> keys_replaced;
    /**
     * allocation stats
     */
    extern std::atomic<uint64_t> logical_allocated;
    extern std::atomic<uint64_t> bytes_in_free_lists;
    /**
    * internal stats
    */
    extern std::atomic<uint64_t> vacuums_performed;
    extern std::atomic<uint64_t> last_vacuum_time;
    extern std::atomic<uint64_t> leaf_nodes_replaced;
    extern std::atomic<uint64_t> maintenance_cycles;
    extern std::atomic<uint64_t> shards;
    extern std::atomic<uint64_t> local_calls;
    extern std::atomic<uint64_t> max_spin;
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
     * queue stats
     */
    extern std::atomic<uint64_t> queue_failures;
    extern std::atomic<uint64_t> queue_added;
    extern std::atomic<uint64_t> queue_processed;

    /**
     *Lock stats
     */
    extern std::atomic<uint64_t> read_locks_active;
    extern std::atomic<uint64_t> write_locks_active;

    /**
     * replication + network stats
     */
    namespace repl {
        extern std::atomic<uint64_t> push_connections_open;
        extern std::atomic<uint64_t> key_find_recv;
        extern std::atomic<uint64_t> bytes_recv;
        extern std::atomic<uint64_t> bytes_sent;
        extern std::atomic<uint64_t> out_queue_size;
        extern std::atomic<uint64_t> instructions_failed;
        extern std::atomic<uint64_t> insert_requests;
        extern std::atomic<uint64_t> remove_requests;
        extern std::atomic<uint64_t> find_requests;
        extern std::atomic<uint64_t> barch_requests;
        extern std::atomic<uint64_t> request_errors;
        extern std::atomic<uint64_t> redis_sessions;
        extern std::atomic<uint64_t> art_sessions;
        extern std::atomic<uint64_t> attempted_routes;
        extern std::atomic<uint64_t> routes_succeeded;
    }

    /**
     * zero the counters that count things that have happened, for CONFIG RESETSTAT.
     *
     * Only those. The gauges above are deliberately left alone, and the distinction is
     * not cosmetic:
     *
     *  - node and leaf counts, logical_allocated, bytes_in_free_lists and shards
     *    describe what the server is holding right now. Zeroing them would not reset a
     *    statistic, it would make the server misreport its own state until the numbers
     *    drifted back.
     *  - read_locks_active, write_locks_active, redis_sessions, art_sessions,
     *    push_connections_open and out_queue_size are incremented and later
     *    decremented. Zeroing one while it is non zero means the matching decrements
     *    wrap it to near UINT64_MAX, which is worse than merely wrong.
     *  - last_vacuum_time is a timestamp and max_leaf_size is a property of the data,
     *    not of activity.
     */
    void reset_statistics();
}

template<typename Ext>
[[ noreturn ]] static void throw_exception(const char *name) {
    ++statistics::exceptions_raised;
    barch::std_err("exception", name);
    throw Ext(name);
}
