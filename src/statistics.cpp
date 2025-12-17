#include "statistics.h"

/**
 * size stats
 */
std::atomic<uint64_t> statistics::n4_nodes = 0;
std::atomic<uint64_t> statistics::n16_nodes = 0;
std::atomic<uint64_t> statistics::n48_nodes = 0;
std::atomic<uint64_t> statistics::n256_nodes = 0;
std::atomic<uint64_t> statistics::node256_occupants = 0;
std::atomic<uint64_t> statistics::leaf_nodes = 0;
std::atomic<uint64_t> statistics::value_bytes_compressed = 0;
std::atomic<uint64_t> statistics::vacuums_performed = 0;
std::atomic<uint64_t> statistics::last_vacuum_time = 0;
std::atomic<uint64_t> statistics::leaf_nodes_replaced = 0;
std::atomic<uint64_t> statistics::pages_evicted = 0;
std::atomic<uint64_t> statistics::keys_evicted = 0;
std::atomic<uint64_t> statistics::pages_defragged = 0;
std::atomic<uint64_t> statistics::exceptions_raised = 0;
std::atomic<uint64_t> statistics::maintenance_cycles = 0;
std::atomic<uint64_t> statistics::shards = 0;
std::atomic<uint64_t> statistics::local_calls = 0;
std::atomic<uint64_t> statistics::max_spin = 0;
std::atomic<uint64_t> statistics::max_leaf_size = 0;
std::atomic<uint64_t> statistics::logical_allocated = 0;
std::atomic<uint64_t> statistics::oom_avoided_inserts = 0;
std::atomic<uint64_t> statistics::keys_found = 0;
std::atomic<uint64_t> statistics::new_keys_added = 0;
std::atomic<uint64_t> statistics::keys_replaced = 0;

/**
 * ops stats
 */
std::atomic<uint64_t> statistics::delete_ops = 0;
std::atomic<uint64_t> statistics::set_ops = 0;
std::atomic<uint64_t> statistics::iter_ops = 0;
std::atomic<uint64_t> statistics::iter_start_ops = 0;
std::atomic<uint64_t> statistics::iter_range_ops = 0;
std::atomic<uint64_t> statistics::range_ops = 0;
std::atomic<uint64_t> statistics::get_ops = 0;
std::atomic<uint64_t> statistics::lb_ops = 0;
std::atomic<uint64_t> statistics::size_ops = 0;
std::atomic<uint64_t> statistics::insert_ops = 0;
std::atomic<uint64_t> statistics::min_ops = 0;
std::atomic<uint64_t> statistics::max_ops = 0;
std::atomic<uint64_t> statistics::incr_ops = 0;
std::atomic<uint64_t> statistics::decr_ops = 0;
std::atomic<uint64_t> statistics::update_ops = 0;

/**
* queue stats
*/

std::atomic<uint64_t> statistics::queue_failures = 0;
std::atomic<uint64_t> statistics::queue_added = 0;
std::atomic<uint64_t> statistics::queue_processed = 0;

namespace statistics::repl {
    std::atomic<uint64_t> push_connections_open = 0;
    std::atomic<uint64_t> key_add_recv = 0;
    std::atomic<uint64_t> key_add_recv_applied = 0;
    std::atomic<uint64_t> key_rem_recv = 0;
    std::atomic<uint64_t> key_find_recv = 0;
    std::atomic<uint64_t> key_rem_recv_applied = 0;
    std::atomic<uint64_t> bytes_recv = 0;
    std::atomic<uint64_t> out_queue_size = 0;
    std::atomic<uint64_t> instructions_failed = 0;
    std::atomic<uint64_t> bytes_sent = 0;
    std::atomic<uint64_t> insert_requests = 0;
    std::atomic<uint64_t> remove_requests = 0;
    std::atomic<uint64_t> find_requests = 0;
    std::atomic<uint64_t> request_errors = 0;
    std::atomic<uint64_t> redis_sessions = 0;
    std::atomic<uint64_t> art_sessions = 0;
    std::atomic<uint64_t> attempted_routes = 0;
    std::atomic<uint64_t> routes_succeeded = 0;

}