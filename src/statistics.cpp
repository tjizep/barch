#include "statistics.h"
#include "constants.h"
/**
 * size stats
 */

constexpr size_t Alignment = con_alignment; //std::hardware_destructive_interference_size;
alignas(Alignment) std::atomic<uint64_t> statistics::n4_nodes = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::n16_nodes = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::n48_nodes = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::n256_nodes = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::node256_occupants = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::leaf_nodes = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::value_bytes_compressed = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::vacuums_performed = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::last_vacuum_time = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::leaf_nodes_replaced = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::pages_evicted = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::keys_evicted = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::pages_defragged = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::vmm_pages_defragged = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::vmm_pages_popped = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::read_locks_active;
alignas(Alignment) std::atomic<uint64_t> statistics::write_locks_active;

alignas(Alignment) std::atomic<uint64_t> statistics::exceptions_raised = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::maintenance_cycles = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::shards = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::local_calls = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::max_spin = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::max_leaf_size = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::logical_allocated = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::bytes_in_free_lists = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::oom_avoided_inserts = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::keys_found = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::new_keys_added = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::keys_replaced = 0;

/**
 * ops stats
 */
alignas(Alignment) std::atomic<uint64_t> statistics::delete_ops = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::set_ops = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::iter_ops = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::iter_start_ops = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::iter_range_ops = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::range_ops = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::get_ops = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::lb_ops = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::size_ops = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::insert_ops = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::min_ops = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::max_ops = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::incr_ops = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::decr_ops = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::update_ops = 0;

/**
* queue stats
*/

alignas(Alignment) std::atomic<uint64_t> statistics::queue_failures = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::queue_added = 0;
alignas(Alignment) std::atomic<uint64_t> statistics::queue_processed = 0;

namespace statistics::repl {
    alignas(Alignment) std::atomic<uint64_t> push_connections_open = 0;
    alignas(Alignment) std::atomic<uint64_t> key_find_recv = 0;
    alignas(Alignment) std::atomic<uint64_t> key_rem_recv_applied = 0;
    alignas(Alignment) std::atomic<uint64_t> bytes_recv = 0;
    alignas(Alignment) std::atomic<uint64_t> out_queue_size = 0;
    alignas(Alignment) std::atomic<uint64_t> instructions_failed = 0;
    alignas(Alignment) std::atomic<uint64_t> bytes_sent = 0;
    alignas(Alignment) std::atomic<uint64_t> insert_requests = 0;
    alignas(Alignment) std::atomic<uint64_t> remove_requests = 0;
    alignas(Alignment) std::atomic<uint64_t> find_requests = 0;
    alignas(Alignment) std::atomic<uint64_t> barch_requests = 0;
    alignas(Alignment) std::atomic<uint64_t> request_errors = 0;
    alignas(Alignment) std::atomic<uint64_t> redis_sessions = 0;
    alignas(Alignment) std::atomic<uint64_t> art_sessions = 0;
    alignas(Alignment) std::atomic<uint64_t> attempted_routes = 0;
    alignas(Alignment) std::atomic<uint64_t> routes_succeeded = 0;

}