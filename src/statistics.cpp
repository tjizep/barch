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
std::atomic<uint64_t> statistics::addressable_bytes_alloc = 0;
std::atomic<uint64_t> statistics::interior_bytes_alloc = 0;
std::atomic<uint64_t> statistics::page_bytes_compressed = 0;
std::atomic<uint64_t> statistics::page_bytes_uncompressed = 0;
std::atomic<uint64_t> statistics::pages_uncompressed = 0;
std::atomic<uint64_t> statistics::pages_compressed = 0;
std::atomic<uint64_t> statistics::max_page_bytes_uncompressed = 0;
std::atomic<uint64_t> statistics::vacuums_performed = 0;
std::atomic<uint64_t> statistics::last_vacuum_time = 0;
std::atomic<uint64_t> statistics::leaf_nodes_replaced = 0;
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