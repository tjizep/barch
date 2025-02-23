#include "statistics.h"

/**
 * size stats
 */
std::atomic<uint64_t> statistics::n4_nodes;
std::atomic<uint64_t> statistics::n16_nodes;
std::atomic<uint64_t> statistics::n48_nodes;
std::atomic<uint64_t> statistics::n256_nodes;
std::atomic<uint64_t> statistics::node256_occupants;
std::atomic<uint64_t> statistics::leaf_nodes;
std::atomic<uint64_t> statistics::node_bytes_alloc;
std::atomic<uint64_t> statistics::interior_bytes_alloc;
/**
 * ops stats
 */
std::atomic<uint64_t> statistics::delete_ops;
std::atomic<uint64_t> statistics::set_ops;
std::atomic<uint64_t> statistics::iter_ops;
std::atomic<uint64_t> statistics::iter_start_ops;
std::atomic<uint64_t> statistics::iter_range_ops;
std::atomic<uint64_t> statistics::range_ops;
std::atomic<uint64_t> statistics::get_ops;
std::atomic<uint64_t> statistics::lb_ops;
std::atomic<uint64_t> statistics::size_ops;
std::atomic<uint64_t> statistics::insert_ops;
std::atomic<uint64_t> statistics::min_ops;
std::atomic<uint64_t> statistics::max_ops;