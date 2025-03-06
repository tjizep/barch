#pragma once
#include <atomic>
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
    extern std::atomic<uint64_t> addressable_bytes_alloc;
    extern std::atomic<uint64_t> interior_bytes_alloc;
    extern std::atomic<uint64_t> page_bytes_compressed;
    extern std::atomic<uint64_t> max_page_bytes_uncompressed;
    extern std::atomic<uint64_t> page_bytes_uncompressed;
    extern std::atomic<uint64_t> pages_uncompressed;
    extern std::atomic<uint64_t> pages_compressed;
    extern std::atomic<uint64_t> vacuums_performed;
    extern std::atomic<uint64_t> last_vacuum_time;
    extern std::atomic<uint64_t> leaf_nodes_replaced;
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

}