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
    extern std::atomic<uint64_t> leaf_nodes;
    extern std::atomic<uint64_t> node_bytes_alloc;
    extern std::atomic<uint64_t> interior_bytes_alloc;
    
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