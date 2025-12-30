//
// Created by teejip on 4/9/25.
//

#include "module.h"
#include "keys.h"
#include "key_space.h"
thread_local uint64_t stream_write_ctr = 0;
thread_local uint64_t stream_read_ctr = 0;

constants Constants{};
barch::key_space_ptr& get_default_ks() {
    static auto ks_node = barch::get_keyspace("");

    return ks_node;
}
uint64_t get_total_memory() {
    return heap::allocated;
}
