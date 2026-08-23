//
// Created by linuxlite on 3/27/25.
//

#ifndef CONSTANTS_H
#define CONSTANTS_H
#include <cstdlib>
enum {
    node_checks = 0,
    page_size = 32768*8, // must be a power of two
    physical_page_size = page_size,
    logical_allocation_padding = 128,
    min_logical_allocation_for_pad = 1024,
    maximum_allocation_size = page_size - 256,
    initial_node_ptr_size = 4, // must be a power of twp
    reserved_address_base = 120000,
    iterate_workers = 4,
    test_memory = 1,
    fl_test_memory = 0,
    initialize_memory = 1, // currently this should always be one - if the program needs to work
    // the middle term is the format revision - bump it whenever what is written to a
    // shard file changes shape, so an older file is refused on load rather than read as
    // something it is not. 11 was the tplain key encoding (TODO 55): a key holding the
    // separator stopped encoding the same as a container key. 12 is the per kind container
    // leads (TODO 53) - a list, hash and ordered set store under different lead bytes now,
    // so containers in a file written at 11 would read as the wrong kind or not at all.
    // 13 is the clock (DONE 55): every stored expiry used to be measured against the time
    // since the machine started and is now a unix time, so a deadline saved at 12 reads as
    // a moment in 1970 and the key it belongs to would vanish on load
    // 14 is the member index marker (DONE 62): it used to encode as a component with no
    // separator, which made an ordered set's index key identical to the key of a set whose
    // name began with an 0x03, so the two could not be told apart at all
    // 16 is stored functions (TODO 98): a shard file can now hold keys led by
    // art::tfunction, and a binary that predates the type does not read them wrongly -
    // it throws in comparable_key or aborts in keys.cpp, and inside valkey an abort
    // deadlocks on the signal handler lock. Refusing the file is the better failure
    storage_version = page_size + 16 + test_memory,
    ticker_size = 16,
    numeric_key_size = 12,
    num32_key_size = 6,
    composite_key_size = 2,
    max_queries_per_call = 32,
    static_bloom_size = 32768*128,
    leaf_type = 1,
    non_leaf_type = 2,
    comparable_key_static_size = 64,
    node_pointer_storage_size = 48,
    log_streams = 0,
    encoding_width = 128,
    encoding_delta = 0,
    key_terminator = 0x01,
    max_top = 100000000000,
    page_extension_on_allocation = 1,
    log_loading_messages = 0,
    log_saving_messages = 0,
    resp_pool_factor = 50,
    tcp_accept_pool_factor = 50,
    con_alignment = 64 //std::hardware_destructive_interference_size
};
inline size_t alloc_pad(size_t size) {

    size_t smod = size % logical_allocation_padding;
    if (size > min_logical_allocation_for_pad && smod !=0) {
        size_t r = size + (logical_allocation_padding - smod);
        if ((r % logical_allocation_padding) != 0 || r < size ) {
            abort();
        }
        return r;
    }
    return size;
}
#endif //CONSTANTS_H
