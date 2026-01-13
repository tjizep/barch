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
    logical_allocation_padding = 64,
    min_logical_allocation_for_pad = 1024,
    maximum_allocation_size = page_size - 256,
    initial_node_ptr_size = 4, // must be a power of twp
    reserved_address_base = 120000,
    iterate_workers = 4,
    test_memory = 1,
    fl_test_memory = 0,
    initialize_memory = 1, // currently this should always be one - if the program needs to work
    storage_version = 10,
    ticker_size = 16,
    numeric_key_size = 12,
    num32_key_size = 6,
    composite_key_size = 2,
    max_queries_per_call = 32,
    static_bloom_size = 32768*128,
    leaf_type = 1,
    non_leaf_type = 2,
    comparable_key_static_size = 64,
    node_pointer_storage_size = 64,
    log_streams = 0,
    encoding_width = 128,
    encoding_delta = 0,
    key_terminator = 0x01,
    max_top = 100000000000,
    page_extension_on_allocation = 1,
    log_loading_messages = 0,
    log_saving_messages = 0,
    resp_pool_factor = 100,
    tcp_accept_pool_factor = 50
};
inline size_t alloc_pad(size_t size) {

    size_t smod = size % logical_allocation_padding;
    if (size > min_logical_allocation_for_pad && smod !=0) {
        size_t r = size + (logical_allocation_padding - smod);
        if ((r % logical_allocation_padding) !=0 || r < size ) {
            abort();
        }
        return r;
    }
return size;
}
#endif //CONSTANTS_H
