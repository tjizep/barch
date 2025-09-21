//
// Created by linuxlite on 3/27/25.
//

#ifndef CONSTANTS_H
#define CONSTANTS_H

enum {
    node_checks = 0,
    page_size = 32768*8, // must be a power of two
    physical_page_size = page_size,
    maximum_allocation_size = page_size - (page_size % 1000),
    initial_node_ptr_size = 4, // must be a power of twp
    reserved_address_base = 120000,
    iterate_workers = 4,
    test_memory = 1,
    allocation_padding = 0,
    initialize_memory = 1,
    storage_version = 10*page_size,
    ticker_size = 256,
    numeric_key_size = 12,
    num32_key_size = 6,
    composite_key_size = 2,
    max_queries_per_call = 32,
    leaf_type = 1,
    non_leaf_type = 2,
    comparable_key_static_size = 32,
    node_pointer_storage_size = 64,
    log_streams = 0,
    encoding_width = 128,
    encoding_delta = 0,
    key_terminator = 0x01,
    max_top = 100000000000,
    page_extension_on_allocation = 1,
    max_process_queue_size = 15000
};
#endif //CONSTANTS_H
