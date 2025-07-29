//
// Created by linuxlite on 3/27/25.
//

#ifndef CONSTANTS_H
#define CONSTANTS_H

enum {
    node_checks = 0,
    page_size = 32768, // must be a power of twp
    physical_page_size = page_size,
    initial_node_ptr_size = 4, // must be a power of twp
    reserved_address_base = 120000,
    iterate_workers = 4,
    test_memory = 0,
    allocation_padding = 0,
    initialize_memory = 1,
    storage_version = 8,
    ticker_size = 64,
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
    key_terminator = 0x01
};
#endif //CONSTANTS_H
