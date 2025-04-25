//
// Created by linuxlite on 3/27/25.
//

#ifndef CONSTANTS_H
#define CONSTANTS_H
enum
{
    page_size = 5100,
    reserved_address_base = 120000,
    auto_vac = 0,
    auto_vac_workers = 4,
    iterate_workers = 4,
    test_memory = 0,
    allocation_padding = 0,
    use_last_page_caching = 0,
    initialize_memory = 1,
    storage_version = 6,
    ticker_size = 32,
    numeric_key_size = 10,
    composite_key_size = 2,
    max_queries_per_call = 32,
    leaf_type = 1,
    non_leaf_type = 2,
    comparable_key_static_size = 48,
    node_pointer_storage_size = 36
};
#endif //CONSTANTS_H
