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
    max_queries_per_call = 32
};
#endif //CONSTANTS_H
