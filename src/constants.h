//
// Created by linuxlite on 3/27/25.
//

#ifndef CONSTANTS_H
#define CONSTANTS_H
enum
{
    page_size = 4096,
    reserved_address_base = 120000,
    auto_vac = 0,
    auto_vac_workers = 4,
    iterate_workers = 4,
    test_memory = 0,
    allocation_padding = 0,
    use_last_page_caching = 0,
    initialize_memory = 1,
    storage_version = 4
};
#endif //CONSTANTS_H
