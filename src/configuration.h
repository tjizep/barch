//
// Created by linuxlite on 3/13/25.
//

#ifndef CONFIGURATION_H
#define CONFIGURATION_H
#include "valkeymodule.h"
#include <limits>
/**
* functions provide access to configuration
*/
namespace art
{
    enum compression_type
    {
        compression_none = 0,
        compression_zstd = 1
    };

    struct configuration_record
    {

        int compression = compression_none;
        uint64_t n_max_memory_bytes {std::numeric_limits<uint64_t>::max()};
        uint64_t maintenance_poll_delay {10};
        uint64_t max_defrag_page_count {1};
        unsigned iteration_worker_count {2};
        float min_fragmentation_ratio = 0.6f;
        bool active_defrag = false;
        bool evict_volatile_lru {false};
        bool evict_allkeys_lru {false};
        bool evict_volatile_lfu {false};
        bool evict_allkeys_lfu {false};
        bool evict_volatile_random {false};
        bool evict_allkeys_random {false};
        bool evict_volatile_ttl {false};

    };

    int register_valkey_configuration(ValkeyModuleCtx* ctx);
    configuration_record get_configuration();
    // all sizes in bytes, time/delay in milliseconds
    bool get_compression_enabled();
    uint64_t get_max_module_memory();
    uint64_t get_maintenance_poll_delay();
    uint64_t get_max_defrag_page_count();
    unsigned get_iteration_worker_count();
    float get_min_fragmentation_ratio();
    bool get_active_defrag();
    bool get_evict_volatile_lru();
    bool get_evict_allkeys_lru();
    bool get_evict_volatile_lfu();
    bool get_evict_allkeys_lfu();
    bool get_evict_volatile_random();
    bool get_evict_allkeys_random();
    bool get_evict_volatile_ttl();

    int set_configuration_value(ValkeyModuleString* name,ValkeyModuleString* value);

}
#endif //CONFIGURATION_H
