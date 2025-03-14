//
// Created by linuxlite on 3/13/25.
//

#ifndef CONFIGURATION_H
#define CONFIGURATION_H
#include "valkeymodule.h"
/**
* functions provide access to configuration
*/
namespace art
{
    bool get_compression_enabled();
    float get_module_memory_ratio();
    float get_min_fragmentation_ratio();
    bool get_active_defrag();
    int register_valkey_configuration(ValkeyModuleCtx* ctx);
    bool get_evict_volatile_lru();
    bool get_evict_allkeys_lru();
    bool get_evict_volatile_lfu();
    bool get_evict_allkeys_lfu();
    bool get_evict_volatile_random();
    bool get_evict_allkeys_random();
    bool get_evict_volatile_ttl();

}
#endif //CONFIGURATION_H
