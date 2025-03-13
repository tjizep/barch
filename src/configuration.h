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
    bool get_lru_evict_enabled();
    bool get_lfu_evict_enabled();
    bool get_compression_enabled();
    float get_module_memory_ratio();
    float get_min_fragmentation_ratio();
    bool get_active_defrag();
    int register_valkey_configuration(ValkeyModuleCtx* ctx);
}
#endif //CONFIGURATION_H
