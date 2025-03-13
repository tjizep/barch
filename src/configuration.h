//
// Created by linuxlite on 3/13/25.
//

#ifndef CONFIGURATION_H
#define CONFIGURATION_H
#include "valkeymodule.h"
/**
* functions provide access to configuration
*/
bool get_lru_evict_enabled(ValkeyModuleCtx* ctx);
bool get_lfu_evict_enabled(ValkeyModuleCtx* ctx);
bool get_compression_enabled(ValkeyModuleCtx* ctx);
float get_module_memory_ratio(ValkeyModuleCtx* ctx);
#endif //CONFIGURATION_H
