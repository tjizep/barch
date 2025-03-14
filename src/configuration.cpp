//
// Created by linuxlite on 3/13/25.
//

#include "valkeymodule.h"
#include "configuration.h"
#include <cstdlib>
#include <string>
#include "art.h"
#define unused_arg
static std::mutex config_mutex {};
static std::string compression_type {};
static std::string eviction_type {};
static std::string max_memory_ratio {};
static std::string min_fragmentation_ratio {};
static std::string active_defrag {};
// Many eviction settings
static bool evict_volatile_lru {false};
static bool evict_allkeys_lru {false};
static bool evict_volatile_lfu {false};
static bool evict_allkeys_lfu {false};
static bool evict_volatile_random {false};
static bool evict_allkeys_random {false};
static bool evict_volatile_ttl {false};

static ValkeyModuleString *GetMaxMemoryRatio(const char *unused_arg, void *unused_arg) {
    std::unique_lock lock(config_mutex);
    return  ValkeyModule_CreateString(nullptr, max_memory_ratio.c_str(),max_memory_ratio.length());;
}

static int SetMaxMemoryRatio(const char *unused_arg, ValkeyModuleString *val, void *unused_arg, ValkeyModuleString **unused_arg)
{
    std::unique_lock lock(config_mutex);
    max_memory_ratio = ValkeyModule_StringPtrLen(val, nullptr);;
    return VALKEYMODULE_OK;
}
// ===========================================================================================================
static ValkeyModuleString *GetCompressionType(const char *unused_arg, void *unused_arg) {
    std::unique_lock lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, compression_type.c_str(),compression_type.length());
}

static int SetCompressionType(const char *unused_arg, ValkeyModuleString *val, void *unused_arg, ValkeyModuleString **unused_arg)
{
    std::unique_lock lock(config_mutex);
    compression_type = ValkeyModule_StringPtrLen(val, nullptr);
    return VALKEYMODULE_OK;
}

static int ApplyCompressionType(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg)
{
    art::get_leaf_compression().set_opt_enable_compression(art::get_compression_enabled());
    art::get_node_compression().set_opt_enable_compression(art::get_compression_enabled());
    return VALKEYMODULE_OK;
}
// ===========================================================================================================
static ValkeyModuleString *GetMinFragmentation(const char *unused_arg, void *unused_arg) {
    std::unique_lock lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, min_fragmentation_ratio.c_str(),min_fragmentation_ratio.length());
}

static int SetMinFragmentation(const char *unused_arg, ValkeyModuleString *val, void *unused_arg, ValkeyModuleString **unused_arg)
{
    std::unique_lock lock(config_mutex);
    min_fragmentation_ratio = ValkeyModule_StringPtrLen(val, nullptr);
    return VALKEYMODULE_OK;
}

static int ApplyMinFragmentation(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg)
{
    art::get_leaf_compression().set_opt_enable_compression(art::get_compression_enabled());
    art::get_node_compression().set_opt_enable_compression(art::get_compression_enabled());
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString *GetActiveDefragType(const char *unused_arg, void *unused_arg) {
    std::unique_lock lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, active_defrag.c_str(),active_defrag.length());
}

static int SetActiveDefragType(const char *unused_arg, ValkeyModuleString *val, void *unused_arg, ValkeyModuleString **unused_arg)
{
    std::unique_lock lock(config_mutex);
    active_defrag = ValkeyModule_StringPtrLen(val, nullptr);
    return VALKEYMODULE_OK;
}

static int ApplyActiveDefragType(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg)
{
    return VALKEYMODULE_OK;
}
// ===========================================================================================================
static ValkeyModuleString *GetEvictionType(const char *unused_arg, void *unused_arg) {

    std::unique_lock lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, eviction_type.c_str(),eviction_type.length());;
}

static int SetEvictionType(const char *unused_arg, ValkeyModuleString *val, void *unused_arg, ValkeyModuleString **unused_arg) {

    std::unique_lock lock(config_mutex);
    eviction_type = ValkeyModule_StringPtrLen(val, nullptr);
    // volatile-lru -> Evict using approximated LRU, only keys with an expire set.
    evict_volatile_lru = (eviction_type.find("volatile-lru") != std::string::npos);
    // allkeys-lru -> Evict any key using approximated LRU.
    evict_allkeys_lru = (eviction_type.find("allkeys-lru") != std::string::npos);
    // volatile-lfu -> Evict using approximated LFU, only keys with an expire set.
    evict_volatile_lfu = (eviction_type.find("volatile-lfu") != std::string::npos);
    // allkeys-lfu -> Evict any key using approximated LFU.
    evict_allkeys_lfu = (eviction_type.find("allkeys-lfu") != std::string::npos);
    // volatile-random -> Remove a random key having an expire set.
    evict_allkeys_lfu = (eviction_type.find("volatile-random") != std::string::npos);
    // allkeys-random -> Remove a random key, any key.
    evict_allkeys_random = (eviction_type.find("volatile-random") != std::string::npos);
    // volatile-ttl -> Remove the key with the nearest expire time (minor TTL)
    evict_volatile_ttl = (eviction_type.find("volatile-ttl") != std::string::npos);
    return VALKEYMODULE_OK;
}

int art::register_valkey_configuration(ValkeyModuleCtx* ctx)
{
    int ret = 0;
    ret |= ValkeyModule_RegisterStringConfig(ctx,  "compression","none",   VALKEYMODULE_CONFIG_DEFAULT, GetCompressionType, SetCompressionType,  ApplyCompressionType, NULL);
    ret |= ValkeyModule_RegisterStringConfig(ctx,  "eviction_policy","none",   VALKEYMODULE_CONFIG_DEFAULT, GetEvictionType, SetEvictionType,  NULL, NULL);
    ret |= ValkeyModule_RegisterStringConfig(ctx,  "max_memory_ratio","0.95",   VALKEYMODULE_CONFIG_DEFAULT, GetMaxMemoryRatio, SetMaxMemoryRatio,  NULL, NULL);
    ret |= ValkeyModule_RegisterStringConfig(ctx,  "min_fragmentation_ratio","0.5",   VALKEYMODULE_CONFIG_DEFAULT, GetMinFragmentation, SetMinFragmentation,  ApplyMinFragmentation, NULL);
    ret |= ValkeyModule_RegisterStringConfig(ctx,  "active_defrag","off",   VALKEYMODULE_CONFIG_DEFAULT, GetActiveDefragType, SetActiveDefragType,  ApplyActiveDefragType, NULL);
    return ret;
}

bool art::get_compression_enabled()
{
    std::unique_lock lock(config_mutex);
    if (compression_type.empty()) return false;
    return compression_type == "zstd";
}
float art::get_module_memory_ratio()
{
    std::unique_lock lock(config_mutex);
    if (max_memory_ratio.empty()) return 0.55;

    return std::stof(max_memory_ratio);

}
float art::get_min_fragmentation_ratio()
{
    std::unique_lock lock(config_mutex);
    if (min_fragmentation_ratio.empty()) return 0.5;

    return std::stof(min_fragmentation_ratio);

}
bool art::get_active_defrag()
{
    std::unique_lock lock(config_mutex);
    if (active_defrag.empty()) return false;
    return active_defrag == "on";
}


bool art::get_evict_volatile_lru()
{
    return evict_volatile_lru;
}
bool art::get_evict_allkeys_lru()
{
    return evict_allkeys_lru;
}
bool art::get_evict_volatile_lfu()
{
    return evict_volatile_lfu;
}
bool art::get_evict_allkeys_lfu()
{
    return evict_allkeys_lfu;
}
bool art::get_evict_volatile_random()
{
    return evict_volatile_random;
};
bool art::get_evict_allkeys_random()
{
    return evict_allkeys_random;
}
bool art::get_evict_volatile_ttl()
{
    return evict_volatile_ttl;
}
