//
// Created by linuxlite on 3/13/25.
//

#include "valkeymodule.h"
#include "configuration.h"
#include <cstdlib>
#include <string>
#include <regex>
#include "art.h"

#define unused_arg
static std::mutex config_mutex{};
static std::string compression_type{};
static std::string eviction_type{};
static std::string max_memory_bytes{};
static std::string min_fragmentation_ratio{};
static std::string active_defrag{};

art::configuration_record record;
static std::vector<std::string> valid_evictions = {
    "volatile-lru", "allkeys-lru", "volatile-lfu", "allkeys-lfu", "volatile-random", "none", "no", "nil", "null"
};
static std::vector<std::string> valid_compression = {"zstd", "none", "off", "no", "null", "nil"};
static std::vector<std::string> valid_defrag = {"on", "true", "off", "yes", "no", "null", "nil"};

bool check_type(const std::string& et, const std::vector<std::string>& valid)
{
    return std::any_of(valid.begin(), valid.end(), [&et](const std::string& val)
    {
        return et.find(val) != std::string::npos;
    });
}

static ValkeyModuleString* GetMaxMemoryRatio(const char*unused_arg, void*unused_arg)
{
    std::unique_lock lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, max_memory_bytes.c_str(), max_memory_bytes.length());;
}

static int SetMaxMemoryBytes(const char*unused_arg, ValkeyModuleString* val, void*unused_arg,
                             ValkeyModuleString**unused_arg)
{
    std::string test_max_memory_bytes = ValkeyModule_StringPtrLen(val, nullptr);
    std::regex check("[0-9]+[k,K,m,M,g,G]?");
    if (!std::regex_match(test_max_memory_bytes, check))
    {
        return VALKEYMODULE_ERR;
    }
    std::unique_lock lock(config_mutex);
    max_memory_bytes = test_max_memory_bytes;
    char* notn = nullptr;
    const char* str = max_memory_bytes.c_str();
    const char* end = str + max_memory_bytes.length();

    uint64_t n_max_memory_bytes = std::strtoll(str, &notn, 10);
    while (notn != nullptr && notn != end)
    {
        switch (*notn)
        {
        case 'k':
        case 'K':
            n_max_memory_bytes = n_max_memory_bytes * 1024;
            break;
        case 'm':
        case 'M':
            n_max_memory_bytes = n_max_memory_bytes * 1024 * 1024;
            break;
        case 'g':
        case 'G':
            n_max_memory_bytes = n_max_memory_bytes * 1024 * 1024 * 1024;
            break;
        default: // just skip other noise
            break;
        }
        ++notn;
    }
    record.n_max_memory_bytes = n_max_memory_bytes;
    return VALKEYMODULE_OK;
}

static int ApplyMaxMemoryRatio(ValkeyModuleCtx*unused(ctx), void*unused(priv), ValkeyModuleString**unused(vks))
{
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString* GetCompressionType(const char*unused_arg, void*unused_arg)
{
    std::unique_lock lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, compression_type.c_str(), compression_type.length());
}

static int SetCompressionType(const char*unused_arg, ValkeyModuleString* val, void*unused_arg,
                              ValkeyModuleString**unused_arg)
{
    std::unique_lock lock(config_mutex);
    std::string test_compression_type = ValkeyModule_StringPtrLen(val, nullptr);
    std::transform(test_compression_type.begin(), test_compression_type.end(), test_compression_type.begin(),
                   ::tolower);

    if (!check_type(test_compression_type, valid_compression))
    {
        return VALKEYMODULE_ERR;
    }
    compression_type = test_compression_type;
    record.compression = (compression_type == "zstd") ? art::compression_zstd : art::compression_none;
    return VALKEYMODULE_OK;
}

static int ApplyCompressionType(ValkeyModuleCtx*unused_arg, void*unused_arg, ValkeyModuleString**unused_arg)
{
    art::get_leaf_compression().set_opt_enable_compression(art::get_compression_enabled());
    art::get_node_compression().set_opt_enable_compression(art::get_compression_enabled());
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString* GetMinFragmentation(const char*unused_arg, void*unused_arg)
{
    std::unique_lock lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, min_fragmentation_ratio.c_str(), min_fragmentation_ratio.length());
}

static int SetMinFragmentation(const char*unused_arg, ValkeyModuleString* val, void*unused_arg,
                               ValkeyModuleString**unused_arg)
{
    std::unique_lock lock(config_mutex);
    min_fragmentation_ratio = ValkeyModule_StringPtrLen(val, nullptr);
    record.min_fragmentation_ratio = std::stof(min_fragmentation_ratio);
    return VALKEYMODULE_OK;
}

static int ApplyMinFragmentation(ValkeyModuleCtx*unused_arg, void*unused_arg, ValkeyModuleString**unused_arg)
{
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString* GetActiveDefragType(const char*unused_arg, void*unused_arg)
{
    std::unique_lock lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, active_defrag.c_str(), active_defrag.length());
}

static int SetActiveDefragType(const char*unused_arg, ValkeyModuleString* val, void*unused_arg,
                               ValkeyModuleString**unused_arg)
{
    std::unique_lock lock(config_mutex);
    std::string test_active_defrag = ValkeyModule_StringPtrLen(val, nullptr);
    std::transform(test_active_defrag.begin(), test_active_defrag.end(), test_active_defrag.begin(), ::tolower);

    if (!check_type(test_active_defrag, valid_defrag))
    {
        return VALKEYMODULE_ERR;
    }

    active_defrag = test_active_defrag;
    record.active_defrag =
        active_defrag == "on" || active_defrag == "true" || active_defrag == "yes";

    return VALKEYMODULE_OK;
}

static int ApplyActiveDefragType(ValkeyModuleCtx*unused_arg, void*unused_arg, ValkeyModuleString**unused_arg)
{
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString* GetEvictionType(const char*unused_arg, void*unused_arg)
{
    std::unique_lock lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, eviction_type.c_str(), eviction_type.length());;
}


static int SetEvictionType(const char*unused_arg, ValkeyModuleString* val, void*unused_arg,
                           ValkeyModuleString**unused_arg)
{
    std::unique_lock lock(config_mutex);
    std::string test_eviction_type = ValkeyModule_StringPtrLen(val, nullptr);
    std::transform(test_eviction_type.begin(), test_eviction_type.end(), test_eviction_type.begin(), ::tolower);

    if (!check_type(test_eviction_type, valid_evictions))
    {
        return VALKEYMODULE_ERR;
    }
    eviction_type = test_eviction_type;
    // volatile-lru -> Evict using approximated LRU, only keys with an expire set.
    record.evict_volatile_lru = (eviction_type.find("volatile-lru") != std::string::npos);
    // allkeys-lru -> Evict any key using approximated LRU.
    record.evict_allkeys_lru = (eviction_type.find("allkeys-lru") != std::string::npos);
    // volatile-lfu -> Evict using approximated LFU, only keys with an expire set.
    record.evict_volatile_lfu = (eviction_type.find("volatile-lfu") != std::string::npos);
    // allkeys-lfu -> Evict any key using approximated LFU.
    record.evict_allkeys_lfu = (eviction_type.find("allkeys-lfu") != std::string::npos);
    // volatile-random -> Remove a random key having an expire set.
    record.evict_volatile_random = (eviction_type.find("volatile-random") != std::string::npos);
    // allkeys-random -> Remove a random key, any key.
    record.evict_allkeys_random = (eviction_type.find("volatile-random") != std::string::npos);
    // volatile-ttl -> Remove the key with the nearest expire time (minor TTL)
    record.evict_volatile_ttl = (eviction_type.find("volatile-ttl") != std::string::npos);
    return VALKEYMODULE_OK;
}

int art::register_valkey_configuration(ValkeyModuleCtx* ctx)
{
    int ret = 0;
    ret |= ValkeyModule_RegisterStringConfig(ctx, "compression", "none", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetCompressionType, SetCompressionType, ApplyCompressionType, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "eviction_policy", "none", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetEvictionType, SetEvictionType, nullptr, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "max_memory_bytes", "1024g", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetMaxMemoryRatio, SetMaxMemoryBytes, ApplyMaxMemoryRatio, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "min_fragmentation_ratio", "0.5", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetMinFragmentation, SetMinFragmentation, ApplyMinFragmentation, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "active_defrag", "off", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetActiveDefragType, SetActiveDefragType, ApplyActiveDefragType, nullptr);
    return ret;
}

int art::set_configuration_value(ValkeyModuleString* Name, ValkeyModuleString* Value)
{
    std::string name = ValkeyModule_StringPtrLen(Name, nullptr);
    if (name == "compression")
    {
        return SetCompressionType(nullptr, Value, nullptr, nullptr);
    }
    else if (name == "eviction_policy")
    {
        return SetEvictionType(nullptr, Value, nullptr, nullptr);
    }
    else if (name == "max_memory_bytes")
    {
        return SetMaxMemoryBytes(nullptr, Value, nullptr, nullptr);
    }
    else if (name == "min_fragmentation_ratio")
    {
        return SetMinFragmentation(nullptr, Value, nullptr, nullptr);
    }
    else if (name == "active_defrag")
    {
        return SetActiveDefragType(nullptr, Value, nullptr, nullptr);
    }
    else
    {
        return VALKEYMODULE_ERR;
    }
}

bool art::get_compression_enabled()
{
    std::unique_lock lock(config_mutex);
    return record.compression == compression_zstd;
}

uint64_t art::get_max_module_memory()
{
    std::unique_lock lock(config_mutex);
    return record.n_max_memory_bytes;
}

float art::get_min_fragmentation_ratio()
{
    std::unique_lock lock(config_mutex);
    return record.min_fragmentation_ratio;
}

bool art::get_active_defrag()
{
    std::unique_lock lock(config_mutex);
    return record.active_defrag;
}


bool art::get_evict_volatile_lru()
{
    std::unique_lock lock(config_mutex);
    return record.evict_volatile_lru;
}

bool art::get_evict_allkeys_lru()
{
    std::unique_lock lock(config_mutex);
    return record.evict_allkeys_lru;
}

bool art::get_evict_volatile_lfu()
{
    std::unique_lock lock(config_mutex);
    return record.evict_volatile_lfu;
}

bool art::get_evict_allkeys_lfu()
{
    std::unique_lock lock(config_mutex);
    return record.evict_allkeys_lfu;
}

bool art::get_evict_volatile_random()
{
    std::unique_lock lock(config_mutex);
    return record.evict_volatile_random;
};

bool art::get_evict_allkeys_random()
{
    std::unique_lock lock(config_mutex);
    return record.evict_allkeys_random;
}

bool art::get_evict_volatile_ttl()
{
    std::unique_lock lock(config_mutex);
    return record.evict_volatile_ttl;
}

art::configuration_record art::get_configuration()
{
    std::unique_lock lock(config_mutex);
    return record;
}
