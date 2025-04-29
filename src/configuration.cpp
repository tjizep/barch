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
art::configuration_record record;
// these values are kept for reflection
static std::mutex config_mutex{};
static std::string compression_type{};
static std::string eviction_type{};
static std::string max_memory_bytes{};
static std::string min_fragmentation_ratio{};
static std::string max_defrag_page_count{};
static std::string iteration_worker_count{};
static std::string maintenance_poll_delay{};
static std::string active_defrag{};
static std::string log_page_access_trace{};
static std::string save_interval{};
static std::string max_modifications_before_save{};
static std::string use_vmm_mem{};

static std::vector<std::string> valid_evictions = {
    "volatile-lru", "allkeys-lru", "volatile-lfu", "allkeys-lfu", "volatile-random", "none", "no", "nil", "null"
};
static std::vector<std::string> valid_compression = {"zstd", "none", "off", "no", "null", "nil"};
static std::vector<std::string> valid_use_vmm_mem = {"on", "true", "off", "yes", "no", "null", "nil", "false"};
static std::vector<std::string> valid_defrag = {"on", "true", "off", "yes", "no", "null", "nil"};
template<typename VT>
bool check_type(const std::string& et, const VT& valid)
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
static ValkeyModuleString* GetUseVMMemory(const char*unused_arg, void*unused_arg)
{
    std::unique_lock lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, use_vmm_mem.c_str(), use_vmm_mem.length());
}

static int SetUseVMMemory(const char*unused_arg, ValkeyModuleString* val, void*unused_arg,
                              ValkeyModuleString**unused_arg)
{
    std::unique_lock lock(config_mutex);
    std::string test_use_vmm_memory = ValkeyModule_StringPtrLen(val, nullptr);
    std::transform(test_use_vmm_memory.begin(), test_use_vmm_memory.end(), test_use_vmm_memory.begin(),
                   ::tolower);

    if (!check_type(test_use_vmm_memory, valid_use_vmm_mem))
    {
        return VALKEYMODULE_ERR;
    }
    use_vmm_mem = test_use_vmm_memory;
    record.use_vmm_memory = (use_vmm_mem == "on" || use_vmm_mem == "true" || use_vmm_mem == "yes");
    return VALKEYMODULE_OK;
}

static int ApplyUseVMMemory(ValkeyModuleCtx*unused_arg, void*unused_arg, ValkeyModuleString**unused_arg)
{
    art::get_leaf_compression().set_opt_use_vmm(record.use_vmm_memory);
    art::get_node_compression().set_opt_use_vmm(record.use_vmm_memory);
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
static ValkeyModuleString* GetMaxDefragPageCount(const char*unused_arg, void*unused_arg)
{
    std::unique_lock lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, max_defrag_page_count.c_str(), max_defrag_page_count.length());
}

static int SetMaxDefragPageCount(const char*unused_arg, ValkeyModuleString* val, void*unused_arg,
                                 ValkeyModuleString**unused_arg)
{
    std::unique_lock lock(config_mutex);
    std::string test_max_defrag_page_count = ValkeyModule_StringPtrLen(val, nullptr);
    std::regex check("[0-9]+");
    if (!std::regex_match(test_max_defrag_page_count, check))
    {
        return VALKEYMODULE_ERR;
    }
    max_defrag_page_count = test_max_defrag_page_count;
    char* ep = nullptr;
    record.max_defrag_page_count = std::strtoll(test_max_defrag_page_count.c_str(), &ep, 10);
    return VALKEYMODULE_OK;
}

static int ApplyMaxDefragPageCount(ValkeyModuleCtx*unused_arg, void*unused_arg, ValkeyModuleString**unused_arg)
{
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString* GetIterationWorkerCount(const char*unused_arg, void*unused_arg)
{
    std::unique_lock lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, iteration_worker_count.c_str(), iteration_worker_count.length());
}

static int SetIterationWorkerCount(const char*unused_arg, ValkeyModuleString* val, void*unused_arg,
                                   ValkeyModuleString**unused_arg)
{
    std::unique_lock lock(config_mutex);
    std::string test_iteration_worker_count = ValkeyModule_StringPtrLen(val, nullptr);
    std::regex check("[0-9]+");
    if (!std::regex_match(test_iteration_worker_count, check))
    {
        return VALKEYMODULE_ERR;
    }
    iteration_worker_count = test_iteration_worker_count;
    char* ep = nullptr;
    record.iteration_worker_count = std::strtoll(test_iteration_worker_count.c_str(), &ep, 10);
    return VALKEYMODULE_OK;
}

static int ApplyIterationWorkerCount(ValkeyModuleCtx*unused_arg, void*unused_arg, ValkeyModuleString**unused_arg)
{
    return VALKEYMODULE_OK;
}
// ===========================================================================================================
static ValkeyModuleString* GetSaveInterval(const char*unused_arg, void*unused_arg)
{
    std::unique_lock lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, save_interval.c_str(), save_interval.length());
}

static int SetSaveInterval(const char*unused_arg, ValkeyModuleString* val, void*unused_arg,
                                   ValkeyModuleString**unused_arg)
{
    std::unique_lock lock(config_mutex);
    std::string test_save_interval = ValkeyModule_StringPtrLen(val, nullptr);
    std::regex check("[0-9]+");
    if (!std::regex_match(test_save_interval, check))
    {
        return VALKEYMODULE_ERR;
    }
    maintenance_poll_delay = test_save_interval;
    char* ep = nullptr;
    record.save_interval = std::strtoll(test_save_interval.c_str(), &ep, 10);
    return VALKEYMODULE_OK;
}

static int ApplySaveInterval(ValkeyModuleCtx*unused_arg, void*unused_arg, ValkeyModuleString**unused_arg)
{
    return VALKEYMODULE_OK;
}
// ===========================================================================================================
static ValkeyModuleString* GetMaxModificationsBeforeSave(const char*unused_arg, void*unused_arg)
{
    std::unique_lock lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, max_modifications_before_save.c_str(), max_modifications_before_save.length());
}

static int SetMaxModificationsBeforeSave(const char*unused_arg, ValkeyModuleString* val, void*unused_arg,
                                   ValkeyModuleString**unused_arg)
{
    std::unique_lock lock(config_mutex);
    std::string test_max_modifications_before_save = ValkeyModule_StringPtrLen(val, nullptr);
    std::regex check("[0-9]+");
    if (!std::regex_match(test_max_modifications_before_save, check))
    {
        return VALKEYMODULE_ERR;
    }
    maintenance_poll_delay = test_max_modifications_before_save;
    char* ep = nullptr;
    record.max_modifications_before_save = std::strtoll(test_max_modifications_before_save.c_str(), &ep, 10);
    return VALKEYMODULE_OK;
}

static int ApplyMaxModificationsBeforeSave(ValkeyModuleCtx*unused_arg, void*unused_arg, ValkeyModuleString**unused_arg)
{
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString* GetMaintenancePollDelay(const char*unused_arg, void*unused_arg)
{
    std::unique_lock lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, maintenance_poll_delay.c_str(), maintenance_poll_delay.length());
}

static int SetMaintenancePollDelay(const char*unused_arg, ValkeyModuleString* val, void*unused_arg,
                                   ValkeyModuleString**unused_arg)
{
    std::unique_lock lock(config_mutex);
    std::string test_maintenance_poll_delay = ValkeyModule_StringPtrLen(val, nullptr);
    std::regex check("[0-9]+");
    if (!std::regex_match(test_maintenance_poll_delay, check))
    {
        return VALKEYMODULE_ERR;
    }
    maintenance_poll_delay = test_maintenance_poll_delay;
    char* ep = nullptr;
    record.maintenance_poll_delay = std::strtoll(test_maintenance_poll_delay.c_str(), &ep, 10);
    return VALKEYMODULE_OK;
}

static int ApplyMaintenancePollDelay(ValkeyModuleCtx*unused_arg, void*unused_arg, ValkeyModuleString**unused_arg)
{
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
static ValkeyModuleString* GetEnablePageTrace(const char*unused_arg, void*unused_arg)
{
    std::unique_lock lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, log_page_access_trace.c_str(), log_page_access_trace.length());
}

static int SetEnablePageTrace(const char*unused_arg, ValkeyModuleString* val, void*unused_arg,
                               ValkeyModuleString**unused_arg)
{
    std::unique_lock lock(config_mutex);
    std::string test_log_page_access_trace = ValkeyModule_StringPtrLen(val, nullptr);
    std::transform(test_log_page_access_trace.begin(), test_log_page_access_trace.end(), test_log_page_access_trace.begin(), ::tolower);

    if (!check_type(test_log_page_access_trace, valid_defrag))
    {
        return VALKEYMODULE_ERR;
    }

    log_page_access_trace = test_log_page_access_trace;
    record.log_page_access_trace =
        log_page_access_trace == "on" || log_page_access_trace == "true" || log_page_access_trace == "yes";

    return VALKEYMODULE_OK;
}

static int ApplyEnablePageTrace(ValkeyModuleCtx*unused_arg, void*unused_arg, ValkeyModuleString**unused_arg)
{
    std::unique_lock lock(config_mutex);
    art::get_leaf_compression().set_opt_trace_page(record.log_page_access_trace);
    art::get_node_compression().set_opt_trace_page(record.log_page_access_trace);
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
    ret |= ValkeyModule_RegisterStringConfig(ctx, "maintenance_poll_delay", "10", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetMaintenancePollDelay, SetMaintenancePollDelay,
                                             ApplyMaintenancePollDelay, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "max_defrag_page_count", "10", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetMaxDefragPageCount, SetMaxDefragPageCount, ApplyMaxDefragPageCount,
                                             nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "iteration_worker_count", "2", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetIterationWorkerCount, SetIterationWorkerCount,
                                             ApplyIterationWorkerCount, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "save_interval", "3600000", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetSaveInterval, SetSaveInterval,
                                             ApplySaveInterval, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "max_modifications_before_save", "13000000", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetMaxModificationsBeforeSave, SetMaxModificationsBeforeSave,
                                             ApplyMaxModificationsBeforeSave, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "log_page_access_trace", "no", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetEnablePageTrace, SetEnablePageTrace,
                                             ApplyEnablePageTrace, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "use_vmm_mem", "false", VALKEYMODULE_CONFIG_DEFAULT,
                                         GetUseVMMemory, SetUseVMMemory,
                                         ApplyUseVMMemory, nullptr);
    return ret;
}

int art::set_configuration_value(ValkeyModuleString* Name, ValkeyModuleString* Value)
{
    std::string name = ValkeyModule_StringPtrLen(Name, nullptr);
    std::string val = ValkeyModule_StringPtrLen(Value, nullptr);
    art::std_log("setting",name,"to",val);

    if (name == "compression")
    {
        int r = SetCompressionType(nullptr, Value, nullptr, nullptr);
        if (r == VALKEYMODULE_OK)
        {
            return ApplyCompressionType(nullptr, nullptr, nullptr);
        }
        return r;
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
    else if (name == "iteration_worker_count")
    {
        return SetIterationWorkerCount(nullptr, Value, nullptr, nullptr);
    }
    else if (name == "maintenance_poll_delay")
    {
        return SetMaintenancePollDelay(nullptr, Value, nullptr, nullptr);
    }  else if (name == "max_defrag_page_count")
    {
        return SetMaxDefragPageCount(nullptr, Value, nullptr, nullptr);
    }
    else if (name == "save_interval")
    {
        return SetSaveInterval(nullptr, Value, nullptr, nullptr);
    }
    else if (name == "max_modifications_before_save")
    {
        return SetMaxModificationsBeforeSave(nullptr, Value, nullptr, nullptr);
    }
    else if (name == "log_page_access_trace")
    {
        auto r = SetEnablePageTrace(nullptr, Value, nullptr, nullptr);
        if (r == VALKEYMODULE_OK)
        {
            return ApplyEnablePageTrace(nullptr, nullptr, nullptr);
        }
        return r;
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

uint64_t art::get_maintenance_poll_delay()
{
    std::unique_lock lock(config_mutex);
    return record.max_defrag_page_count;
}

uint64_t art::get_max_defrag_page_count()
{
    std::unique_lock lock(config_mutex);
    return record.max_defrag_page_count;
}

unsigned art::get_iteration_worker_count()
{
    std::unique_lock lock(config_mutex);
    return record.iteration_worker_count;
}
uint64_t art::get_save_interval()
{
    std::unique_lock lock(config_mutex);
    return record.save_interval;
}
uint64_t art::get_max_modifications_before_save()
{
    std::unique_lock lock(config_mutex);
    return record.max_modifications_before_save;
}
bool art::get_log_page_access_trace()
{
    std::unique_lock lock(config_mutex);
    return record.log_page_access_trace;
}
bool art::get_use_vmm_memory() {
    std::unique_lock lock(config_mutex);
    return record.use_vmm_memory;
}