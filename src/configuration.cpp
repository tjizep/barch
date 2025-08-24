//
// Created by linuxlite on 3/13/25.
//

#include "valkeymodule.h"
#include "configuration.h"
#include <cstdlib>
#include <string>
#include <regex>
#include "art.h"
#include "module.h"
#include "server.h"

#define unused_arg
art::configuration_record record;
// these values are kept for reflection
static std::recursive_mutex config_mutex{};
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
static std::string external_host{};
static std::string bind_interface{"127.0.0.1"};
static std::string listen_port{};
static std::string rpc_max_buffer{};
static std::string rpc_client_max_wait_ms{};
static std::string jump_factor{};
static std::string ordered_keys{};
static std::vector<std::string> valid_evictions = {
    "volatile-lru", "allkeys-lru", "volatile-lfu", "allkeys-lfu", "volatile-random", "none", "no", "nil", "null"
};
static std::vector<std::string> valid_compression = {"zstd", "none", "off", "no", "null", "nil"};
static std::vector<std::string> valid_use_vmm_mem = {"on", "true", "off", "yes", "no", "null", "nil", "false"};
static std::vector<std::string> valid_defrag = {"on", "true", "off", "yes", "no", "null", "nil"};
static std::vector<std::string> valid_ordered_keys = {"on", "true", "off", "yes", "no", "null", "nil", "false"};

template<typename VT>
bool check_type(const std::string &et, const VT &valid) {
    return std::any_of(valid.begin(), valid.end(), [&et](const std::string &val) {
        return et.find(val) != std::string::npos;
    });
}

static ValkeyModuleString *GetRPCMaxBuffer(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, max_memory_bytes.c_str(), max_memory_bytes.length());;
}

static int SetRPCMaxBuffer(const std::string& test_rpc_max_buffer) {
    std::regex check("[0-9]+[k,K,m,M,g,G]?");
    if (!std::regex_match(test_rpc_max_buffer, check)) {
        return VALKEYMODULE_ERR;
    }
    std::lock_guard lock(config_mutex);
    rpc_max_buffer = test_rpc_max_buffer;
    char *notn = nullptr;
    const char *str = rpc_max_buffer.c_str();
    const char *end = str + rpc_max_buffer.length();

    uint64_t n_rpc_max_buffer = std::strtoll(str, &notn, 10);
    while (notn != nullptr && notn != end) {
        switch (*notn) {
            case 'k':
            case 'K':
                n_rpc_max_buffer = n_rpc_max_buffer * 1024;
                break;
            case 'm':
            case 'M':
                n_rpc_max_buffer = n_rpc_max_buffer * 1024 * 1024;
                break;
            case 'g':
            case 'G':
                n_rpc_max_buffer = n_rpc_max_buffer * 1024 * 1024 * 1024;
                break;
            default: // just skip other noise
                break;
        }
        ++notn;
    }
    record.rpc_max_buffer = n_rpc_max_buffer;
    return VALKEYMODULE_OK;
}

static int SetRPCMaxBuffer(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                             ValkeyModuleString **unused_arg) {
    std::string test_rpc_max_buffer = ValkeyModule_StringPtrLen(val, nullptr);
    return SetRPCMaxBuffer(test_rpc_max_buffer);
}
static int ApplyRPCMaxBuffer(ValkeyModuleCtx *unused(ctx), void *unused(priv), ValkeyModuleString **unused(vks)) {
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString *GetRPCClientMaxWait(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, max_memory_bytes.c_str(), max_memory_bytes.length());;
}

static int SetRPCClientMaxWait(const std::string& test_rpc_client_max_wait_ms) {
    std::regex check("[0-9]+");
    if (!std::regex_match(test_rpc_client_max_wait_ms, check)) {
        return VALKEYMODULE_ERR;
    }
    std::lock_guard lock(config_mutex);
    rpc_client_max_wait_ms = test_rpc_client_max_wait_ms;
    char *notn = nullptr;
    const char *str = rpc_max_buffer.c_str();
    const char *end = str + rpc_max_buffer.length();

    uint64_t n_rpc_max_client_wait_ms = std::strtoll(str, &notn, 10);
    if (notn != nullptr && notn != end) {
        return VALKEYMODULE_ERR;
    }
    record.rpc_client_max_wait_ms = n_rpc_max_client_wait_ms;
    return VALKEYMODULE_OK;
}

static int SetRPCClientMaxWait(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                             ValkeyModuleString **unused_arg) {
    std::string test_rpc_client_max_wait_ms = ValkeyModule_StringPtrLen(val, nullptr);
    return SetRPCClientMaxWait(test_rpc_client_max_wait_ms);
}
static int ApplyRPCClientMaxWait(ValkeyModuleCtx *unused(ctx), void *unused(priv), ValkeyModuleString **unused(vks)) {
    return VALKEYMODULE_OK;
}


// ===========================================================================================================

static ValkeyModuleString *GetMaxMemoryRatio(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, max_memory_bytes.c_str(), max_memory_bytes.length());;
}

static int SetMaxMemoryBytes(const std::string& test_max_memory_bytes) {
    std::regex check("[0-9]+[k,K,m,M,g,G]?");
    if (!std::regex_match(test_max_memory_bytes, check)) {
        return VALKEYMODULE_ERR;
    }
    std::lock_guard lock(config_mutex);
    max_memory_bytes = test_max_memory_bytes;
    char *notn = nullptr;
    const char *str = max_memory_bytes.c_str();
    const char *end = str + max_memory_bytes.length();

    uint64_t n_max_memory_bytes = std::strtoll(str, &notn, 10);
    while (notn != nullptr && notn != end) {
        switch (*notn) {
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

static int SetMaxMemoryBytes(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                             ValkeyModuleString **unused_arg) {
    std::string test_max_memory_bytes = ValkeyModule_StringPtrLen(val, nullptr);
    return SetMaxMemoryBytes(test_max_memory_bytes);
}
static int ApplyMaxMemoryRatio(ValkeyModuleCtx *unused(ctx), void *unused(priv), ValkeyModuleString **unused(vks)) {
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString *GetUseVMMemory(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, use_vmm_mem.c_str(), use_vmm_mem.length());
}

static int SetUseVMMemory(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                          ValkeyModuleString **unused_arg) {
    std::lock_guard lock(config_mutex);
    std::string test_use_vmm_memory = ValkeyModule_StringPtrLen(val, nullptr);
    std::transform(test_use_vmm_memory.begin(), test_use_vmm_memory.end(), test_use_vmm_memory.begin(),
                   ::tolower);

    if (!check_type(test_use_vmm_memory, valid_use_vmm_mem)) {
        return VALKEYMODULE_ERR;
    }
    use_vmm_mem = test_use_vmm_memory;
    record.use_vmm_memory = (use_vmm_mem == "on" || use_vmm_mem == "true" || use_vmm_mem == "yes");
    return VALKEYMODULE_OK;
}

static int ApplyUseVMMemory(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    //get_art()->get_leaves().set_opt_use_vmm(record.use_vmm_memory);
    //art::get_nodes().set_opt_use_vmm(record.use_vmm_memory);
    return VALKEYMODULE_OK;
}
// ===========================================================================================================
static ValkeyModuleString *GetExternalHost(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, external_host.c_str(), external_host.length());
}

static int SetExternalHost(const std::string& test_external_host) {
    std::lock_guard lock(config_mutex);
    if (test_external_host.empty()) {
        return VALKEYMODULE_ERR;
    }
    external_host = test_external_host;
    record.external_host = external_host;
    return VALKEYMODULE_OK;
}
static int SetExternalHost(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                          ValkeyModuleString **unused_arg) {
    std::string test_external_host = ValkeyModule_StringPtrLen(val, nullptr);
    return SetExternalHost(test_external_host);
}
static int ApplyExternalHost(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    //get_art()->get_leaves().set_opt_use_vmm(record.use_vmm_memory);
    //art::get_nodes().set_opt_use_vmm(record.use_vmm_memory);
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString *GetListenPort(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, listen_port.c_str(), listen_port.length());
}

static int SetListenPort(const std::string& test_listen_port) {
    std::lock_guard lock(config_mutex);
    if (test_listen_port.empty()) {
        return VALKEYMODULE_ERR;
    }
    listen_port = test_listen_port;
    record.listen_port = atoi(external_host.c_str());
    return VALKEYMODULE_OK;
}

static int SetListenPort(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                          ValkeyModuleString **unused_arg) {
    std::string test_listen_port = ValkeyModule_StringPtrLen(val, nullptr);
    return SetListenPort(test_listen_port);
}

static int ApplyListenPort(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    barch::server::stop();
    barch::server::start(record.bind_interface, record.listen_port);
    //get_art()->get_leaves().set_opt_use_vmm(record.use_vmm_memory);
    //art::get_nodes().set_opt_use_vmm(record.use_vmm_memory);
    return VALKEYMODULE_OK;
}
// ===========================================================================================================
static ValkeyModuleString *GetCompressionType(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, compression_type.c_str(), compression_type.length());
}

static int SetCompressionType(const std::string& val) {
    std::lock_guard lock(config_mutex);
    std::string test_compression_type = val;
    std::transform(test_compression_type.begin(), test_compression_type.end(), test_compression_type.begin(),
                   ::tolower);

    if (!check_type(test_compression_type, valid_compression)) {
        return VALKEYMODULE_ERR;
    }
    compression_type = test_compression_type;
    record.compression = (compression_type == "zstd") ? art::compression_zstd : art::compression_none;
    return VALKEYMODULE_OK;
}
static int SetCompressionType(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                              ValkeyModuleString **unused_arg) {
    std::string value = ValkeyModule_StringPtrLen(val, nullptr);
    return SetCompressionType(value);
}
static int ApplyCompressionType(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    //art::get_leaves().set_opt_enable_compression(art::get_compression_enabled());
    //art::get_nodes().set_opt_enable_compression(art::get_compression_enabled());
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString *GetMaxDefragPageCount(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, max_defrag_page_count.c_str(), max_defrag_page_count.length());
}

static int SetMaxDefragPageCount(const std::string& val) {
    std::lock_guard lock(config_mutex);
    std::string test_max_defrag_page_count = val;
    std::regex check("[0-9]+");
    if (!std::regex_match(test_max_defrag_page_count, check)) {
        return VALKEYMODULE_ERR;
    }
    max_defrag_page_count = test_max_defrag_page_count;
    char *ep = nullptr;
    record.max_defrag_page_count = std::strtoll(test_max_defrag_page_count.c_str(), &ep, 10);
    return VALKEYMODULE_OK;
}
static int SetMaxDefragPageCount(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                                 ValkeyModuleString **unused_arg) {
    std::string value = ValkeyModule_StringPtrLen(val, nullptr);
    return SetMaxDefragPageCount(value);
}
static int ApplyMaxDefragPageCount(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString *GetIterationWorkerCount(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, iteration_worker_count.c_str(), iteration_worker_count.length());
}

static int SetIterationWorkerCount(const std::string& test_iteration_worker_count) {
    std::lock_guard lock(config_mutex);
    std::regex check("[0-9]+");
    if (!std::regex_match(test_iteration_worker_count, check)) {
        return VALKEYMODULE_ERR;
    }
    iteration_worker_count = test_iteration_worker_count;
    char *ep = nullptr;
    record.iteration_worker_count = std::strtoll(test_iteration_worker_count.c_str(), &ep, 10);
    return VALKEYMODULE_OK;
}
static int SetIterationWorkerCount(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                                   ValkeyModuleString **unused_arg) {
    std::string test_iteration_worker_count = ValkeyModule_StringPtrLen(val, nullptr);
    return SetIterationWorkerCount(test_iteration_worker_count);
}
static int ApplyIterationWorkerCount(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString *GetSaveInterval(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, save_interval.c_str(), save_interval.length());
}

static int SetSaveInterval(const std::string& test_save_interval) {
    std::lock_guard lock(config_mutex);
    std::regex check("[0-9]+");
    if (!std::regex_match(test_save_interval, check)) {
        return VALKEYMODULE_ERR;
    }
    maintenance_poll_delay = test_save_interval;
    char *ep = nullptr;
    record.save_interval = std::strtoll(test_save_interval.c_str(), &ep, 10);
    return VALKEYMODULE_OK;
}
static int SetSaveInterval(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                           ValkeyModuleString **unused_arg) {
    std::string test_save_interval = ValkeyModule_StringPtrLen(val, nullptr);
    return SetSaveInterval(test_save_interval);
}

static int ApplySaveInterval(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString *GetMaxModificationsBeforeSave(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, max_modifications_before_save.c_str(),
                                     max_modifications_before_save.length());
}

static int SetMaxModificationsBeforeSave(const std::string& test_max_modifications_before_save) {
    std::lock_guard lock(config_mutex);

    std::regex check("[0-9]+");
    if (!std::regex_match(test_max_modifications_before_save, check)) {
        return VALKEYMODULE_ERR;
    }
    maintenance_poll_delay = test_max_modifications_before_save;
    char *ep = nullptr;
    record.max_modifications_before_save = std::strtoll(test_max_modifications_before_save.c_str(), &ep, 10);
    return VALKEYMODULE_OK;
}
static int SetMaxModificationsBeforeSave(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                                         ValkeyModuleString **unused_arg) {
    std::string test_max_modifications_before_save = ValkeyModule_StringPtrLen(val, nullptr);
    return SetMaxModificationsBeforeSave(test_max_modifications_before_save);
}

static int ApplyMaxModificationsBeforeSave(ValkeyModuleCtx *unused_arg, void *unused_arg,
                                           ValkeyModuleString **unused_arg) {
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString *GetMaintenancePollDelay(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, maintenance_poll_delay.c_str(), maintenance_poll_delay.length());
}

static int SetMaintenancePollDelay(const std::string& test_maintenance_poll_delay) {
    std::lock_guard lock(config_mutex);
    std::regex check("[0-9]+");
    if (!std::regex_match(test_maintenance_poll_delay, check)) {
        return VALKEYMODULE_ERR;
    }
    maintenance_poll_delay = test_maintenance_poll_delay;
    char *ep = nullptr;
    record.maintenance_poll_delay = std::strtoll(test_maintenance_poll_delay.c_str(), &ep, 10);
    return VALKEYMODULE_OK;
}

static int SetMaintenancePollDelay(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                                   ValkeyModuleString **unused_arg) {
    std::string test_maintenance_poll_delay = ValkeyModule_StringPtrLen(val, nullptr);
    return SetMaintenancePollDelay(test_maintenance_poll_delay);
}

static int ApplyMaintenancePollDelay(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString *GetMinFragmentation(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, min_fragmentation_ratio.c_str(), min_fragmentation_ratio.length());
}

static int SetMinFragmentation(const std::string& val) {
    std::lock_guard lock(config_mutex);
    min_fragmentation_ratio = val;
    record.min_fragmentation_ratio = std::stof(min_fragmentation_ratio);
    return VALKEYMODULE_OK;
}

static int SetMinFragmentation(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                               ValkeyModuleString **unused_arg) {
    std::string min_fragmentation_ratio = ValkeyModule_StringPtrLen(val, nullptr);
    return SetMinFragmentation(min_fragmentation_ratio);
}

static int ApplyMinFragmentation(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    return VALKEYMODULE_OK;
}
// ===========================================================================================================
static ValkeyModuleString *GetOrderedKeys(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, ordered_keys.c_str(), ordered_keys.length());
}

static int SetOrderedKeys(std::string test_ordered_keys) {
    std::lock_guard lock(config_mutex);
    std::transform(test_ordered_keys.begin(), test_ordered_keys.end(), test_ordered_keys.begin(), ::tolower);

    if (!check_type(test_ordered_keys, valid_ordered_keys)) {
        return VALKEYMODULE_ERR;
    }

    ordered_keys = test_ordered_keys;
    record.ordered_keys =
            ordered_keys == "on" || ordered_keys == "true" || ordered_keys == "yes";

    return VALKEYMODULE_OK;
}
static int SetOrderedKeys(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                               ValkeyModuleString **unused_arg) {
    std::string test_ordered_keys = ValkeyModule_StringPtrLen(val, nullptr);
    return SetOrderedKeys(test_ordered_keys);
}
static int ApplyOrderedKeys(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    for (auto s : art::get_shard_count()) {
        get_art(s)->opt_ordered_keys = record.ordered_keys;
    }
    return VALKEYMODULE_OK;
}


// ===========================================================================================================
static ValkeyModuleString *GetActiveDefragType(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, active_defrag.c_str(), active_defrag.length());
}

static int SetActiveDefragType(std::string test_active_defrag) {
    std::lock_guard lock(config_mutex);
    std::transform(test_active_defrag.begin(), test_active_defrag.end(), test_active_defrag.begin(), ::tolower);

    if (!check_type(test_active_defrag, valid_defrag)) {
        return VALKEYMODULE_ERR;
    }

    active_defrag = test_active_defrag;
    record.active_defrag =
            active_defrag == "on" || active_defrag == "true" || active_defrag == "yes";

    return VALKEYMODULE_OK;
}
static int SetActiveDefragType(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                               ValkeyModuleString **unused_arg) {
    std::string test_active_defrag = ValkeyModule_StringPtrLen(val, nullptr);
    return SetActiveDefragType(test_active_defrag);
}
static int ApplyActiveDefragType(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString *GetEnablePageTrace(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, log_page_access_trace.c_str(), log_page_access_trace.length());
}

static int SetEnablePageTrace(std::string test_log_page_access_trace) {
    std::lock_guard lock(config_mutex);
    std::transform(test_log_page_access_trace.begin(), test_log_page_access_trace.end(),
                   test_log_page_access_trace.begin(), ::tolower);

    if (!check_type(test_log_page_access_trace, valid_defrag)) {
        return VALKEYMODULE_ERR;
    }

    log_page_access_trace = test_log_page_access_trace;
    record.log_page_access_trace =
            log_page_access_trace == "on" || log_page_access_trace == "true" || log_page_access_trace == "yes";

    return VALKEYMODULE_OK;
}
static int SetEnablePageTrace(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                              ValkeyModuleString **unused_arg) {
    std::string test_log_page_access_trace = ValkeyModule_StringPtrLen(val, nullptr);
    return SetEnablePageTrace(test_log_page_access_trace);
}

static int ApplyEnablePageTrace(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    std::lock_guard lock(config_mutex);
    //art::get_leaves().set_opt_trace_page(record.log_page_access_trace);
    //art::get_nodes().set_opt_trace_page(record.log_page_access_trace);
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString *GetEvictionType(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(config_mutex);
    return ValkeyModule_CreateString(nullptr, eviction_type.c_str(), eviction_type.length());;
}


static int SetEvictionType(std::string test_eviction_type) {
    std::lock_guard lock(config_mutex);
    std::transform(test_eviction_type.begin(), test_eviction_type.end(), test_eviction_type.begin(), ::tolower);

    if (!check_type(test_eviction_type, valid_evictions)) {
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

static int SetEvictionType(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                           ValkeyModuleString **unused_arg) {
    std::string test_eviction_type = ValkeyModule_StringPtrLen(val, nullptr);
    return SetEvictionType(test_eviction_type);
}
static int ApplyEvictionType(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    std::lock_guard lock(config_mutex);
    bool lru = (record.evict_volatile_lru || record.evict_allkeys_lru) ;
    bool lfu = (record.evict_volatile_lfu || record.evict_allkeys_lfu) ;
    for (auto shard : art::get_shard_count()) {
        auto t = get_art(shard);
        storage_release r(t->latch);

        t->nodes.set_opt_enable_lfu(lfu);
        t->leaves.set_opt_enable_lfu(lfu);

        t->nodes.set_opt_enable_lru(lru);
        t->leaves.set_opt_enable_lru(lru);
    }
    return VALKEYMODULE_OK;
}

int art::register_valkey_configuration(ValkeyModuleCtx *ctx) {
    int ret = 0;
    ret |= ValkeyModule_RegisterStringConfig(ctx, "compression", "none", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetCompressionType, SetCompressionType, ApplyCompressionType, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "eviction_policy", "none", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetEvictionType, SetEvictionType, ApplyEvictionType, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "max_memory_bytes", "32g", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetMaxMemoryRatio, SetMaxMemoryBytes, ApplyMaxMemoryRatio, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "min_fragmentation_ratio", "0.5", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetMinFragmentation, SetMinFragmentation, ApplyMinFragmentation, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "active_defrag", "on", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetActiveDefragType, SetActiveDefragType, ApplyActiveDefragType, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "maintenance_poll_delay", "40", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetMaintenancePollDelay, SetMaintenancePollDelay,
                                             ApplyMaintenancePollDelay, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "max_defrag_page_count", "10", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetMaxDefragPageCount, SetMaxDefragPageCount, ApplyMaxDefragPageCount,
                                             nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "iteration_worker_count", "2", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetIterationWorkerCount, SetIterationWorkerCount,
                                             ApplyIterationWorkerCount, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "save_interval", "360000", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetSaveInterval, SetSaveInterval,
                                             ApplySaveInterval, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "max_modifications_before_save", "43000000",
                                             VALKEYMODULE_CONFIG_DEFAULT,
                                             GetMaxModificationsBeforeSave, SetMaxModificationsBeforeSave,
                                             ApplyMaxModificationsBeforeSave, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "log_page_access_trace", "no", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetEnablePageTrace, SetEnablePageTrace,
                                             ApplyEnablePageTrace, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "use_vmm_mem", "yes", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetUseVMMemory, SetUseVMMemory,
                                             ApplyUseVMMemory, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "listen_port", "yes", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetListenPort, SetListenPort,
                                             ApplyListenPort, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "external_host", "yes", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetExternalHost, SetExternalHost,
                                             ApplyExternalHost, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "rpc_max_buffer", "262144", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetRPCMaxBuffer, SetRPCMaxBuffer,
                                             ApplyRPCMaxBuffer, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "rpc_client_max_wait_ms", "30000", VALKEYMODULE_CONFIG_DEFAULT,
                                                 GetRPCClientMaxWait, SetRPCClientMaxWait,
                                                 ApplyRPCClientMaxWait, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "ordered_keys", "yes", VALKEYMODULE_CONFIG_DEFAULT,
                                                     GetOrderedKeys, SetOrderedKeys,
                                                     ApplyOrderedKeys, nullptr);

    return ret;
}

int art::set_configuration_value(ValkeyModuleString *Name, ValkeyModuleString *Value) {
    std::string name = ValkeyModule_StringPtrLen(Name, nullptr);
    std::string val = ValkeyModule_StringPtrLen(Value, nullptr);
    return set_configuration_value(name, val);
}
int art::set_configuration_value(const std::string& name, const std::string &val) {
    art::std_log("setting", name, "to", val);

    if (name == "compression") {
        int r = SetCompressionType(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyCompressionType(nullptr, nullptr, nullptr);
        }
        return r;
    } else if (name == "eviction_policy") {
        int r = SetEvictionType(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyEvictionType(nullptr, nullptr, nullptr);
        }
        return r;
    } else if (name == "max_memory_bytes") {
        return SetMaxMemoryBytes(val);
    } else if (name == "min_fragmentation_ratio") {
        return SetMinFragmentation(val);
    } else if (name == "active_defrag") {
        return SetActiveDefragType(val);
    } else if (name == "iteration_worker_count") {
        return SetIterationWorkerCount(val);
    } else if (name == "maintenance_poll_delay") {
        return SetMaintenancePollDelay(val);
    } else if (name == "max_defrag_page_count") {
        return SetMaxDefragPageCount(val);
    } else if (name == "save_interval") {
        return SetSaveInterval(val);
    } else if (name == "max_modifications_before_save") {
        return SetMaxModificationsBeforeSave(val);
    } else if (name == "external_host") {
        return SetExternalHost(val);
    } else if (name == "rpc_max_buffer") {
        return SetRPCMaxBuffer(val);
    } else if (name == "listen_port") {
        auto r = SetListenPort(val);
        if ( VALKEYMODULE_OK == r) {
            return ApplyListenPort(nullptr, nullptr, nullptr);
        }
        return r;
    } else if (name == "log_page_access_trace") {
        auto r = SetEnablePageTrace(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyEnablePageTrace(nullptr, nullptr, nullptr);
        }
        return r;
    } else if (name == "rpc_client_max_wait_ms") {
        auto r = SetRPCClientMaxWait(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyRPCClientMaxWait(nullptr, nullptr, nullptr);
        }
        return r;
    }else if (name == "ordered_keys") {
        auto r = SetOrderedKeys(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyOrderedKeys(nullptr, nullptr, nullptr);
        }
        return r;
    } else {
        return VALKEYMODULE_ERR;
    }
}

bool art::get_compression_enabled() {
    std::lock_guard lock(config_mutex);
    return record.compression == compression_zstd;
}

uint64_t art::get_max_module_memory() {
    return record.n_max_memory_bytes;
}

float art::get_min_fragmentation_ratio() {
    std::lock_guard lock(config_mutex);
    return record.min_fragmentation_ratio;
}

bool art::get_active_defrag() {
    std::lock_guard lock(config_mutex);
    return record.active_defrag;
}


bool art::get_evict_volatile_lru() {
    std::lock_guard lock(config_mutex);
    return record.evict_volatile_lru;
}

bool art::get_evict_allkeys_lru() {
    std::lock_guard lock(config_mutex);
    return record.evict_allkeys_lru;
}

bool art::get_evict_volatile_lfu() {
    std::lock_guard lock(config_mutex);
    return record.evict_volatile_lfu;
}

bool art::get_evict_allkeys_lfu() {
    std::lock_guard lock(config_mutex);
    return record.evict_allkeys_lfu;
}

bool art::get_evict_volatile_random() {
    std::lock_guard lock(config_mutex);
    return record.evict_volatile_random;
};

bool art::get_evict_allkeys_random() {
    std::lock_guard lock(config_mutex);
    return record.evict_allkeys_random;
}

bool art::get_evict_volatile_ttl() {
    std::lock_guard lock(config_mutex);
    return record.evict_volatile_ttl;
}

const art::configuration_record& art::get_configuration() {
    std::lock_guard lock(config_mutex);
    return record;
}

uint64_t art::get_maintenance_poll_delay() {
    std::lock_guard lock(config_mutex);
    return record.max_defrag_page_count;
}

uint64_t art::get_max_defrag_page_count() {
    std::lock_guard lock(config_mutex);
    return record.max_defrag_page_count;
}

unsigned art::get_iteration_worker_count() {
    std::lock_guard lock(config_mutex);
    return record.iteration_worker_count;
}

uint64_t art::get_save_interval() {
    std::lock_guard lock(config_mutex);
    return record.save_interval;
}

uint64_t art::get_max_modifications_before_save() {
    std::lock_guard lock(config_mutex);
    return record.max_modifications_before_save;
}
uint64_t art::get_rpc_max_buffer() {
    return record.rpc_max_buffer;
}

int64_t art::get_rpc_max_client_wait_ms() {
    return record.rpc_client_max_wait_ms;
}

bool art::get_log_page_access_trace() {
    std::lock_guard lock(config_mutex);
    return record.log_page_access_trace;
}

std::chrono::seconds art::get_rpc_connect_to_s() {
    return std::chrono::seconds(record.rpc_connect_to_s);
}
std::chrono::seconds art::get_rpc_read_to_s() {
    return std::chrono::seconds(record.rpc_read_to_s);
}
std::chrono::seconds art::get_rpc_write_to_s() {
    return std::chrono::seconds(record.rpc_write_to_s);
}
bool art::get_use_vmm_memory() {
    std::lock_guard lock(config_mutex);
    return record.use_vmm_memory;
}
bool art::get_ordered_keys() {
    return record.ordered_keys;
}

uint64_t art::get_internal_shards() {
    return record.internal_shards;
}

static std::vector<size_t> init_shard_sizes() {
    std::vector<size_t> r;
    for (size_t s = 0; s < art::get_internal_shards();++s) {
        r.push_back(s);
    }
    return r;
}
static std::vector<size_t> shards = init_shard_sizes();
const std::vector<size_t>& art::get_shard_count() {
    return shards;
}