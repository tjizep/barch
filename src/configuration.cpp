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
#include "rpc/server.h"

#define unused_arg
struct config_state {
    config_state() = default;
    std::recursive_mutex config_mutex{};
    barch::configuration_record record;
    // these values are kept for reflection

    std::string compression_type{};
    std::string eviction_type{};
    std::string max_memory_bytes{};
    std::string min_fragmentation_ratio{};
    std::string max_defrag_page_count{};
    std::string iteration_worker_count{};
    std::string maintenance_poll_delay{};
    std::string active_defrag{};
    std::string log_page_access_trace{};
    std::string save_interval{};
    std::string max_modifications_before_save{};
    std::string use_vmm_mem{};
    std::string external_host{};
    std::string bind_interface{"0.0.0.0"};
    std::string listen_port{};
    std::string rpc_max_buffer{};
    std::string rpc_client_max_wait_ms{};
    std::string jump_factor{};
    std::string ordered_keys{};
    std::string server_port{};
    std::string server_binding{};
    std::vector<std::string> valid_evictions = {
        "volatile-lru", "allkeys-lru", "volatile-lfu", "allkeys-lfu", "volatile-random", "none", "no", "nil", "null"
    };
    std::vector<std::string> valid_compression = {"zstd", "none", "off", "no", "null", "nil"};
    std::vector<std::string> valid_use_vmm_mem = {"on", "true", "off", "yes", "no", "null", "nil", "false"};
    std::vector<std::string> valid_defrag = {"on", "true", "off", "yes", "no", "null", "nil"};
    // we want alloc tests but the db has to be created with alloc tests in the first place
    std::vector<std::string> valid_alloc_tests = {"on", "true", "off", "yes", "no", "null", "nil"};
    std::vector<std::string> valid_ordered_keys = {"on", "true", "off", "yes", "no", "null", "nil", "false"};
};

static config_state& state() {
    static config_state s;
    return s;
}
static barch::configuration_record& config() {
    return state().record;
}
template<typename VT>
bool check_type(const std::string &et, const VT &valid) {
    return std::any_of(valid.begin(), valid.end(), [&et](const std::string &val) {
        return et.find(val) != std::string::npos;
    });
}

static ValkeyModuleString *GetRPCMaxBuffer(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().max_memory_bytes.c_str(), state().max_memory_bytes.length());;
}

static int SetRPCMaxBuffer(const std::string& test_rpc_max_buffer) {
    std::regex check("[0-9]+[k,K,m,M,g,G]?");
    if (!std::regex_match(test_rpc_max_buffer, check)) {
        return VALKEYMODULE_ERR;
    }
    std::lock_guard lock(state().config_mutex);
    state().rpc_max_buffer = test_rpc_max_buffer;
    char *notn = nullptr;
    const char *str = state().rpc_max_buffer.c_str();
    const char *end = str + state().rpc_max_buffer.length();

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
    config().rpc_max_buffer = n_rpc_max_buffer;
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
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().max_memory_bytes.c_str(), state().max_memory_bytes.length());;
}

static int SetRPCClientMaxWait(const std::string& test_rpc_client_max_wait_ms) {
    std::regex check("[0-9]+");
    if (!std::regex_match(test_rpc_client_max_wait_ms, check)) {
        return VALKEYMODULE_ERR;
    }
    std::lock_guard lock(state().config_mutex);
    state().rpc_client_max_wait_ms = test_rpc_client_max_wait_ms;
    char *notn = nullptr;
    const char *str = state().rpc_max_buffer.c_str();
    const char *end = str + state().rpc_max_buffer.length();

    uint64_t n_rpc_max_client_wait_ms = std::strtoll(str, &notn, 10);
    if (notn != nullptr && notn != end) {
        return VALKEYMODULE_ERR;
    }
    config().rpc_client_max_wait_ms = n_rpc_max_client_wait_ms;
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
static ValkeyModuleString *GetServerPort(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().max_memory_bytes.c_str(), state().max_memory_bytes.length());;
}

static int SetServerPort(const std::string& test_server_port) {
    std::regex check("[0-9]+");
    if (!std::regex_match(test_server_port, check)) {
        return VALKEYMODULE_ERR;
    }
    std::lock_guard lock(state().config_mutex);
    state().server_port = test_server_port;
    char *notn = nullptr;
    const char *str = state().server_port.c_str();
    const char *end = str + state().server_port.length();

    uint64_t n_server_port = std::strtoll(str, &notn, 10);
    if (notn != nullptr && notn != end) {
        return VALKEYMODULE_ERR;
    }
    if (n_server_port > 65535) {
        return VALKEYMODULE_ERR;
    }
    config().server_port = n_server_port;
    return VALKEYMODULE_OK;
}

static int SetServerPort(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                             ValkeyModuleString **unused_arg) {
    std::string test_server_port = ValkeyModule_StringPtrLen(val, nullptr);
    return SetServerPort(test_server_port);
}
static int ApplyServerPort(ValkeyModuleCtx *unused(ctx), void *unused(priv), ValkeyModuleString **unused(vks)) {
    barch::server::stop();
    barch::server::start(config().server_binding,config().server_port);
    return VALKEYMODULE_OK;
}


// ===========================================================================================================

static ValkeyModuleString *GetMaxMemoryRatio(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().max_memory_bytes.c_str(), state().max_memory_bytes.length());;
}

static int SetMaxMemoryBytes(const std::string& test_max_memory_bytes) {
    std::regex check("[0-9]+[k,K,m,M,g,G]?");
    if (!std::regex_match(test_max_memory_bytes, check)) {
        return VALKEYMODULE_ERR;
    }
    std::lock_guard lock(state().config_mutex);
    state().max_memory_bytes = test_max_memory_bytes;
    char *notn = nullptr;
    const char *str = state().max_memory_bytes.c_str();
    const char *end = str + state().max_memory_bytes.length();

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
    config().n_max_memory_bytes = n_max_memory_bytes;
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
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().use_vmm_mem.c_str(), state().use_vmm_mem.length());
}

static int SetUseVMMemory(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                          ValkeyModuleString **unused_arg) {
    std::lock_guard lock(state().config_mutex);
    std::string test_use_vmm_memory = ValkeyModule_StringPtrLen(val, nullptr);
    std::transform(test_use_vmm_memory.begin(), test_use_vmm_memory.end(), test_use_vmm_memory.begin(),
                   ::tolower);

    if (!check_type(test_use_vmm_memory, state().valid_use_vmm_mem)) {
        return VALKEYMODULE_ERR;
    }
    state().use_vmm_mem = test_use_vmm_memory;
    config().use_vmm_memory = (state().use_vmm_mem == "on" || state().use_vmm_mem == "true" || state().use_vmm_mem == "yes");
    return VALKEYMODULE_OK;
}

static int ApplyUseVMMemory(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    //get_art()->get_leaves().set_opt_use_vmm(record.use_vmm_memory);
    //art::get_nodes().set_opt_use_vmm(record.use_vmm_memory);
    return VALKEYMODULE_OK;
}
// ===========================================================================================================
static ValkeyModuleString *GetExternalHost(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().external_host.c_str(), state().external_host.length());
}

static int SetExternalHost(const std::string& test_external_host) {
    std::lock_guard lock(state().config_mutex);
    if (test_external_host.empty()) {
        return VALKEYMODULE_ERR;
    }
    state().external_host = test_external_host;
    config().external_host = state().external_host;
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
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().listen_port.c_str(), state().listen_port.length());
}

static int SetListenPort(const std::string& test_listen_port) {
    std::lock_guard lock(state().config_mutex);
    if (test_listen_port.empty()) {
        return VALKEYMODULE_ERR;
    }
    state().listen_port = test_listen_port;
    config().listen_port = atoi(state().external_host.c_str());
    return VALKEYMODULE_OK;
}

static int SetListenPort(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                          ValkeyModuleString **unused_arg) {
    std::string test_listen_port = ValkeyModule_StringPtrLen(val, nullptr);
    return SetListenPort(test_listen_port);
}

static int ApplyListenPort(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    barch::server::stop();
    barch::server::start(config().bind_interface, config().listen_port);
    //get_art()->get_leaves().set_opt_use_vmm(record.use_vmm_memory);
    //art::get_nodes().set_opt_use_vmm(record.use_vmm_memory);
    return VALKEYMODULE_OK;
}
// ===========================================================================================================
static ValkeyModuleString *GetCompressionType(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().compression_type.c_str(), state().compression_type.length());
}

static int SetCompressionType(const std::string& val) {
    std::lock_guard lock(state().config_mutex);
    std::string test_compression_type = val;
    std::transform(test_compression_type.begin(), test_compression_type.end(), test_compression_type.begin(),
                   ::tolower);

    if (!check_type(test_compression_type, state().valid_compression)) {
        return VALKEYMODULE_ERR;
    }
    state().compression_type = test_compression_type;
    config().compression = (state().compression_type == "zstd") ? barch::compression_zstd : barch::compression_none;
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
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().max_defrag_page_count.c_str(), state().max_defrag_page_count.length());
}

static int SetMaxDefragPageCount(const std::string& val) {
    std::lock_guard lock(state().config_mutex);
    std::string test_max_defrag_page_count = val;
    std::regex check("[0-9]+");
    if (!std::regex_match(test_max_defrag_page_count, check)) {
        return VALKEYMODULE_ERR;
    }
    state().max_defrag_page_count = test_max_defrag_page_count;
    char *ep = nullptr;
    config().max_defrag_page_count = std::strtoll(test_max_defrag_page_count.c_str(), &ep, 10);
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
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().iteration_worker_count.c_str(), state().iteration_worker_count.length());
}

static int SetIterationWorkerCount(const std::string& test_iteration_worker_count) {
    std::lock_guard lock(state().config_mutex);
    std::regex check("[0-9]+");
    if (!std::regex_match(test_iteration_worker_count, check)) {
        return VALKEYMODULE_ERR;
    }
    state().iteration_worker_count = test_iteration_worker_count;
    char *ep = nullptr;
    config().iteration_worker_count = std::strtoll(test_iteration_worker_count.c_str(), &ep, 10);
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
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().save_interval.c_str(), state().save_interval.length());
}

static int SetSaveInterval(const std::string& test_save_interval) {
    std::lock_guard lock(state().config_mutex);
    std::regex check("[0-9]+");
    if (!std::regex_match(test_save_interval, check)) {
        return VALKEYMODULE_ERR;
    }
    state().maintenance_poll_delay = test_save_interval;
    char *ep = nullptr;
    config().save_interval = std::strtoll(test_save_interval.c_str(), &ep, 10);
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
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().max_modifications_before_save.c_str(),
                                     state().max_modifications_before_save.length());
}

static int SetMaxModificationsBeforeSave(const std::string& test_max_modifications_before_save) {
    std::lock_guard lock(state().config_mutex);

    std::regex check("[0-9]+");
    if (!std::regex_match(test_max_modifications_before_save, check)) {
        return VALKEYMODULE_ERR;
    }
    state().maintenance_poll_delay = test_max_modifications_before_save;
    char *ep = nullptr;
    config().max_modifications_before_save = std::strtoll(test_max_modifications_before_save.c_str(), &ep, 10);
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
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().maintenance_poll_delay.c_str(), state().maintenance_poll_delay.length());
}

static int SetMaintenancePollDelay(const std::string& test_maintenance_poll_delay) {
    std::lock_guard lock(state().config_mutex);
    std::regex check("[0-9]+");
    if (!std::regex_match(test_maintenance_poll_delay, check)) {
        return VALKEYMODULE_ERR;
    }
    state().maintenance_poll_delay = test_maintenance_poll_delay;
    char *ep = nullptr;
    config().maintenance_poll_delay = std::strtoll(test_maintenance_poll_delay.c_str(), &ep, 10);
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
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().min_fragmentation_ratio.c_str(), state().min_fragmentation_ratio.length());
}

static int SetMinFragmentation(const std::string& val) {
    std::lock_guard lock(state().config_mutex);
    state().min_fragmentation_ratio = val;
    config().min_fragmentation_ratio = std::stof(state().min_fragmentation_ratio);
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
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().ordered_keys.c_str(), state().ordered_keys.length());
}

static int SetOrderedKeys(std::string test_ordered_keys) {
    std::lock_guard lock(state().config_mutex);
    std::transform(test_ordered_keys.begin(), test_ordered_keys.end(), test_ordered_keys.begin(), ::tolower);

    if (!check_type(test_ordered_keys, state().valid_ordered_keys)) {
        return VALKEYMODULE_ERR;
    }

    state().ordered_keys = test_ordered_keys;
    config().ordered_keys =
            state().ordered_keys == "on" || state().ordered_keys == "true" || state().ordered_keys == "yes";
    for (auto s : get_default_ks()->get_shards()) {
        s->opt_ordered_keys = config().ordered_keys;
    }
    return VALKEYMODULE_OK;
}
static int SetOrderedKeys(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                               ValkeyModuleString **unused_arg) {
    std::string test_ordered_keys = ValkeyModule_StringPtrLen(val, nullptr);
    return SetOrderedKeys(test_ordered_keys);
}
static int ApplyOrderedKeys(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    for (auto s : get_default_ks()->get_shards()) {
        s->opt_ordered_keys = config().ordered_keys;
    }
    return VALKEYMODULE_OK;
}


// ===========================================================================================================
static ValkeyModuleString *GetActiveDefragType(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().active_defrag.c_str(), state().active_defrag.length());
}

static int SetActiveDefragType(std::string test_active_defrag) {
    std::lock_guard lock(state().config_mutex);
    std::transform(test_active_defrag.begin(), test_active_defrag.end(), test_active_defrag.begin(), ::tolower);

    if (!check_type(test_active_defrag, state().valid_defrag)) {
        return VALKEYMODULE_ERR;
    }

    state().active_defrag = test_active_defrag;
    config().active_defrag =
            state().active_defrag == "on" || state().active_defrag == "true" || state().active_defrag == "yes";

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
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().log_page_access_trace.c_str(), state().log_page_access_trace.length());
}

static int SetEnablePageTrace(std::string test_log_page_access_trace) {
    std::lock_guard lock(state().config_mutex);
    std::transform(test_log_page_access_trace.begin(), test_log_page_access_trace.end(),
                   test_log_page_access_trace.begin(), ::tolower);

    if (!check_type(test_log_page_access_trace, state().valid_defrag)) {
        return VALKEYMODULE_ERR;
    }

    state().log_page_access_trace = test_log_page_access_trace;
    config().log_page_access_trace =
            state().log_page_access_trace == "on" || state().log_page_access_trace == "true" || state().log_page_access_trace == "yes";

    return VALKEYMODULE_OK;
}
static int SetEnablePageTrace(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                              ValkeyModuleString **unused_arg) {
    std::string test_log_page_access_trace = ValkeyModule_StringPtrLen(val, nullptr);
    return SetEnablePageTrace(test_log_page_access_trace);
}

static int ApplyEnablePageTrace(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    std::lock_guard lock(state().config_mutex);
    //art::get_leaves().set_opt_trace_page(record.log_page_access_trace);
    //art::get_nodes().set_opt_trace_page(record.log_page_access_trace);
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString *GetEvictionType(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().eviction_type.c_str(), state().eviction_type.length());;
}


static int SetEvictionType(std::string test_eviction_type) {
    std::lock_guard lock(state().config_mutex);
    std::transform(test_eviction_type.begin(), test_eviction_type.end(), test_eviction_type.begin(), ::tolower);

    if (!check_type(test_eviction_type, state().valid_evictions)) {
        return VALKEYMODULE_ERR;
    }
    state().eviction_type = test_eviction_type;
    // volatile-lru -> Evict using approximated LRU, only keys with an expire set.
    config().evict_volatile_lru = (state().eviction_type.find("volatile-lru") != std::string::npos);
    // allkeys-lru -> Evict any key using approximated LRU.
    config().evict_allkeys_lru = (state().eviction_type.find("allkeys-lru") != std::string::npos);
    // volatile-lfu -> Evict using approximated LFU, only keys with an expire set.
    config().evict_volatile_lfu = (state().eviction_type.find("volatile-lfu") != std::string::npos);
    // allkeys-lfu -> Evict any key using approximated LFU.
    config().evict_allkeys_lfu = (state().eviction_type.find("allkeys-lfu") != std::string::npos);
    // volatile-random -> Remove a random key having an expire set.
    config().evict_volatile_random = (state().eviction_type.find("volatile-random") != std::string::npos);
    // allkeys-random -> Remove a random key, any key.
    config().evict_allkeys_random = (state().eviction_type.find("volatile-random") != std::string::npos);
    // volatile-ttl -> Remove the key with the nearest expire time (minor TTL)
    config().evict_volatile_ttl = (state().eviction_type.find("volatile-ttl") != std::string::npos);
    return VALKEYMODULE_OK;
}

static int SetEvictionType(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                           ValkeyModuleString **unused_arg) {
    std::string test_eviction_type = ValkeyModule_StringPtrLen(val, nullptr);
    return SetEvictionType(test_eviction_type);
}
static int ApplyEvictionType(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    std::lock_guard lock(state().config_mutex);
    bool lfu = (config().evict_volatile_lfu || config().evict_allkeys_lfu) ;
    for (auto t : get_default_ks()->get_shards()) {
        storage_release r(t);

        t->get_ap().get_nodes().set_opt_enable_lfu(lfu);
        t->get_ap().get_leaves().set_opt_enable_lfu(lfu);
        t->opt_evict_all_keys_lru = config().evict_allkeys_lru;
        t->opt_evict_volatile_keys_lru = config().evict_volatile_lru;
        t->opt_evict_all_keys_lfu = config().evict_allkeys_lfu;
        t->opt_evict_volatile_keys_lfu = config().evict_volatile_lfu;
        t->opt_evict_all_keys_random = config().evict_allkeys_random;
    }
    return VALKEYMODULE_OK;
}

int barch::register_valkey_configuration(ValkeyModuleCtx *ctx) {
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

    ret |= ValkeyModule_RegisterStringConfig(ctx, "maintenance_poll_delay", "140", VALKEYMODULE_CONFIG_DEFAULT,
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

    ret |= ValkeyModule_RegisterStringConfig(ctx, "server_port", "14000", VALKEYMODULE_CONFIG_DEFAULT,
                                                     GetServerPort, SetServerPort,
                                                     ApplyServerPort, nullptr);

    return ret;
}

int barch::set_configuration_value(ValkeyModuleString *Name, ValkeyModuleString *Value) {
    std::string name = ValkeyModule_StringPtrLen(Name, nullptr);
    std::string val = ValkeyModule_StringPtrLen(Value, nullptr);
    return set_configuration_value(name, val);
}
int barch::set_configuration_value(const std::string& name, const std::string &val) {
    barch::std_log("setting", name, "to", val);

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
    }else if (name == "server_port") {
        auto r = SetServerPort(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyServerPort(nullptr, nullptr, nullptr);
        }
        return r;
    } else {
        return VALKEYMODULE_ERR;
    }
}

bool barch::get_compression_enabled() {
    std::lock_guard lock(state().config_mutex);
    return config().compression == compression_zstd;
}

uint64_t barch::get_max_module_memory() {
    return config().n_max_memory_bytes;
}

float barch::get_min_fragmentation_ratio() {
    std::lock_guard lock(state().config_mutex);
    return config().min_fragmentation_ratio;
}

bool barch::get_active_defrag() {
    std::lock_guard lock(state().config_mutex);
    return config().active_defrag;
}


bool barch::get_evict_volatile_lru() {
    std::lock_guard lock(state().config_mutex);
    return config().evict_volatile_lru;
}

bool barch::get_evict_allkeys_lru() {
    std::lock_guard lock(state().config_mutex);
    return config().evict_allkeys_lru;
}

bool barch::get_evict_volatile_lfu() {
    std::lock_guard lock(state().config_mutex);
    return config().evict_volatile_lfu;
}

bool barch::get_evict_allkeys_lfu() {
    std::lock_guard lock(state().config_mutex);
    return config().evict_allkeys_lfu;
}

bool barch::get_evict_volatile_random() {
    std::lock_guard lock(state().config_mutex);
    return config().evict_volatile_random;
};

bool barch::get_evict_allkeys_random() {
    std::lock_guard lock(state().config_mutex);
    return config().evict_allkeys_random;
}

bool barch::get_evict_volatile_ttl() {
    std::lock_guard lock(state().config_mutex);
    return config().evict_volatile_ttl;
}

const barch::configuration_record& barch::get_configuration() {
    std::lock_guard lock(state().config_mutex);
    return state().record;
}

uint64_t barch::get_maintenance_poll_delay() {
    std::lock_guard lock(state().config_mutex);
    return config().maintenance_poll_delay;
}

uint64_t barch::get_max_defrag_page_count() {
    std::lock_guard lock(state().config_mutex);
    return config().max_defrag_page_count;
}

unsigned barch::get_iteration_worker_count() {
    std::lock_guard lock(state().config_mutex);
    return config().iteration_worker_count;
}

uint64_t barch::get_save_interval() {
    std::lock_guard lock(state().config_mutex);
    return config().save_interval;
}

uint64_t barch::get_max_modifications_before_save() {
    std::lock_guard lock(state().config_mutex);
    return config().max_modifications_before_save;
}
uint64_t barch::get_rpc_max_buffer() {
    return config().rpc_max_buffer;
}

int64_t barch::get_rpc_max_client_wait_ms() {
    return config().rpc_client_max_wait_ms;
}

bool barch::get_log_page_access_trace() {
    std::lock_guard lock(state().config_mutex);
    return config().log_page_access_trace;
}

std::chrono::seconds barch::get_rpc_connect_to_s() {
    return std::chrono::seconds(config().rpc_connect_to_s);
}
std::chrono::seconds barch::get_rpc_read_to_s() {
    return std::chrono::seconds(config().rpc_read_to_s);
}
std::chrono::seconds barch::get_rpc_write_to_s() {
    return std::chrono::seconds(config().rpc_write_to_s);
}
bool barch::get_use_vmm_memory() {
    std::lock_guard lock(state().config_mutex);
    return config().use_vmm_memory;
}
bool barch::get_ordered_keys() {
    return config().ordered_keys;
}

uint64_t barch::get_internal_shards() {
    return config().internal_shards;
}

uint64_t barch::get_server_port() {
    std::lock_guard lock(state().config_mutex);
    return config().server_port;
}

static std::vector<size_t> init_shard_sizes() {
    std::vector<size_t> r;
    for (size_t s = 0; s < barch::get_internal_shards();++s) {
        r.push_back(s);
    }
    return r;
}
static std::vector<size_t> shards = init_shard_sizes();
const std::vector<size_t>& barch::get_shard_count() {
    return shards;
}