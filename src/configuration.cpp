//
// Created by linuxlite on 3/13/25.
//

#include "../external/include/valkeymodule.h"
#include "configuration.h"
#include "sharded_store.h"
#include <cstdlib>
#include <algorithm>
#include <limits>
#include <string>
#include <regex>
#include "art/art.h"
#include "module.h"
#include "rpc/server.h"
#include "rpc/restarter.h"
#include "function_sync.h"
#define unused_arg
static bool is_on(const std::string& val) {
    return (val == "on" || val == "true" || val == "yes");
}

static bool is_float(const std::string& buffer) {
    if (buffer.empty()) return false;
    char* ptr = nullptr;
    const char * cdata = buffer.c_str();
    const char * ed = cdata + buffer.length();
    //auto dv =
        std::strtod(cdata, &ptr);
    if (ptr != ed) return false;
    return true;
}



struct config_state {
    config_state() = default;
    std::recursive_mutex config_mutex{};
    barch::configuration_record record;
    // these values are kept for reflection

    heap::string compression_type{"none"};
    heap::string min_compressed_size{};
    heap::string eviction_type{"none"};
    // what SELECT <n> puts in front of the number to name the key space it selects. See
    // the SELECT note in the docs: barch has named key spaces where redis has numbered
    // databases, so a number has to become a name somehow, and which name is a choice
    heap::string db_number_prefix{"db"};
    heap::string max_memory_bytes{};
    heap::string max_resp_connections{"2000"};
    heap::string min_fragmentation_ratio{};
    heap::string pre_evict_thresh{};
    heap::string max_defrag_page_count{};
    heap::string max_scan_iterators{};
    heap::string iteration_worker_count{};
    heap::string maintenance_poll_delay{};
    heap::string active_defrag{};
    heap::string log_page_access_trace{};
    heap::string save_interval{};
    heap::string max_modifications_before_save{};
    heap::string use_vmm_mem{};
    heap::string external_host{};
    heap::string bind_interface{"0.0.0.0"};
    heap::string listen_port{};
    heap::string rpc_max_buffer{};
    heap::string rpc_client_max_wait_ms{};
    heap::string foreign_timeout_ms{};
    heap::string foreign_pool_max_age_ms{};
    heap::string foreign_script_insns{};
    heap::string function_slice_insns{};
    heap::string function_deadline_ms{};
    heap::string function_max_depth{};
    heap::string jump_factor{};
    heap::string ordered_keys{};
    heap::string hybrid_keys{};
    heap::string functions_dir{"off"};
    heap::string functions_sync_ms{"0"};
    heap::string functions_git_pull{"off"};
    heap::string functions_git_branch{"main"};
    heap::string functions_git_commit{"off"};
    heap::string functions_git_ssh_key{"off"};
    heap::string server_port{};
    heap::string server_binding{};
    heap::string static_bloom_filter{};
    heap::vector<std::string> valid_evictions = {
        "volatile-lru", "allkeys-lru", "volatile-lfu", "allkeys-lfu", "volatile-random", "none", "no", "nil", "null"
    };
    heap::vector<std::string> valid_on_off = {"on", "true", "off", "yes", "no", "null", "nil", "false"};

    heap::vector<std::string> valid_compression = {"zstd", "none", "off", "no", "null", "nil"};
    heap::vector<std::string> valid_use_vmm_mem = valid_on_off;
    heap::vector<std::string> valid_defrag = valid_on_off;
    // we want alloc tests but the db has to be created with alloc tests in the first place
    heap::vector<std::string> valid_alloc_tests = valid_on_off;
    heap::vector<std::string> valid_ordered_keys = valid_on_off;
    heap::vector<std::string> valid_hybrid_keys = valid_on_off;

    heap::string tls_pem_certificate_chain_file{};
    heap::string tls_private_key_file{};
    heap::string tls_tmp_dh_file{};

};

static config_state& state() {
    static config_state s;
    return s;
}
static barch::configuration_record& config() {
    return state().record;
}
static restarter restart;

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

    uint64_t n_rpc_max_buffer = std::strtoull(str, &notn, 10);
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
    // this used to read state().rpc_max_buffer, so the wait was set from whatever that
    // other variable happened to hold - zero on a server where it had never been set
    const char *str = state().rpc_client_max_wait_ms.c_str();
    const char *end = str + state().rpc_client_max_wait_ms.length();

    uint64_t n_rpc_max_client_wait_ms = std::strtoull(str, &notn, 10);
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

static ValkeyModuleString *GetForeignTimeoutMs(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().foreign_timeout_ms.c_str(),
                                     state().foreign_timeout_ms.length());
}

static int SetForeignTimeoutMs(const std::string& val) {
    std::regex check("[0-9]+");
    if (!std::regex_match(val, check)) {
        return VALKEYMODULE_ERR;
    }
    std::lock_guard lock(state().config_mutex);
    state().foreign_timeout_ms = val;
    char *end = nullptr;
    config().foreign_timeout_ms = std::strtoull(val.c_str(), &end, 10);
    return VALKEYMODULE_OK;
}

static int SetForeignTimeoutMs(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                               ValkeyModuleString **unused_arg) {
    return SetForeignTimeoutMs(ValkeyModule_StringPtrLen(val, nullptr));
}

static int ApplyForeignTimeoutMs(ValkeyModuleCtx *unused(ctx), void *unused(priv), ValkeyModuleString **unused(vks)) {
    return VALKEYMODULE_OK;
}

static ValkeyModuleString *GetForeignPoolMaxAgeMs(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().foreign_pool_max_age_ms.c_str(),
                                     state().foreign_pool_max_age_ms.length());
}

static int SetForeignPoolMaxAgeMs(const std::string& val) {
    std::regex check("[0-9]+");
    if (!std::regex_match(val, check)) {
        return VALKEYMODULE_ERR;
    }
    std::lock_guard lock(state().config_mutex);
    state().foreign_pool_max_age_ms = val;
    char *end = nullptr;
    config().foreign_pool_max_age_ms = std::strtoull(val.c_str(), &end, 10);
    return VALKEYMODULE_OK;
}

static int SetForeignPoolMaxAgeMs(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                                  ValkeyModuleString **unused_arg) {
    return SetForeignPoolMaxAgeMs(ValkeyModule_StringPtrLen(val, nullptr));
}

static int ApplyForeignPoolMaxAgeMs(ValkeyModuleCtx *unused(ctx), void *unused(priv), ValkeyModuleString **unused(vks)) {
    return VALKEYMODULE_OK;
}

static ValkeyModuleString *GetForeignScriptInsns(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().foreign_script_insns.c_str(),
                                     state().foreign_script_insns.length());
}

static int SetForeignScriptInsns(const std::string& val) {
    std::regex check("[0-9]+");
    if (!std::regex_match(val, check)) {
        return VALKEYMODULE_ERR;
    }
    std::lock_guard lock(state().config_mutex);
    state().foreign_script_insns = val;
    char *end = nullptr;
    uint64_t n = std::strtoull(val.c_str(), &end, 10);
    if (n == 0) n = 1;
    config().foreign_script_insns = n;
    return VALKEYMODULE_OK;
}

static int SetForeignScriptInsns(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                                 ValkeyModuleString **unused_arg) {
    return SetForeignScriptInsns(ValkeyModule_StringPtrLen(val, nullptr));
}

static int ApplyForeignScriptInsns(ValkeyModuleCtx *unused(ctx), void *unused(priv), ValkeyModuleString **unused(vks)) {
    return VALKEYMODULE_OK;
}
/*
 * A function's instruction slice and its deadline.
 *
 * These used to be borrowed from foreign: the budget was `foreign_script_insns` and
 * the bound was the space's `foreign_query_timeout_ms`. A fill and a command a client
 * invoked are not the same risk, and since a script yields rather than dying the
 * instruction count is a *slice* size while the deadline is the real bound - so they
 * are named for what they now are. See TODO 98 I.2.
 */
static ValkeyModuleString *GetFunctionSliceInsns(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().function_slice_insns.c_str(),
                                     state().function_slice_insns.length());
}

static int SetFunctionSliceInsns(const std::string& val) {
    std::regex check("[0-9]+");
    if (!std::regex_match(val, check)) {
        return VALKEYMODULE_ERR;
    }
    std::lock_guard lock(state().config_mutex);
    state().function_slice_insns = val;
    char *end = nullptr;
    uint64_t n = std::strtoull(val.c_str(), &end, 10);
    if (n == 0) n = 1;
    config().function_slice_insns = n;
    return VALKEYMODULE_OK;
}

static int SetFunctionSliceInsns(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                                 ValkeyModuleString **unused_arg) {
    return SetFunctionSliceInsns(ValkeyModule_StringPtrLen(val, nullptr));
}

static int ApplyFunctionSliceInsns(ValkeyModuleCtx *unused(ctx), void *unused(priv), ValkeyModuleString **unused(vks)) {
    return VALKEYMODULE_OK;
}
// ===========================================================================================================
static ValkeyModuleString *GetFunctionDeadlineMs(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().function_deadline_ms.c_str(),
                                     state().function_deadline_ms.length());
}

static int SetFunctionDeadlineMs(const std::string& val) {
    std::regex check("[0-9]+");
    if (!std::regex_match(val, check)) {
        return VALKEYMODULE_ERR;
    }
    std::lock_guard lock(state().config_mutex);
    state().function_deadline_ms = val;
    char *end = nullptr;
    uint64_t n = std::strtoull(val.c_str(), &end, 10);
    if (n == 0) n = 1;
    config().function_deadline_ms = n;
    return VALKEYMODULE_OK;
}

static int SetFunctionDeadlineMs(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                                 ValkeyModuleString **unused_arg) {
    return SetFunctionDeadlineMs(ValkeyModule_StringPtrLen(val, nullptr));
}

static int ApplyFunctionDeadlineMs(ValkeyModuleCtx *unused(ctx), void *unused(priv), ValkeyModuleString **unused(vks)) {
    return VALKEYMODULE_OK;
}
static ValkeyModuleString *GetFunctionMaxDepth(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().function_max_depth.c_str(),
                                     state().function_max_depth.length());
}

static int SetFunctionMaxDepth(const std::string& val) {
    std::regex check("[0-9]+");
    if (!std::regex_match(val, check)) {
        return VALKEYMODULE_ERR;
    }
    std::lock_guard lock(state().config_mutex);
    state().function_max_depth = val;
    char *end = nullptr;
    uint64_t n = std::strtoull(val.c_str(), &end, 10);
    if (n == 0) n = 1;
    config().function_max_depth = n;
    return VALKEYMODULE_OK;
}

static int SetFunctionMaxDepth(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                               ValkeyModuleString **unused_arg) {
    return SetFunctionMaxDepth(ValkeyModule_StringPtrLen(val, nullptr));
}

static int ApplyFunctionMaxDepth(ValkeyModuleCtx *unused(ctx), void *unused(priv), ValkeyModuleString **unused(vks)) {
    return VALKEYMODULE_OK;
}
// ===========================================================================================================
static ValkeyModuleString *GetServerPort(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().server_port.c_str(), state().server_port.length());;
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

    uint64_t n_server_port = std::strtoull(str, &notn, 10);
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
    restart.asynch_restart(config().server_binding,config().server_port,false);
    return VALKEYMODULE_OK;
}
// ===========================================================================================================
static ValkeyModuleString *GetServerBinding(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().server_binding.c_str(), state().server_binding.length());;
}

static int SetServerBinding(const std::string& test_server_binding) {
    std::lock_guard lock(state().config_mutex);
    //if (test_server_binding.empty()) return VALKEYMODULE_ERR;
    state().server_binding = test_server_binding;
    if (test_server_binding.find_first_of("//") != std::string::npos) {
        state().server_port = "0";
        config().server_port = 0;
    }
    config().server_binding = test_server_binding;
    return VALKEYMODULE_OK;
}

static int SetServerBinding(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                             ValkeyModuleString **unused_arg) {
    std::string test = ValkeyModule_StringPtrLen(val, nullptr);
    return SetServerBinding(test);
}

static int ApplyServerBinding(ValkeyModuleCtx *unused(ctx), void *unused(priv), ValkeyModuleString **unused(vks)) {
    restart.asynch_restart(config().server_binding,config().server_port,false);
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

    // strtoull, not strtoll: the field is unsigned and its default is UINT64_MAX, so a
    // signed parse saturated at INT64_MAX and reading the value back then setting it
    // again silently halved the limit
    uint64_t n_max_memory_bytes = std::strtoull(str, &notn, 10);
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
static int ApplyMaxMemoryBytes(ValkeyModuleCtx *unused(ctx), void *unused(priv), ValkeyModuleString **unused(vks)) {
    return VALKEYMODULE_OK;
}

// ===========================================================================================================

static ValkeyModuleString *GetMaxRESPConnections(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().max_resp_connections.c_str(), state().max_resp_connections.length());;
}

static int SetMaxRESPConnections(const std::string& test_max_resp_connections) {
    std::regex check("[0-9]+");
    if (!std::regex_match(test_max_resp_connections, check)) {
        return VALKEYMODULE_ERR;
    }
    std::lock_guard lock(state().config_mutex);
    state().max_resp_connections = test_max_resp_connections;
    char *notn = nullptr;
    const char *str = state().max_resp_connections.c_str();
    const char *end = str + state().max_resp_connections.length();

    uint64_t n_max_resp_connections = std::strtoull(str, &notn, 10);
    if (notn == nullptr || notn != end) {
        return VALKEYMODULE_ERR;
    }
    if (n_max_resp_connections < 2 || n_max_resp_connections > 1600000) {
        return VALKEYMODULE_ERR;
    }
    config().max_resp_connections = n_max_resp_connections;
    return VALKEYMODULE_OK;
}

static int SetMaxRESPConnections(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                             ValkeyModuleString **unused_arg) {
    std::string test = ValkeyModule_StringPtrLen(val, nullptr);
    return SetMaxRESPConnections(test);
}
static int ApplyMaxRESPConnections(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString *GetUseVMMemory(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().use_vmm_mem.c_str(), state().use_vmm_mem.length());
}

// takes a plain string as well, so set_configuration_value can reach it - every other
// variable has this overload and this one did not, which is why CONFIG SET could
// register the variable but never change it
static int SetUseVMMemory(const std::string& val) {
    std::lock_guard lock(state().config_mutex);
    std::string test_use_vmm_memory = val;
    std::transform(test_use_vmm_memory.begin(), test_use_vmm_memory.end(), test_use_vmm_memory.begin(),
                   ::tolower);

    if (!check_type(test_use_vmm_memory, state().valid_use_vmm_mem)) {
        return VALKEYMODULE_ERR;
    }
    state().use_vmm_mem = test_use_vmm_memory;
    config().use_vmm_memory = (state().use_vmm_mem == "on" || state().use_vmm_mem == "true" || state().use_vmm_mem == "yes");
    return VALKEYMODULE_OK;
}

static int SetUseVMMemory(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                          ValkeyModuleString **unused_arg) {
    return SetUseVMMemory(std::string{ValkeyModule_StringPtrLen(val, nullptr)});
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
static ValkeyModuleString *GetStaticBloomFilter(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().static_bloom_filter.c_str(), state().static_bloom_filter.length());
}

static int SetStaticBloomFilter(const std::string& valu) {
    std::lock_guard lock(state().config_mutex);
    if (valu.empty()) {
        return VALKEYMODULE_ERR;
    }
    std::string val = valu;
    std::transform(val.begin(), val.end(), val.begin(),::tolower);

    if (!check_type(val, state().valid_on_off)) {
        return VALKEYMODULE_ERR;
    }

    state().static_bloom_filter = val;
    config().static_bloom_filter = is_on(state().static_bloom_filter.c_str());

    return VALKEYMODULE_OK;
}
static int SetStaticBloomFilter(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                          ValkeyModuleString **unused_arg) {
    std::string s = ValkeyModule_StringPtrLen(val, nullptr);
    return SetStaticBloomFilter(s);
}

static int ApplyStaticBloomFilter(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    barch::all_shards([](auto& shard) {
        storage_release l(shard);
        shard->create_bloom(config().static_bloom_filter);
        shard->load_bloom();
    });
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
    barch::server::start(config().bind_interface, config().listen_port, false);
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
// tls_pem_certificate_chain_file
static ValkeyModuleString *GetTlsPemCertificateChainFile(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().tls_pem_certificate_chain_file.c_str(), state().tls_pem_certificate_chain_file.length());
}

static int SetTlsPemCertificateChainFile(const std::string& val) {
    std::lock_guard lock(state().config_mutex);
    std::string test_val = val;
    // TODO: check access
    state().tls_pem_certificate_chain_file = test_val;
    config().tls_pem_certificate_chain_file = test_val;
    return VALKEYMODULE_OK;
}
static int SetTlsPemCertificateChainFile(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                              ValkeyModuleString **unused_arg) {
    std::string value = ValkeyModule_StringPtrLen(val, nullptr);
    if (value.empty()) {
        return VALKEYMODULE_ERR;
    }
    return SetTlsPemCertificateChainFile(value);
}
static int ApplyTlsPemCertificateChainFile(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    return VALKEYMODULE_OK;
}
// ===========================================================================================================
// tls_private_key_file

static ValkeyModuleString *GetTlsPrivateKeyFile(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().tls_private_key_file.c_str(), state().tls_private_key_file.length());
}

static int SetTlsPrivateKeyFile(const std::string& val) {
    std::lock_guard lock(state().config_mutex);
    std::string test_val = val;
    // TODO: check access
    state().tls_private_key_file = test_val;
    config().tls_private_key_file = test_val;
    return VALKEYMODULE_OK;
}
static int SetTlsPrivateKeyFile(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                              ValkeyModuleString **unused_arg) {
    std::string value = ValkeyModule_StringPtrLen(val, nullptr);
    if (value.empty()) {
        return VALKEYMODULE_ERR;
    }
    return SetTlsPrivateKeyFile(value);
}
static int ApplyTlsPrivateKeyFile(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
// dh is for diffie helman
// tls_tmp_dh_file

static ValkeyModuleString *GetTlsTmpDhFile(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().tls_tmp_dh_file.c_str(), state().tls_tmp_dh_file.length());
}

static int SetTlsTmpDhFile(const std::string& val) {
    std::lock_guard lock(state().config_mutex);
    std::string test_val = val;
    // TODO: check access
    state().tls_tmp_dh_file = test_val;
    config().tls_tmp_dh_file = test_val;
    return VALKEYMODULE_OK;
}
static int SetTlsTmpDhFile(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                              ValkeyModuleString **unused_arg) {
    std::string value = ValkeyModule_StringPtrLen(val, nullptr);
    if (value.empty()) {
        return VALKEYMODULE_ERR;
    }
    return SetTlsTmpDhFile(value);
}
static int ApplyTlsTmpDhFile(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    return VALKEYMODULE_OK;
}

// ===========================================================================================================
static ValkeyModuleString *GetMaxScanIterators(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().max_scan_iterators.c_str(), state().max_scan_iterators.length());
}

static int SetMaxScanIterators(const std::string& val) {
    std::lock_guard lock(state().config_mutex);
    std::string test_max_scan_iterators = val;
    std::regex check("[0-9]+");
    if (!std::regex_match(test_max_scan_iterators, check)) {
        return VALKEYMODULE_ERR;
    }
    state().max_scan_iterators = test_max_scan_iterators;
    char *ep = nullptr;
    config().max_scan_iterators = std::strtoull(test_max_scan_iterators.c_str(), &ep, 10);
    return VALKEYMODULE_OK;
}
static int SetMaxScanIterators(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                               ValkeyModuleString **unused_arg) {
    std::string value = ValkeyModule_StringPtrLen(val, nullptr);
    return SetMaxScanIterators(value);
}
static int ApplyMaxScanIterators(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
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
    config().max_defrag_page_count = std::strtoull(test_max_defrag_page_count.c_str(), &ep, 10);
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
    config().iteration_worker_count = std::strtoull(test_iteration_worker_count.c_str(), &ep, 10);
    if (config().iteration_worker_count <= 0) {
        config().iteration_worker_count = 1;
    }
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
    config().save_interval = std::strtoull(test_save_interval.c_str(), &ep, 10);
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
    config().max_modifications_before_save = std::strtoull(test_max_modifications_before_save.c_str(), &ep, 10);
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
    config().maintenance_poll_delay = std::strtoull(test_maintenance_poll_delay.c_str(), &ep, 10);
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
    if (!is_float(val)) {
        return VALKEYMODULE_ERR;
    }
    double test_val = std::stof(val);
    if (test_val < 0 || test_val > 100) {
        return VALKEYMODULE_ERR;
    }

    state().min_fragmentation_ratio = val;
    config().min_fragmentation_ratio = std::stof(state().min_fragmentation_ratio.c_str());
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
static ValkeyModuleString *GetPreEvictThresh(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().pre_evict_thresh.c_str(), state().pre_evict_thresh.length());
}

static int SetPreEvictThresh(const std::string& val) {
    std::lock_guard lock(state().config_mutex);
    if (!is_float(val)) {
        return VALKEYMODULE_ERR;
    }
    double test_val = std::stof(val);
    if (test_val < 0 || test_val > 0.99) {
        return VALKEYMODULE_ERR;
    }
    state().pre_evict_thresh = val;
    // stod, not stof: the field is a double, and parsing it as a float first meant
    // 0.85 came back as 0.8500000238418579
    config().pre_evict_thresh = std::stod(state().pre_evict_thresh.c_str());
    return VALKEYMODULE_OK;
}

static int SetPreEvictThresh(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                               ValkeyModuleString **unused_arg) {
    std::string preet = ValkeyModule_StringPtrLen(val, nullptr);
    return SetPreEvictThresh(preet);
}

static int ApplyPreEvictThresh(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    return VALKEYMODULE_OK;
}
// ===========================================================================================================
static ValkeyModuleString *GetMinCompressedSize(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().min_compressed_size.c_str(), state().min_compressed_size.length());
}

static int SetMinCompressedSize(const std::string& val) {
    std::lock_guard lock(state().config_mutex);
    std::regex check("[0-9]+");
    if (!std::regex_match(val, check)) {
        return VALKEYMODULE_ERR;
    }
    state().min_compressed_size = val;
    config().min_compressed_size = std::stoull(state().min_compressed_size.c_str());
    return VALKEYMODULE_OK;
}

static int SetMinCompressedSize(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                               ValkeyModuleString **unused_arg) {
    std::string min_compressed_size = ValkeyModule_StringPtrLen(val, nullptr);
    return SetMinCompressedSize(min_compressed_size);
}

static int ApplyMinCompressedSize(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
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
    barch::sharded_store store(get_default_ks());
    store.each_shard([](const barch::shard_ptr& s) {
        if (!s)
            abort_with("invalid shard");
        s->opt_ordered_keys = config().ordered_keys;
    });
    return VALKEYMODULE_OK;
}
static int SetOrderedKeys(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                               ValkeyModuleString **unused_arg) {
    std::string test_ordered_keys = ValkeyModule_StringPtrLen(val, nullptr);
    return SetOrderedKeys(test_ordered_keys);
}
static int ApplyOrderedKeys(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    get_default_ks()->opt_ordered_keys = config().ordered_keys;
    barch::sharded_store store(get_default_ks());
    store.each_shard([](const barch::shard_ptr& s) { s->opt_ordered_keys = config().ordered_keys; });
    return VALKEYMODULE_OK;
}

static ValkeyModuleString *GetHybridKeys(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().hybrid_keys.c_str(), state().hybrid_keys.length());
}

static int SetHybridKeys(std::string test_hybrid_keys) {
    std::lock_guard lock(state().config_mutex);
    std::transform(test_hybrid_keys.begin(), test_hybrid_keys.end(), test_hybrid_keys.begin(), ::tolower);

    if (!check_type(test_hybrid_keys, state().valid_hybrid_keys)) {
        return VALKEYMODULE_ERR;
    }

    state().hybrid_keys = test_hybrid_keys;
    config().hybrid_keys =
            state().hybrid_keys == "on" || state().hybrid_keys == "true" || state().hybrid_keys == "yes";
    return VALKEYMODULE_OK;
}
static int SetHybridKeys(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                               ValkeyModuleString **unused_arg) {
    std::string test_hybrid_keys = ValkeyModule_StringPtrLen(val, nullptr);
    return SetHybridKeys(test_hybrid_keys);
}
static int ApplyHybridKeys(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    auto spc = get_default_ks();
    ks_unique ul(spc);
    spc->opt_hybrid_keys = config().hybrid_keys;
    barch::sharded_store store(spc);
    store.each_shard([](const barch::shard_ptr& s) {
        if (!s)
            abort_with("invalid shard");
        s->opt_hybrid_keys = config().hybrid_keys;
        s->apply_hybrid_keys();
    });
    return VALKEYMODULE_OK;
}

static ValkeyModuleString *GetFunctionsDir(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().functions_dir.c_str(), state().functions_dir.length());
}
static int SetFunctionsDir(const std::string& val) {
    std::lock_guard lock(state().config_mutex);
    state().functions_dir = val;
    config().functions_dir = val;
    return VALKEYMODULE_OK;
}
static int SetFunctionsDir(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                           ValkeyModuleString **unused_arg) {
    return SetFunctionsDir(ValkeyModule_StringPtrLen(val, nullptr));
}
static int ApplyFunctionsDir(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    if (barch::get_functions_dir().empty())
        barch::stop_function_sync();
    else
        barch::start_function_sync();
    return VALKEYMODULE_OK;
}

static ValkeyModuleString *GetFunctionsSyncMs(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().functions_sync_ms.c_str(), state().functions_sync_ms.length());
}
static int SetFunctionsSyncMs(const std::string& val) {
    std::lock_guard lock(state().config_mutex);
    std::regex check("[0-9]+");
    if (!std::regex_match(val, check))
        return VALKEYMODULE_ERR;
    state().functions_sync_ms = val;
    config().functions_sync_ms = std::stoull(val);
    return VALKEYMODULE_OK;
}
static int SetFunctionsSyncMs(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                              ValkeyModuleString **unused_arg) {
    return SetFunctionsSyncMs(ValkeyModule_StringPtrLen(val, nullptr));
}
static int ApplyFunctionsSyncMs(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    barch::request_function_sync();
    return VALKEYMODULE_OK;
}

static ValkeyModuleString *GetFunctionsGitPull(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().functions_git_pull.c_str(), state().functions_git_pull.length());
}
static int SetFunctionsGitPull(std::string val) {
    std::lock_guard lock(state().config_mutex);
    std::transform(val.begin(), val.end(), val.begin(), ::tolower);
    if (!check_type(val, state().valid_on_off))
        return VALKEYMODULE_ERR;
    state().functions_git_pull = val;
    config().functions_git_pull = val == "on" || val == "true" || val == "yes";
    return VALKEYMODULE_OK;
}
static int SetFunctionsGitPull(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                               ValkeyModuleString **unused_arg) {
    return SetFunctionsGitPull(ValkeyModule_StringPtrLen(val, nullptr));
}
static int ApplyFunctionsGitPull(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    return VALKEYMODULE_OK;
}

static ValkeyModuleString *GetFunctionsGitBranch(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().functions_git_branch.c_str(),
                                     state().functions_git_branch.length());
}
static int SetFunctionsGitBranch(const std::string& val) {
    std::lock_guard lock(state().config_mutex);
    if (val.empty())
        return VALKEYMODULE_ERR;
    state().functions_git_branch = val;
    config().functions_git_branch = val;
    return VALKEYMODULE_OK;
}
static int SetFunctionsGitBranch(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                                 ValkeyModuleString **unused_arg) {
    return SetFunctionsGitBranch(ValkeyModule_StringPtrLen(val, nullptr));
}
static int ApplyFunctionsGitBranch(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    return VALKEYMODULE_OK;
}

static ValkeyModuleString *GetFunctionsGitCommit(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().functions_git_commit.c_str(),
                                     state().functions_git_commit.length());
}
static int SetFunctionsGitCommit(const std::string& val) {
    std::lock_guard lock(state().config_mutex);
    state().functions_git_commit = val;
    config().functions_git_commit = val;
    return VALKEYMODULE_OK;
}
static int SetFunctionsGitCommit(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                                 ValkeyModuleString **unused_arg) {
    return SetFunctionsGitCommit(ValkeyModule_StringPtrLen(val, nullptr));
}
static int ApplyFunctionsGitCommit(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    return VALKEYMODULE_OK;
}

static ValkeyModuleString *GetFunctionsGitSshKey(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().functions_git_ssh_key.c_str(),
                                     state().functions_git_ssh_key.length());
}
static int SetFunctionsGitSshKey(const std::string& val) {
    std::lock_guard lock(state().config_mutex);
    state().functions_git_ssh_key = val;
    config().functions_git_ssh_key = val;
    return VALKEYMODULE_OK;
}
static int SetFunctionsGitSshKey(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                                 ValkeyModuleString **unused_arg) {
    return SetFunctionsGitSshKey(ValkeyModule_StringPtrLen(val, nullptr));
}
static int ApplyFunctionsGitSshKey(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
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
static ValkeyModuleString *GetDbNumberPrefix(const char *unused_arg, void *unused_arg) {
    std::lock_guard lock(state().config_mutex);
    return ValkeyModule_CreateString(nullptr, state().db_number_prefix.c_str(),
                                     state().db_number_prefix.length());
}

static int SetDbNumberPrefix(const std::string& prefix) {
    std::lock_guard lock(state().config_mutex);
    // an empty prefix would make SELECT 1 name the space "1", which is what SELECT did
    // before it knew about numbers - allowed, because someone may want exactly that
    for (auto ch : prefix) {
        if (ch == ':' || ch == ' ') {
            return VALKEYMODULE_ERR;   // ':' separates the space from the command on the wire
        }
    }
    state().db_number_prefix = prefix.c_str();
    return VALKEYMODULE_OK;
}

// the module api hands the value as a ValkeyModuleString, so there is an overload for it
// beside the plain one that CONFIG SET and the tests use, as eviction_policy does
static int SetDbNumberPrefix(const char *unused_arg, ValkeyModuleString *val, void *unused_arg,
                             ValkeyModuleString **unused_arg) {
    size_t len = 0;
    const char *s = ValkeyModule_StringPtrLen(val, &len);
    return SetDbNumberPrefix(std::string(s, len));
}

static int ApplyDbNumberPrefix(ValkeyModuleCtx *unused_arg, void *unused_arg, ValkeyModuleString **unused_arg) {
    return VALKEYMODULE_OK;
}

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
    barch::sharded_store store(get_default_ks());
    store.each_shard_write([&](const barch::shard_ptr& t) {
        t->get_ap().get_nodes().set_opt_enable_lfu(lfu);
        t->get_ap().get_leaves().set_opt_enable_lfu(lfu);
        t->opt_evict_all_keys_lru = config().evict_allkeys_lru;
        t->opt_evict_volatile_keys_lru = config().evict_volatile_lru;
        t->opt_evict_all_keys_lfu = config().evict_allkeys_lfu;
        t->opt_evict_volatile_keys_lfu = config().evict_volatile_lfu;
        t->opt_evict_all_keys_random = config().evict_allkeys_random;
    });
    return VALKEYMODULE_OK;
}

int barch::register_valkey_configuration(ValkeyModuleCtx *ctx) {
    int ret = 0;
    ret |= ValkeyModule_RegisterStringConfig(ctx, "compression", "none", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetCompressionType, SetCompressionType, ApplyCompressionType, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "eviction_policy", "none", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetEvictionType, SetEvictionType, ApplyEvictionType, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "db_number_prefix", "db", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetDbNumberPrefix, SetDbNumberPrefix, ApplyDbNumberPrefix, nullptr);

    auto physical = heap::get_physical_memory_bytes();
    auto def = physical;// - physical / 4ull;

    ret |= ValkeyModule_RegisterStringConfig(ctx, "max_memory_bytes", std::to_string(def).c_str(), VALKEYMODULE_CONFIG_DEFAULT,
                                             GetMaxMemoryRatio, SetMaxMemoryBytes, ApplyMaxMemoryBytes, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "max_resp_connections", "2000", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetMaxRESPConnections, SetMaxRESPConnections, ApplyMaxRESPConnections, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "min_fragmentation_ratio", "0.5", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetMinFragmentation, SetMinFragmentation, ApplyMinFragmentation, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "pre_evict_thresh", "0.85", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetPreEvictThresh, SetPreEvictThresh, ApplyPreEvictThresh, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "active_defrag", "on", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetActiveDefragType, SetActiveDefragType, ApplyActiveDefragType, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "maintenance_poll_delay", "440", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetMaintenancePollDelay, SetMaintenancePollDelay,
                                             ApplyMaintenancePollDelay, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "max_scan_iterators", "128", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetMaxScanIterators, SetMaxScanIterators, ApplyMaxScanIterators,
                                             nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "max_defrag_page_count", "10", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetMaxDefragPageCount, SetMaxDefragPageCount, ApplyMaxDefragPageCount,
                                             nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "min_compressed_size", "64", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetMinCompressedSize, SetMinCompressedSize, ApplyMinCompressedSize,
                                             nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "iteration_worker_count", "1", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetIterationWorkerCount, SetIterationWorkerCount,
                                             ApplyIterationWorkerCount, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "save_interval", "60000", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetSaveInterval, SetSaveInterval,
                                             ApplySaveInterval, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "max_modifications_before_save", "4300000",
                                             VALKEYMODULE_CONFIG_DEFAULT,
                                             GetMaxModificationsBeforeSave, SetMaxModificationsBeforeSave,
                                             ApplyMaxModificationsBeforeSave, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "log_page_access_trace", "no", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetEnablePageTrace, SetEnablePageTrace,
                                             ApplyEnablePageTrace, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "use_vmm_mem", "yes", VALKEYMODULE_CONFIG_DEFAULT,
                                             GetUseVMMemory, SetUseVMMemory,
                                             ApplyUseVMMemory, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "static_bloom_filter", "no", VALKEYMODULE_CONFIG_DEFAULT,
                                         GetStaticBloomFilter, SetStaticBloomFilter,
                                         ApplyStaticBloomFilter, nullptr);

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

    ret |= ValkeyModule_RegisterStringConfig(ctx, "foreign_timeout_ms", "300000", VALKEYMODULE_CONFIG_DEFAULT,
                                                 GetForeignTimeoutMs, SetForeignTimeoutMs,
                                                 ApplyForeignTimeoutMs, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "foreign_pool_max_age_ms", "30000", VALKEYMODULE_CONFIG_DEFAULT,
                                                 GetForeignPoolMaxAgeMs, SetForeignPoolMaxAgeMs,
                                                 ApplyForeignPoolMaxAgeMs, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "foreign_script_insns", "1000000", VALKEYMODULE_CONFIG_DEFAULT,
                                                 GetForeignScriptInsns, SetForeignScriptInsns,
                                                 ApplyForeignScriptInsns, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "function_slice_insns", "1000000", VALKEYMODULE_CONFIG_DEFAULT,
                                                 GetFunctionSliceInsns, SetFunctionSliceInsns,
                                                 ApplyFunctionSliceInsns, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "function_deadline_ms", "1000", VALKEYMODULE_CONFIG_DEFAULT,
                                                 GetFunctionDeadlineMs, SetFunctionDeadlineMs,
                                                 ApplyFunctionDeadlineMs, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "function_max_depth", "100", VALKEYMODULE_CONFIG_DEFAULT,
                                                 GetFunctionMaxDepth, SetFunctionMaxDepth,
                                                 ApplyFunctionMaxDepth, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "ordered_keys", "yes", VALKEYMODULE_CONFIG_DEFAULT,
                                                     GetOrderedKeys, SetOrderedKeys,
                                                     ApplyOrderedKeys, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "hybrid_keys", "yes", VALKEYMODULE_CONFIG_DEFAULT,
                                                     GetHybridKeys, SetHybridKeys,
                                                     ApplyHybridKeys, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "functions_dir", "off", VALKEYMODULE_CONFIG_DEFAULT,
                                                     GetFunctionsDir, SetFunctionsDir,
                                                     ApplyFunctionsDir, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "functions_sync_ms", "0", VALKEYMODULE_CONFIG_DEFAULT,
                                                     GetFunctionsSyncMs, SetFunctionsSyncMs,
                                                     ApplyFunctionsSyncMs, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "functions_git_pull", "off", VALKEYMODULE_CONFIG_DEFAULT,
                                                     GetFunctionsGitPull, SetFunctionsGitPull,
                                                     ApplyFunctionsGitPull, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "functions_git_branch", "main", VALKEYMODULE_CONFIG_DEFAULT,
                                                     GetFunctionsGitBranch, SetFunctionsGitBranch,
                                                     ApplyFunctionsGitBranch, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "functions_git_commit", "off", VALKEYMODULE_CONFIG_DEFAULT,
                                                     GetFunctionsGitCommit, SetFunctionsGitCommit,
                                                     ApplyFunctionsGitCommit, nullptr);
    ret |= ValkeyModule_RegisterStringConfig(ctx, "functions_git_ssh_key", "off", VALKEYMODULE_CONFIG_DEFAULT,
                                                     GetFunctionsGitSshKey, SetFunctionsGitSshKey,
                                                     ApplyFunctionsGitSshKey, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "server_port", "14000", VALKEYMODULE_CONFIG_DEFAULT,
                                                     GetServerPort, SetServerPort,
                                                     ApplyServerPort, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "server_binding", "127.0.0.1", VALKEYMODULE_CONFIG_DEFAULT,
                                                     GetServerBinding, SetServerBinding,
                                                     ApplyServerBinding, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "tls_pem_certificate_chain_file", "server.crt", VALKEYMODULE_CONFIG_DEFAULT,
                                                         GetTlsPemCertificateChainFile, SetTlsPemCertificateChainFile,
                                                         ApplyTlsPemCertificateChainFile, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "tls_private_key_file", "server.key", VALKEYMODULE_CONFIG_DEFAULT,
                                                         GetTlsPrivateKeyFile, SetTlsPrivateKeyFile,
                                                         ApplyTlsPrivateKeyFile, nullptr);

    ret |= ValkeyModule_RegisterStringConfig(ctx, "tls_tmp_dh_file", "server.dh", VALKEYMODULE_CONFIG_DEFAULT,
                                                         GetTlsTmpDhFile, SetTlsTmpDhFile,
                                                         ApplyTlsTmpDhFile, nullptr);

    return ret;
}

int barch::set_configuration_value(ValkeyModuleString *Name, ValkeyModuleString *Value) {
    std::string name = ValkeyModule_StringPtrLen(Name, nullptr);
    std::string val = ValkeyModule_StringPtrLen(Value, nullptr);
    return set_configuration_value(name, val);
}
// ---- redis configuration names --------------------------------------------------
//
// Redis clients probe for settings by redis's names, and barch's own are different -
// max_memory_bytes, not maxmemory. These map one onto the other where the meaning
// genuinely matches, so a client can read, and in most cases write, a setting without
// knowing barch's vocabulary.
//
// Three kinds appear here:
//
//  - an alias, where a redis name means exactly one barch variable and values are
//    written the same way. GET and SET both work and pass straight through.
//  - an alias needing a value translated, where the meaning matches but the spelling
//    of a value does not: redis says noeviction where barch says none.
//  - a fixed answer, where barch has no such setting but can say something true about
//    it. appendonly is no because there is no append only file at all. These read, and
//    refuse to be set, rather than accepting a write that would quietly do nothing.
//
// A redis name that is none of those is deliberately absent. CONFIG GET then returns
// nothing for it, which is what redis does for a parameter it does not know, and is a
// better answer than a plausible looking lie. `databases` is the one worth naming:
// barch's key spaces are named rather than numbered and SELECT takes a name, so any
// number here would mislead a client about what SELECT will accept.
namespace {
    struct redis_alias { const char* redis_name; const char* barch_name; };
    const redis_alias redis_aliases[] = {
        {"maxmemory",          "max_memory_bytes"},
        {"maxmemory-policy",   "eviction_policy"},
        {"maxclients",         "max_resp_connections"},
        {"bind",               "server_binding"},
        {"port",               "server_port"},
        {"tls-cert-file",      "tls_pem_certificate_chain_file"},
        {"tls-key-file",       "tls_private_key_file"},
        {"tls-dh-params-file", "tls_tmp_dh_file"},
    };
    struct redis_fixed { const char* redis_name; const char* value; const char* why; };
    const redis_fixed redis_fixed_values[] = {
        {"appendonly",      "no", "there is no append only file"},
        {"appendfsync",     "no", "there is no append only file"},
        {"cluster-enabled", "no", "barch is not a cluster member"},
        {"daemonize",       "no", "barch runs inside its host server"},
        {"timeout",         "0",  "idle connections are not closed on a timer"},
    };
    /**
     * Redis writes byte sizes with a unit, and its units are not barch's: redis reads
     * 1k as 1000 and 1kb as 1024, where barch's parser takes a single letter and treats
     * k as 1024. Passing the string through would therefore be wrong by about 5% for
     * every value written the decimal way, and would not parse at all for the two
     * letter forms. So a redis size is resolved to a plain byte count here, which both
     * sides agree on. Returns false if it is not a size redis would accept either.
     */
    bool redis_bytes_to_plain(const std::string& v, std::string& plain) {
        size_t at = 0;
        while (at < v.size() && isdigit((unsigned char) v[at])) ++at;
        if (at == 0) return false;
        uint64_t n = std::strtoull(v.substr(0, at).c_str(), nullptr, 10);
        std::string unit = v.substr(at);
        std::transform(unit.begin(), unit.end(), unit.begin(), ::tolower);
        uint64_t mul = 0;
        if (unit.empty())      mul = 1;
        else if (unit == "b")  mul = 1;
        else if (unit == "k")  mul = 1000ull;
        else if (unit == "kb") mul = 1024ull;
        else if (unit == "m")  mul = 1000ull * 1000;
        else if (unit == "mb") mul = 1024ull * 1024;
        else if (unit == "g")  mul = 1000ull * 1000 * 1000;
        else if (unit == "gb") mul = 1024ull * 1024 * 1024;
        else return false;
        if (mul != 1 && n > std::numeric_limits<uint64_t>::max() / mul) return false;
        plain = std::to_string(n * mul);
        return true;
    }

    // barch spells "do not evict" as none, and also accepts no/nil/null; redis spells
    // it noeviction. Every other policy name is already the redis one
    std::string policy_to_redis(const std::string& v) {
        if (v == "none" || v == "no" || v == "nil" || v == "null") return "noeviction";
        return v;
    }
    std::string policy_from_redis(const std::string& v) {
        if (v == "noeviction") return "none";
        return v;
    }
}

const std::vector<std::string>& barch::redis_configuration_names() {
    static const std::vector<std::string> names = []() {
        std::vector<std::string> r;
        for (const auto& a : redis_aliases) r.emplace_back(a.redis_name);
        for (const auto& f : redis_fixed_values) r.emplace_back(f.redis_name);
        r.emplace_back("save");
        std::sort(r.begin(), r.end());
        return r;
    }();
    return names;
}

/**
 * The environment name a setting answers to: BARCH_ followed by its own name in upper
 * case, with hyphens as underscores because an environment name cannot carry one. So
 * max_memory_bytes reads BARCH_MAX_MEMORY_BYTES, and the redis name maxmemory-policy
 * reads BARCH_MAXMEMORY_POLICY.
 */
static std::string environment_name_of(const std::string& name) {
    std::string e = "BARCH_" + name;
    std::transform(e.begin(), e.end(), e.begin(), ::toupper);
    std::replace(e.begin(), e.end(), '-', '_');
    return e;
}

size_t barch::apply_environment_configuration() {
    size_t applied = 0;
    auto from_env = [&applied](const std::string& name) {
        auto env = environment_name_of(name);
        const char* value = std::getenv(env.c_str());
        if (value == nullptr) return;

        std::string why;
        if (is_read_only_configuration(name, why)) {
            // say so rather than ignore it - somebody who exported it expects an effect
            barch::err({"ignoring", env, "-", why});
            return;
        }
        if (set_configuration_value(name, value) == VALKEYMODULE_OK) {
            barch::log({"configured", name, "from", env});
            ++applied;
        } else {
            barch::err({"could not apply", env, "=", value, "to", name});
        }
    };
    // the redis names first and barch's own second, so that if a setting is exported
    // under both - BARCH_MAXMEMORY and BARCH_MAX_MEMORY_BYTES - barch's own name is the
    // one that lands, being the more specific of the two
    for (const auto& name : redis_configuration_names()) from_env(name);
    for (const auto& name : configuration_names()) from_env(name);
    return applied;
}

bool barch::is_read_only_configuration(const std::string& name, std::string& why) {
    for (const auto& f : redis_fixed_values) {
        if (name == f.redis_name) { why = f.why; return true; }
    }
    if (name == "save") {
        why = "redis allows several interval and change pairs and barch has room for "
              "one - set save_interval and max_modifications_before_save instead";
        return true;
    }
    return false;
}

bool barch::get_redis_configuration_value(const std::string& name, std::string& value) {
    for (const auto& a : redis_aliases) {
        if (name == a.redis_name) {
            if (!get_configuration_value(a.barch_name, value)) return false;
            if (name == "maxmemory-policy") value = policy_to_redis(value);
            return true;
        }
    }
    for (const auto& f : redis_fixed_values) {
        if (name == f.redis_name) { value = f.value; return true; }
    }
    if (name == "save") {
        // redis writes this as "<seconds> <changes>" pairs. barch has exactly one such
        // pair: the interval it saves on, and the modification count that also triggers
        // a save. It is read only because redis allows several pairs and barch has room
        // for one, so a write could silently lose part of what was asked for
        std::string interval, mods;
        if (!get_configuration_value("save_interval", interval)) return false;
        if (!get_configuration_value("max_modifications_before_save", mods)) return false;
        value = std::to_string(std::strtoull(interval.c_str(), nullptr, 10) / 1000) + " " + mods;
        return true;
    }
    return false;
}

int barch::set_configuration_value(const std::string& name, const std::string &val) {
    barch::log({"setting", name, "to", val});

    // a redis name is resolved to the barch variable it means before anything else
    std::string why;
    if (is_read_only_configuration(name, why)) {
        barch::err({"cannot set", name, "-", why});
        return VALKEYMODULE_ERR;
    }
    for (const auto& a : redis_aliases) {
        if (name == a.redis_name) {
            if (name == "maxmemory-policy") {
                return set_configuration_value(a.barch_name, policy_from_redis(val));
            }
            if (name == "maxmemory") {
                std::string plain;
                if (!redis_bytes_to_plain(val, plain)) return VALKEYMODULE_ERR;
                return set_configuration_value(a.barch_name, plain);
            }
            return set_configuration_value(a.barch_name, val);
        }
    }

    if (name == "compression") {
        int r = SetCompressionType(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyCompressionType(nullptr, nullptr, nullptr);
        }
        return r;
    } else if (name == "db_number_prefix") {
        return SetDbNumberPrefix(val);
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
    } else if (name == "pre_evict_thresh") {
        return SetPreEvictThresh(val);
    } else if (name == "active_defrag") {
        return SetActiveDefragType(val);
    } else if (name == "iteration_worker_count") {
        return SetIterationWorkerCount(val);
    } else if (name == "maintenance_poll_delay") {
        return SetMaintenancePollDelay(val);
    } else if (name == "max_defrag_page_count") {
        return SetMaxDefragPageCount(val);
    } else if (name == "max_scan_iterators") {
        return SetMaxScanIterators(val);
    } else if (name == "save_interval") {
        return SetSaveInterval(val);
    } else if (name == "max_modifications_before_save") {
        return SetMaxModificationsBeforeSave(val);
    } else if (name == "external_host") {
        return SetExternalHost(val);
    } else if (name == "use_vmm_mem") {
        return SetUseVMMemory(val);
    } else if (name == "static_bloom_filter") {
        auto r = SetStaticBloomFilter(val);
        if ( VALKEYMODULE_OK == r) {
            return ApplyStaticBloomFilter(nullptr, nullptr, nullptr);
        }
        return r;
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
    } else if (name == "foreign_timeout_ms") {
        auto r = SetForeignTimeoutMs(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyForeignTimeoutMs(nullptr, nullptr, nullptr);
        }
        return r;
    } else if (name == "foreign_pool_max_age_ms") {
        auto r = SetForeignPoolMaxAgeMs(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyForeignPoolMaxAgeMs(nullptr, nullptr, nullptr);
        }
        return r;
    } else if (name == "foreign_script_insns") {
        auto r = SetForeignScriptInsns(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyForeignScriptInsns(nullptr, nullptr, nullptr);
        }
        return r;
    } else if (name == "function_slice_insns") {
        auto r = SetFunctionSliceInsns(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyFunctionSliceInsns(nullptr, nullptr, nullptr);
        }
        return r;
    } else if (name == "function_deadline_ms") {
        auto r = SetFunctionDeadlineMs(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyFunctionDeadlineMs(nullptr, nullptr, nullptr);
        }
        return r;
    } else if (name == "function_max_depth") {
        auto r = SetFunctionMaxDepth(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyFunctionMaxDepth(nullptr, nullptr, nullptr);
        }
        return r;
    }else if (name == "ordered_keys") {
        auto r = SetOrderedKeys(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyOrderedKeys(nullptr, nullptr, nullptr);
        }
        return r;
    }else if (name == "hybrid_keys") {
        auto r = SetHybridKeys(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyHybridKeys(nullptr, nullptr, nullptr);
        }
        return r;
    }else if (name == "functions_dir") {
        auto r = SetFunctionsDir(val);
        if (r == VALKEYMODULE_OK)
            return ApplyFunctionsDir(nullptr, nullptr, nullptr);
        return r;
    }else if (name == "functions_sync_ms") {
        auto r = SetFunctionsSyncMs(val);
        if (r == VALKEYMODULE_OK)
            return ApplyFunctionsSyncMs(nullptr, nullptr, nullptr);
        return r;
    }else if (name == "functions_git_pull") {
        auto r = SetFunctionsGitPull(val);
        if (r == VALKEYMODULE_OK)
            return ApplyFunctionsGitPull(nullptr, nullptr, nullptr);
        return r;
    }else if (name == "functions_git_branch") {
        auto r = SetFunctionsGitBranch(val);
        if (r == VALKEYMODULE_OK)
            return ApplyFunctionsGitBranch(nullptr, nullptr, nullptr);
        return r;
    }else if (name == "functions_git_commit") {
        auto r = SetFunctionsGitCommit(val);
        if (r == VALKEYMODULE_OK)
            return ApplyFunctionsGitCommit(nullptr, nullptr, nullptr);
        return r;
    }else if (name == "functions_git_ssh_key") {
        auto r = SetFunctionsGitSshKey(val);
        if (r == VALKEYMODULE_OK)
            return ApplyFunctionsGitSshKey(nullptr, nullptr, nullptr);
        return r;
    }else if (name == "server_port") {
        auto r = SetServerPort(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyServerPort(nullptr, nullptr, nullptr);
        }
        return r;
    }else if (name == "server_binding") {
        auto r = SetServerBinding(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyServerBinding(nullptr, nullptr, nullptr);
        }
        return r;
    }else if (name == "min_compressed_size") {
        auto r = SetMinCompressedSize(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyMinCompressedSize(nullptr, nullptr, nullptr);
        }
        return r;
    } else if (name == "max_resp_connections") {
        auto r = SetMaxRESPConnections(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyMaxRESPConnections(nullptr, nullptr, nullptr);
        }
        return r;
    } else if (name == "tls_pem_certificate_chain_file") {
        auto r = SetTlsPemCertificateChainFile(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyTlsPemCertificateChainFile(nullptr, nullptr, nullptr);
        }
        return r;
    }else if (name == "tls_private_key_file") {
        auto r = SetTlsPrivateKeyFile(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyTlsPrivateKeyFile(nullptr, nullptr, nullptr);
        }
        return r;
    }else if (name == "tls_tmp_dh_file") {
        auto r = SetTlsTmpDhFile(val);
        if (r == VALKEYMODULE_OK) {
            return ApplyTlsTmpDhFile(nullptr, nullptr, nullptr);
        }
        return r;
    }else {
        return VALKEYMODULE_ERR;
    }




}

bool barch::get_compression_enabled() {
    //std::lock_guard lock(state().config_mutex);
    return config().compression == compression_zstd;
}

uint64_t barch::get_max_module_memory() {
    return config().n_max_memory_bytes;
}

float barch::get_min_fragmentation_ratio() {
    std::lock_guard lock(state().config_mutex);
    return config().min_fragmentation_ratio;
}
double barch::get_pre_evict_thresh() {
    //std::lock_guard lock(state().config_mutex);
    return config().pre_evict_thresh;
}
uint64_t barch::get_min_compressed_size() {
    //std::lock_guard lock(state().config_mutex);
    return config().min_compressed_size;
}

bool barch::get_active_defrag() {
    //std::lock_guard lock(state().config_mutex);
    return config().active_defrag;
}


bool barch::get_evict_volatile_lru() {
    //std::lock_guard lock(state().config_mutex);
    return config().evict_volatile_lru;
}

bool barch::get_evict_allkeys_lru() {
    //std::lock_guard lock(state().config_mutex);
    return config().evict_allkeys_lru;
}

bool barch::get_evict_volatile_lfu() {
    //std::lock_guard lock(state().config_mutex);
    return config().evict_volatile_lfu;
}

bool barch::get_evict_allkeys_lfu() {
    //std::lock_guard lock(state().config_mutex);
    return config().evict_allkeys_lfu;
}

bool barch::get_evict_volatile_random() {
    //std::lock_guard lock(state().config_mutex);
    return config().evict_volatile_random;
};

bool barch::get_evict_allkeys_random() {
    //std::lock_guard lock(state().config_mutex);
    return config().evict_allkeys_random;
}

bool barch::get_evict_volatile_ttl() {
    //std::lock_guard lock(state().config_mutex);
    return config().evict_volatile_ttl;
}

std::string barch::get_db_number_prefix() {
    std::lock_guard lock(state().config_mutex);
    return state().db_number_prefix.c_str();
}

std::string barch::get_eviction_policy() {
    //std::lock_guard lock(state().config_mutex);
    return state().eviction_type.c_str();
}

bool barch::get_static_bloom_filter() {
    return config().static_bloom_filter;
}


bool barch::get_use_minimum_threads() {
    return config().use_minimum_threads;
}
const barch::configuration_record& barch::get_configuration() {
    std::lock_guard lock(state().config_mutex);
    return state().record;
}

uint64_t barch::get_maintenance_poll_delay() {
    std::lock_guard lock(state().config_mutex);
    return config().maintenance_poll_delay;
}

uint64_t barch::get_max_scan_iterators() {
    std::lock_guard lock(state().config_mutex);
    return config().max_scan_iterators;
}

uint64_t barch::get_max_defrag_page_count() {
    std::lock_guard lock(state().config_mutex);
    return config().max_defrag_page_count;
}
uint64_t barch::get_max_resp_connections() {
    std::lock_guard lock(state().config_mutex);
    return config().max_resp_connections;
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

uint64_t barch::get_rpc_max_client_wait_ms() {
    return config().rpc_client_max_wait_ms;
}

uint64_t barch::get_foreign_timeout_ms() {
    std::lock_guard lock(state().config_mutex);
    return config().foreign_timeout_ms;
}

uint64_t barch::get_foreign_pool_max_age_ms() {
    std::lock_guard lock(state().config_mutex);
    return config().foreign_pool_max_age_ms;
}

uint64_t barch::get_foreign_script_insns() {
    std::lock_guard lock(state().config_mutex);
    return config().foreign_script_insns;
}

uint64_t barch::get_function_slice_insns() {
    std::lock_guard lock(state().config_mutex);
    return config().function_slice_insns;
}

uint64_t barch::get_function_deadline_ms() {
    std::lock_guard lock(state().config_mutex);
    return config().function_deadline_ms;
}

uint64_t barch::get_function_max_depth() {
    std::lock_guard lock(state().config_mutex);
    return config().function_max_depth;
}

bool barch::get_log_page_access_trace() {
    //std::lock_guard lock(state().config_mutex);
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
    //std::lock_guard lock(state().config_mutex);
    return config().use_vmm_memory;
}
bool barch::get_ordered_keys() {
    return config().ordered_keys;
}

bool barch::get_hybrid_keys() {
    return config().hybrid_keys;
}

static bool cfg_off(const std::string& s) {
    return s.empty() || s == "off" || s == "none" || s == "no";
}

std::string barch::get_functions_dir() {
    return cfg_off(config().functions_dir) ? std::string() : config().functions_dir;
}
uint64_t barch::get_functions_sync_ms() {
    return config().functions_sync_ms;
}
bool barch::get_functions_git_pull() {
    return config().functions_git_pull;
}
std::string barch::get_functions_git_branch() {
    return config().functions_git_branch;
}
std::string barch::get_functions_git_commit() {
    return cfg_off(config().functions_git_commit) ? std::string() : config().functions_git_commit;
}
std::string barch::get_functions_git_ssh_key() {
    return cfg_off(config().functions_git_ssh_key) ? std::string() : config().functions_git_ssh_key;
}

uint64_t barch::get_internal_shards() {
    return config().internal_shards;
}

uint64_t barch::get_server_port() {
    std::lock_guard lock(state().config_mutex);
    return config().server_port;
}

std::string barch::get_server_binding() {
    std::lock_guard lock(state().config_mutex);
    return config().server_binding;
}


static std::vector<size_t> init_shard_sizes() {
    std::vector<size_t> r;

    auto shards = barch::get_internal_shards();

    for (size_t s = 0; s < shards;++s) {
        r.push_back(s);
    }
    return r;
}

const std::vector<size_t>& barch::get_shard_count() {
    static std::vector<size_t> shards = init_shard_sizes();
    return shards;
}
namespace barch{
    std::string get_tls_pem_certificate_chain_file() {
        std::lock_guard lock(state().config_mutex);
        return config().tls_pem_certificate_chain_file;
    }
    std::string get_tls_private_key_file() {
        std::lock_guard lock(state().config_mutex);
        return config().tls_private_key_file;
    }
    std::string get_tls_tmp_dh_file() {
        std::lock_guard lock(state().config_mutex);
        return config().tls_tmp_dh_file;
    }
}

// ===========================================================================================================
// Reflection for CONFIG GET.
//
// These read the live record rather than the state() reflection strings. Those strings
// are only filled in when something sets the variable, so a variable still on its
// default would read back empty. The two enum shaped ones are the exception: their
// parsed form in the record is an int or a set of booleans, and their reflection string
// carries a real default, so they are read from there.
//
// Every value produced here has to be something set_configuration_value will take back,
// which is what lets a client round trip a variable it has read.
static std::string cfg_bool(bool v) {
    return v ? "on" : "off";
}
template<typename F>
static std::string cfg_float(F v) {
    // formatted at the width it is stored at - widening a float to a double first
    // turns 0.6 into 0.6000000238418579
    std::string s = fmt::format("{}", v);
    if (s.find('.') == std::string::npos && s.find('e') == std::string::npos) {
        s += ".0"; // is_float insists on seeing a decimal point
    }
    return s;
}

const std::vector<std::string>& barch::configuration_names() {
    static const std::vector<std::string> names = {
        "active_defrag", "compression", "db_number_prefix", "eviction_policy",
        "external_host", "foreign_pool_max_age_ms", "foreign_script_insns",
        "function_slice_insns", "function_deadline_ms", "function_max_depth",
        "foreign_timeout_ms",
        "iteration_worker_count", "listen_port", "log_page_access_trace",
        "maintenance_poll_delay", "max_defrag_page_count", "max_memory_bytes",
        "max_modifications_before_save", "max_resp_connections", "max_scan_iterators",
        "min_compressed_size", "min_fragmentation_ratio", "ordered_keys", "hybrid_keys",
        "functions_dir", "functions_sync_ms", "functions_git_pull", "functions_git_branch",
        "functions_git_commit", "functions_git_ssh_key",
        "pre_evict_thresh", "rpc_client_max_wait_ms", "rpc_max_buffer", "save_interval",
        "server_binding", "server_port", "static_bloom_filter",
        "tls_pem_certificate_chain_file", "tls_private_key_file", "tls_tmp_dh_file",
        "use_vmm_mem"
    };
    return names;
}

static bool get_native_configuration_value(const std::string& name, std::string& value) {
    std::lock_guard lock(state().config_mutex);
    const auto& c = config();
    if (name == "active_defrag")                    value = cfg_bool(c.active_defrag);
    else if (name == "compression")                 value = state().compression_type.c_str();
    else if (name == "db_number_prefix")            value = state().db_number_prefix.c_str();
    else if (name == "eviction_policy")             value = state().eviction_type.c_str();
    else if (name == "external_host")               value = c.external_host;
    else if (name == "foreign_pool_max_age_ms")     value = std::to_string(c.foreign_pool_max_age_ms);
    else if (name == "foreign_script_insns")        value = std::to_string(c.foreign_script_insns);
    else if (name == "function_slice_insns")        value = std::to_string(c.function_slice_insns);
    else if (name == "function_deadline_ms")        value = std::to_string(c.function_deadline_ms);
    else if (name == "function_max_depth")          value = std::to_string(c.function_max_depth);
    else if (name == "foreign_timeout_ms")          value = std::to_string(c.foreign_timeout_ms);
    else if (name == "iteration_worker_count")      value = std::to_string(c.iteration_worker_count);
    else if (name == "listen_port")                 value = std::to_string(c.listen_port);
    else if (name == "log_page_access_trace")       value = cfg_bool(c.log_page_access_trace);
    else if (name == "maintenance_poll_delay")      value = std::to_string(c.maintenance_poll_delay);
    else if (name == "max_defrag_page_count")       value = std::to_string(c.max_defrag_page_count);
    else if (name == "max_memory_bytes")            value = std::to_string(c.n_max_memory_bytes);
    else if (name == "max_modifications_before_save") value = std::to_string(c.max_modifications_before_save);
    else if (name == "max_resp_connections")        value = std::to_string(c.max_resp_connections);
    else if (name == "max_scan_iterators")          value = std::to_string(c.max_scan_iterators);
    else if (name == "min_compressed_size")         value = std::to_string(c.min_compressed_size);
    else if (name == "min_fragmentation_ratio")     value = cfg_float(c.min_fragmentation_ratio);
    else if (name == "ordered_keys")                value = cfg_bool(c.ordered_keys);
    else if (name == "hybrid_keys")                 value = cfg_bool(c.hybrid_keys);
    else if (name == "functions_dir")                value = c.functions_dir;
    else if (name == "functions_sync_ms")            value = std::to_string(c.functions_sync_ms);
    else if (name == "functions_git_pull")           value = cfg_bool(c.functions_git_pull);
    else if (name == "functions_git_branch")         value = c.functions_git_branch.empty() ? "main" : c.functions_git_branch;
    else if (name == "functions_git_commit")         value = c.functions_git_commit.empty() ? "off" : c.functions_git_commit;
    else if (name == "functions_git_ssh_key")        value = c.functions_git_ssh_key;
    else if (name == "pre_evict_thresh")            value = cfg_float(c.pre_evict_thresh);
    else if (name == "rpc_client_max_wait_ms")      value = std::to_string(c.rpc_client_max_wait_ms);
    else if (name == "rpc_max_buffer")              value = std::to_string(c.rpc_max_buffer);
    else if (name == "save_interval")               value = std::to_string(c.save_interval);
    else if (name == "server_binding")              value = c.server_binding;
    else if (name == "server_port")                 value = std::to_string(c.server_port);
    else if (name == "static_bloom_filter")         value = cfg_bool(c.static_bloom_filter);
    else if (name == "tls_pem_certificate_chain_file") value = c.tls_pem_certificate_chain_file;
    else if (name == "tls_private_key_file")        value = c.tls_private_key_file;
    else if (name == "tls_tmp_dh_file")             value = c.tls_tmp_dh_file;
    else if (name == "use_vmm_mem")                 value = cfg_bool(c.use_vmm_memory);
    else return false;
    return true;
}

bool barch::get_configuration_value(const std::string& name, std::string& value) {
    if (get_native_configuration_value(name, value)) return true;
    return get_redis_configuration_value(name, value);
}
