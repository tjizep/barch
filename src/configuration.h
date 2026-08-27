//
// Created by linuxlite on 3/13/25.
//

#ifndef CONFIGURATION_H
#define CONFIGURATION_H
#include "../external/include/valkeymodule.h"
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include "sastam.h"
/**
* functions provide access to configuration
*/
namespace barch {
    enum compression_type {
        compression_none = 0,
        compression_zstd = 1
    };

    struct configuration_record {
        int compression = compression_none;
        uint64_t n_max_memory_bytes{std::numeric_limits<uint64_t>::max()};
        uint64_t maintenance_poll_delay{80};
        uint64_t max_defrag_page_count{8};
        // how many SCAN cursors one connection may hold open at once
        uint64_t max_scan_iterators{128};
        uint64_t save_interval{3000 * 1000};
        uint64_t max_modifications_before_save{10000000};
        uint64_t rpc_max_buffer{32768*4};
        uint64_t rpc_client_max_wait_ms{30000};
        uint64_t foreign_timeout_ms{300000};
        uint64_t foreign_pool_max_age_ms{30000};
        uint64_t foreign_script_insns{1000000};
        /** a function's instruction slice, and the wall clock bound on a whole call */
        uint64_t function_slice_insns{1000000};
        uint64_t function_deadline_ms{1000};
        /** how deep a chain of nested script calls may go - TODO 98 E */
        uint64_t function_max_depth{100};
        uint64_t rpc_connect_to_s{30};
        uint64_t rpc_read_to_s{30};
        uint64_t rpc_write_to_s{30};
        uint64_t internal_shards{347};//std::thread::hardware_concurrency()*4+3};
        uint64_t server_port{14000};
        uint64_t max_resp_connections{2000};
        std::string server_binding{"0.0.0.0"};

        std::string tls_pem_certificate_chain_file{"server.crt"};
        std::string tls_private_key_file{"server.key"};
        std::string tls_tmp_dh_file{"server.dh"};

        unsigned iteration_worker_count{4};
        float min_fragmentation_ratio = 0.6f;
        double pre_evict_thresh = 0.85;
        uint64_t min_compressed_size {64};
        bool ordered_keys{true};
        bool hybrid_keys{true};
        bool use_vmm_memory{true};
        bool static_bloom_filter{false};
        bool active_defrag{true};
        bool evict_volatile_lru{false};
        bool evict_allkeys_lru{false};
        bool evict_volatile_lfu{false};
        bool evict_allkeys_lfu{false};
        bool evict_volatile_random{false};
        bool evict_allkeys_random{false};
        bool evict_volatile_ttl{false};
        bool log_page_access_trace{false};
        bool use_minimum_threads{false};
        std::string external_host{"localhost"};
        std::string bind_interface{"127.0.0.1"};
        int listen_port{12145};
        /** checkout of luau functions; "off" means the watcher is idle */
        std::string functions_dir{"off"};
        uint64_t functions_sync_ms{0};
        bool functions_git_pull{false};
        std::string functions_git_branch{"main"};
        /** path, file:/path, or env:VAR pointing at a read-only deploy key */
        std::string functions_git_ssh_key{"off"};
    };

    int register_valkey_configuration(ValkeyModuleCtx *ctx);

    const configuration_record& get_configuration();

    // all sizes in bytes, time/delay in milliseconds
    bool get_compression_enabled();

    uint64_t get_max_module_memory();

    uint64_t get_maintenance_poll_delay();

    uint64_t get_save_interval();

    uint64_t get_max_modifications_before_save();

    uint64_t get_max_defrag_page_count();

    uint64_t get_max_scan_iterators();

    uint64_t get_max_resp_connections();

    unsigned get_iteration_worker_count();

    float get_min_fragmentation_ratio();
    double get_pre_evict_thresh();

    uint64_t get_min_compressed_size();

    bool get_active_defrag();

    bool get_evict_volatile_lru();

    bool get_evict_allkeys_lru();

    bool get_evict_volatile_lfu();

    bool get_evict_allkeys_lfu();

    bool get_evict_volatile_random();

    bool get_evict_allkeys_random();

    bool get_evict_volatile_ttl();

    bool get_log_page_access_trace();

    bool get_use_vmm_memory();

    bool get_ordered_keys();

    bool get_hybrid_keys();

    bool get_static_bloom_filter();
    std::string get_functions_dir();
    uint64_t get_functions_sync_ms();
    bool get_functions_git_pull();
    std::string get_functions_git_branch();
    std::string get_functions_git_ssh_key();
    std::string get_tls_pem_certificate_chain_file();
    std::string get_tls_private_key_file();
    std::string get_tls_tmp_dh_file();

    std::string get_eviction_policy();
    /** what SELECT <n> puts before the number to name the space it selects; "db" by default */
    std::string get_db_number_prefix();
    uint64_t get_internal_shards();

    uint64_t get_rpc_max_buffer();

    uint64_t get_rpc_max_client_wait_ms();
    uint64_t get_foreign_timeout_ms();
    uint64_t get_foreign_pool_max_age_ms();
    uint64_t get_foreign_script_insns();
    uint64_t get_function_slice_insns();
    uint64_t get_function_deadline_ms();
    uint64_t get_function_max_depth();
    uint64_t get_server_port();
    std::string get_server_binding();
    std::chrono::seconds get_rpc_connect_to_s();
    std::chrono::seconds get_rpc_read_to_s() ;
    std::chrono::seconds get_rpc_write_to_s() ;
    bool get_use_minimum_threads();
    int set_configuration_value(ValkeyModuleString *name, ValkeyModuleString *value);
    int set_configuration_value(const std::string& name, const std::string &val);
    /**
     * the current value of a configuration variable as text. Read from the live record
     * rather than the reflection strings, because those are only filled in once
     * something has set them - a variable left at its default would read empty.
     * The text is always in a form set_configuration_value accepts back.
     * @return false when there is no such variable
     */
    bool get_configuration_value(const std::string& name, std::string& value);
    /** every configuration variable name, in order */
    const std::vector<std::string>& configuration_names();

    /**
     * Take configuration from the environment. Every setting answers to BARCH_ followed
     * by its name in upper case - max_memory_bytes is BARCH_MAX_MEMORY_BYTES - and the
     * redis names work too, with hyphens written as underscores, so BARCH_MAXMEMORY and
     * BARCH_MAXMEMORY_POLICY are both understood. Values are in the form CONFIG SET
     * takes, so BARCH_MAX_MEMORY_BYTES=100m means what it looks like.
     *
     * Call this after whatever else configures the process, not before: as a valkey
     * module that means after ValkeyModule_LoadConfigs, which applies a registered
     * default to every setting the config file does not mention and would otherwise
     * undo this.
     *
     * @return how many settings were taken from the environment
     */
    size_t apply_environment_configuration();

    /**
     * The redis configuration names barch also answers to. Separate from
     * configuration_names() so that list keeps meaning "barch's own settings" - CONFIG
     * GET walks both.
     */
    const std::vector<std::string>& redis_configuration_names();

    /**
     * Resolve a redis configuration name. Returns false for a name that is not one of
     * them, so a caller can tell "redis calls this something else" from "no such
     * setting". get_configuration_value() already falls through to this, so most
     * callers do not need it directly.
     */
    bool get_redis_configuration_value(const std::string& name, std::string& value);

    /**
     * True for a setting barch reports but cannot change - appendonly, because there is
     * no append only file to turn on. `why` is filled in with a reason fit to send back
     * to whoever asked, so a refusal says something more useful than that it failed.
     */
    bool is_read_only_configuration(const std::string& name, std::string& why);

    const std::vector<size_t>& get_shard_count();
}
#endif //CONFIGURATION_H
