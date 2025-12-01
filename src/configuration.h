//
// Created by linuxlite on 3/13/25.
//

#ifndef CONFIGURATION_H
#define CONFIGURATION_H
#include "valkeymodule.h"
#include <limits>
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
        uint64_t max_defrag_page_count{6};
        uint64_t save_interval{30 * 1000};
        uint64_t max_modifications_before_save{100000};
        uint64_t rpc_max_buffer{32768*4};
        uint64_t rpc_client_max_wait_ms{30000};
        uint64_t rpc_connect_to_s{30};
        uint64_t rpc_read_to_s{30};
        uint64_t rpc_write_to_s{30};
        uint64_t internal_shards{512};
        uint64_t server_port{14000};
        std::string server_binding{"0.0.0.0"};

        unsigned iteration_worker_count{4};
        float min_fragmentation_ratio = 0.6f;
        uint64_t min_compressed_size = {64};
        bool ordered_keys{true};
        bool use_vmm_memory{true};
        bool active_defrag{true};
        bool evict_volatile_lru{false};
        bool evict_allkeys_lru{false};
        bool evict_volatile_lfu{false};
        bool evict_allkeys_lfu{false};
        bool evict_volatile_random{false};
        bool evict_allkeys_random{false};
        bool evict_volatile_ttl{false};
        bool log_page_access_trace{false};
        std::string external_host{"localhost"};
        std::string bind_interface{"127.0.0.1"};
        int listen_port{12145};
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

    unsigned get_iteration_worker_count();

    float get_min_fragmentation_ratio();

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

    uint64_t get_internal_shards();

    uint64_t get_rpc_max_buffer();

    int64_t get_rpc_max_client_wait_ms();
    uint64_t get_server_port();
    std::chrono::seconds get_rpc_connect_to_s();
    std::chrono::seconds get_rpc_read_to_s() ;
    std::chrono::seconds get_rpc_write_to_s() ;
    int set_configuration_value(ValkeyModuleString *name, ValkeyModuleString *value);
    int set_configuration_value(const std::string& name, const std::string &val);

    const std::vector<size_t>& get_shard_count();
}
#endif //CONFIGURATION_H
