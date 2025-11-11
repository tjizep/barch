//
// Created by teejip on 11/11/25.
//

#ifndef BARCH_CONSTANTS_H
#define BARCH_CONSTANTS_H
// constants for rpc
enum {
    rpc_server_version = 21,
    rpc_server_version_min = 21,
    rpc_server_version_max = 21,
    rpc_client_max_wait_default_ms = 30000,
    rpc_io_buffer_size = 1024 * 8,
    rpc_max_param_buffer_size = 1024 * 1024 * 10,
    rpc_resp_asynch_reads = 1,
    rpc_resp_asynch_writes = 0
};
#endif //BARCH_CONSTANTS_H