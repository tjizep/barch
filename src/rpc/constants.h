//
// Created by teejip on 11/11/25.
//

#ifndef BARCH_CONSTANTS_H
#define BARCH_CONSTANTS_H
// constants for rpc
enum {
    rpc_server_version = 21,
    rpc_max_param_buffer_size = 1024 * 1024 * 10,
    asynch_proccess_workers = 4,
    rpc_io_buffer_size = 1024 * 32,
    debug_repl = 0,
};
#endif //BARCH_CONSTANTS_H