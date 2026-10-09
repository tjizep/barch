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
    // threads for calls that wait on a Raft commit, and do little else meanwhile:
    // how many replicated writes the node has in flight at once - TODO 626
    raft_wait_workers = 64,
    rpc_io_buffer_size = 1024 * 32,
    // how much of a streamed reply (KEYS) may wait for the client before the
    // worker writing it waits too - TODO 488
    rpc_stream_high_water = 1024 * 256,
    // how far a connection's replies may back up before it stops taking requests,
    // and it picks up again under half of this - TODO 490
    rpc_output_high_water = 1024 * 1024,
    debug_repl = 0,
};
#endif //BARCH_CONSTANTS_H