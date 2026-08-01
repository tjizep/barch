//
// Created by teejip on 8/1/26
//

#ifndef CONNECTION_API_H
#define CONNECTION_API_H
#include "../external/include/valkeymodule.h"
#include "barch_apis.h"

/*
 * the connection and session commands: the handshake, per connection state, and
 * the transaction markers a client wraps a batch in.
 */
extern "C" {
    int HELLO(caller& call, const arg_t& argv);
    int CLIENT(caller& call, const arg_t& arg_v);
    int MULTI(caller& call, const arg_t& arg_v);
    int EXEC(caller& call, const arg_t& arg_v);
    int PING(caller& call, const arg_t& argv);
    // COMMAND is not registered for RESP - see the note in register_connection_api
    int COMMAND(caller& call, const arg_t& argv);
}

/** register these commands with the valkey module */
int add_connection_api(ValkeyModuleCtx *ctx);

/** register them for RESP, into the table functions_by_name() builds */
void register_connection_api(function_map& r);

#endif //CONNECTION_API_H
