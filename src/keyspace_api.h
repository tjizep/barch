//
// Created by teejip on 8/1/26
//

#ifndef KEYSPACE_API_H
#define KEYSPACE_API_H
#include "../external/include/valkeymodule.h"
#include "barch_apis.h"

/*
 * the key space and database commands: choosing a space, its dependencies and
 * options, its size, and saving, clearing and transacting over it.
 */
extern "C" {
    int USE(caller& call, const arg_t& argv);
    int UNLOAD(caller& call, const arg_t& argv);
    int SPACES(caller& call, const arg_t& argv);
    int KSPACE(caller& call, const arg_t& argv);
    int KSOPTIONS(caller& call, const arg_t& argv);
    // size in the current key space, and across every one of them
    int SIZE(caller& call, const arg_t& argv);
    int SIZEALL(caller& call, const arg_t& argv);
    int SAVE(caller& call, const arg_t& argv);
    int SAVEALL(caller& call, const arg_t& argv);
    int CLEAR(caller& call, const arg_t& argv);
    int CLEARALL(caller& call, const arg_t& argv);
    // transaction markers - registered with the valkey module only
    int BEGIN(caller& call, const arg_t& argv);
    int COMMIT(caller& call, const arg_t& argv);
    int ROLLBACK(caller& call, const arg_t& argv);
}

/** register these commands with the valkey module */
int add_keyspace_api(ValkeyModuleCtx *ctx);

/** register them for RESP, into the table functions_by_name() builds */
void register_keyspace_api(function_map& r);

#endif //KEYSPACE_API_H
