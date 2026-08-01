//
// Created by teejip on 8/1/26.
//

#ifndef KEYS_API_H
#define KEYS_API_H
#include "../external/include/valkeymodule.h"
#include "barch_apis.h"

extern "C" {
    int SET(caller& call, const arg_t& argv);
    int APPEND(caller& call, const arg_t& argv);
    int PREPEND(caller& call, const arg_t& argv);
    int KEYS(caller& call, const arg_t& argv);
    int VALUES(caller& call, const arg_t& argv);
    int INCR(caller& call, const arg_t& argv);
    int INCRBY(caller& call, const arg_t& argv);
    int UINCRBY(caller& call, const arg_t& argv);
    int DECR(caller& call, const arg_t& argv);
    int DECRBY(caller& call, const arg_t& argv);
    int UDECRBY(caller& call, const arg_t& argv);
    int EXISTS(caller& call, const arg_t& argv);
    int EXPIRE(caller& call, const arg_t& argv);
    int MSET(caller& call, const arg_t& argv);
    int ADD(caller& call, const arg_t& argv);
    int GET(caller& call, const arg_t& argv);
    int SCAN(caller& call, const arg_t& argv);
    int LENGTH(caller& call, const arg_t& argv);
    int MGET(caller& call, const arg_t& argv);
    int MIN(caller& call, const arg_t& argv);
    int MAX(caller& call, const arg_t& );
    int LB(caller& call, const arg_t& argv);
    int UB(caller& call, const arg_t& argv);
    int RANGE(caller& call, const arg_t& argv);
    int COUNT(caller& call, const arg_t& argv);
    int REM(caller& call, const arg_t& argv);
    int TTL(caller& call, const arg_t& argv);
}

/** register the key commands with the valkey module */
int add_keys_api(ValkeyModuleCtx *ctx);

/** register the key commands for RESP, into the table functions_by_name() builds */
void register_keys_api(function_map& r);

#endif //KEYS_API_H
