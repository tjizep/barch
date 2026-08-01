//
// Created by teejip on 4/9/25.
//

#ifndef HASH_API_H
#define HASH_API_H

#include "../external/include/valkeymodule.h"
#include "barch_apis.h"

extern "C" {
    int HSET(caller& cc, const arg_t& args);
    int HEXPIREAT(caller& call, const arg_t& args);
    int HEXPIRE(caller& call, const arg_t& args);
    // HGETEX and HQUERY are implemented but not registered for RESP - see the note in
    // register_hash_api. HUPDATEEX is not a command at all: it is the helper HGETEX
    // drives, and takes arguments a barch_function does not have.
    //int HGETEX(caller& call, const arg_t &argv);
    int HMGET(caller& call, const arg_t& argv);
    int HINCRBY(caller& call, const arg_t &argv);
    int HINCRBYFLOAT(caller& call, const arg_t &argv);
    int HDEL(caller& call, const arg_t &argv);
    int HGETDEL(caller& call, const arg_t &argv);
    int HTTL(caller& call, const arg_t& argv);
    int HGET(caller& call, const arg_t& argv);
    int HLEN(caller& call, const arg_t& argv);
    int HEXPIRETIME(caller& call, const arg_t& argv);
    int HGETALL(caller& call, const arg_t& argv);
    int HKEYS(caller& call, const arg_t& argv);
    int HEXISTS(caller& call, const arg_t& argv);
}

/** register the hash commands with the valkey module */
int add_hash_api(ValkeyModuleCtx *ctx);

/** register the hash commands for RESP, into the table functions_by_name() builds */
void register_hash_api(function_map& r);

#endif //HASH_API_H
