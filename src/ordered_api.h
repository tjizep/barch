//
// Created by teejip on 4/9/25.
//

#ifndef ORDERED_API_H
#define ORDERED_API_H
#include "../external/include/valkeymodule.h"
#include "barch_apis.h"

extern "C" {
    int ZADD(caller& call, const arg_t &argv);
    int ZREM(caller& call, const arg_t& argv);
    int ZINCRBY(caller& call, const arg_t& argv);
    int ZRANGE(caller& call, const arg_t& argv);
    int ZCARD(caller& call, const arg_t& argv);
    int ZCOUNT(caller& call, const arg_t& argv);
    int ZDIFF(caller& call, const arg_t& argv);
    int ZDIFFSTORE(caller& call, const arg_t& argv);
    int ZINTERSTORE(caller& call, const arg_t& argv);
    int ZINTERCARD(caller& call, const arg_t& argv);
    int ZINTER(caller& call, const arg_t& argv);
    int ZPOPMIN(caller& call, const arg_t& argv);
    int ZPOPMAX(caller& call, const arg_t& argv);
    int ZMPOP(caller& call, const arg_t& argv);
    int BZMPOP(caller& call, const arg_t& argv);
    int ZREVRANGE(caller& call, const arg_t& argv);
    int ZRANGEBYSCORE(caller& call, const arg_t& argv);
    int ZREVRANGEBYSCORE(caller& call, const arg_t& argv);
    int ZREMRANGEBYLEX(caller& call, const arg_t& argv);
    int ZRANGEBYLEX(caller& call, const arg_t& argv);
    int ZREVRANGEBYLEX(caller& call, const arg_t& argv);
    int ZRANK(caller& call, const arg_t& argv);
    int ZFASTRANK(caller& call, const arg_t& argv);
}

/** register the ordered set commands with the valkey module */
int add_ordered_api(ValkeyModuleCtx *ctx);

/** register the ordered set commands for RESP, into the table functions_by_name() builds */
void register_ordered_api(function_map& r);

#endif //ORDERED_API_H
