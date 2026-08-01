//
// Created by teejip on 8/12/25.
//

#ifndef BARCH_INFO_API_H
#define BARCH_INFO_API_H
#include "barch_apis.h"

#include "../external/include/valkeymodule.h"

extern "C" {
    int INFO(caller& call, const arg_t& argv);
    int STATS(caller& call, const arg_t& argv);
    int OPS(caller& call, const arg_t& argv);
    int HEAPBYTES(caller& call, const arg_t& argv);
}

/** register the info commands with the valkey module */
int add_info_api(ValkeyModuleCtx *ctx);

/** register the info commands for RESP, into the table functions_by_name() builds */
void register_info_api(function_map& r);

#endif //BARCH_INFO_API_H
