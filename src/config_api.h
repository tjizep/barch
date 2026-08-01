//
// Created by teejip on 8/1/26
//

#ifndef CONFIG_API_H
#define CONFIG_API_H
#include "../external/include/valkeymodule.h"
#include "barch_apis.h"

/*
 * the configuration commands. TRAIN sits here rather than with the data commands
 * because what it changes is a server wide setting - the compression dictionary.
 */
extern "C" {
    int CONFIG(caller& call, const arg_t& argv);
    int TRAIN(caller& call, const arg_t& argv);
}

/** register these commands with the valkey module */
int add_config_api(ValkeyModuleCtx *ctx);

/** register them for RESP, into the table functions_by_name() builds */
void register_config_api(function_map& r);

#endif //CONFIG_API_H
