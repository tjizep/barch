//
// Created by teejip on 7/7/25.
//

#ifndef LIST_API_H
#define LIST_API_H
#include "barch_apis.h"

extern "C" {
    int LBACK(caller& cc, const arg_t& args);
    int LFRONT(caller& cc, const arg_t& args);
    int LPUSH(caller& cc, const arg_t& args);
    int RPUSH(caller& cc, const arg_t& args);
    int LPOP(caller& cc, const arg_t& args);
    int RPOP(caller& cc, const arg_t& args);
    int LLEN(caller& cc, const arg_t& args);
    int BLPOP(caller& cc, const arg_t& args);
    int BRPOP(caller& cc, const arg_t& args);
}

/**
 * register the list commands for RESP, into the table functions_by_name() builds.
 * there is no valkey module registration for lists - they are reachable over RESP only.
 */
void register_list_api(function_map& r);

#endif //LIST_API_H
