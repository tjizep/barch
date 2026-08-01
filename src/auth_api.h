//
// Created by teejip on 7/26/25.
//

#ifndef AUTH_API_H
#define AUTH_API_H
#include <string>

#include "sastam.h"

#include "barch_apis.h"

extern "C" {
    int AUTH(caller& call, const arg_t& argv);
    int ACL(caller& call, const arg_t& argv);
}

const heap::vector<bool>& get_all_acl();
void save_auth();

/** register the auth commands for RESP, into the table functions_by_name() builds */
void register_auth_api(function_map& r);
#endif //AUTH_API_H
