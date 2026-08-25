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

/**
 * A user's rights in one key space, as the differences from their global ones.
 *
 * Stored per category rather than as a whole vector, because that is what it means:
 * `KSPACE ACL KS1 SETUSER alice -write` says "as alice, but no writing here". A space
 * with no entry leaves the user exactly as they are, so nothing that works today
 * changes. See TODO 135.
 */
namespace barch {
    typedef heap::string_map<heap::string_map<bool>> space_overrides;

    /** every per-space override this user holds, read at AUTH */
    space_overrides read_space_overrides(const std::string& user);

    /** the rights a user has in one space: their global vector with the overrides on top */
    heap::vector<bool> apply_overrides(const heap::vector<bool>& global,
                                       const heap::string_map<bool>& over);
    /** write one space's overrides for a user; an empty map removes them */
    void write_space_overrides(const std::string& user, const std::string& space,
                               const heap::string_map<bool>& cats);
}
void save_auth();

/** register the auth commands for RESP, into the table functions_by_name() builds */
void register_auth_api(function_map& r);
#endif //AUTH_API_H
