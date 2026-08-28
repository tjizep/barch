//
// Created by teejip on 8/23/26.
//

#ifndef FUNCTION_API_H
#define FUNCTION_API_H
#include "caller.h"
#include "barch_apis.h"
#include "foreign/driver.h"
#include "key_space.h"
#include "sastam.h"

/*
 * Stored Luau functions. A function is an ordinary key under art::tfunction, so it
 * persists, replicates and exports like anything else, and the key space it lives in
 * is the one the command ran in - `space:SETF` or `USE space` then `SETF`.
 *
 * These are deliberately not a flag on SET and REM. The redis clones stay
 * bit-compatible that way, and more importantly defining a function is code, so it
 * carries its own ACL category rather than riding on "may write a string".
 */
extern "C" {
    int SETF(caller& call, const arg_t& argv);
    int GETF(caller& call, const arg_t& argv);
    int REMF(caller& call, const arg_t& argv);
    int KEYSF(caller& call, const arg_t& argv);
    int CALLF(caller& call, const arg_t& argv);
    int FUNCTIONS(caller& call, const arg_t& argv);
}

namespace barch {
namespace functions {
    /**
     * Find a stored function to answer for `name` and, if there is one, hand back
     * something the caller can run like any other command.
     *
     * `from_space` is the part before the dot in `KS1.PRINT_NAME`, empty when the name
     * carried none. An empty one resolves in the selected space and then in
     * the default space, where the globals live. A dotted name is always this
     * lookup, even when the name is a builtin - `HNSW.SET` is the stored function,
     * `HNSW:SET` is SET. See TODO 160.
     */
    /**
     * Returns what to run for `name`, or null if there is no such function.
     *
     * The answer is kept on the connection, so a name called twice is resolved once -
     * the lookup that decides a bare name is a function was the largest single thing
     * a call did. A pointer rather than a value, so the caller does not copy a
     * std::function per call either. See TODO 98 F5.
     */
    const barch_function* resolve(caller& call, const std::string& from_space,
                                  const std::string& name);

    /**
     * the source stored for `name`, looked up in `space` and then in the global one.
     *
     * Exposed for the foreign fill path, which names a function in its configuration
     * rather than carrying the script there - see TODO 139. False means no function
     * of that name, which is not the same as an error.
     */
    bool source_of(const barch::key_space_ptr& space, const std::string& name,
                   std::string& out);

    /**
     * the same store_access `barch.store` and `barch.space.NAME` use.
     * `store_for_owner` skips ACL - a private scratch space has no user.
     */
    barch::foreign::store_access store_for(const barch::key_space_ptr& space,
                                           const heap::vector<bool>& acl);
    barch::foreign::store_access store_for_owner(const barch::key_space_ptr& space);

    /** SETF/REMF/KEYSF without a client. false fills err and writes nothing. */
    bool install(const barch::key_space_ptr& space, const std::string& name,
                 const std::string& source, std::string& err);
    bool remove(const barch::key_space_ptr& space, const std::string& name);
    heap::vector<std::string> names(const barch::key_space_ptr& space);
    bool source_in(const barch::key_space_ptr& space, const std::string& name,
                   std::string& out);
}
}

/** register the function commands for RESP, into the table functions_by_name() builds */
void register_function_api(function_map& r);

#endif //FUNCTION_API_H
