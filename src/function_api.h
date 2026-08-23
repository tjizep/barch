//
// Created by teejip on 8/23/26.
//

#ifndef FUNCTION_API_H
#define FUNCTION_API_H
#include "caller.h"
#include "barch_apis.h"

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
}

/** register the function commands for RESP, into the table functions_by_name() builds */
void register_function_api(function_map& r);

#endif //FUNCTION_API_H
