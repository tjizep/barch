//
// Locks in a space replicated with Raft - TODO 614, docs/CLUSTERING.md.
//
#ifndef BARCH_LOCK_API_H
#define BARCH_LOCK_API_H

#include "barch_apis.h"

extern "C" {
    int LOCK(caller& call, const arg_t& argv);
    int UNLOCK(caller& call, const arg_t& argv);
    int LOCKINFO(caller& call, const arg_t& argv);
}

void register_lock_api(function_map& r);

#endif //BARCH_LOCK_API_H
