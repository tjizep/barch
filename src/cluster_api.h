//
// CLUSTER - TODO 610.
//
#ifndef BARCH_CLUSTER_API_H
#define BARCH_CLUSTER_API_H

#include "barch_apis.h"

extern "C" {
    int CLUSTER(caller& call, const arg_t& argv);
}

void register_cluster_api(function_map& r);

#endif //BARCH_CLUSTER_API_H
