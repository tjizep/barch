//
// GRAPH - the graph store over RESP. See graph_api.cpp and TODO 382.
//
#ifndef BARCH_GRAPH_API_H
#define BARCH_GRAPH_API_H

#include "barch_apis.h"

int GRAPH(caller& call, const arg_t& argv);
int cmd_GRAPH(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc);
void register_graph_api(function_map& r);

#endif //BARCH_GRAPH_API_H
