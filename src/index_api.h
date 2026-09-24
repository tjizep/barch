//
// INDEX - secondary indexes over the fields of composite keys. See index_api.cpp,
// perm_index.h and TODO 422.
//
#ifndef BARCH_INDEX_API_H
#define BARCH_INDEX_API_H

#include "barch_apis.h"

int INDEX(caller& call, const arg_t& argv);
void register_index_api(function_map& r);

#endif //BARCH_INDEX_API_H
