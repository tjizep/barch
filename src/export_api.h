//
// Created by teejip on 8/10/26.
//
// A logical export: the data written out as the commands that would put it back, rather
// than as the pages it happens to live in. See DONE 61.
//
#ifndef BARCH_EXPORT_API_H
#define BARCH_EXPORT_API_H
#include "barch_apis.h"

extern "C" {
    int EXPORT(caller& call, const arg_t& argv);
    int IMPORT(caller& call, const arg_t& argv);
}

int cmd_EXPORT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc);
int cmd_IMPORT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc);

void register_export_api(function_map& r);

#endif //BARCH_EXPORT_API_H
