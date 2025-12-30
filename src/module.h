//
// Created by teejip on 4/9/25.
//

#ifndef MODULE_H
#define MODULE_H

#include "art/art.h"
#include "abstract_shard.h"
#include "key_space.h"
#define NAME(x) "B." #x , cmd_##x

struct constants {
    ValkeyModuleString *OK = nullptr;
    ValkeyModuleString *FIELDS = nullptr;

    void init(ValkeyModuleCtx *ctx) {
        OK = ValkeyModule_CreateString(ctx, "OK", 2);
        FIELDS = ValkeyModule_CreateString(ctx, "FIELDS", 6);
    }
};

extern constants Constants;

void all_shards(const std::function<void(const barch::shard_ptr&)>& cb );
uint64_t get_total_memory();
heap::vector<barch::shard_ptr> get_arts();
barch::key_space_ptr& get_default_ks();
#endif //MODULE_H
