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
/**
 * memory taken up by barch's own structures. it is accumulated as each key space is constructed
 * and loaded so it covers the shards, their arenas and anything persisted that came back with
 * them, but not what gets written afterwards - the analogue of redis's used_memory_startup
 */
void add_startup_memory(uint64_t bytes);
uint64_t get_startup_memory();
heap::vector<barch::shard_ptr> get_arts();
barch::key_space_ptr& get_default_ks();
#endif //MODULE_H
