//
// Created by teejip on 4/9/25.
//

#ifndef MODULE_H
#define MODULE_H

#include "art.h"
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

std::shared_mutex &get_lock();
void all_shards(const std::function<void(const barch::shard_ptr&)>& cb );
barch::shard_ptr _get_art(size_t shard);
size_t _get_shard(art::value_type key);
size_t _get_shard(const char * k, size_t l);
size_t _get_shard(const std::string& key);
size_t _get_shard(ValkeyModuleString **argv);
barch::shard_ptr  _get_art(ValkeyModuleString **argv);
barch::shard_ptr  _get_art(art::value_type key);
uint64_t get_total_memory();
heap::vector<barch::shard_ptr> get_arts();
barch::key_space_ptr& get_default_ks();
#endif //MODULE_H
