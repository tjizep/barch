//
// Created by teejip on 4/9/25.
//

#ifndef MODULE_H
#define MODULE_H

#include "art.h"
#include "shard.h"
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
barch::shard *get_art(size_t shard);
size_t get_shard(barch::value_type key);
size_t get_shard(const char * k, size_t l);
size_t get_shard(const std::string& key);
size_t get_shard(ValkeyModuleString **argv);
barch::shard * get_art(ValkeyModuleString **argv);
barch::shard * get_art(barch::value_type key);
uint64_t get_total_memory();
std::vector<barch::shard *>& get_arts();
#endif //MODULE_H
