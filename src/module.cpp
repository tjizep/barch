//
// Created by teejip on 4/9/25.
//

#include "module.h"
#include "keys.h"
#include "key_space.h"
thread_local uint64_t stream_write_ctr = 0;
thread_local uint64_t stream_read_ctr = 0;

static std::shared_mutex shared{};
constants Constants{};
static auto ks_node = barch::get_keyspace("");
barch::key_space_ptr& get_default_ks() {
    return ks_node;
}
std::shared_mutex &get_lock() {
    return shared;
}
heap::vector<barch::shard_ptr> get_arts() {
    return ks_node->get_shards();
}
std::shared_ptr<barch::abstract_shard> get_art(size_t s) {
    return ks_node->get(s);
}
size_t get_shard(const char* key, size_t key_len) {
    return ks_node->get_shard_index(key, key_len);
}

size_t get_shard(const std::string& key) {
    return ks_node->get_shard_index(key);
}

size_t get_shard(art::value_type key) {
   return ks_node->get_shard_index(key.chars(),key.size);
}

std::shared_ptr<barch::abstract_shard> get_art(art::value_type key) {
    return ks_node->get(key);
}

size_t get_shard(ValkeyModuleString **argv) {
    return ks_node->get_shard_index(argv);
}

barch::shard_ptr get_art(ValkeyModuleString **argv) {
    return ks_node->get(argv);
}

uint64_t get_total_memory() {
    return heap::allocated;
}
