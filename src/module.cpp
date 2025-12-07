//
// Created by teejip on 4/9/25.
//

#include "module.h"
#include "keys.h"
#include "key_space.h"
thread_local uint64_t stream_write_ctr = 0;
thread_local uint64_t stream_read_ctr = 0;
static std::shared_mutex& get_mut() {
    static std::shared_mutex shared{};
    return shared;
}
constants Constants{};
barch::key_space_ptr& get_default_ks() {
    static auto ks_node = barch::get_keyspace("");

    return ks_node;
}
std::shared_mutex &get_lock() {
    return get_mut();
}
heap::vector<barch::shard_ptr> get_arts() {
    return get_default_ks()->get_shards();
}
std::shared_ptr<barch::abstract_shard> get_art(size_t s) {
    return get_default_ks()->get(s);
}
size_t get_shard(const char* key, size_t key_len) {
    return get_default_ks()->get_shard_index(key, key_len);
}

size_t get_shard(const std::string& key) {
    return get_default_ks()->get_shard_index(key);
}

size_t get_shard(art::value_type key) {
   return get_default_ks()->get_shard_index(key.chars(),key.size);
}

std::shared_ptr<barch::abstract_shard> get_art(art::value_type key) {
    return get_default_ks()->get(key);
}

size_t get_shard(ValkeyModuleString **argv) {
    return get_default_ks()->get_shard_index(argv);
}

barch::shard_ptr get_art(ValkeyModuleString **argv) {
    return get_default_ks()->get(argv);
}

uint64_t get_total_memory() {
    return heap::allocated;
}
