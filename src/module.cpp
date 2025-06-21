//
// Created by teejip on 4/9/25.
//

#include "module.h"
#include "keys.h"
thread_local uint64_t stream_write_ctr = 0;
thread_local uint64_t stream_read_ctr = 0;

static std::shared_mutex shared{};
constants Constants{};
static std::vector<art::tree *> shards{};
std::shared_mutex &get_lock() {
    return shared;
}

art::tree *get_art(size_t s) {
    if (shards.empty()) {
        shards.resize(art::get_shard_count().size());
        size_t shard_num = 0;
        for (auto &shard : shards) {
            shard = new(heap::allocate<art::tree>(1)) art::tree(nullptr, 0, shard_num);
            shard->load();
            ++shard_num;
        }
        statistics::shards = shards.size();
    }
    if (shards.empty()) {
        abort_with("shard configuration is empty");
    }
    return shards[s % shards.size()];
}
size_t get_shard(const char* key, size_t key_len) {
    if (art::get_shard_count().size() == 1) {
        return 0;
    }
    auto converted = conversion::convert(key, key_len);
    auto shard_key = converted.get_value();

    uint64_t hash = 0; //ankerl::unordered_dense::detail::wyhash::hash(key, key_len);

    memcpy(&hash, shard_key.bytes, std::min<uint64_t>(8,shard_key.size));
    return hash % art::get_shard_count().size();
}

size_t get_shard(const std::string& key) {
   return get_shard(key.c_str(), key.size());
}

size_t get_shard(art::value_type key) {
   return get_shard(key.chars(), key.size);
}
art::tree* get_art(art::value_type key) {
    return get_art(get_shard(key.chars(), key.size));
}
size_t get_shard(ValkeyModuleString **argv) {
    size_t nlen = 0;
    const char *n = ValkeyModule_StringPtrLen(argv[1], &nlen);
    if (key_ok(n, nlen) != 0) {
        abort_with("invalid shard key");
    }
   return get_shard(n,nlen);
}

art::tree * get_art(ValkeyModuleString **argv) {
    return get_art(get_shard(argv));
}