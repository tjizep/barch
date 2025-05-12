//
// Created by teejip on 4/9/25.
//

#include "module.h"
#include "keys.h"
art::tree *ad{};
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
            //shard->load();
            ++shard_num;
        }
    }
    return shards[s % shards.size()];
}

art::tree *get_art(art::value_type key) {
    size_t hash = ankerl::unordered_dense::detail::wyhash::hash(key.chars(), key.size);
    return get_art (hash % art::get_shard_count().size()) ;
}
art::value_type get_shard(ValkeyModuleString **argv) {
    size_t nlen = 0;
    const char *n = ValkeyModule_StringPtrLen(argv[1], &nlen);
    if (key_ok(n, nlen) != 0) {
        abort_with("invalid shard key");
    }
    art::value_type shard_key{n, nlen};
    return shard_key;
}

art::tree * get_art(ValkeyModuleString **argv) {
    return get_art(get_shard(argv));
}