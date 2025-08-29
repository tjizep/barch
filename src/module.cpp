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
        std::vector<std::thread> loaders{shards.size()};
        size_t shard_num = 0;
        auto start_time = std::chrono::high_resolution_clock::now();
        for (auto &shard : shards) {
            loaders[shard_num] = std::thread([shard_num, &shard]() {
                shard = new(heap::allocate<art::tree>(1)) art::tree(nullptr, 0, shard_num);
                shard->load();
            });
            ++shard_num;
        }
        for (auto &loader : loaders) {
            if (loader.joinable())
                loader.join();
        }
        statistics::shards = shards.size();
        auto end_time = std::chrono::high_resolution_clock::now();
        double millis = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
        art::std_log("Loaded",shards.size(),"shards in", millis/1000.0f, "s");
    }
    if (shards.empty()) {
        abort_with("shard configuration is empty");
    }
    auto r = shards[s % shards.size()];
    r->set_thread_ap();
    return r;
}
size_t get_shard(const char* key, size_t key_len) {
    if (art::get_shard_count().size() == 1) {
        return 0;
    }
    auto shard_key = art::value_type{key,key_len};

    uint64_t hash = ankerl::unordered_dense::detail::wyhash::hash(shard_key.chars(), shard_key.size);

    size_t hshard = hash % art::get_shard_count().size();
    return hshard;
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

uint64_t get_total_memory() {

    return heap::allocated;
}
