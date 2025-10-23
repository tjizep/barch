//
// Created by teejip on 10/22/25.
//

#include "key_space.h"
#include <thread>
#include "queue_server.h"
#include "shard.h"
#include "keys.h"

namespace barch {
    static std::mutex lock{};
    static heap::map<std::string, key_space_ptr> spaces{};

    key_space_ptr get_keyspace(const std::string &name) {

        std::unique_lock l(lock);
        auto s = spaces.find(name);
        if (s != spaces.end()) {
            return s->second;
        }
        heap::allocator<key_space> alloc;
        // cannot create keyspace without memory
        auto ks = std::allocate_shared<key_space>(alloc, name);
        spaces[name] = ks;
        return ks;
    }

    bool flush_keyspace(const std::string& name) {
        key_space_ptr removed;
        {
            std::unique_lock l(lock);
            auto s = spaces.find(name);
            if (s != spaces.end()) {
                removed = s->second;
                spaces.erase(s);
            }
        }
        return removed != nullptr; // destruction happens in callers thread - so hopefully no dl because shared ptr
    }
    key_space::key_space(const std::string &name) :name(name) {
        if (shards.empty()) {
            decltype(shards) shards_out;
            shards_out.resize(barch::get_shard_count().size());
            std::vector<std::thread> loaders{shards_out.size()};
            size_t shard_num = 0;
            auto start_time = std::chrono::high_resolution_clock::now();
            heap::allocator<barch::shard> alloc;
            for (auto &shard : shards_out) {
                loaders[shard_num] = std::thread([shard_num, &shard, &alloc, name]() {
                    shard = std::allocate_shared<barch::shard>(alloc,  name, 0, shard_num);
                    shard->load(true);
                });
                ++shard_num;
            }
            for (auto &loader : loaders) {
                if (loader.joinable())
                    loader.join();
            }
            statistics::shards = shards_out.size();
            auto end_time = std::chrono::high_resolution_clock::now();
            double millis = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
            barch::std_log("Loaded",shards.size(),"shards in", millis/1000.0f, "s");
            start_queue_server() ;
            shards.swap(shards_out);
        }
    }
    std::shared_ptr<abstract_shard> key_space::get(size_t shard) {

        if (shards.empty()) {
            abort_with("shard configuration is empty");
        }
        auto r = shards[shard % shards.size()];
        if (r == nullptr) {
            abort_with("shard not found");
        }
        r->set_thread_ap();
        return r;
    }
    size_t key_space::get_shard_index(const char* key, size_t key_len) {
        if (barch::get_shard_count().size() == 1) {
            return 0;
        }
        auto shard_key = art::value_type{key,key_len};

        uint64_t hash = ankerl::unordered_dense::detail::wyhash::hash(shard_key.chars(), shard_key.size);

        size_t hshard = hash % barch::get_shard_count().size();
        return hshard;
    }
    size_t key_space::get_shard_index(const std::string& key) {
        return get_shard_index(key.c_str(), key.size());
    }
    size_t key_space::get_shard_index(ValkeyModuleString **argv) {
        size_t nlen = 0;
        const char *n = ValkeyModule_StringPtrLen(argv[1], &nlen);
        if (key_ok(n, nlen) != 0) {
            abort_with("invalid shard key");
        }
        return get_shard_index(n,nlen);
    }
    shard_ptr key_space::get(ValkeyModuleString **argv) {
        return get(get_shard_index(argv));
    }
    shard_ptr key_space::get(art::value_type key) {
        return get(get_shard_index(key.chars(), key.size));
    }
    [[nodiscard]] std::string key_space::get_name() const {
        return name;
    };
    heap::vector<shard_ptr> key_space::get_shards() {
        return shards;
    };
} // barch