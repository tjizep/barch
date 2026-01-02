//
// Created by teejip on 10/22/25.
//

#ifndef BARCH_KEY_SPACE_H
#define BARCH_KEY_SPACE_H
#include <string>
#include "../external/include/valkeymodule.h"
#include "abstract_shard.h"
#include "merge_options.h"
#include "value_type.h"

namespace barch {
    class key_space {
    public:
        typedef std::shared_ptr<key_space> key_space_ptr;

    private:
        heap::vector<shard_ptr> shards{};
        decltype(std::chrono::high_resolution_clock::now) start_time;
        std::string name{};
        key_space_ptr src;

        moodycamel::LightweightSemaphore thread_control{};
        moodycamel::LightweightSemaphore thread_exit{};
        std::thread tmaintain{}; // a maintenance thread to perform defragmentation and eviction (if required)
        bool exiting = false;
        void start_maintain();
    public:
        key_space(const std::string &name);
        virtual  ~key_space();
        shard_ptr get_local();
        shard_ptr get(size_t shard);
        shard_ptr get(art::value_type key);
        shard_ptr get_type_aware(art::value_type key);
        shard_ptr get(ValkeyModuleString **argv) ;
        [[nodiscard]] std::string get_name() const;
        [[nodiscard]] std::string get_canonical_name() const;
        const heap::vector<shard_ptr>& get_shards() ;
        size_t get_shard_index(const char* key, size_t key_len);
        size_t get_shard_index(art::value_type key);
        size_t get_shard_index(const std::string& key);
        size_t get_shard_index(ValkeyModuleString **argv) ;
        void depends(const key_space_ptr& dependant);
        [[nodiscard]] key_space_ptr source() const;
        void merge(key_space_ptr into, merge_options options);
        void merge(merge_options options);
        void each_shard(std::function<void(shard_ptr)> f);
        [[nodiscard]] size_t get_shard_count() const;
    };
    typedef key_space::key_space_ptr key_space_ptr;
    const std::string& get_ks_pattern_error();
    bool is_keyspace(const std::string& name_);
    bool check_ks_name(const std::string& name_);
    std::string ks_undecorate(const std::string& name);
    key_space_ptr get_keyspace(const std::string &name);
    void all_shards(const std::function<void(const shard_ptr&)>& cb );
    bool flush_keyspace(const std::string& name);
    bool unload_keyspace(const std::string& name);
    void all_spaces(const std::function<void(const std::string& name, const barch::key_space_ptr&)>& cb );
} // barch
template<typename Locker>
struct ordered_lock {
    heap::vector<Locker> locks;
    ordered_lock() {
        locks.reserve(barch::get_shard_count().size());
    };
    explicit ordered_lock(const barch::shard_ptr& t) {
        locks.reserve(barch::get_shard_count().size());
        lock(t);
    }
    explicit ordered_lock(const barch::key_space_ptr& spc) {
        locks.reserve(barch::get_shard_count().size());
        lock_space(spc);
    }
    ordered_lock(const ordered_lock&) = delete;
    ordered_lock(ordered_lock&&) = default;
    ordered_lock& operator=(const ordered_lock&) = delete;
    ordered_lock& operator=(ordered_lock&&) = default;
    ~ordered_lock() {
       release();
    };
    void lock(const barch::shard_ptr& t) {
        release();
        locks.emplace_back(t);
    }
    void lock_space(const barch::key_space_ptr& spc) {
        release();
        if (!spc) return;
        for (auto s : spc->get_shards()) {
            locks.emplace_back(s);
        }
    }

    void release() {
        while (!locks.empty()) {
            locks.pop_back();
        }
        locks.clear();
    }
};

#endif //BARCH_KEY_SPACE_H