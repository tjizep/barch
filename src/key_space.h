//
// Created by teejip on 10/22/25.
//

#ifndef BARCH_KEY_SPACE_H
#define BARCH_KEY_SPACE_H
#include <string>
#include "valkeymodule.h"
#include "abstract_shard.h"
#include "value_type.h"
#include "moodycamel/blockingconcurrentqueue.h"
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

        void start_maintain();
    public:
        key_space(const std::string &name);
        virtual  ~key_space();
        shard_ptr get(size_t shard);
        shard_ptr get(art::value_type key);
        shard_ptr get(ValkeyModuleString **argv) ;
        [[nodiscard]] std::string get_name() const;
        [[nodiscard]] std::string get_canonical_name() const;
        const heap::vector<shard_ptr>& get_shards() ;
        size_t get_shard_index(const char* key, size_t key_len);
        size_t get_shard_index(art::value_type key);
        size_t get_shard_index(const std::string& key);
        size_t get_shard_index(ValkeyModuleString **argv) ;
        void depends(const key_space_ptr& dependant);
        key_space_ptr source() const;
        void merge(key_space_ptr into);
        void merge();
        void each_shard(std::function<void(shard_ptr)> f);
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
    ordered_lock(const ordered_lock&) = delete;
    ordered_lock(ordered_lock&&) = default;
    ordered_lock& operator=(const ordered_lock&) = delete;
    ~ordered_lock() {
        while (!locks.empty()) {
            locks.pop_back();
        }
    };
    void lock(const barch::shard_ptr& t) {
        locks.emplace_back(t);
    }
    void lock_space(barch::key_space_ptr& spc) {
        release();
        for (auto s : spc->get_shards()) {
            locks.emplace_back(s);
        }
    }

    void release() {
        while (!locks.empty()) {
            locks.pop_back();
        }
    }
};

#endif //BARCH_KEY_SPACE_H