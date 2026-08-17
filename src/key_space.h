//
// Created by teejip on 10/22/25.
//

#ifndef BARCH_KEY_SPACE_H
#define BARCH_KEY_SPACE_H
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include "../external/include/valkeymodule.h"
#include "abstract_shard.h"
#include "merge_options.h"
#include "range_index.h"
#include "value_type.h"

namespace barch {
    namespace foreign { struct sql_backend; }
    class key_space {
    public:
        typedef std::shared_ptr<key_space> key_space_ptr;
        typedef key_space* key_space_ref;
        bool opt_ordered_keys = barch::get_ordered_keys();
        size_t opt_shard_count = barch::get_shard_count().size();
        /**
         * Route keys to shards by the range they fall in rather than by their hash, so
         * that a shard holds a contiguous span of the key order. Off by default, set
         * per key space through the configuration space as `<name>.range_sharded`, and
         * never on for the default `node` space or for `configuration`.
         *
         * Only meaningful with opt_ordered_keys: an unordered space keeps its keys in a
         * hash table, where a range means nothing. Asking for it on an unordered space
         * is refused when the space is built rather than half honoured.
         *
         * What it selects is `rindex` below, and the two differences that follow from
         * it: a route is a binary search of the boundaries rather than a hash, and a
         * key can change shard while the space is running, which hash routing never
         * does. Everything that routes then locks has to allow for the second - see
         * route_moved.
         */
        bool opt_range_sharded = false;

        enum class foreign_kind { off, mysql, postgres, luau, fake };
        foreign_kind opt_foreign = foreign_kind::off;
        std::string foreign_dsn{};
        std::string foreign_host{};
        std::string foreign_user{};
        std::string foreign_password{};
        std::string foreign_database{};
        std::string foreign_query{};
        std::string foreign_script{};
        std::string luau_bytecode{};
        uint64_t foreign_script_insns{0};
        uint64_t foreign_port{0};
        uint64_t missing_ttl{0};
        uint64_t foreign_timeout_ms{0};
        uint64_t foreign_query_timeout_ms{1000};
        uint64_t foreign_max_inflight{32};
        uint64_t foreign_pool_size{8};
        std::atomic<uint32_t> foreign_inflight{0};
        std::shared_ptr<foreign::sql_backend> sql{};

        std::mutex fake_mu{};
        std::unordered_map<std::string, std::string> fake_source{};
        bool fake_fail{false};
        uint64_t fake_delay_ms{0};
        std::atomic<uint64_t> fake_queries{0};

        [[nodiscard]] bool has_foreign() const { return opt_foreign != foreign_kind::off; }
        [[nodiscard]] const char *foreign_kind_name() const;
        [[nodiscard]] uint64_t waiter_timeout_ms() const;
        [[nodiscard]] uint64_t script_insns() const;
        /** fail in-flight fills and wake waiters before the shards go. */
        void fail_foreign_flights();
    private:
        heap::vector<barch::shard_ptr> shards{};
        /** only read when opt_range_sharded; see range_index.h */
        range_index rindex{};
        decltype(std::chrono::high_resolution_clock::now) start_time;
        std::string name{};
        key_space_ptr src;

        moodycamel::LightweightSemaphore thread_control{};
        moodycamel::LightweightSemaphore thread_exit{};
        std::thread tmaintain{}; // a maintenance thread to perform defragmentation and eviction (if required)
        bool exiting = false;
        std::mutex lock{};

        void start_maintain();
        /** build rindex from the loaded shards, repartitioning them first if they need it */
        void build_range_index();

    public:
        key_space(const std::string &name);
        virtual  ~key_space();
        shard_ptr get_local();
        shard_ptr get(size_t shard);
        shard_ptr get(art::value_type key);
        shard_ptr get(ValkeyModuleString **argv) ;
        shard_ref get_ref(size_t shard);
        shard_ref get_ref(art::value_type key);
        shard_ref get_ref(ValkeyModuleString **argv) ;
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

        // ---- range routing ----

        [[nodiscard]] bool is_range_sharded() const { return opt_range_sharded; }
        /**
         * true if a key can change shard while the space is running. Hash routing is a
         * pure function of the key and so never does; range routing moves boundaries to
         * keep the shards even. Used by the route-then-lock-then-route-again check.
         */
        [[nodiscard]] bool routes_move() const { return opt_range_sharded; }
        /**
         * true if a snapshot or a replace of the shards has to freeze the space.
         *
         * Hash routing has no partition state: a key's shard is a function of the
         * key. Range routing does: the table, and which shard holds which key,
         * change while the space runs. SAVE, LOAD, RELOAD and SAVEALL lock when
         * this is true (DONE 70, 71, 72). A later method that can move a key
         * returns true here and inherits those four freezes. Today only range
         * sharding does.
         */
        [[nodiscard]] bool is_stateful_sharding() const;
        /**
         * true if key no longer belongs to `t`.
         *
         * This is the check that makes routing safe under a moving partition. Route,
         * take the lock, then ask this: a false answer means no rebalance can take the
         * key out of `t` while the lock is held, because moving it needs that same lock.
         * A true answer means the boundary moved in between, and the caller has a lock
         * on the wrong shard and should drop it and route again.
         */
        bool route_moved(art::value_type key, const shard_ptr& t);
        /** the routing table, for the ordered operations that walk shards in key order */
        range_index& routes() { return rindex; }
        bool buffer_insert(const std::string& key, const std::string& value);
        size_t hash_buf_size() const ;
    };
    typedef key_space::key_space_ptr key_space_ptr;
    typedef key_space::key_space_ref key_space_ref;
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
        if (spc)
            locks.reserve(spc->get_shard_count());
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