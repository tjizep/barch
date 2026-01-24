//
// Created by teejip on 10/21/25.
//

#ifndef BARCH_ABSTRACT_SHARD_H
#define BARCH_ABSTRACT_SHARD_H
#include <memory>
#include <shared_mutex>
#include "art/art.h"
#include "art/key_options.h"
#include "merge_options.h"
#include "rpc/abstract_session.h"

namespace barch {
    class abstract_shard : public std::enable_shared_from_this<abstract_shard>{
    public:
        typedef std::shared_ptr<abstract_shard> shard_ptr;
        typedef std::vector<bool> bloom_t;
        bloom_t bloom{};
        void add_bloom(art::value_type key) {
            if (static_bloom_size != bloom.size()) return;
            uint64_t ash = ankerl::unordered_dense::detail::wyhash::hash(key.chars(), key.size);
            bool bval = bloom[ash % static_bloom_size];
            if (!bval)
                bloom[ash % static_bloom_size] = true;
        }


        bool is_bloom(art::value_type key) const {
            if (static_bloom_size != bloom.size()) return true; // yes we assume the key exists
            uint64_t ash = ankerl::unordered_dense::detail::wyhash::hash(key.chars(), key.size);
            return bloom[ash % static_bloom_size] ;
        }
        void create_bloom(bool enable) {
            bloom_t ebl;
            bloom = std::move(ebl);
            if (enable) {
                opt_static_bloom_filter = true;
                bloom.resize(static_bloom_size);
            }else {
                opt_static_bloom_filter = false;
            }

        }
    private:
        bool opt_static_bloom_filter = barch::get_static_bloom_filter();
    public:
        bool has_static_bloom_filter() const {
            return opt_static_bloom_filter;
        }
        bool opt_ordered_keys = barch::get_ordered_keys();
        bool opt_evict_all_keys_lru = barch::get_evict_allkeys_lru();
        bool opt_evict_all_keys_lfu = barch::get_evict_allkeys_lfu();
        bool opt_evict_all_keys_random = barch::get_evict_allkeys_random();
        bool opt_evict_volatile_keys_lru = barch::get_evict_volatile_lru();
        bool opt_evict_volatile_keys_lfu = barch::get_evict_volatile_lfu();
        bool opt_evict_volatile_keys_random = false; //barch::get_evict_volatile_lfu();
        bool opt_evict_volatile_ttl = barch::get_evict_volatile_ttl();
        bool opt_active_defrag = barch::get_active_defrag();
        bool opt_drop_on_release = false;
        uint64_t lock_to_ms = 1*1000*60;

        void lock_shared() {
            if (!get_latch().try_lock_shared_for(std::chrono::milliseconds(lock_to_ms))) {
                throw_exception<std::runtime_error>("read lock wait time exceeded");
            }

        }
        void lock_unique() {
            if (!get_latch().try_lock_for(std::chrono::milliseconds(lock_to_ms))) {
                throw_exception<std::runtime_error>("write lock wait time exceeded");
            }
        }
        void unlock_shared() {
            get_latch().unlock_shared();
        }
        void unlock_unique() {
            get_latch().unlock();
        }
        abstract_shard() = default;

        virtual ~abstract_shard() = default;
        virtual bool remove_leaf_from_uset(art::value_type key) = 0;
        virtual heap::shared_mutex& get_latch() = 0;
        virtual void set_thread_ap() = 0;
        virtual bool publish(std::string host, int port) = 0;
        virtual uint64_t get_tree_size() const = 0;
        virtual uint64_t get_size() const = 0;
        virtual uint64_t get_hash_size() const = 0;
        virtual void maintenance() = 0;
        virtual void load_bloom() = 0;
        virtual uint64_t bytes_in_free_list() = 0;
        /**
         * register a pull source on this shard/tree
         * currently non-existing hosts will also be added (they can come online later)
         * but at a perf cost if keys are not found
         * keys can also be retrieved asynchronously becoming available later but at greater
         * throughput
         * @param host
         * @param port
         * @return true if host and port combo does not exist
         */
        virtual bool pull(std::string host, int port) = 0;

        virtual void run_defrag() = 0;

        virtual bool save(bool stats) = 0;

        virtual bool send(std::ostream& out) = 0;

        virtual bool load(bool stats) = 0;

        virtual bool reload() = 0;

        virtual bool retrieve(std::istream& in) = 0;

        virtual void begin() = 0;

        virtual void commit() = 0;

        virtual void rollback() = 0;

        virtual void clear() = 0;

        virtual bool insert(const art::key_options& options, art::value_type key, art::value_type value, bool update, const art::NodeResult &fc) = 0;

        virtual bool hash_insert(const art::key_options &options, art::value_type key, art::value_type value, bool update, const art::NodeResult &fc) = 0;
        virtual bool hash_erase(logical_address lad) = 0;
        virtual bool tree_insert(const art::key_options &options, art::value_type key, art::value_type value, bool update, const art::NodeResult &fc) = 0;

        virtual bool opt_rpc_insert(const art::key_options& options, art::value_type unfiltered_key, art::value_type value, bool update, const art::NodeResult &fc) = 0;
        virtual bool opt_insert(const art::key_options& options, art::value_type key, art::value_type value, bool update, const art::NodeResult &fc) = 0;

        virtual bool insert(art::value_type key, art::value_type value, bool update, const art::NodeResult &fc) = 0;
        virtual bool insert(art::value_type key, art::value_type value, bool update) = 0;
        virtual bool evict(art::value_type key) = 0;
        virtual bool evict(const art::leaf* l) = 0;
        virtual bool remove(art::value_type key, const art::NodeResult &fc) = 0;
        // does not replicate
        virtual bool tree_remove(art::value_type key, const art::NodeResult &fc) = 0;
        virtual bool remove(art::value_type key) = 0;
        virtual void merge(const shard_ptr& to, merge_options options) = 0;
        virtual void merge(merge_options options) = 0;
        /**
         * find a key. if the key does not exist pull sources will be queried for the key
         * if the key is no-were a null is returned
         * @param key any valid value
         * @return not null key if it exists (incl. pull sources)
         */
        virtual art::node_ptr search(art::value_type key) = 0;
        virtual art::node_ptr lower_bound(art::value_type key) = 0;
        virtual art::node_ptr lower_bound(art::trace_list &trace, art::value_type key) = 0;
        virtual void glob(const art::keys_spec &spec, art::value_type pattern, bool value, const std::function<bool(const art::leaf &)> &cb)  = 0;
        virtual shard_ptr sources() = 0;
        virtual void depends(const std::shared_ptr<abstract_shard> & source) = 0;
        virtual void release(const std::shared_ptr<abstract_shard> & source) = 0;
        virtual art::node_ptr tree_minimum() const = 0;
        virtual art::node_ptr tree_maximum() const = 0;
        virtual art::node_ptr get_last_leaf_added() const = 0;
        virtual art::node_ptr make_leaf(art::value_type key, art::value_type v, art::key_options opts) = 0;
        virtual art::node_ptr make_leaf(art::value_type key, art::value_type v, art::leaf::ExpiryType ttl , bool is_volatile , bool is_compressed ) = 0;
        virtual art::value_type filter_key(art::value_type) const = 0;
        virtual art::node_ptr get_root() const = 0;
        virtual int range(art::value_type key, art::value_type key_end, art::CallBack cb, void *data) = 0;
        virtual int range(art::value_type key, art::value_type key_end, art::LeafCallBack cb) = 0;
        virtual bool update(art::value_type key, const std::function<art::node_ptr(const art::node_ptr &leaf)> &updater) = 0;
        virtual void queue_consume() = 0;
        virtual alloc_pair& get_ap() = 0;
        virtual const alloc_pair& get_ap() const = 0;
        virtual size_t get_shard_number() const = 0;
        virtual size_t get_queue_size() const = 0;
        virtual size_t inc_queue_size() = 0;
        virtual size_t dec_queue_size() = 0;
        // blocking functions
        // this function MUST be pre-locked by the caller using this shards latch
        // add multiple rpc blocks (or callbacks) (called by session in asynch thread)
        virtual void add_rpc_blocks(const heap::vector<std::string>& keys, const abstract_session_ptr& ptr) = 0;
        // this function MUST be pre-locked by the caller using this shards latch
        // add a rpc block (called by session in asynch thread)
        virtual void add_rpc_block(const std::string& key, const abstract_session_ptr& ptr) = 0;
        // this function MUST be pre-locked by the caller using this shards latch
        // remove scheduled blocks on the key without scheduling calls (called by session in asynch thread)
        // blocks are only removed for this shard - the caller must maintain the associated
        // key space for this shard
        virtual void erase_rpc_blocks(const heap::vector<std::string>& keys, const abstract_session_ptr& ptr) = 0;
        // this function MUST be pre-locked by the caller using this shards latch
        // remove scheduled blocks on the key without scheduling calls (called by session in asynch thread)
        // blocks are only removed for this shard - the caller must maintain the associated
        // key space for this shard
        virtual void erase_rpc_block(const std::string& keys, const abstract_session_ptr& ptr) = 0;
        // this function MUST be pre-locked by the caller using this shards latch
        // schedule all asynch sessions that's blocking on this key on this shard to run once
        // so the blocks are also erased
        virtual void call_unblock(const std::string& key) = 0;
    };
    typedef abstract_shard::shard_ptr shard_ptr;
    /**
     * gets per module per node type statistics for all art_node* types
     * @return art_statistics
     */
    art_statistics get_statistics();

    /**
     * get statistics for each operation performed
     */
    art_ops_statistics get_ops_statistics();

    /**
     * get replication and network statistics
     */
    art_repl_statistics get_repl_statistics();


}


struct storage_release {
    barch::shard_ptr  t{};
    barch::shard_ptr sources_locked{};
    bool lock = true;

    storage_release() = delete;
    storage_release(const storage_release&) = delete;
    storage_release(storage_release&&) = default;
    storage_release& operator=(storage_release&&) = default;
    storage_release& operator=(const storage_release&) = delete;
    explicit storage_release(const barch::shard_ptr& t, bool lock = true) : t(t) , lock(lock){
        if (!lock) return;
        sources_locked = t->sources();
        auto s = sources_locked;
        // TODO: this may cause deadlock
        while (s) {
            s->lock_shared();
            s = s->sources();
        }
        t->lock_unique();

    }
    ~storage_release() {
        if (!t) return;
        if (!lock) return;
        t->unlock_unique();
        // TODO: this may cause deadlock we've got to at least test
        auto s = sources_locked;
        while (s) {
            s->unlock_shared();
            s = s->sources();
        }
    }
};
typedef storage_release storage_write_lock;
struct read_lock {
    barch::shard_ptr t{};
    barch::shard_ptr sources_locked{};
    bool lock = true;
    void clear() {
        t = nullptr;
        sources_locked = nullptr;
    }
    read_lock() = default;
    read_lock(const read_lock&) = delete;
    read_lock(read_lock&& r)  noexcept {
        t = r.t;
        sources_locked = r.sources_locked;
        r.clear();
        lock = r.lock;

    };
    read_lock& operator=(read_lock&& r)  noexcept {
        t = r.t;
        lock = r.lock;
        sources_locked = r.sources_locked;
        r.clear();
        return *this;
    }
    read_lock& operator=(const read_lock&) = delete;

    explicit read_lock(const barch::shard_ptr& t, bool lock = true) : t(t), lock(lock) {
        if (!lock) return;
        sources_locked = t->sources();
        auto s = sources_locked;
        // TODO: this may cause deadlock
        while (s) {
            s->lock_shared();
            s = s->sources();
        }
        t->lock_shared();
    }

    ~read_lock() {
        if (!t) return;
        if (!lock) return;
        t->unlock_shared();

        // TODO: this may cause deadlock we've got to at least test
        auto s = sources_locked;
        while (s) {
            s->unlock_shared();
            s = s->sources();
        }
    }
};

/**
* evict a lru page
*/
uint64_t art_evict_lru(barch::shard_ptr t);
template<typename SFun>
size_t shard_thread_processor(size_t count, SFun && sfun ) {
    std::vector<std::thread> loaders;
    loaders.resize(std::max<size_t>(1, std::thread::hardware_concurrency()/2));
    size_t shard_num = 0;

    size_t remaining_shards = count;
    while (remaining_shards > 0) {
        size_t active_shards = std::min<size_t>(remaining_shards,loaders.size());
        for (size_t l = 0; l < active_shards; ++l) {
            loaders[l] = std::thread([shard_num, sfun]() {
                sfun(shard_num);
            });

            ++shard_num;
        }
        size_t iterations = 0;
        for (auto &loader : loaders) {
            if (loader.joinable())
                loader.join();
            --active_shards;
            --remaining_shards;
            ++iterations;
            if (active_shards == 0) {
                break;
            }

        }
    }
    return shard_num;
}
#endif //BARCH_ABSTRACT_SHARD_H