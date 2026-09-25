//
// Created by teejip on 10/21/25.
//

#ifndef BARCH_ABSTRACT_SHARD_H
#define BARCH_ABSTRACT_SHARD_H
#include <atomic>
#include <memory>
#include <shared_mutex>
#include <utility>

//#include "key_space.h"
#include "art/art.h"
#include "art/key_options.h"
#include "merge_options.h"
#include "configuration.h"
#include "shared_mutex.h"
#include "rpc/abstract_session.h"
#include "shared_mutex.h"

#include "aof_log.h"
#include "index_sink.h"
#include "source_chain.h"

namespace barch {

    class abstract_shard : public std::enable_shared_from_this<abstract_shard>{
    public:
        /**
         * The indexes over this shard's space, or null when it has none - TODO 422.
         * Told about every write and erase that takes effect, under this shard's
         * latch. It points at a queue the process keeps for its whole life
         * (perm_index.cpp), so storing and clearing it needs no ordering with anything.
         */
        std::atomic<index_sink*> index_to{nullptr};

        typedef std::shared_ptr<abstract_shard> shard_ptr;
        typedef abstract_shard* shard_ref;

        // bit-packed, same specialization as std::vector<bool>, through the
        // tracking allocator. four million bits, 512 KiB, when the filter is on.
        typedef heap::vector<bool> bloom_t;
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
            opt_static_bloom_filter = enable;
            if (enable) {
                if (bloom.size() != static_bloom_size)
                    bloom.resize(static_bloom_size);
            } else if (!bloom.empty()) {
                bloom_t ebl;
                bloom = std::move(ebl);
            }
        }
    private:
        bool opt_static_bloom_filter = barch::get_static_bloom_filter();
    public:
        bool has_static_bloom_filter() const {
            return opt_static_bloom_filter;
        }
        /*
         * Atomic for the same reason the evict flags are: KSPACE ORDERED and
         * KSPACE HYBRID write them from a session thread while maintenance is
         * reading them - get_size() picks the tree or the hash by
         * opt_ordered_keys on the maintenance thread. Relaxed: a pass either
         * sees the new setting or the next one does. See TODO 223.
         */
        std::atomic<bool> opt_ordered_keys{barch::get_ordered_keys()};
        // ART owns the leaves; the overflow hash is only an index of 32-bit
        // pointers. GET and in-place SET can use it. Anything that needs a
        // trace still walks the tree. Off unless ordered_keys is on.
        std::atomic<bool> opt_hybrid_keys{barch::get_hybrid_keys()};
        bool hybrid_active() const { return opt_ordered_keys && opt_hybrid_keys; }
        /*
         * Whether this space compresses cold keys. Mirrored from key_space,
         * which reads `<space>.compression` and defaults to the server setting.
         * Kept on the shard like opt_ordered_keys above, because the background
         * pass runs per shard and has no route back to the space. See TODO 300.
         */
        std::atomic<bool> opt_compression{barch::get_compression_enabled()};
        virtual void apply_hybrid_keys() = 0;
        /*
         * KSPACE EVICT writes these from a session thread while the maintenance
         * thread is reading them to decide whether to run an eviction pass, so
         * they are atomic - same reasoning as the shutdown flags in DONE 204.
         * Relaxed is enough: nothing is ordered against them, a pass either
         * sees the new setting or picks it up on the next tick. See TODO 214.
         */
        std::atomic<bool> opt_evict_all_keys_lru{barch::get_evict_allkeys_lru()};
        std::atomic<bool> opt_evict_all_keys_lfu{barch::get_evict_allkeys_lfu()};
        std::atomic<bool> opt_evict_all_keys_random{barch::get_evict_allkeys_random()};
        std::atomic<bool> opt_evict_volatile_keys_lru{barch::get_evict_volatile_lru()};
        std::atomic<bool> opt_evict_volatile_keys_lfu{barch::get_evict_volatile_lfu()};
        std::atomic<bool> opt_evict_volatile_keys_random{false};
        std::atomic<bool> opt_evict_volatile_ttl{barch::get_evict_volatile_ttl()};
        /*
         * The two flags above are what the sweeps test. The leaf bit they sweep
         * is set from a second pair on the alloc pair, and nothing used to copy
         * one to the other, so the bit was never set and the clock in
         * run_sweep_lru_keys had nothing to clear - it evicted every leaf it
         * looked at. Call this after writing either flag. See TODO 305.
         */
        virtual void apply_lru_options() = 0;
        bool opt_active_defrag = barch::get_active_defrag();
        bool opt_drop_on_release = false;
        bool saving = false;
        uint64_t lock_to_ms = 1*1000*60;

        void lock_shared() {
            if (get_latch().try_lock_shared())
                return;
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
        virtual barch::latch_t& get_latch() = 0;
        /**
         * The key space this shard belongs to, which is what a per space
         * dictionary is keyed on - TODO 300. Comes from the allocator that owns
         * the leaves, so it is the same string however it is reached.
         */
        [[nodiscard]] virtual const std::string& space_name() const = 0;
        /*
         * How many shards the space this belongs to is cut into, and what the
         * file on disk said when it was loaded. 0 means unknown - a shard built
         * outside a key space, or a file written before this was recorded.
         *
         * A store loaded with a different count than it was saved with does not
         * fail, it half works: routing hashes modulo the current count, so keys
         * end up looked for in shards that never held them. See TODO 314.
         */
        /**
         * The change log of the key space this shard belongs to, or null when
         * that space keeps none - TODO 355.
         *
         * Mirrored from the space when its shards are built, the same way
         * `opt_compression` is: a shard has no route back to its space by
         * design (see the note on `space_name()`), and a lookup per write would
         * be a map and a lock on the hot path.
         *
         * One log per space, so every shard appends to the same file behind the
         * log's own mutex - see the note in aof_log.h about what that costs.
         */
        std::shared_ptr<aof::log> change_log{};
        std::atomic<uint64_t> space_shards{0};
        std::atomic<uint64_t> saved_space_shards{0};
        virtual bool publish(std::string host, int port) = 0;
        virtual uint64_t get_tree_size() const = 0;
        // get_size() should be thread safe
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
        /**
         * The snapshot beside a mapped arena - TODO 262. Default is "nothing to
         * write", so a shard kind that holds no arena is not made to care.
         */
        virtual bool save_snapshot() { return true; }

        virtual bool send(std::ostream& out) = 0;

        virtual bool load(bool stats) = 0;
        /** caller already holds the shard write lock. LOAD takes the space. */
        virtual bool load_holding_lock() = 0;

        virtual bool reload() = 0;
        /** caller already holds the shard write lock. RELOAD takes the space. */
        virtual bool reload_holding_lock() = 0;

        virtual bool retrieve(std::istream& in) = 0;

        virtual void begin() = 0;
        /** begin, for a caller already holding this shard's write latch - TODO 416 */
        virtual void begin_holding_lock() = 0;

        virtual void commit() = 0;

        virtual void rollback() = 0;

        virtual void clear() = 0;

        /**
         * Every page of the leaf arena, or of the node arena, one at a time on the
         * calling thread, as the page number and the page's bytes up to its write
         * position - TODO 416. Inside a transaction these are the pages as they
         * were at begin, however much has changed since. `f` runs with no latch
         * held and answers false to stop. False back means the walk was cut short:
         * `f` stopped it, or the transaction it started in ended under it, which
         * `err` says.
         */
        virtual bool each_page(bool nodes,
                               const std::function<bool(size_t page, const heap::buffer<uint8_t>& data)>& f,
                               std::string& err) = 0;

        /** where a page walk is: what each_page does, a page at a time, for a cursor */
        struct page_walk {
            bool nodes{false};
            bool in_tx{false};
            uint64_t generation{0};
            heap::vector<size_t> pages{};   // sorted
        };
        /** take the page list - the BEGIN-time one inside a transaction */
        virtual void start_page_walk(bool nodes, page_walk& w) = 0;
        /**
         * Copy one page of a walk. 1 is a page in `out`, 0 is nothing to hand over
         * (freed since, or empty), -1 is an error in `err` - the transaction the
         * walk started in has ended.
         */
        virtual int read_walk_page(const page_walk& w, size_t page, heap::buffer<uint8_t>& out,
                                   std::string& err) = 0;

        /** whether a transaction is open here, and which one - TODO 416 */
        virtual void tx_state(bool& in_tx, uint64_t& generation) = 0;
        /** the same, for a caller already holding this shard's latch */
        virtual void tx_state_holding_lock(bool& in_tx, uint64_t& generation) = 0;
        /**
         * The rest of a shard file, for a backup that has the pages from a walk -
         * TODO 416. `free_list_bytes` is one arena's allocator part: see
         * logical_allocator::write_free_list. `stats_bytes` is the shard's block that
         * follows the leaf arena's state in the file: counters, root, size, options.
         * Inside a transaction both are as they were at BEGIN. `in_tx` and
         * `generation` say which state they come from, the way tx_state does.
         */
        virtual void free_list_bytes(bool nodes, std::string& out, bool& in_tx, uint64_t& generation) = 0;
        virtual void stats_bytes(std::string& out, bool& in_tx, uint64_t& generation) = 0;

        /**
         * Streaming save and load - TODO 418. `stream_save` writes this shard to
         * `out` as it stood at BEGIN, so it needs a transaction open; the bytes
         * are copied under a read latch a page at a time and written with no latch
         * held, which is what lets `out` hand blocks to a script. `stream_load`
         * replaces the shard with a stream collected into memory, refused inside a
         * transaction and refused before anything is touched when the header or
         * trailer don't match. See shard.cpp for the format.
         */
        virtual bool stream_save(uint64_t shard_no, uint64_t shard_count, uint64_t generation,
                                 std::ostream& out, std::string& err) = 0;
        virtual bool stream_load(const char* data, size_t len, uint64_t shard_no,
                                 uint64_t shard_count, std::string& err) = 0;

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
        virtual art::node_ptr local_leaf(art::value_type key) = 0;
        /**
         * Write `buf` at `offset` in the value for `key`. Caller holds the write lock.
         *
         * The value grows to at least offset+buf.size, with any gap zero-filled.
         * A missing key or a tomb becomes a new value. A compressed leaf is
         * decompressed, patched, and stored uncompressed — the same latency
         * trade APPEND and SETRANGE make, so a hot counter does not run the
         * dictionary on every write.
         *
         * When the existing uncompressed leaf is already large enough the write
         * is a memcpy into it and nothing is reallocated.
         *
         * false if the pair would not fit in a leaf.
         */
        virtual bool setBufferAt(art::value_type key, art::value_type buf, size_t offset = 0) = 0;
        /**
         * Pointer into the leaf's value from `offset` to the end.
         *
         * Caller holds at least a read lock; the pointer is only valid while
         * that lock is held. Missing, tomb, expired, compressed (the stored
         * bytes are not the logical value), or offset past the end: `{ {}, false }`.
         */
        virtual std::pair<art::value_type, bool> getBufferAt(art::value_type key, size_t offset = 0) = 0;
        virtual bool is_present(art::value_type key) = 0;
        virtual art::node_ptr lower_bound(art::value_type key) = 0;
        virtual art::node_ptr lower_bound(art::trace_list &trace, art::value_type key) = 0;

        /**
         * searces glob patterns on the underlying shards using a special thread pool for this long running task
         * does not need to be locked
         * searches both hash and tree data
         * @param spec for max count
         * @param pattern glob pattern
         * @param value for searching values instead of keys
         * @param cb callback when matching data is found
         */
        virtual void glob(const art::keys_spec &spec, art::value_type pattern, bool value, const std::function<bool(const art::leaf &)> &cb,
                          const art::glob_page_list *only = nullptr, art::glob_page_list *hits = nullptr)  = 0;
        virtual shard_ptr sources() = 0;
        virtual void depends(const std::shared_ptr<abstract_shard> & source) = 0;
        virtual void release(const std::shared_ptr<abstract_shard> & source) = 0;
        virtual art::node_ptr tree_minimum() const = 0;
        virtual art::node_ptr tree_maximum() const = 0;
        virtual art::node_ptr get_last_leaf_added() const = 0;
        virtual art::node_ptr make_leaf(art::value_type key, art::value_type v, art::key_options opts) = 0;
        virtual art::node_ptr make_leaf(art::value_type key, art::value_type v, art::leaf::ExpiryType ttl , bool is_volatile , bool is_compressed ) = 0;
        virtual art::node_ptr get_root() const = 0;
        virtual art::node_ptr first() const = 0 ; // can return nullptr
        virtual size_t page(size_t page, heap::vector<uint8_t>&) const = 0;
        virtual size_t next_page(size_t page ) const = 0;
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
    typedef abstract_shard::shard_ref shard_ref;
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


/*
 * Both locks below take the source chain shared and then the shard itself.
 * Either step can time out and throw from the constructor, and then the
 * destructor never runs, so whatever was taken has to be let go right there.
 * They used to keep the sources held for good, and every later writer to
 * those shards hung - TODO 453.
 */
struct storage_release {
    barch::shard_ptr t{};
    barch::shard_ptr sources_locked{};
    size_t sources_held = 0;
    bool lock = true;

    bool is_locked = false;

    storage_release() = delete;
    storage_release(const storage_release&) = delete;
    storage_release(storage_release&&) = default;
    storage_release& operator=(storage_release&&) = default;
    storage_release& operator=(const storage_release&) = delete;
    explicit storage_release(const barch::shard_ptr& t, bool lock = true) : t(t) , lock(lock){
        if (!lock) return;
        sources_locked = t->sources();
        sources_held = barch::lock_source_chain(sources_locked); // lets go of its own on a throw
        statistics::read_locks_active += sources_held;
        try {
            t->lock_unique();
        } catch (...) {
            barch::unlock_source_chain(sources_locked, sources_held);
            statistics::read_locks_active -= sources_held;
            throw;
        }
        is_locked = true;
        ++statistics::write_locks_active;

    }
    ~storage_release() {
        if (!t) return;
        if (!lock) return;
        if (is_locked) {
            t->unlock_unique();
            --statistics::write_locks_active;
        }
        barch::unlock_source_chain(sources_locked, sources_held);
        statistics::read_locks_active -= sources_held;
    }
};
typedef storage_release storage_write_lock;

template<typename ShardRef>
struct read_lock_t {
    ShardRef t{};
    barch::shard_ptr sources_locked{};
    size_t sources_held = 0;
    bool lock = true;
    bool is_locked = false;
    void clear() {
        t = nullptr;
        sources_locked = nullptr;
        sources_held = 0;
        is_locked = false;
    }
    read_lock_t() = default;
    read_lock_t(const read_lock_t&) = delete;
    read_lock_t(read_lock_t&& r)  noexcept {
        t = r.t;
        sources_locked = r.sources_locked;
        sources_held = r.sources_held;
        lock = r.lock;
        is_locked = r.is_locked;
        r.clear();
        r.lock = false;

    };
    read_lock_t& operator=(read_lock_t&& r)  noexcept {
        t = r.t;
        lock = r.lock;
        sources_locked = r.sources_locked;
        sources_held = r.sources_held;
        is_locked = r.is_locked;
        r.clear();
        r.lock = false;
        return *this;
    }
    read_lock_t& operator=(const read_lock_t&) = delete;

    explicit read_lock_t(const ShardRef& t, bool lock = true) : t(t), lock(lock) {
        if (!lock) return;
        if (!t) return;
        sources_locked = t->sources();
        sources_held = barch::lock_source_chain(sources_locked); // lets go of its own on a throw
        statistics::read_locks_active += sources_held;
        try {
            t->lock_shared();
        } catch (...) {
            barch::unlock_source_chain(sources_locked, sources_held);
            statistics::read_locks_active -= sources_held;
            throw;
        }
        is_locked = true;
        ++statistics::read_locks_active;
    }

    ~read_lock_t() {
        if (!t) return;
        if (!lock) return;
        if (is_locked) {
            t->unlock_shared();
            --statistics::read_locks_active;
        }
        barch::unlock_source_chain(sources_locked, sources_held);
        statistics::read_locks_active -= sources_held;
    }
};
typedef read_lock_t<barch::shard_ptr> read_lock;
typedef read_lock_t<barch::shard_ref> ref_read_lock;
/**
* evict a lru page
*/
uint64_t art_evict_lru(barch::shard_ptr t);
template<typename SFun>
size_t shard_thread_processor(size_t count, SFun && sfun ) {
    std::vector<std::thread> loaders;
    const size_t max_loader_threads = std::thread::hardware_concurrency()/2;
    for (size_t shard_num = 0; shard_num < count; ++shard_num) {
        loaders.emplace_back( std::thread([shard_num, sfun]() {
            sfun(shard_num);
        }));
        if (loaders.size() > max_loader_threads) {
            for (auto &t : loaders) {
                if (t.joinable()) t.join();
            }
            loaders.clear();
        }


    }
    for (auto &t : loaders) {
        if (t.joinable()) t.join();
    }
    return count;
}

namespace barch {
    /**
     * A transaction holds its wakes until it finishes.
     *
     * A blocked client must not see the inside of a MULTI. `MULTI; ZADD zset 0 foo;
     * DEL zset; EXEC` used to wake a waiter on the ZADD, which popped `foo` before the
     * DEL could remove it - so the DEL then answered 0 and the client was unblocked by a
     * member that, from outside, never existed. Redis signals the key during the
     * transaction and only acts on the signal once the whole thing is done, by which
     * time the key is gone and the client is still waiting.
     *
     * `defer_wakes` is that hold, as an RAII scope around the EXEC loop. Wakes raised
     * inside it are collected by shard and key, deduplicated, and sent when it ends.
     * It nests, so a transaction inside anything else that already defers is harmless.
     */
    bool wakes_deferred();
    void defer_wake(abstract_shard* shard, const std::string& key);
    struct defer_wakes {
        defer_wakes();
        ~defer_wakes();
        defer_wakes(const defer_wakes&) = delete;
        defer_wakes& operator=(const defer_wakes&) = delete;
    };
}

#endif //BARCH_ABSTRACT_SHARD_H