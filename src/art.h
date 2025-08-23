#pragma once
#include <cstdint>
#include <functional>
#include <set>
#include <netinet/in.h>

#include "composite.h"
#include "nodes.h"
#include "logical_allocator.h"
#include "keyspec.h"
#include "value_type.h"
#include "vector_stream.h"
#include "server.h"
typedef std::unique_lock<std::shared_mutex> write_lock;
typedef std::shared_lock<std::shared_mutex> read_lock; // C++ 14
extern std::shared_mutex &get_lock();

/**
 * context management
 */
typedef std::unique_lock<std::shared_mutex> storage_release;
typedef std::shared_lock<std::shared_mutex> read_release;

/**
 * global statistics
 */

struct art_statistics {
    art_statistics() {}
    ~art_statistics() {}
    int64_t leaf_nodes {};
    int64_t node4_nodes {};
    int64_t node16_nodes {};
    int64_t node48_nodes {};
    int64_t node256_nodes {};
    int64_t node256_occupants {};
    int64_t bytes_allocated {};
    int64_t bytes_interior {};
    int64_t heap_bytes_allocated {};
    int64_t page_bytes_compressed {};
    int64_t pages_uncompressed {};
    int64_t pages_compressed {};
    int64_t max_page_bytes_uncompressed {};
    int64_t page_bytes_uncompressed {};
    int64_t vacuums_performed {};
    int64_t last_vacuum_time {};
    int64_t leaf_nodes_replaced {};
    int64_t pages_evicted {};
    int64_t keys_evicted {};
    int64_t pages_defragged {};
    int64_t exceptions_raised {};
    int64_t maintenance_cycles {};
    int64_t shards {};
    int64_t local_calls {};
    int64_t max_spin {};
    int64_t logical_allocated {};
    int64_t oom_avoided_inserts {};
    int64_t keys_found {};
    int64_t new_keys_added {};
    int64_t keys_replaced {};
};

struct art_ops_statistics {
    art_ops_statistics(){}
    ~art_ops_statistics(){}
    int64_t delete_ops {};
    int64_t set_ops {};
    int64_t iter_ops {};
    int64_t iter_range_ops {};
    int64_t range_ops {};
    int64_t get_ops {};
    int64_t lb_ops {};
    int64_t size_ops {};
    int64_t insert_ops {};
    int64_t min_ops {};
    int64_t max_ops {};
};

struct art_repl_statistics {
    int64_t key_add_recv{};
    int64_t key_add_recv_applied{};
    int64_t key_rem_recv{};
    int64_t key_rem_recv_applied{};
    int64_t bytes_recv{};
    int64_t bytes_sent{};
    int64_t out_queue_size{};
    int64_t instructions_failed{};
    int64_t insert_requests{};
    int64_t remove_requests{};
    int64_t find_requests{};
    int64_t request_errors{};
    int64_t redis_sessions{};
    int64_t attempted_routes{};
    int64_t routes_succeeded{};
};
typedef std::function<int(void *data, art::value_type key, art::value_type value)> CallBack;
typedef std::function<int(const art::node_ptr &)> LeafCallBack;
typedef std::function<void(const art::node_ptr &)> NodeResult;


/**
 * art tree and company
 */
namespace art {
    node_ptr alloc_node_ptr(alloc_pair& alloc, unsigned ptrsize, unsigned nt, const children_t &c);

    struct kv_buf {
        uint8_t buf[32];
        kv_buf(key_options options, value_type k, value_type v) {
            if (k.size + v.size + 2 < sizeof(buf)) {
                memcpy(buf, k.bytes, k.size+1);
                memcpy(&buf[k.size+1], v.bytes, v.size);
                key = {&buf[0], k.size};
                value = {&buf[k.size+1], v.size};
            }
            opts = options;
        }
        kv_buf(const kv_buf& r) {
            memcpy(buf, r.buf, r.value.size + r.key.size + 1);
            key = {&buf[0], r.key.size};
            value = {&buf[r.key.size+1], r.value.size};
            opts = r.opts;
        }
        kv_buf& operator=(const kv_buf& r) {
            memcpy(buf, r.buf, sizeof(buf));
            key = {&buf[0], r.key.size};
            value = {&buf[r.key.size+1], r.value.size};
            opts = r.opts;
            return *this;
        }
        bool operator==(const kv_buf& r) const {
            return  key == r.key;
        }
        bool operator<(const kv_buf& r) const {
            return  key < r.key;
        }
        key_options opts{};
        value_type key{};
        value_type value{};
    };
    struct kv_hash{
        size_t operator()(const kv_buf& k) const {
            uint64_t hash = ankerl::unordered_dense::detail::wyhash::hash(k.key.chars(), k.key.size);
            return hash;
        }
    };
    struct hashed_key {
        uint32_t addr{};
        hashed_key(value_type , alloc_pair* ) ;
        hashed_key(value_type , const alloc_pair* ) ;

        const leaf* get_leaf() const;
        value_type get_key() const;

        hashed_key(const node_ptr& la) ;
        hashed_key(const logical_address& la) ;

        hashed_key& operator=(const node_ptr& nl) {
            addr = nl.logical.address();
            return *this;
        }

        bool operator==(const hashed_key& r) const {
            return get_key() == r.get_key();
        }

        bool operator<(const hashed_key& r) const {
            return get_key() < r.get_key();
        }
        size_t hash() const {
            auto key = get_key();
            size_t r = ankerl::unordered_dense::detail::wyhash::hash(key.chars(), key.size);
            return r;
        }
    };
    struct hk_hash{
        size_t operator()(const hashed_key& k) const {
           return k.hash();
        }
    };
    struct tree : public alloc_pair{

    private:
        trace_list trace{};

        mutable std::string temp_key{};
        bool with_stats{true};
        mutable heap::set<hashed_key,hk_hash> h{};
    public:
        size_t get_hash_size() {
            return jump_size;
        }
        void log_trace() const ;
        value_type filter_key(value_type key) const;
        composite query{};
        composite cmd_ZADD_q1{};
        composite cmd_ZADD_qindex{};
        bool mexit = false;
        bool transacted = false;
        std::thread tmaintain{}; // a maintenance thread to perform defragmentation and eviction (if required)
        node_ptr root = nullptr;
        uint64_t size = 0;
        // to support a transaction
        node_ptr save_root = nullptr;
        uint64_t save_size = 0;
        vector_stream save_stats{};
        std::shared_mutex save_load_mutex{};
        bool opt_use_trace = true;
        node_ptr last_leaf_added{};
        barch::repl::client repl_client{};
        uint64_t jump_size{};

        bool opt_ordered_keys;
        void clear_trace() {
            if (opt_use_trace)
                trace.clear();

        }

        void pop_trace() {
            if (opt_use_trace)
                trace.pop_back();
        }

        void push_trace(const trace_element &te) {
            if (opt_use_trace)
                trace.push_back(te);
        }

        void start_maintain();

        tree(const tree &) = delete;

        tree(const node_ptr &root, uint64_t size, size_t shard) :
        alloc_pair(shard),
        root(root),
        size(size),
        opt_ordered_keys(get_ordered_keys()) {
            repl_client.shard = shard;
            barch::repl::clear_route(shard);
            start_maintain();

        }
        tree(const std::string& name,const node_ptr &root, uint64_t size, size_t shard) :
        alloc_pair(shard,name),
        with_stats(false),
        root(root),
        size(size),
        opt_ordered_keys(true) {
            repl_client.shard = shard;
            barch::repl::clear_route(shard);
            start_maintain();
        }
        tree& operator=(const tree&) = delete;

        ~tree();
        void load_hash();
        void clear_hash() ;
        bool uncache_leaf(value_type key);
        void cache_leaf(const node_ptr& leaf) const;
        node_ptr get_cached(value_type key) const;

        bool publish(std::string host, int port);

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
        bool pull(std::string host, int port);

        void run_defrag();

        bool save(bool stats = true);

        bool send(std::ostream& out);

        bool load(bool stats = true);

        bool retrieve(std::istream& in);

        void begin();

        void commit();

        void rollback();

        void clear();

        void update_trace(int direction);

        bool insert(const key_options& options, value_type key, value_type value, bool update, const NodeResult &fc);

        bool jumpsert(const key_options &options, value_type key, value_type value, bool update, const NodeResult &fc);

        bool opt_insert(const key_options& options, value_type key, value_type value, bool update, const NodeResult &fc);
        bool insert(value_type key, value_type value, bool update, const NodeResult &fc);
        bool insert(value_type key, value_type value, bool update = true);

        bool remove(value_type key, const NodeResult &fc);
        bool remove(value_type key);

        /**
         * find a key. if the key does not exist pull sources will be queried for the key
         * if the key is no-were a null is returned
         * @param key any valid value
         * @return not null key if it exists (incl. pull sources)
         */
        node_ptr search(value_type key);

        bool update(value_type key, const std::function<node_ptr(const node_ptr &leaf)> &updater);

        /**
         * leaf allocation
         * @param key
         * @param v value associated with key
         * @param ttl how long it may live
         * @param is_volatile it may be evicted if the lru/lfu-evict volatile flags are on
         * @return address of leaf created
         */
        node_ptr make_leaf(value_type key, value_type v, leaf::ExpiryType ttl = 0, bool is_volatile = false) ;
        node_ptr alloc_node_ptr(unsigned ptrsize, unsigned nt, const art::children_t &c);
        node_ptr alloc_8_node_ptr(unsigned nt);


    };
}

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int tree_destroy(art::tree *t);

/**
 * Returns the size of the ART tree.
 */
uint64_t art_size(art::tree *t);

/**
 * inserts a new value into the art tree
 * a key cannot contain any embedded nulls. a terminating null char will
 * be added if it does not exist
 * TODO: no checks for embedded nulls are currently done
 * @arg t the tree
 * @arg key the key the key cannot have embedded 0 chars a terminating 0 char will be added if it does not exist
 * @arg key_len the length of the key
 * @arg value opaque value.
 * @return null if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void art_insert
(art::tree *t
 , const art::key_options &options
 , art::value_type key
 , art::value_type value
 , const NodeResult &fc
);

void art_insert
(art::tree *t
 , const art::key_options &options
 , art::value_type key
 , art::value_type value
 , bool replace
 , const NodeResult &fc
 , bool use_cache = true);

/**
 * inserts a new value into the art tree (not replacing)
 * check above for notes on embedded nulls
 * @arg t the tree
 * @arg key the key
 * @arg key_len the length of the key
 * @arg value opaque value.
 * @return null if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void art_insert_no_replace(art::tree *t, const art::key_options &options, art::value_type key, art::value_type value,
                           const NodeResult &fc);

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void art_delete(art::tree *t, art::value_type key);

void art_delete(art::tree *t, art::value_type key, const NodeResult &fc);

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
art::node_ptr art_search(const art::tree *t, art::value_type key);

/**
 * Returns the minimum valued leaf
 * @return The minimum leaf or NULL
 */
art::node_ptr art_minimum(const art::tree *t);

/**
 * Returns the maximum valued leaf
 * @return The maximum leaf or NULL
 */
namespace art {
    /**
     * call 'updater' if key exists. updater can return a new leaf to replace the existing one.
     * if 'updater' returns null then nothing is updated
     * @param t the art tree with root node
     * @param key key to find
     * @param updater function to call for supplying modified key
     */
    bool update(tree *t, value_type key, const std::function<node_ptr(const node_ptr &leaf)> &updater);

    art::node_ptr maximum(art::tree *t);

    /**
     * Returns the lower bound value of a given key
     * lower bound is defined as first value not less than the key parameter
     * @arg t The tree
     * @arg key The key
     * @arg key_len The length of the key
     * @return the lower bound or NULL if there is no value not less than key
     */

    node_ptr lower_bound(const tree *t, value_type key);

    /**
     *  gets the thread local trace list for the last lb operation
     *  it can be used to navigate from there
     * @return the trace list for the last lb operation
     */
    trace_list& get_tlb();
}

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each. The call back gets a
 * key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int art_iter(art::tree *t, CallBack cb, void *data);

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each that matches a given prefix.
 * The call back gets a key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg prefix The prefix of keys to read
 * @arg prefix_len The length of the prefix
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int art_iter_prefix(art::tree *t, art::value_type prefix, CallBack cb, void *data);

/**
 * iterates through a range from small to large from key to key_end
 * the first key is located in constant time
 * @return 0 on success, or the return of the callback.
 */
namespace art {
    struct iterator {
        tree *t;
        trace_list tl{};
        node_ptr c{};

        /**
         * performs a lower-bound search and returns an iterator that may not always be valid
         * the life-time of the iterator must not exceed that of the t or key parameters
         * @param t the art tree
         * @param key iterator will start at keys not less than key
         * @return an iterator
         */

        iterator(tree* t, value_type key);

        /**
         * starts the iterator at the first key (left most) in the tree
         */
        iterator(tree* t);

        iterator(const iterator &it) = default;

        iterator(iterator &&it) = default;

        iterator &operator=(iterator &&it) = default;

        iterator &operator=(const iterator &it) = default;

        bool next();

        bool previous();

        bool last();

        [[nodiscard]] const leaf *l() const;

        [[nodiscard]] value_type key() const;

        [[nodiscard]] value_type value() const;

        [[nodiscard]] bool end() const;

        [[nodiscard]] bool ok() const;

        [[nodiscard]] node_ptr current() const;

        bool update(std::function<node_ptr(const leaf *l)> updater);

        bool update(value_type value);

        bool update(value_type value, int64_t ttl, bool volat);

        bool update(int64_t ttl, bool volat);

        bool update(int64_t ttl);

        [[nodiscard]] bool remove() const;

        [[nodiscard]] int64_t distance(const iterator &other) const;

        [[nodiscard]] int64_t distance(value_type other, bool traced = false) const;

        [[nodiscard]] int64_t fast_distance(const iterator &other) const;

        void log_trace() const;
    };

    node_ptr find(const tree* t, value_type key);

    int range(const tree *t, value_type key, value_type key_end, CallBack cb, void *data);

    int range(const tree *t, value_type key, value_type key_end, LeafCallBack cb);

    int64_t distance(const tree *t, const trace_list &a, const trace_list &b);

    int64_t fast_distance(const trace_list &a, const trace_list &b);
}


/**
* evict a lru page
*/
uint64_t art_evict_lru(art::tree *t);

namespace art {
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
    /**
     * glob match all the key value pairs except the deleted ones
     * This is a multi threaded iterator and care should be taken
     */
    void glob(tree *t, const keys_spec &spec, value_type pattern, const std::function<bool(const leaf &)> &cb);
}
