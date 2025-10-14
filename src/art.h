#pragma once
#include <functional>
#include <mutex>
#include <netinet/in.h>

#include "composite.h"
#include "nodes.h"
#include "logical_allocator.h"
#include "keyspec.h"
#include "value_type.h"
#include "vector_stream.h"
#include "rpc/server.h"
#include "overflow_hash.h"
typedef std::unique_lock<std::shared_mutex> write_lock;
extern std::shared_mutex &get_lock();

/**
 * context management
 */
//typedef std::unique_lock<std::shared_mutex> storage_release;
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
    int64_t queue_reorders {};
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

namespace art {
    node_ptr alloc_node_ptr(alloc_pair& alloc, unsigned ptrsize, unsigned nt, const children_t &c);

    struct tree :  public alloc_pair {
        tree(const std::string& name, size_t shard_number, node_ptr rrrr, uint64_t ssss)
        : alloc_pair(shard_number,name), root(rrrr), size(ssss) {}
        node_ptr root{};
        uint64_t size{};
        bool opt_use_trace = true;
        trace_list trace{};
        mutable std::string temp_key{};
        node_ptr last_leaf_added{};
        void update_trace(int direction);
        value_type filter_key(value_type key) const;
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
        void log_trace() const ;

        /**
         * leaf allocation
         * @param key
         * @param v value associated with key
         * @param ttl how long it may live
         * @param is_volatile it may be evicted if the lru/lfu-evict volatile flags are on
         * @return address of leaf created
         */
        node_ptr make_leaf(value_type key, value_type v, leaf::ExpiryType ttl = 0, bool is_volatile = false) ;
        node_ptr alloc_node_ptr(unsigned ptrsize, unsigned nt, const children_t &c);
        node_ptr alloc_8_node_ptr(unsigned nt);
        virtual ~tree() = default;
    };
}

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int tree_destroy(art::tree *t);

/**
 * inserts a new value into the art tree
 * a key cannot contain any embedded nulls. a terminating null char will
 * be added if it does not exist
 * TODO: no checks for embedded nulls are currently done
 * @arg t the tree
 * @arg key the key the key cannot have embedded 0 chars a terminating 0 char will be added if it does not exist
 * @arg key_len the length of the key
 * @arg value opaque value.
 * @return true was inserted or replaced, otherwise
 * The old value is captured in the callback
 * exceptions may happen
 */
bool art_insert
(art::tree *t
 , const art::key_options &options
 , art::value_type key
 , art::value_type value
 , const NodeResult &fc
);

bool art_insert
(art::tree *t
 , const art::key_options &options
 , art::value_type key
 , art::value_type value
 , bool replace
 , const NodeResult &fc);

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
    bool update(art::tree *t, value_type key, const std::function<node_ptr(const node_ptr &leaf)> &updater);

    art::node_ptr maximum(art::tree *t);

    /**
     * Returns the lower bound value of a given key
     * lower bound is defined as first value not less than the key parameter
     * @arg t The tree
     * @arg key The key
     * @arg key_len The length of the key
     * @return the lower bound or NULL if there is no value not less than key
     */

    node_ptr lower_bound(const art::tree *t, value_type key);

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
        art::trace_list tl{};
        art::node_ptr c{};

        /**
         * performs a lower-bound search and returns an iterator that may not always be valid
         * the life-time of the iterator must not exceed that of the t or key parameters
         * @param t the art tree
         * @param key iterator will start at keys not less than key
         * @return an iterator
         */

        iterator(tree* t, art::value_type key);

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

    node_ptr find(const art::tree* t, value_type key);

    int range(const tree *t, value_type key, value_type key_end, CallBack cb, void *data);

    int range(const tree *t, value_type key, value_type key_end, LeafCallBack cb);

    int64_t distance(const tree *t, const trace_list &a, const trace_list &b);

    int64_t fast_distance(const trace_list &a, const trace_list &b);
    /**
     * glob match all the key value pairs except the deleted ones
     * This is a multi threaded iterator and care should be taken
     */
    void glob(tree *t, const keys_spec &spec, value_type pattern, const std::function<bool(const leaf &)> &cb);

    /**
     * sometimes the shard needs to know this fact
     * @param dl
     * @param value
     * @param options
     * @return if it can be overwritten directly
     */
    bool is_leaf_direct_replacement(const art::leaf* dl, art::value_type value, const art::key_options &options);

    /**
     * filter a key - may throw if key is malformed
     * @param temp_key
     * @param key
     * @return the reconditioned key that will be compatible with an art
     */
    value_type s_filter_key(std::string& temp_key, value_type key);
}

/**
* evict a lru page
*/
uint64_t art_evict_lru(art::tree *t);

