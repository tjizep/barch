#pragma once
#include <cstdint>
#include <functional>
#include "nodes.h"
#include "compress.h"
#include "keyspec.h"
#include "value_type.h"
#include "vector_stream.h"

typedef std::unique_lock<std::shared_mutex> write_lock;
typedef std::shared_lock<std::shared_mutex> read_lock; // C++ 14
//!typedef basic_ovectorstream<std::vector<char> >    ovectorstream;
extern std::shared_mutex& get_lock();
/**
 * context management
 */
struct compressed_release
{
    compressed_release();
    ~compressed_release();
};

/**
 * global statistics
 */

struct art_statistics
{
    int64_t leaf_nodes;
    int64_t node4_nodes;
    int64_t node16_nodes;
    int64_t node48_nodes;
    int64_t node256_nodes;
    int64_t node256_occupants;
    int64_t bytes_allocated;
    int64_t bytes_interior;
    int64_t heap_bytes_allocated;
    int64_t page_bytes_compressed;
    int64_t pages_uncompressed;
    int64_t pages_compressed;
    int64_t max_page_bytes_uncompressed;
    int64_t page_bytes_uncompressed;
    int64_t vacuums_performed;
    int64_t last_vacuum_time;
    int64_t leaf_nodes_replaced;
    int64_t pages_evicted;
    int64_t keys_evicted;
    int64_t pages_defragged;
    int64_t exceptions_raised;
};

struct art_ops_statistics
{
    int64_t delete_ops;
    int64_t set_ops;
    int64_t iter_ops;
    int64_t iter_range_ops;
    int64_t range_ops;
    int64_t get_ops;
    int64_t lb_ops;
    int64_t size_ops;
    int64_t insert_ops;
    int64_t min_ops;
    int64_t max_ops;
};

typedef std::function<int(void* data, art::value_type key, art::value_type value)> CallBack;
typedef std::function<void(art::node_ptr l)> NodeResult;


/**
 * art tree and company
 */
namespace art
{
    bool has_leaf_compression();
    bool has_node_compression();
    bool init_leaf_compression();
    bool init_node_compression();
    void destroy_node_compression();
    void destroy_leaf_compression();

    struct tree
    {
        bool mexit = false;
        bool transacted = false;
        std::thread tmaintain{}; // a maintenance thread to perform defragmentation and eviction (if required)
        art::node_ptr root = nullptr;
        uint64_t size = 0;
        // to support a transaction
        art::node_ptr save_root = nullptr;
        uint64_t save_size = 0;
        vector_stream save_stats{};
        std::shared_mutex save_load_mutex{};
        void start_maintain();
        tree(const tree&) = delete;

        tree(const art::node_ptr& root, uint64_t size) : root(root), size(size)
        {
            start_maintain();
        }

        ~tree();
        void run_defrag();
        bool save();
        bool load();
        void begin();
        void commit();
        void rollback();
        void clear();
    };
}

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art::tree* t);

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int tree_destroy(art::tree* t);

/**
 * Returns the size of the ART tree.
 */
uint64_t art_size(art::tree* t);

/**
 * inserts a new value into the art tree
 * @arg t the tree
 * @arg key the key
 * @arg key_len the length of the key
 * @arg value opaque value.
 * @return null if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void art_insert(art::tree* t, const art::key_spec& options, art::value_type key, art::value_type value,
                const NodeResult& fc);

/**
 * inserts a new value into the art tree (not replacing)
 * @arg t the tree
 * @arg key the key
 * @arg key_len the length of the key
 * @arg value opaque value.
 * @return null if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void art_insert_no_replace(art::tree* t, const art::key_spec& options, art::value_type key, art::value_type value,
                           const NodeResult& fc);

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void art_delete(art::tree* t, art::value_type key, const NodeResult& fc);

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
art::node_ptr art_search(art::trace_list& trace, const art::tree* t, art::value_type key);

/**
 * Returns the minimum valued leaf
 * @return The minimum leaf or NULL
 */
art::node_ptr art_minimum(art::tree* t);

/**
 * Returns the maximum valued leaf
 * @return The maximum leaf or NULL
 */
namespace art
{
    /**
     * call 'updater' if key exists. updater can return a new leaf to replace the existing one.
     * if 'updater' returns null then nothing is updated
     * @param t the art tree with root node
     * @param key key to find
     * @param updater function to call for supplying modified key
     */
    void update(tree* t, value_type key, const std::function<node_ptr(const node_ptr& leaf)>& updater);

    art::node_ptr maximum(art::tree* t);

    /**
     * Returns the lower bound value of a given key
     * lower bound is defined as first value not less than the key parameter
     * @arg t The tree
     * @arg key The key
     * @arg key_len The length of the key
     * @return the lower bound or NULL if there is no value not less than key
     */

    node_ptr lower_bound(const art::tree* t, art::value_type key);
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
int art_iter(art::tree* t, CallBack cb, void* data);

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
int art_iter_prefix(art::tree* t, art::value_type prefix, CallBack cb, void* data);
/**
 * iterates through a range from small to large from key to key_end
 * the first key is located in log(n) time
 * @return 0 on success, or the return of the callback.
 */
namespace art
{
    int range(const art::tree* t, art::value_type key, art::value_type key_end, CallBack cb, void* data);
}


/**
* evict a lru page
*/
uint64_t art_evict_lru(art::tree* t);

namespace art
{
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
     * glob match all the key value pairs except the deleted ones
     * This is a multi threaded iterator and care should be taken
     */
    void glob(tree* t, const keys_spec& spec, value_type pattern, const std::function<bool(const leaf&)>& cb);
}
