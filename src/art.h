#pragma once
#include <cstdint>
#include <functional>
#include "nodes.h"
#include "compress.h"

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

struct art_statistics {
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
};

struct art_ops_statistics {
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

typedef int(*art_callback)(void *data, value_type key, value_type value);
typedef std::function<void(node_ptr l)> NodeResult;



/**
 * Main struct, points to root.
 */
struct art_tree{
    node_ptr root = nullptr;
    uint64_t size = 0;
    art_tree(node_ptr root, uint64_t size) : root(root), size(size) {}
};

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art_tree *t);

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int art_tree_destroy(art_tree *t);

/**
 * Returns the size of the ART tree.
 */
uint64_t art_size(art_tree *t);

/**
 * inserts a new value into the art tree
 * @arg t the tree
 * @arg key the key
 * @arg key_len the length of the key
 * @arg value opaque value.
 * @return null if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void art_insert(art_tree *t, value_type key, value_type value, NodeResult fc);

/**
 * inserts a new value into the art tree (not replacing)
 * @arg t the tree
 * @arg key the key
 * @arg key_len the length of the key
 * @arg value opaque value.
 * @return null if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void art_insert_no_replace(art_tree *t, value_type key, value_type value, const NodeResult& fc);

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void art_delete(art_tree *t, const unsigned char *key, int key_len, const NodeResult& fc);

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
node_ptr art_search(trace_list& trace, const art_tree *t, const unsigned char *key, unsigned key_len);

/**
 * Returns the minimum valued leaf
 * @return The minimum leaf or NULL
 */
const art_leaf* art_minimum(art_tree *t);

/**
 * Returns the maximum valued leaf
 * @return The maximum leaf or NULL
 */
const art_leaf* art_maximum(art_tree *t);

/**
 * Returns the lower bound value of a given key
 * lower bound is defined as first value not less than the key parameter
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return the lower bound or NULL if there is no value not less than key
 */
node_ptr art_lower_bound(const art_tree *t, value_type key);

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
int art_iter(art_tree *t, art_callback cb, void *data);

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
int art_iter_prefix(art_tree *t, const unsigned char *prefix, int prefix_len, art_callback cb, void *data);
/**
 * iterates through a range from small to large from key to key_end
 * the first key is located in log(n) time
 * @return 0 on success, or the return of the callback.
 */
int art_range(const art_tree *t, value_type key, value_type key_end, art_callback cb, void *data);

/**
 * gets per module per node type statistics for all art_node* types  
 * @return art_statistics 
 */
art_statistics art_get_statistics();

/**
 * get statistics for each operation performed
 */
art_ops_statistics art_get_ops_statistics();
