#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <cassert>
#include <vector>
#include <atomic>
#include "art.h"
#include "valkeymodule.h"
#include "statistics.h"
#include "compress.h"

compress & get_leaf_compression()
{
    static compress leaf_compression;
    return leaf_compression;
};

compressed_release::~compressed_release()
{
    get_leaf_compression().release_decompressed();
}
extern void free_leaf_node(node_ptr n);
/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art_tree *t) {
    t->root = nullptr;
    t->size = 0;
    return 0;
}
// Recursively destroys the tree
static void destroy_node(node_ptr n) {
    // Break if null
    if (!n) return;

    if (n.is_leaf){
        free_leaf_node(n);
        return;
    }
    // Handle each node type
    int i, idx;
    switch (n->type()) {
        case node_4:
        case node_16:
        case node_256:
            for (i=0;i<n->num_children;i++) {
                free_node(n->get_node(i));
            }
            break;
        case node_48:
            for (i=0;i<256;i++) {
                idx = n->get_key(i); 
                if (!idx) continue; 
                
                free_node(n->get_node(i));
                
            }
            break;
        default:
            abort();
    }

    // Free ourself on the way up
    free_node(n);
}

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int art_tree_destroy(art_tree *t) {
    destroy_node(t->root);
    return 0;
}

/**
 * find first not less than
 */
static trace_element lower_bound_child(node_ptr n, const unsigned char * key, int key_len, int depth, int * is_equal) {

    unsigned char c = 0x00;
    if (!n) return {};
    if (n.is_leaf) return {};

    c = key[std::min(depth, key_len)];
    auto r = n->lower_bound_child(c);
    *is_equal = r.second;
    return r.first;
}

static node_ptr find_child(node_ptr n, unsigned char c) {
    if(!n) return nullptr;
    if(n.is_leaf) return nullptr;

    unsigned i = n->index(c);
    return n ->get_child(i);
}


/**
 * return last element of trace unless its empty
 */
static trace_element& last_el(trace_list& trace){
    if(trace.empty())
        abort();
    return *(trace.rbegin());
}
static trace_element first_child_off(node_ptr n);
static trace_element last_child_off(node_ptr n);
static node_ptr maximum(node_ptr n);
/**
 * assuming that the path to each leaf is not the same depth
 * we always have to check and extend if required
 * @return false if any non leaf node has no child 
 */
static bool extend_trace_min(node_ptr root, trace_list& trace){
    if (trace.empty()) {
        trace.push_back(first_child_off(root));
    };
    trace_element u = last_el(trace); 
    while (!u.child.is_leaf) {
        u = first_child_off(u.child);
        if(u.empty()) return false;
        trace.push_back(u);
    }
    return true;
}

static bool extend_trace_max(node_ptr root, trace_list& trace){
    if (trace.empty()) {
        trace.push_back(last_child_off(root));
    };
    trace_element u = last_el(trace); 
    while (!u.child.is_leaf) {
        u = last_child_off(u.child);
        if (u.empty()) return false;
        trace.push_back(u);
    }
    return true;
}



/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_search(trace_list& trace, const art_tree *t, const unsigned char *key, unsigned key_len) {
    ++statistics::get_ops;
    node_ptr n = t->root;
    unsigned depth = 0;
    while (!n.null()) {
        // Might be a leaf
        if(n.is_leaf){

            auto * l = n.leaf();
            if (0 == l->compare(key, key_len, depth)) {
                return l->value;
            }
            return nullptr;
        }
        // Bail if the prefix does not match
        if (n->partial_len) {
            unsigned prefix_len = n->check_prefix(key, key_len, depth);
            if (prefix_len != std::min<unsigned>(max_prefix_llength, n->partial_len))
                return nullptr;
            depth = depth + n->partial_len;
            if (depth >= key_len){
                return nullptr;
            }
        }
        node_ptr p = n;
        unsigned at = n->index(key[depth]);
        n = n->get_child(at);
        trace_element te = {p,n,at, key[depth]};
        trace.push_back(te);
        depth++;
    }
    return nullptr;
}

// Find the maximum leaf under a node
static node_ptr maximum(node_ptr n) {
    // Handle base cases
    if (n.null()) return nullptr;
    if (n.is_leaf) return n;
    return maximum(n->last());
}


// Find the minimum leaf under a node
static node_ptr minimum(const node_ptr& n) {
    // Handle base cases
    if (n.null()) return nullptr;
    if (n.is_leaf) return n;
    return minimum(n->get_child(n->first_index()));
}

/**
 * Searches for the lower bound key
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return nullptr if the item was not found, otherwise
 * the leaf containing the value pointer is returned.
 */
static node_ptr lower_bound(trace_list& trace, const art_tree *t, const unsigned char *key, int key_len) {
    node_ptr n = t->root;
    int depth = 0, is_equal = 0;

    while (!n.null()) {
        if (n.is_leaf) {
            // Check if the expanded path matches
            if (n.leaf()->compare(key, key_len, depth) >= 0) {
                return n;
            }
            return nullptr;
        }

        // Bail if the prefix does not match
        if (n->partial_len) {
            int prefix_len = n->check_prefix(key, key_len, depth);
            if (prefix_len != std::min<int>(max_prefix_llength, n->partial_len))
                break;
            depth += n->partial_len;
        }


        trace_element te = lower_bound_child(n, key, key_len, depth, &is_equal);
        if(!te.empty()){
            trace.push_back(te);
        }
        n = te.child;
        depth++;
    }
    if (!extend_trace_max(t->root, trace)) return nullptr;
    return last_el(trace).child;
}
static trace_element first_child_off(node_ptr n){
    if(!n) return {nullptr, nullptr, 0};
    if(n.is_leaf) return {nullptr, nullptr, 0};

    return {n,n->get_child(n->first_index()),0};
}
static trace_element last_child_off(node_ptr n){
    if(!n) return {nullptr, nullptr, 0};
    if(n.is_leaf) return {nullptr, nullptr, 0};
    unsigned idx = n->last_index();

    return {n,n->get_child(idx),idx};
}

static trace_element increment_te(const trace_element &te){
    if (!(const art_node*)te.parent) return {nullptr, nullptr, 0};
    if (te.parent.is_leaf) return {nullptr, nullptr, 0};

    art_node * n = te.parent.node;
    return n->next(te);
}


static bool increment_trace(const node_ptr& root, trace_list& trace){
    for(auto r = trace.rbegin(); r != trace.rend(); ++r){
        trace_element te = increment_te(*r);
        if(te.empty())
            continue; // goto the parent further back and try to increment that 
        *r = te;
        if (r != trace.rbegin()){
            auto u = r;
            // go forward
            do {
                --u;
                te = first_child_off(te.child);
                if(te.empty())
                    return false;
                *u = te;

            } while(u != trace.rbegin());
        }
        return extend_trace_min(root, trace);
    
    }
    return false;
}

void* art_lower_bound(const art_tree *t, const unsigned char *key, int key_len) {
    ++statistics::lb_ops;
    node_ptr al;
    trace_list tl;
    al = lower_bound(tl, t, key, key_len);
    if (al) {
        return al.leaf()->value;
    }
    return nullptr;
}

int art_range(const art_tree *t, const unsigned char *key, int key_len, const unsigned char *key_end, int key_end_len, art_callback cb, void *data) {
    ++statistics::range_ops;
    trace_list tl;
    auto lb = lower_bound(tl, t, key, key_len);
    if(lb.null()) return 0;
    const art_leaf* al = lb.leaf();
    if (al) {
        do {
            node_ptr n = last_el(tl).child;
            if(n.is_leaf){
                const art_leaf * leaf = n.leaf();
                if(leaf->compare(key_end, key_end_len, 0) < 0) { // upper bound is not
                    ++statistics::iter_range_ops;
                    int r = cb(data, leaf->key, leaf->key_len, leaf->value); 
                    if( r != 0) 
                        return r;
                } else {
                    return 0;
                }
            } else {
                abort();
            }
        
        } while(increment_trace(t->root,tl));
        
    }
    return 0;
}
/**
 * Returns the minimum valued leaf
 */
const art_leaf* art_minimum(art_tree *t) {
    ++statistics::min_ops;
    auto l = minimum(t->root);
    if(l.null()) return nullptr;
    return l.leaf();
}

/**
 * Returns the maximum valued leaf
 */
const art_leaf* art_maximum(art_tree *t) {
    ++statistics::max_ops;
    auto l = maximum(t->root);
    if (l.null()) return nullptr;
    return l.leaf();
}

static node_ptr make_leaf(const unsigned char *key, int key_len, void *value) {
    auto logical = get_leaf_compression().new_address(sizeof(art_leaf) + key_len);
    auto *l = get_leaf_compression().resolve<art_leaf>(logical);
    ++statistics::leaf_nodes;
    statistics::node_bytes_alloc += (int64_t)(sizeof(art_leaf)+key_len);
    l->value = value;
    l->key_len = key_len;
    memcpy(l->key, key, key_len);
    return logical;
}

static unsigned longest_common_prefix(const art_leaf *l1, const art_leaf *l2, int depth) {
    unsigned max_cmp = std::min<unsigned>(l1->key_len, l2->key_len) - depth;
    unsigned idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (l1->key[depth+idx] != l2->key[depth+idx])
            return idx;
    }
    return idx;
}


/**
 * Calculates the index at which the prefixes mismatch
 */
;
static int prefix_mismatch(const node_ptr n, const unsigned char *key, int key_len, int depth) {
    int max_cmp = std::min<int>(std::min<int>(max_prefix_llength, n->partial_len), key_len - depth);
    int idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (n->partial[idx] != key[depth+idx])
            return idx;
    }

    // If the prefix is short we can avoid finding a leaf
    if (n->partial_len > max_prefix_llength) {
        // Prefix is longer than what we've checked, find a leaf
        const art_leaf *l = minimum(n).leaf();
        max_cmp = std::min<unsigned short>(l->key_len, key_len) - depth;
        for (; idx < max_cmp; idx++) {
            if (l->key[idx+depth] != key[depth+idx])
                return idx;
        }
    }
    return idx;
}
static void* recursive_insert(trace_list& trace, art_tree* t, node_ptr n, node_ptr &ref, const unsigned char *key, int key_len, void *value, int depth, int *old, int replace) {
    // If we are at a nullptr node, inject a leaf
    if (n.null()) {
        ref = make_leaf(key, key_len, value);
        return nullptr;
    }
    // If we are at a leaf, we need to replace it with a node
    if (n.is_leaf) {
        art_leaf *l = n.leaf();
        node_ptr l1 = n;
        // Check if we are updating an existing value
        if (l->compare(key, key_len, depth) == 0) {

            *old = 1;
            void *old_val = l->value;
            if(replace) l->value = value;
            return old_val;
        }
        // Create a new leaf
        node_ptr l2 = make_leaf(key, key_len, value);

        // New value, we must split the leaf into a node_4, pasts the new children to get optimal pointer size
        auto *new_node = alloc_node(initial_node, {l1, l2});
        // Determine longest prefix
        unsigned longest_prefix = longest_common_prefix(l, l2.leaf(), depth);
        new_node->partial_len = longest_prefix;
        memcpy(new_node->partial, key+depth, std::min<unsigned>(max_prefix_llength, longest_prefix));
        // Add the leafs to the new node_4
        ref = new_node;
        ref->add_child(l->key[depth+longest_prefix], ref, l1);
        ref->add_child(l2.leaf()->key[depth+longest_prefix], ref, l2);
        return nullptr;
    }

    // Check if given node has a prefix
    if (n->partial_len) {
        // Determine if the prefixes differ, since we need to split
        int prefix_diff = prefix_mismatch(n, key, key_len, depth);
        if ((uint32_t)prefix_diff >= n->partial_len) {
            depth += n->partial_len;
            goto RECURSE_SEARCH;
        }

        // TODO: do fast child adding (by adding multiple children at once)
        // Create a new node and a new leaf
        node_ptr new_leaf = make_leaf(key, key_len, value);
        art_node *new_node = alloc_node(initial_node, {n, new_leaf}); // pass children to get opt. ptr size
        ref = new_node;
        new_node->partial_len = prefix_diff;
        memcpy(new_node->partial, n->partial, std::min<int>(max_prefix_llength, prefix_diff));
        // Adjust the prefix of the old node
        if (n->partial_len <= max_prefix_llength) {
            ref->add_child(n->partial[prefix_diff], ref, n);
            n->partial_len -= (prefix_diff+1);
            memmove(n->partial, n->partial+prefix_diff+1,
                    std::min<int>(max_prefix_llength, n->partial_len));
        } else {
            n->partial_len -= (prefix_diff+1);
            art_leaf *l = minimum(n).leaf();
            ref->add_child(l->key[depth+prefix_diff], ref, n);
            memcpy(n->partial, l->key+depth+prefix_diff+1,
                    std::min<int>(max_prefix_llength, n->partial_len));
        }

        // Insert the new leaf (safely considering optimal pointer sizes)

        ref->add_child(key[depth+prefix_diff], ref, new_leaf);

        return nullptr;
    }
    // if node doesnt have a prefix search more
RECURSE_SEARCH:;

    // Find a child to recurse to
    unsigned pos = n->index(key[depth]);
    node_ptr child = n->get_node(pos);
    if(!n.is_leaf)
    {
        trace_element te = {n,child,pos, key[depth]};
        trace.push_back(te);
    }
    if (!child.null()) {
        node_ptr nc = child;

        auto r = recursive_insert(trace, t, child, nc, key, key_len, value, depth+1, old, replace);
        if (nc != child) {
            n = n->expand_pointers(ref, {nc});
            n->set_child(pos, nc);
        }
        return r;
    }

    // No child, node goes within us
    node_ptr l = make_leaf(key, key_len, value);
    n->add_child(key[depth], ref, l);
    return nullptr;
}

/**
 * inserts a new value into the art tree
 * @arg t the tree
 * @arg key the key
 * @arg key_len the length of the key
 * @arg value opaque value.
 * @return null if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void* art_insert(art_tree *t, const unsigned char *key, int key_len, void *value) {
    
    int old_val = 0;
    trace_list trace;
    trace.reserve(key_len);
    void *old = recursive_insert(trace, t, t->root, t->root, key, key_len, value, 0, &old_val, 1);
    if (!old_val){
        t->size++;
        ++statistics::insert_ops;
    } else {
        ++statistics::set_ops;
    }
    return old;
}

/**
 * inserts a new value into the art tree (no replace)
 * @arg t the tree
 * @arg key the key
 * @arg key_len the length of the key
 * @arg value opaque value.
 * @return null if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void* art_insert_no_replace(art_tree *t, const unsigned char *key, int key_len, void *value) {
    ++statistics::insert_ops;
    trace_list trace;
    trace.reserve(key_len);
    int old_val = 0;
    void *old = recursive_insert(trace, t, t->root, t->root, key, key_len, value, 0, &old_val, 0);
    if (!old_val){
         t->size++;     
    }
    return old;
}


static void remove_child(node_ptr n, node_ptr& ref, unsigned char c, unsigned pos) {
    n->remove(ref, pos, c);
}

static const node_ptr recursive_delete(node_ptr n, node_ptr &ref, const unsigned char *key, int key_len, int depth) {
    // Search terminated
    if (n.null()) return nullptr;

    // Handle hitting a leaf node
    if (n.is_leaf) {
        const art_leaf *l = n.leaf();
        if (l->compare(key, key_len, depth) == 0) {
            ref = nullptr;
            return n;
        }
        return nullptr;
    }

    // Bail if the prefix does not match
    if (n->partial_len) {
        unsigned prefix_len = n->check_prefix(key, key_len, depth);
        if (prefix_len != std::min<unsigned>(max_prefix_llength, n->partial_len)) {
            return nullptr;
        }
        depth = depth + n->partial_len;
    }

    // Find child node
    unsigned idx = n->index(key[depth]);
    node_ptr child = n->get_node(idx);
    if (child.null())
        return nullptr;

    // If the child is leaf, delete from this node
    if (child.is_leaf) {
        const art_leaf *l = child.leaf();
        if (l->compare(key, key_len, depth) == 0) {
            remove_child(n, ref, key[depth], idx);
            return child;
        }
        return nullptr;


    } else {
        // Recurse
        node_ptr new_child = child;
        auto r = recursive_delete(child, new_child, key, key_len, depth+1);
        if (new_child != child) {
            if(!n->ok_child(new_child))
            {
                ref = n->expand_pointers(ref,{new_child});
                ref->set_child(idx, new_child);
            }else
                n->set_child(idx, new_child);
        }
        return r;
    }
}

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return nullptr if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_delete(art_tree *t, const unsigned char *key, int key_len) {
    ++statistics::delete_ops;
    node_ptr l = recursive_delete(t->root, t->root, key, key_len, 0);
    if (!l.null()) {
        t->size--;
        void *old = l.leaf()->value;
        free_node(l);
        
        return old;
    } else
        return nullptr;
}

// Recursively iterates over the tree
static int recursive_iter(node_ptr n, art_callback cb, void *data) {
    // Handle base cases
    if (n.null()) return 0;
    if (n.is_leaf) {
        const art_leaf *l = n.leaf();
        ++statistics::iter_ops;
        return cb(data, static_cast<const unsigned char*>(l->key), l->key_len, l->value);
    }

    int idx, res;
    switch (n->type()) {
        case node_4:
            for (int i=0; i < n->num_children; i++) {
                res = recursive_iter(n->get_child(i), cb, data);
                if (res) return res;
            }
            break;

        case node_16:
            for (int i=0; i < n->num_children; i++) {
                res = recursive_iter(n->get_child(i), cb, data);
                if (res) return res;
            }
            break;

        case node_48:
            for (int i=0; i < 256; i++) {
                idx = n->get_key(i);
                if (!idx) continue;

                res = recursive_iter(n->get_child(idx-1), cb, data);
                if (res) return res;
            }
            break;

        case node_256:
            for (int i=0; i < 256; i++) {
                if (!n->has_child(i)) continue;
                res = recursive_iter(n->get_child(i), cb, data);
                if (res) return res;
            }
            break;

        default:
            abort();
    }
    return 0;
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
int art_iter(art_tree *t, art_callback cb, void *data) {
    ++statistics::iter_start_ops;
    if (!t) {
        return -1;
    }
    return recursive_iter(t->root, cb, data);
}

/**
 * Checks if a leaf prefix matches
 * @return 0 on success.
 */
static int leaf_prefix_compare(const art_leaf *n, const unsigned char *prefix, int prefix_len) {
    // Fail if the key length is too short
    if (n->key_len < (uint32_t)prefix_len) return 1;

    // Compare the keys
    return memcmp(n->key, prefix, prefix_len);
}

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
int art_iter_prefix(art_tree *t, const unsigned char *key, int key_len, art_callback cb, void *data) {
    ++statistics::iter_start_ops;
    
    if (!t) {
        return -1;
    }
    
    node_ptr child;
    node_ptr n = t->root;
    int prefix_len, depth = 0;
    while (n) {
        // Might be a leaf
        if (n.is_leaf) {
            // Check if the expanded path matches
            if (0 == leaf_prefix_compare(n.leaf(), key, key_len)) {
                auto *l = (art_leaf*)n.leaf();
                return cb(data, (const unsigned char*)l->key, l->key_len, l->value);
            }
            return 0;
        }

        // If the depth matches the prefix, we need to handle this node
        if (depth == key_len) {
            const art_leaf *l = minimum(n).leaf();
            if (0 == leaf_prefix_compare(l, key, key_len))
               return recursive_iter(n, cb, data);
            return 0;
        }

        // Bail if the prefix does not match
        if (n->partial_len) {
            prefix_len = prefix_mismatch(n, key, key_len, depth);

            // Guard if the mis-match is longer than the max_prefix_llength
            if ((uint32_t)prefix_len > n->partial_len) {
                prefix_len = n->partial_len;
            }

            // If there is no match, search is terminated
            if (!prefix_len) {
                return 0;

            // If we've matched the prefix, iterate on this node
            } else if (depth + prefix_len == key_len) {
                return recursive_iter(n, cb, data);
            }

            // if there is a full match, go deeper
            depth = depth + n->partial_len;
        }

        // Recursively search
        n = find_child(n, key[depth]);
        depth++;
    }
    return 0;
}
/**
 * just return the size
 */
uint64_t art_size(art_tree *t) {
    ++statistics::size_ops;

    if(t == nullptr)
        return 0;
    
    return t->size;
}

art_statistics art_get_statistics(){
    art_statistics as{};
    as.leaf_nodes = statistics::leaf_nodes;
    as.node4_nodes = statistics::n4_nodes;
    as.node16_nodes = statistics::n16_nodes;
    as.node256_nodes = statistics::n256_nodes;
    as.node256_occupants = as.node256_nodes ? (statistics::node256_occupants / as.node256_nodes ) : 0ll;
    as.node48_nodes = statistics::n48_nodes;
    as.bytes_allocated = statistics::node_bytes_alloc;
    as.bytes_interior = statistics::interior_bytes_alloc;
    return as;
}

art_ops_statistics art_get_ops_statistics(){

    art_ops_statistics os{};
    os.delete_ops = statistics::delete_ops;
    os.get_ops = statistics::get_ops;
    os.insert_ops = statistics::insert_ops;
    os.iter_ops = statistics::iter_ops;
    os.iter_range_ops = statistics::iter_range_ops;
    os.lb_ops = statistics::lb_ops;
    os.max_ops = statistics::max_ops;
    os.min_ops = statistics::min_ops;
    os.range_ops = statistics::range_ops;
    os.set_ops = statistics::set_ops;
    os.size_ops = statistics::size_ops;
    return os;
}


