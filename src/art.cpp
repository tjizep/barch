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

compressed_release::compressed_release()
{
    get_leaf_compression().enter_context();
}
compressed_release::~compressed_release()
{
    get_leaf_compression().release_context();
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
    if (n.null()) return;

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
            for (i=0;i<n->data().num_children;i++) {
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
    if (n.null()) return {};
    if (n.is_leaf) return {};

    c = key[std::min(depth, key_len)];
    auto r = n->lower_bound_child(c);
    *is_equal = r.second;
    return r.first;
}

static node_ptr find_child(node_ptr n, unsigned char c) {
    if(n.null()) return nullptr;
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
node_ptr art_search(trace_list& , const art_tree *t, value_type key) {
    ++statistics::get_ops;
    node_ptr n = t->root;
    unsigned depth = 0;
    while (!n.null()) {
        // Might be a leaf
        if(n.is_leaf){

            const auto * l = n.const_leaf();
            if (0 == l->compare(key.bytes, key.length(), depth)) {
                return n;
            }
            return nullptr;
        }
        // Bail if the prefix does not match
        if (n->data().partial_len) {
            unsigned prefix_len = n->check_prefix(key.bytes, key.length(), depth);
            if (prefix_len != std::min<unsigned>(max_prefix_llength, n->data().partial_len))
                return nullptr;
            depth = depth + n->data().partial_len;
            if (depth >= key.length()){
                return nullptr;
            }
        }
        //node_ptr p = n;
        unsigned at = n->index(key[depth]);
        n = n->get_child(at);
        //trace_element te = {p,n,at, key[depth]};
        //trace.push_back(te);
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
static node_ptr lower_bound(trace_list& trace, const art_tree *t, value_type key) {
    node_ptr n = t->root;
    int depth = 0, is_equal = 0;

    while (!n.null()) {
        if (n.is_leaf) {
            // Check if the expanded path matches
            if (n.const_leaf()->compare(key.bytes, key.length(), depth) >= 0) {
                return n;
            }
            return nullptr;
        }

        // Bail if the prefix does not match
        if (n->data().partial_len) {
            int prefix_len = n->check_prefix(key.bytes, key.length(), depth);
            if (prefix_len != std::min<int>(max_prefix_llength, n->data().partial_len))
                break;
            depth += n->data().partial_len;
        }


        trace_element te = lower_bound_child(n, key.bytes, key.length(), depth, &is_equal);
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
    if(n.null()) return {nullptr, nullptr, 0};
    if(n.is_leaf) return {nullptr, nullptr, 0};

    return {n,n->get_child(n->first_index()),0};
}
static trace_element last_child_off(node_ptr n){
    if(n.null()) return {nullptr, nullptr, 0};
    if(n.is_leaf) return {nullptr, nullptr, 0};
    unsigned idx = n->last_index();

    return {n,n->get_child(idx),idx};
}

static trace_element increment_te(const trace_element &te){
    if (te.parent.null()) return {nullptr, nullptr, 0};
    if (te.parent.is_leaf) return {nullptr, nullptr, 0};

    const art_node * n = te.parent.get_node();
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

node_ptr art_lower_bound(const art_tree *t, value_type key) {
    ++statistics::lb_ops;
    node_ptr al;
    trace_list tl;
    al = lower_bound(tl, t, key);
    if (!al.null()) {
        return al;
    }
    return nullptr;
}

int art_range(const art_tree *t, value_type key, value_type key_end, art_callback cb, void *data) {
    ++statistics::range_ops;
    trace_list tl;
    auto lb = lower_bound(tl, t, key);
    if(lb.null()) return 0;
    const art_leaf* al = lb.const_leaf();
    if (al) {
        do {
            node_ptr n = last_el(tl).child;
            if(n.is_leaf){
                const art_leaf * leaf = n.const_leaf();
                if(leaf->compare(key_end.bytes, key_end.size, 0) < 0) { // upper bound is not
                    ++statistics::iter_range_ops;
                    int r = cb(data, leaf->get_key(), leaf->get_value());
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
node_ptr art_minimum(art_tree *t) {
    ++statistics::min_ops;
    auto l = minimum(t->root);
    if(l.null()) return nullptr;
    return l;
}

/**
 * Returns the maximum valued leaf
 */
node_ptr art_maximum(art_tree *t) {
    ++statistics::max_ops;
    auto l = maximum(t->root);
    if (l.null()) return nullptr;
    return l;
}

static node_ptr make_leaf(value_type key, value_type v) {
    unsigned val_len = v.size;
    unsigned key_len = key.length();
    // NB the + 1 is for a hidden 0 byte contained in the key not reflected by length()
    auto logical = get_leaf_compression().new_address(sizeof(art_leaf) + key_len + 1 + val_len);
    auto *l = new(get_leaf_compression().read<art_leaf>(logical)) art_leaf(key_len, val_len);
    ++statistics::leaf_nodes;
    statistics::addressable_bytes_alloc += (int64_t)(sizeof(art_leaf)+ key_len + 1 + val_len);
    l->set_key(key);
    l->set_value(v);
    return logical;
}

static unsigned longest_common_prefix(const art_leaf *l1, const art_leaf *l2, int depth) {
    unsigned max_cmp = std::min<unsigned>(l1->key_len, l2->key_len) - depth;
    unsigned idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (l1->key()[depth+idx] != l2->key()[depth+idx])
            return idx;
    }
    return idx;
}


/**
 * Calculates the index at which the prefixes mismatch
 */
;
static int prefix_mismatch(const node_ptr n, value_type key, int depth) {
    int max_cmp = std::min<int>(std::min<int>(max_prefix_llength, n->data().partial_len), key.length() - depth);
    int idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (n->data().partial[idx] != key[depth+idx])
            return idx;
    }

    // If the prefix is short we can avoid finding a leaf
    if (n->data().partial_len > max_prefix_llength) {
        // Prefix is longer than what we've checked, find a leaf
        const art_leaf *l = minimum(n).const_leaf();
        max_cmp = std::min<unsigned>(l->key_len, key.length()) - depth;
        for (; idx < max_cmp; idx++) {
            if (l->key()[idx+depth] != key[depth+idx])
                return idx;
        }
    }
    return idx;
}
static node_ptr recursive_insert(art_tree* t, node_ptr n, node_ptr &ref, value_type key,  value_type value, int depth, int *old, int replace) {
    // If we are at a nullptr node, inject a leaf
    if (n.null()) {
        ref = make_leaf(key, value);
        return nullptr;
    }
    // If we are at a leaf, we need to replace it with a node
    if (n.is_leaf) {
        const art_leaf *l = n.const_leaf();
        // Check if we are updating an existing value
        if (l->compare(key, depth) == 0) {

            *old = 1;
            if (replace)
            {
                art_leaf *dl = n.leaf();
                if (dl->val_len == value.size)
                {
                    dl->set_value(value);
                }else
                {
                    ref = make_leaf(key, value); // create a new leaf to carry the new value
                    ++statistics::leaf_nodes_replaced;
                    return n;
                }

            }
            return nullptr;
        }
        node_ptr l1 = n;
        // Create a new leaf
        node_ptr l2 = make_leaf(key, value);

        // New value, we must split the leaf into a node_4, pasts the new children to get optimal pointer size
        auto new_stored = alloc_node_ptr(initial_node, {l1, l2});
        auto *new_node = new_stored.get_node();
        // Determine longest prefix
        unsigned longest_prefix = longest_common_prefix(l, l2.const_leaf(), depth);
        new_node->data().partial_len = longest_prefix;
        memcpy(new_node->data().partial, key.bytes+depth, std::min<unsigned>(max_prefix_llength, longest_prefix));
        // Add the leafs to the new node_4
        ref = new_node;
        ref->add_child(l->key()[depth+longest_prefix], ref, l1);
        ref->add_child(l2.const_leaf()->key()[depth+longest_prefix], ref, l2);
        return nullptr;
    }

    // Check if given node has a prefix
    if (n->data().partial_len) {
        // Determine if the prefixes differ, since we need to split
        int prefix_diff = prefix_mismatch(n, key, depth);
        if ((uint32_t)prefix_diff >= n->data().partial_len) {
            depth += n->data().partial_len;
            goto RECURSE_SEARCH;
        }

        // TODO: do fast child adding (by adding multiple children at once)
        // Create a new node and a new leaf
        node_ptr new_leaf = make_leaf(key, value);
        auto new_node = alloc_node_ptr(initial_node, {n, new_leaf}); // pass children to get opt. ptr size
        ref = new_node;
        new_node->data().partial_len = prefix_diff;
        memcpy(new_node->data().partial, n->data().partial, std::min<int>(max_prefix_llength, prefix_diff));
        // Adjust the prefix of the old node
        if (n->data().partial_len <= max_prefix_llength) {
            ref->add_child(n->data().partial[prefix_diff], ref, n);
            n->data().partial_len -= (prefix_diff+1);
            memmove(n->data().partial, n->data().partial+prefix_diff+1,
                    std::min<int>(max_prefix_llength, n->data().partial_len));
        } else {
            n->data().partial_len -= (prefix_diff+1);
            const auto *l = minimum(n).const_leaf();
            ref->add_child(l->get_key()[depth+prefix_diff], ref, n);
            memcpy(n->data().partial, l->key()+depth+prefix_diff+1,
                    std::min<int>(max_prefix_llength, n->data().partial_len));
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
        //trace_element te = {n,child,pos, key[depth]};
        //trace.push_back(te);
    }
    if (!child.null()) {
        node_ptr nc = child;

        auto r = recursive_insert(t, child, nc, key, value, depth+1, old, replace);
        if (nc != child) {
            n = n->expand_pointers(ref, {nc});
            n->set_child(pos, nc);
        }
        return r;
    }

    // No child, node goes within us
    node_ptr l = make_leaf(key, value);
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
void art_insert(art_tree *t, value_type key, value_type value, std::function<void(node_ptr l)> fc) {
    
    int old_val = 0;
    node_ptr old = recursive_insert(t, t->root, t->root, key, value, 0, &old_val, 1);
    if (!old_val){
        t->size++;
        ++statistics::insert_ops;
    } else {
        ++statistics::set_ops;
    }
    if (!old.null())
    {
        if(!old.is_leaf)
        {
            abort();
        }
        fc(old);
        free_leaf_node(old);
    }

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
void art_insert_no_replace(art_tree *t, value_type key, value_type value, const NodeResult& fc) {
    ++statistics::insert_ops;
    int old_val = 0;
    node_ptr r = recursive_insert(t, t->root, t->root, key, value, 0, &old_val, 0);
    if (r.null()){
         t->size++;     
    }
    if (!r.null()) fc(r);
}


static void remove_child(node_ptr n, node_ptr& ref, unsigned char c, unsigned pos) {
    n->remove(ref, pos, c);
}

static const node_ptr recursive_delete(node_ptr n, node_ptr &ref, value_type key, int depth) {
    // Search terminated
    if (n.null()) return nullptr;

    // Handle hitting a leaf node
    if (n.is_leaf) {
        const art_leaf *l = n.const_leaf();
        if (l->compare(key.bytes, key.length(), depth) == 0) {
            ref = nullptr;
            return n;
        }
        return nullptr;
    }

    // Bail if the prefix does not match
    if (n->data().partial_len) {
        unsigned prefix_len = n->check_prefix(key.bytes, key.length(), depth);
        if (prefix_len != std::min<unsigned>(max_prefix_llength, n->data().partial_len)) {
            return nullptr;
        }
        depth = depth + n->data().partial_len;
    }

    // Find child node
    unsigned idx = n->index(key[depth]);
    node_ptr child = n->get_node(idx);
    if (child.null())
        return nullptr;

    // If the child is leaf, delete from this node
    if (child.is_leaf) {
        const art_leaf *l = child.const_leaf();
        if (l->compare(key.bytes, key.length(), depth) == 0) {
            remove_child(n, ref, key[depth], idx);
            return child;
        }
        return nullptr;


    } else {
        // Recurse
        node_ptr new_child = child;
        auto r = recursive_delete(child, new_child, key, depth+1);
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
void art_delete(art_tree *t, value_type key, const NodeResult& fc) {
    ++statistics::delete_ops;
    node_ptr l = recursive_delete(t->root, t->root, key, 0);
    if (!l.null()) {
        t->size--;
        fc(l);
        free_node(l);
    }
}

// Recursively iterates over the tree
static int recursive_iter(node_ptr n, art_callback cb, void *data) {
    // Handle base cases
    if (n.null()) return 0;
    if (n.is_leaf) {
        const art_leaf *l = n.const_leaf();
        ++statistics::iter_ops;
        return cb(data, l->get_key(), l->get_value());
    }

    int idx, res;
    switch (n->type()) {
        case node_4:
            for (int i=0; i < n->data().num_children; i++) {
                res = recursive_iter(n->get_child(i), cb, data);
                if (res) return res;
            }
            break;

        case node_16:
            for (int i=0; i < n->data().num_children; i++) {
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
static int leaf_prefix_compare(const art_leaf *n, value_type prefix) {
    // Fail if the key length is too short
    if (n->key_len < (uint32_t)prefix.length()) return 1;

    // Compare the keys
    return memcmp(n->key(), prefix.bytes, prefix.length());
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
int art_iter_prefix(art_tree *t, value_type key, art_callback cb, void *data) {
    ++statistics::iter_start_ops;
    
    if (!t) {
        return -1;
    }
    
    node_ptr n = t->root;
    unsigned prefix_len, depth = 0;
    while (!n.null()) {
        // Might be a leaf
        if (n.is_leaf) {
            // Check if the expanded path matches
            if (0 == leaf_prefix_compare(n.const_leaf(), key)) {
                const auto *l = n.const_leaf();
                return cb(data, l->get_key(), l->get_value());
            }
            return 0;
        }

        // If the depth matches the prefix, we need to handle this node
        if (depth == key.length()) {
            const art_leaf *l = minimum(n).const_leaf();
            if (0 == leaf_prefix_compare(l, key))
               return recursive_iter(n, cb, data);
            return 0;
        }

        // Bail if the prefix does not match
        if (n->data().partial_len) {
            prefix_len = prefix_mismatch(n, key, depth);

            // Guard if the mis-match is longer than the max_prefix_llength
            if (prefix_len > n->data().partial_len) {
                prefix_len = n->data().partial_len;
            }

            // If there is no match, search is terminated
            if (!prefix_len) {
                return 0;

            // If we've matched the prefix, iterate on this node
            } else if (depth + prefix_len == key.length()) {
                return recursive_iter(n, cb, data);
            }

            // if there is a full match, go deeper
            depth = depth + n->data().partial_len;
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
    as.heap_bytes_allocated = (int64_t)heap::allocated;
    as.leaf_nodes = (int64_t)statistics::leaf_nodes;
    as.node4_nodes = (int64_t)statistics::n4_nodes;
    as.node16_nodes = (int64_t)statistics::n16_nodes;
    as.node256_nodes = (int64_t)statistics::n256_nodes;
    as.node256_occupants = as.node256_nodes ? ((int64_t)statistics::node256_occupants / as.node256_nodes ) : 0ll;
    as.node48_nodes = (int64_t)statistics::n48_nodes;
    as.bytes_allocated = (int64_t)statistics::addressable_bytes_alloc;
    as.bytes_interior = (int64_t)statistics::interior_bytes_alloc;
    as.page_bytes_compressed = (int64_t)statistics::page_bytes_compressed;
    as.page_bytes_uncompressed = (int64_t)statistics::page_bytes_uncompressed;
    as.pages_uncompressed = (int64_t)statistics::pages_uncompressed;
    as.pages_compressed = (int64_t)statistics::pages_compressed;
    as.max_page_bytes_uncompressed = (int64_t)statistics::max_page_bytes_uncompressed;
    as.vacuums_performed = (int64_t)statistics::vacuums_performed;
    as.last_vacuum_time = (int64_t)statistics::last_vacuum_time;
    as.leaf_nodes_replaced = (int64_t)statistics::leaf_nodes_replaced;
    return as;
}

art_ops_statistics art_get_ops_statistics(){

    art_ops_statistics os{};
    os.delete_ops = (int64_t)statistics::delete_ops;
    os.get_ops = (int64_t)statistics::get_ops;
    os.insert_ops = (int64_t)statistics::insert_ops;
    os.iter_ops = (int64_t)statistics::iter_ops;
    os.iter_range_ops = (int64_t)statistics::iter_range_ops;
    os.lb_ops = (int64_t)statistics::lb_ops;
    os.max_ops = (int64_t)statistics::max_ops;
    os.min_ops = (int64_t)statistics::min_ops;
    os.range_ops = (int64_t)statistics::range_ops;
    os.set_ops = (int64_t)statistics::set_ops;
    os.size_ops = (int64_t)statistics::size_ops;
    return os;
}


