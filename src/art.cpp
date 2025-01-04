#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <assert.h>
#include <vector>
#include <atomic>
#include "art.h"
#include "valkeymodule.h"
#include "statistics.h"
#include "vector.h"


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
        free_node(n);
        return;
    }
    // Handle each node type
    int i, idx;
    switch (n->type()) {
        case NODE4:
        case NODE16:
        case NODE256:
            for (i=0;i<n->num_children;i++) {
                free_node(n->get_node(i));
            }
            break;
        case NODE48:
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
 * Returns the size of the ART tree.
 */


static int leaf_compare(const art_leaf *n, const unsigned char *key, int key_len, int depth) {
    (void)depth;
    // Fail if the key lengths are different
    if (n->key_len != (uint32_t)key_len) return 1;

    // Compare the keys starting at the depth
    return memcmp(n->key, key, key_len);
}


/**
 * find first not less than
 */
static trace_element lower_bound_child(node_ptr n, const unsigned char * key, int key_len, int depth, int * is_equal) {

    int i, uc;
    unsigned char c = 0x00;
    if (!n) return {nullptr, nullptr, 0};
    if (n.is_leaf) return {nullptr, nullptr, 0};

    c = key[std::min(depth, key_len)];
    switch (n->type()) {
        case NODE4:
            {
                auto p = get_node<art_node4>(n.node);
                for (i=0 ; i < n->num_children; i++) {

                    if (p->keys[i] >= c && p->has_child(i)){
                        *is_equal = p->keys[i] == c;
                        return {n,p->get_child(i),i};
                    }
                }
            }
            break;

        
        case NODE16:
            {
                auto p = get_node<art_node16>(n.node);
                int mask = (1 << n->num_children) - 1;
                unsigned bf = bits_oper16(p->keys, nuchar<16>(c), mask, OPERATION_BIT::eq | OPERATION_BIT::gt); // inverse logic
                if (bf) {
                    i = __builtin_ctz(bf);
                    return {n,p->get_child(i),i};
                }
            }
            break;
        

        case NODE48:
            {
                auto p = get_node<art_node48>(n.node);
                /*
                * find first not less than
                * todo: make lb faster by adding bit map index and using __builtin_ctz as above 
                */
                uc = c;
                for (; uc < 256;uc++){
                    i = p->keys[uc];
                    if (i > 0) {
                        *is_equal = (i == c);
                        return {n,p->get_child(i-1),i-1};
                    }
                }
            }
            break;

        case NODE256:
            {   
                auto p = get_node<art_node256>(n.node);
                for (i = c; i < 256; ++i) {
                    if (p->has_child(i)) {// because nodes are ordered accordingly
                        *is_equal = (i == c);
                        return {n,p->get_child(i),i};
                    }
                }
            }
            break;

        default:
            abort();
    }
    return {nullptr, nullptr, 0};
}

static node_ptr find_child(node_ptr n, unsigned char c) {
    int i;
    if(!n) return nullptr;
    if(n.is_leaf) return nullptr;

    i = n->index(c);
    return n ->get_child(i);
}

// Simple inlined if
static inline int min(int a, int b) {
    return (a < b) ? a : b;
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
static bool extend_trace_min(art_node * root, trace_list& trace){
    if (trace.empty()) {
        trace.push_back(first_child_off(root));
    };
    trace_element u = last_el(trace); 
    while(!u.child.is_leaf){
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
        u = last_child_off(u.child.node);
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
void* art_search(const art_tree *t, const unsigned char *key, int key_len) {
    statistics::get_ops++;
    node_ptr n = t->root;
    int prefix_len, depth = 0;
    while (n) {
        // Might be a leaf
        if(n.is_leaf){
            art_leaf * l = n.leaf();
            if (0 == l->compare(key, key_len, depth)) {
                return l->value;
            }
            return NULL;
        }
        // Bail if the prefix does not match
        if (n->partial_len) {
            prefix_len = n->check_prefix(key, key_len, depth);
            if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len))
                return NULL;
            depth = depth + n->partial_len;
            if (depth >= key_len){
                return NULL;
            }
        }
        unsigned at = n->index(key[depth]);
        n = n->get_child(at);
        
        depth++;
    }
    return NULL;
}

// Find the maximum leaf under a node
static node_ptr maximum(node_ptr n) {
    // Handle base cases
    if (!n) return nullptr;
    if (n.is_leaf) return n;

    int idx;
    switch (n->type()) {
        case NODE4:
        case NODE16:
            return maximum(n->get_node(n->num_children-1));        
        case NODE48:
            idx=255;
            while (!n->get_key(idx)) idx--;
            idx = n->get_key(idx) - 1;
            return maximum(n->get_node(idx));
        case NODE256:
            idx=255;
            while (n->get_node(idx).null()) idx--;
            return maximum(n->get_node(idx));
        default:
            abort();
    }
}


// Find the minimum leaf under a node
static node_ptr minimum(const node_ptr n) {
    // Handle base cases
    if (n.null()) return nullptr;
    if (n.is_leaf) return n;

    int idx;
    switch (n->type()) {
        case NODE4:
        case NODE16:
            return minimum(n->get_node(0));        
        case NODE48:
            idx=0;
            while (!n->get_key(idx)) idx++;
            idx = n->get_key(idx) - 1;
            return minimum(n->get_node(idx));
        case NODE256:
            idx=0;
            while (n->get_node(idx).null()) idx++;
            return minimum(n->get_node(idx));
        default:
            abort();
    }
}

/**
 * Searches for the lower bound key
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the leaf containing the value pointer is returned.
 */
static const node_ptr lower_bound(trace_list& trace, const art_tree *t, const unsigned char *key, int key_len) {
    node_ptr n = t->root;
    int prefix_len, depth = 0, is_equal = 0;

    while (n) {
        if (n.is_leaf) {
            // Check if the expanded path matches
            if (leaf_compare(n.leaf(), key, key_len, depth) >= 0) {
                return n;
            }
            return nullptr;
        }

        // Bail if the prefix does not match
        if (n->partial_len) {
            prefix_len = n->check_prefix(key, key_len, depth);
            if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len))
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
    int idx;
    if(!n) return {nullptr, nullptr, 0};
    if(n.is_leaf) return {nullptr, nullptr, 0};
    idx = n->last_index();

    return {n,n->get_child(idx),idx};
}

static trace_element increment_te(const trace_element &te){
    int i, uc;
    if (!te.el) return {nullptr, nullptr, 0};
    if (te.el.is_leaf) return {nullptr, nullptr, 0};

    art_node * n = te.el.node; 
    switch (n->type()) {
        case NODE4:
            i = te.child_ix + 1;
            if (i < n->num_children) {
                return {n,n->get_child(i),i};
            }
            break;
        
        case NODE16:
            i = te.child_ix + 1;
            if (i < n->num_children) {
                return {n,n->get_child(i),i};// the keys are ordered so fine I think
            }
            break;
        

        case NODE48:{
            auto *p = get_node<art_node48>(n);
            uc = te.child_ix + 1;
            for (; uc < 256;uc++){
                i = p->keys[uc];
                if(i > 0){
                    return {n,p->get_child(i-1),i-1};
                }
            }
            }
            break;

        case NODE256: {
            auto *p = get_node<art_node256>(n);
            for (i = te.child_ix+1; i < 256; ++i) {
                if (p->has_child(i)) {// because nodes are ordered accordingly
                    return {n,p->get_child(i),i};
                }
            }
            }
            break;

        default:
            abort();
    }
    return {nullptr, nullptr, 0};
}


static bool increment_trace(node_ptr root, trace_list& trace){
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
    statistics::lb_ops++;
    node_ptr al;
    trace_list tl;
    al = lower_bound(tl, t, key, key_len);
    if (al) {
        return al.leaf()->value;
    }
    return NULL;
}

int art_range(const art_tree *t, const unsigned char *key, int key_len, const unsigned char *key_end, int key_end_len, art_callback cb, void *data) {
    statistics::range_ops++;
    trace_list tl;
    const art_leaf* al;
    al = lower_bound(tl, t, key, key_len).leaf();
    if (al) {
        do {
            node_ptr n = last_el(tl).child;
            if(n.is_leaf){
                art_leaf * leaf = n.leaf();
                if(leaf_compare(leaf, key_end, key_end_len, 0) < 0) { // upper bound is not
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
art_leaf* art_minimum(art_tree *t) {
    statistics::min_ops++;
    return minimum(t->root).leaf();
}

/**
 * Returns the maximum valued leaf
 */
art_leaf* art_maximum(art_tree *t) {
    statistics::max_ops++;
    return maximum(t->root).leaf();
}

static art_leaf* make_leaf(const unsigned char *key, int key_len, void *value) {
    art_leaf *l = (art_leaf*)ValkeyModule_Calloc(1, sizeof(art_leaf)+key_len);
    statistics::leaf_nodes++;
    statistics::node_bytes_alloc += (sizeof(art_leaf)+key_len);
    l->value = value;
    l->key_len = key_len;
    memcpy(l->key, key, key_len);
    return l;
}

static int longest_common_prefix(art_leaf *l1, art_leaf *l2, int depth) {
    int max_cmp = min(l1->key_len, l2->key_len) - depth;
    int idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (l1->key[depth+idx] != l2->key[depth+idx])
            return idx;
    }
    return idx;
}


/**
 * Calculates the index at which the prefixes mismatch
 */
static int prefix_mismatch(const node_ptr n, const unsigned char *key, int key_len, int depth) {
    int max_cmp = min(min(MAX_PREFIX_LEN, n->partial_len), key_len - depth);
    int idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (n->partial[idx] != key[depth+idx])
            return idx;
    }

    // If the prefix is short we can avoid finding a leaf
    if (n->partial_len > MAX_PREFIX_LEN) {
        // Prefix is longer than what we've checked, find a leaf
        art_leaf *l = minimum(n).leaf();
        max_cmp = min(l->key_len, key_len)- depth;
        for (; idx < max_cmp; idx++) {
            if (l->key[idx+depth] != key[depth+idx])
                return idx;
        }
    }
    return idx;
}

static void* recursive_insert(node_ptr n, node_ptr &ref, const unsigned char *key, int key_len, void *value, int depth, int *old, int replace) {
    // If we are at a NULL node, inject a leaf
    if (!n) {
        ref = make_leaf(key, key_len, value);
        return NULL;
    }

    // If we are at a leaf, we need to replace it with a node
    if (n.is_leaf) {
        art_leaf *l = n.leaf();

        // Check if we are updating an existing value
        if (l->compare(key, key_len, depth) == 0) {
            *old = 1;
            void *old_val = l->value;
            if(replace) l->value = value;
            return old_val;
        }

        // New value, we must split the leaf into a node4
        art_node4 *new_node = alloc_node<art_node4>();

        // Create a new leaf
        art_leaf *l2 = make_leaf(key, key_len, value);

        // Determine longest prefix
        int longest_prefix = longest_common_prefix(l, l2, depth);
        new_node->partial_len = longest_prefix;
        memcpy(new_node->partial, key+depth, min(MAX_PREFIX_LEN, longest_prefix));
        // Add the leafs to the new node4
        ref = new_node;
        new_node->add_child(l->key[depth+longest_prefix], ref, l);
        new_node->add_child(l2->key[depth+longest_prefix], ref, l2);
        return NULL;
    }

    // Check if given node has a prefix
    if (n->partial_len) {
        // Determine if the prefixes differ, since we need to split
        int prefix_diff = prefix_mismatch(n, key, key_len, depth);
        if ((uint32_t)prefix_diff >= n->partial_len) {
            depth += n->partial_len;
            goto RECURSE_SEARCH;
        }

        // Create a new node
        art_node4 *new_node = alloc_node<art_node4>();
        ref = new_node;
        new_node->partial_len = prefix_diff;
        memcpy(new_node->partial, n->partial, min(MAX_PREFIX_LEN, prefix_diff));

        // Adjust the prefix of the old node
        if (n->partial_len <= MAX_PREFIX_LEN) {
            new_node->add_child(n->partial[prefix_diff], ref, n);
            n->partial_len -= (prefix_diff+1);
            memmove(n->partial, n->partial+prefix_diff+1,
                    min(MAX_PREFIX_LEN, n->partial_len));
        } else {
            n->partial_len -= (prefix_diff+1);
            art_leaf *l = minimum(n).leaf();
            new_node->add_child(l->key[depth+prefix_diff], ref, n);
            memcpy(n->partial, l->key+depth+prefix_diff+1,
                    min(MAX_PREFIX_LEN, n->partial_len));
        }

        // Insert the new leaf
        art_leaf *l = make_leaf(key, key_len, value);
        new_node->add_child(key[depth+prefix_diff], ref, l);
        return NULL;
    }

RECURSE_SEARCH:;

    // Find a child to recurse to
    unsigned pos = n->index(key[depth]);
    node_ptr child = n->get_node(pos);
    if (child) {
        node_ptr nc = child;
        auto r = recursive_insert(child, nc, key, key_len, value, depth+1, old, replace);
        if (nc != child) {
            n->set_child(pos, nc);
        }
        return r;
    }

    // No child, node goes within us
    art_leaf *l = make_leaf(key, key_len, value);
    n->add_child(key[depth], ref, l);
    return NULL;
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
    void *old = recursive_insert(t->root, t->root, key, key_len, value, 0, &old_val, 1);
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

    int old_val = 0;
    void *old = recursive_insert(t->root, t->root, key, key_len, value, 0, &old_val, 0);
    if (!old_val){
         t->size++;     
    }
    return old;
}


static void remove_child(node_ptr n, node_ptr& ref, unsigned char c, unsigned pos) {
    n->remove(ref, pos, c);
}

static art_leaf* recursive_delete(node_ptr n, node_ptr &ref, const unsigned char *key, int key_len, int depth) {
    // Search terminated
    if (!n) return NULL;

    // Handle hitting a leaf node
    if (n.is_leaf) {
        art_leaf *l = n.leaf();
        if (l->compare(key, key_len, depth) == 0) {
            ref = nullptr;
            return l;
        }
        return NULL;
    }

    // Bail if the prefix does not match
    if (n->partial_len) {
        unsigned prefix_len = n->check_prefix(key, key_len, depth);
        if (prefix_len != std::min<unsigned>(MAX_PREFIX_LEN, n->partial_len)) {
            return NULL;
        }
        depth = depth + n->partial_len;
    }

    // Find child node
    unsigned idx = n->index(key[depth]);
    node_ptr child = n->get_node(idx);
    if (!child) 
        return NULL;

    // If the child is leaf, delete from this node
    if (child.is_leaf) {
        art_leaf *l = child.leaf();
        if (l->compare(key, key_len, depth) == 0) {
            remove_child(n, ref, key[depth], idx);
            return l;
        }
        return NULL;

    // Recurse
    } else {
        node_ptr new_child = child;
        auto r = recursive_delete(child, new_child, key, key_len, depth+1);
        if (new_child != child) {
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
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_delete(art_tree *t, const unsigned char *key, int key_len) {
    ++statistics::delete_ops;
    art_leaf *l = recursive_delete(t->root, t->root, key, key_len, 0);
    if (l) {
        t->size--;
        void *old = l->value;
        free_node(l);
        
        return old;
    } else
        return NULL;
}

// Recursively iterates over the tree
static int recursive_iter(node_ptr n, art_callback cb, void *data) {
    // Handle base cases
    if (!n) return 0;
    if (n.is_leaf) {
        art_leaf *l = n.leaf();
        ++statistics::iter_ops;
        return cb(data, (const unsigned char*)l->key, l->key_len, l->value);
    }

    int idx, res;
    switch (n->type()) {
        case NODE4:
            for (int i=0; i < n->num_children; i++) {
                res = recursive_iter(n->get_child(i), cb, data);
                if (res) return res;
            }
            break;

        case NODE16:
            for (int i=0; i < n->num_children; i++) {
                res = recursive_iter(n->get_child(i), cb, data);
                if (res) return res;
            }
            break;

        case NODE48:
            for (int i=0; i < 256; i++) {
                idx = n->get_key(i);
                if (!idx) continue;

                res = recursive_iter(n->get_child(idx-1), cb, data);
                if (res) return res;
            }
            break;

        case NODE256:
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
                art_leaf *l = (art_leaf*)n.leaf();
                return cb(data, (const unsigned char*)l->key, l->key_len, l->value);
            }
            return 0;
        }

        // If the depth matches the prefix, we need to handle this node
        if (depth == key_len) {
            art_leaf *l = minimum(n).leaf();
            if (0 == leaf_prefix_compare(l, key, key_len))
               return recursive_iter(n, cb, data);
            return 0;
        }

        // Bail if the prefix does not match
        if (n->partial_len) {
            prefix_len = prefix_mismatch(n, key, key_len, depth);

            // Guard if the mis-match is longer than the MAX_PREFIX_LEN
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

    if(t == NULL) 
        return 0;
    
    return t->size;
}

art_statistics art_get_statistics(){
    art_statistics as;
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

    art_ops_statistics os;
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


