#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <cassert>
#include <vector>
#include <atomic>

#include "art.h"
#include "statistics.h"
#include "nodes.h"

#include <algorithm>

uint64_t get_node_base()
{
    static char * base_val = nullptr;
    static int64_t base = 0;

    if(!base_val)
    {
        base_val = (char*)heap::allocate(4);
        base = reinterpret_cast<int64_t>(base_val);
    }
    return base;
}
void free_leaf_node(node_ptr n){
    if(n.null()) return;
    unsigned kl = n.const_leaf()->key_len;
    unsigned vl = n.const_leaf()->val_len;
    //ValkeyModule_Free(n);
    get_leaf_compression().free(n.logical, sizeof(art_leaf) + kl + 1 + vl);
    --statistics::leaf_nodes;
    statistics::node_bytes_alloc -= (sizeof(art_leaf) + kl + 1 + vl);
}

void free_node(node_ptr n){
    if (n.is_leaf) {
        free_leaf_node(n);
    } else {
        free_node(n.node);
    }

}
/**
 * free a node while updating statistics 
 */
void free_node(art_node *n) {
    // Break if null
    if (!n) return;
    size_t alloc_size = n->alloc_size();
    n->~art_node();
    // Free ourself on the way up
    heap::free(n,alloc_size);
}
/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */

template<typename art_node_t>
art_node* alloc_any_node() {
    auto r =  new (heap::allocate<art_node_t>(1)) art_node_t();
    return r;
}
art_node* alloc_node(unsigned nt, const children_t& c) {
    node_ptr ref;
    switch (nt)
    {
    case node_4:
        return (art_node*)alloc_any_node<art_node4_4>()->expand_pointers(ref, c); // : alloc_any_node<art_node4_8>() ;
    case node_16:
        return (art_node*)alloc_any_node<art_node16_4>()->expand_pointers(ref, c); // optimize pointer sizes
    case node_48:
        return (art_node*)alloc_any_node<art_node48_4>()->expand_pointers(ref, c);
    case node_256:
        return (art_node*)alloc_any_node<art_node256_4>()->expand_pointers(ref, c);
    default:
        abort();
    }

}
art_node* alloc_8_node(unsigned nt) {
    switch (nt)
    {
    case node_4:
        return alloc_any_node<art_node4_8>(); // : alloc_any_node<art_node4_8>() ;
    case node_16:
        return alloc_any_node<art_node16_8>(); // optimize pointer sizes
    case node_48:
        return alloc_any_node<art_node48_8>();
    case node_256:
        return alloc_any_node<art_node256_8>();
    default:
        abort();
    }

}

art_node::art_node () = default;
art_node::~art_node() {
    check_object();
    partial_len = 255;
};
/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */

unsigned art_node::check_prefix(const unsigned char *key, unsigned key_len, unsigned depth) {
    unsigned max_cmp = std::min<int>(std::min<int>(partial_len, max_prefix_llength), (int)key_len - (int)depth);
    unsigned idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (partial[idx] != key[depth+idx])
            return idx;
    }
    return idx;
}



