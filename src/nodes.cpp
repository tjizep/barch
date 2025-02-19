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
        base_val = (char*)ValkeyModule_Calloc(1,4);
        base = reinterpret_cast<int64_t>(base_val);
    }
    return base;
}
void free_leaf_node(node_ptr n){
    if(n.null()) return;
    unsigned kl = n.leaf()->key_len;
    //ValkeyModule_Free(n);
    get_leaf_compression().free(n.logical, sizeof(art_leaf) + kl);
    --statistics::leaf_nodes;
    statistics::node_bytes_alloc -= (sizeof(art_leaf) + kl);
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

    n->~art_node();
    // Free ourself on the way up
    ValkeyModule_Free(n);
}
/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */

template<typename art_node_t>
art_node* alloc_any_node() {
    auto r =  new (ValkeyModule_Calloc(1, sizeof(art_node_t))) art_node_t();
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
        return alloc_any_node<art_node256>();
    default:
        abort();
    }

}
art_node* alloc_8_node(unsigned nt) {
    node_ptr ref;
    node_ptr n;
    switch (nt)
    {
    case node_4:
        return alloc_any_node<art_node4_8>(); // : alloc_any_node<art_node4_8>() ;
    case node_16:
        return alloc_any_node<art_node16_8>(); // optimize pointer sizes
    case node_48:
        return alloc_any_node<art_node48_8>();
    case node_256:
        return alloc_any_node<art_node256>();
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

art_node256::art_node256() { 
    statistics::node_bytes_alloc += sizeof(art_node256);
    statistics::interior_bytes_alloc += sizeof(art_node256);
    ++statistics::n256_nodes;
}

art_node256::~art_node256() {
    statistics::node256_occupants -= num_children;
    statistics::node_bytes_alloc -= sizeof(art_node256);
    statistics::interior_bytes_alloc -= sizeof(art_node256);
    --statistics::n256_nodes;
}
uint8_t art_node256::type() const {
    return node_256;
}

unsigned art_node256::index(unsigned char c) const {
    if (children[c].exists())
        return c;
    return 256;
}
    
 void art_node256::remove(node_ptr& ref, unsigned pos, unsigned char key) {
    if(key != pos) {
        abort();
    }
    children[key] = nullptr;
    types.set(key, false);
    num_children--;
    --statistics::node256_occupants;
    // Resize to a node48 on underflow, not immediately to prevent
    // trashing if we sit on the 48/49 boundary
    if (num_children == 37) {
        auto *new_node = alloc_node(node_48, {});
        ref = new_node;
        new_node->copy_header(this);
    
        pos = 0;
        for (unsigned i = 0; i < 256; i++) {
            if (has_any(i)) {
                new_node->set_child(pos, get_child(i)); //[pos] = n->children[i];
                new_node->set_key(i, pos + 1);
                pos++;
            }
        }
        
        free_node(this);   
    }
}

void art_node256::add_child(unsigned char c, node_ptr&, node_ptr child) {
    if(!has_child(c)) {
        ++statistics::node256_occupants;
        ++num_children; // just to keep stats ok
    }
    set_child(c, child);
}
node_ptr art_node256::last() const {
    return get_child(last_index());
}
unsigned art_node256::last_index() const {
    unsigned idx = 255;
    while (children[idx].empty()) idx--;
    return idx;
}

unsigned art_node256::first_index() const {
    unsigned uc = 0; // ?
    for (; uc < 256; uc++){
        if(children[uc].exists()) {
            return uc;
        }
    }
    return uc;
}

std::pair<trace_element, bool> art_node256::lower_bound_child(unsigned char c)
{
    for (unsigned i = c; i < 256; ++i) {
        if (has_child(i)) {
            // because nodes are ordered accordingly
            return {{this,get_child(i), i}, (i == c)};
        }
    }
    return {{nullptr,nullptr,256},false};
}

trace_element art_node256::next(const trace_element& te)
{
    for (unsigned i = te.child_ix+1; i < 256; ++i) { // these aren't sparse so shouldn't take long
        if (has_child(i)) {// because nodes are ordered accordingly
            return {this,get_child(i),i};
        }
    }
    return {};
}

trace_element art_node256::previous(const trace_element& te)
{
    if(!te.child_ix) return {};
    for (unsigned i = te.child_ix-1; i > 0; --i) { // these aren't sparse so shouldn't take long
        if (has_child(i)) {// because nodes are ordered accordingly
            return {this,get_child(i),i};
        }
    }
    return {};
}

