#include <cstdlib>
#include <cstring>
#include <atomic>

#include "art.h"
#include "statistics.h"
#include "nodes.h"

#include <algorithm>

void free_leaf_node(node_ptr n){
    if(n.null()) return;
    unsigned kl = n.const_leaf()->key_len;
    unsigned vl = n.const_leaf()->val_len;
    get_leaf_compression().free(n.logical, sizeof(art_leaf) + kl + 1 + vl);
    --statistics::leaf_nodes;
    statistics::addressable_bytes_alloc -= (sizeof(art_leaf) + kl + 1 + vl);
}

void free_node(node_ptr n){
    n.free_from_storage();
}

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
template<typename Type4, typename Type8>
static art_node* make_node(node_ptr_storage& ptr, compressed_address a, node_data* node)
{
    if (node->pointer_size == 4)
    {
        return ptr.emplace<Type4>(a, node);
    }else if (node->pointer_size == 8)
    {
        return  ptr.emplace<Type8>(a, node);
    }
    abort();
}
node_ptr resolve_read_node(compressed_address address)
{
    auto* node= get_leaf_compression().read<node_data>(address);
    node_ptr_storage ptr;
    switch (node->type)
    {
    case node_4:
        return  make_node<art_node4_4,art_node4_8>(ptr, address, node);
    case node_16:
        return make_node<art_node16_4,art_node16_8>(ptr, address, node);
    case node_48:
        return make_node<art_node48_4,art_node48_8>(ptr, address, node);
    case node_256:
        return make_node<art_node256_4,art_node256_8>(ptr, address, node);
    default:
        abort();
    }
}
node_ptr resolve_write_node(compressed_address address)
{
    auto* node= get_leaf_compression().modify<node_data>(address);
    node_ptr_storage ptr;
    switch (node->type)
    {
    case node_4:
        return  make_node<art_node4_4,art_node4_8>(ptr, address, node);
    case node_16:
        return make_node<art_node16_4,art_node16_8>(ptr, address, node);
    case node_48:
        return make_node<art_node48_4,art_node48_8>(ptr, address, node);
    case node_256:
        return make_node<art_node256_4,art_node256_8>(ptr, address, node);
    default:
        abort();
    }
}
node_ptr alloc_node_ptr(unsigned nt, const children_t& c)
{
    node_ptr ref;

    node_ptr_storage ptr;
    switch (nt)
    {
    case node_4:
        return ptr.emplace<art_node4_4>()->create().expand_pointers(ref, c);
    case node_16:
        return ptr.emplace<art_node16_4>()->create().expand_pointers(ref, c);
    case node_48:
        return ptr.emplace<art_node48_4>()->create().expand_pointers(ref, c);
    case node_256:
        return ptr.emplace<art_node256_4>()->create().expand_pointers(ref, c);
    default:
        abort();
    }
}
node_ptr alloc_8_node_ptr(unsigned nt)
{

    node_ptr_storage ptr;
    switch (nt)
    {
    case node_4:
        return ptr.emplace<art_node4_8>();
    case node_16:
        return ptr.emplace<art_node16_8>();
    case node_48:
        return ptr.emplace<art_node48_8>();
    case node_256:
        return ptr.emplace<art_node256_8>();
    default:
        abort();
    }
}

art_node::art_node () = default;
art_node::~art_node() = default;
/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */

unsigned art_node::check_prefix(const unsigned char *key, unsigned key_len, unsigned depth) {
    unsigned max_cmp = std::min<int>(std::min<int>(data().partial_len, max_prefix_llength), (int)key_len - (int)depth);
    unsigned idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (data().partial[idx] != key[depth+idx])
            return idx;
    }
    return idx;
}



