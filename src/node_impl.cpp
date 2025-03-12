#include <cstdlib>
#include <cstring>
#include <atomic>

#include "art.h"
#include "statistics.h"
#include "nodes.h"
#include "node_impl.h"

#include <algorithm>

art::node_ptr art::make_leaf(value_type key, value_type v) {
    unsigned val_len = v.size;
    unsigned key_len = key.length();
    // NB the + 1 is for a hidden 0 byte contained in the key not reflected by length()
    auto logical = art::get_leaf_compression().new_address(sizeof(leaf) + key_len + 1 + val_len);
    auto *l = new(get_leaf_compression().read<leaf>(logical)) leaf(key_len, val_len);
    ++statistics::leaf_nodes;
    statistics::addressable_bytes_alloc += (int64_t)(sizeof(leaf)+ key_len + 1 + val_len);
    l->set_key(key);
    l->set_value(v);
    return logical;
}


void art::free_leaf_node(art::leaf* l, compressed_address logical){
    if(l == nullptr) return;
    unsigned kl = l->key_len;
    l->key_len = 0;
    l->val_len += kl; // set the deleted property

    art::get_leaf_compression().free(logical, l->byte_size());
    --statistics::leaf_nodes;
    statistics::addressable_bytes_alloc -= (l->byte_size());
}

void art::free_leaf_node(art::node_ptr n)
{
    free_leaf_node(n.l(),n.logical);
}

void art::free_node(art::node_ptr n){
    n.free_from_storage();
}

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
template<typename Type4, typename Type8>
static art::node* make_node(art::node_ptr_storage& ptr, compressed_address a, art::node_data* node)
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
art::node_ptr art::resolve_read_node(compressed_address address)
{
    auto* node= art::get_node_compression().read<art::node_data>(address);
    art::node_ptr_storage ptr;
    switch (node->type)
    {
    case art::node_4:
        return  make_node<art::node4_4,art::node4_8>(ptr, address, node);
    case art::node_16:
        return make_node<art::node16_4,art::node16_8>(ptr, address, node);
    case art::node_48:
        return make_node<art::node48_4,art::node48_8>(ptr, address, node);
    case art::node_256:
        return make_node<art::node256_4,art::node256_8>(ptr, address, node);
    default:
        abort();
    }
}
art::node_ptr art::resolve_write_node(compressed_address address)
{
    auto* node= art::get_node_compression().modify<art::node_data>(address);
    art::node_ptr_storage ptr;
    switch (node->type)
    {
    case art::node_4:
        return  make_node<art::node4_4,art::node4_8>(ptr, address, node);
    case art::node_16:
        return make_node<art::node16_4,art::node16_8>(ptr, address, node);
    case art::node_48:
        return make_node<art::node48_4,art::node48_8>(ptr, address, node);
    case art::node_256:
        return make_node<art::node256_4,art::node256_8>(ptr, address, node);
    default:
        abort();
    }
}
art::node_ptr art::alloc_node_ptr(unsigned nt, const art::children_t& c)
{
    art::node_ptr ref;

    art::node_ptr_storage ptr;
    switch (nt)
    {
    case art::node_4:
        return ptr.emplace<art::node4_4>()->create().expand_pointers(ref, c);
    case art::node_16:
        return ptr.emplace<art::node16_4>()->create().expand_pointers(ref, c);
    case art::node_48:
        return ptr.emplace<art::node48_4>()->create().expand_pointers(ref, c);
    case art::node_256:
        return ptr.emplace<art::node256_4>()->create().expand_pointers(ref, c);
    default:
        abort();
    }
}
art::node_ptr alloc_8_node_ptr(unsigned nt)
{

    art::node_ptr_storage ptr;
    switch (nt)
    {
    case art::node_4:
        return ptr.emplace<art::node4_8>();
    case art::node_16:
        return ptr.emplace<art::node16_8>();
    case art::node_48:
        return ptr.emplace<art::node48_8>();
    case art::node_256:
        return ptr.emplace<art::node256_8>();
    default:
        abort();
    }
}



/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */

unsigned art::node::check_prefix(const unsigned char *key, unsigned key_len, unsigned depth) {
    unsigned max_cmp = std::min<int>(std::min<int>(data().partial_len, max_prefix_llength), (int)key_len - (int)depth);
    unsigned idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (data().partial[idx] != key[depth+idx])
            return idx;
    }
    return idx;
}

art::tree::~tree()
{
    mexit = true;
    if(tmaintain.joinable())
        tmaintain.join();
}

void art::tree::start_maintain()
{
    tmaintain = std::thread([&]() -> void
    {
        while (!this->mexit)
        {

        }
    });
}


