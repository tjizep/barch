#include <cstdlib>
#include <cstring>
#include <atomic>
#include <random>

#include "art.h"
#include "../statistics.h"
#include "nodes.h"
#include "node_impl.h"
#include "../time_conversion.h"
#include "../module.h"
#include <algorithm>


namespace art {
    void set_leaf_lru(art::leaf * l) {
        l->set_lru();
    }
    node_ptr tree::tree_make_leaf(value_type key, value_type v, key_options options) {
        return art::make_leaf(*this, key, v, options.get_expiry(), options.is_volatile(), options.is_compressed());
    }

    node_ptr tree::tree_make_leaf(value_type key, value_type v, leaf::ExpiryType ttl, bool is_volatile, bool is_compressed) {
        return art::make_leaf(*this, key, v, ttl, is_volatile, is_compressed);
    }
    //
    // TODO: this function's interface can lend itself to crashes when used with parameter pointers which
    // are sourced from a tree leaf allocator. currently the solution is to allocate and copy these params
    // which could be deleterious to performance.
    //

    node_ptr make_leaf(alloc_pair& alloc, value_type key, value_type v, key_options options) {
        return make_leaf(alloc, key, v, options.get_expiry(), options.is_volatile(), options.is_compressed());
    }
    node_ptr make_leaf(alloc_pair& alloc, value_type key, value_type v, leaf::ExpiryType ttl, bool is_volatile, bool is_compressed ) {
        unsigned val_len = v.size;
        unsigned key_len = key.length();
        auto &leaves = alloc.get_leaves();
        // copying is slow - so check if the address may get reallocated
        if (leaves.is_from(v.bytes) || leaves.is_from(key.bytes)) {
            key = alloc.copy_key(key);
            v = alloc.copy_value(v);
        }
        size_t leaf_size = leaf::make_size(key_len,val_len,ttl,is_volatile);
        // NB the + 1 is for a hidden 0 byte ay the end of the key not reflected by length()
        logical_address logical{&alloc};
        auto ldata = alloc.get_leaves().new_address(logical, leaf_size);
        auto *l = new(ldata) leaf(key_len, val_len, ttl, is_volatile, is_compressed);
        if (alloc.is_debug) {
            barch::std_log("allocated leaf at", logical.address(),"size", leaf_size);
        }
        ++statistics::leaf_nodes;
        l->set_key(key);
        l->set_value(v);
        if (alloc.opt_all_keys_lru) {
            l->set_lru();
        }
        if (is_volatile && alloc.opt_volatile_keys_lru ) {
            l->set_lru();
        }
        if (l->byte_size() != leaf_size) {
            abort_with("invalid leaf size");
        }
        statistics::max_leaf_size = std::max<uint64_t>(statistics::max_leaf_size, l->byte_size());
        return logical;
    }

}

void art::free_leaf_node(leaf *l, logical_address logical) {
    if (l == nullptr) return;
    if (l->bad()) {
        abort_with("freeing bad leaf");
    }
    auto &ap = logical.get_ap<alloc_pair>();
    ap.remove_leaf(logical);
    l->set_deleted();
    logical.get_ap<alloc_pair>().get_leaves().free(logical, l->byte_size());
    --statistics::leaf_nodes;
}

void art::free_leaf_node(art::node_ptr n) {
    free_leaf_node(n.l(), n.logical);
}

void art::free_node(art::node_ptr n) {
    n.free_from_storage();
}

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
template<typename Type4, typename Type8>
static art::node *make_node(art::node_ptr_storage &ptr, logical_address a, art::node_data *node) {
    if (node->pointer_size == 4) {
        return ptr.emplace<Type4>(a, node);
    } else if (node->pointer_size == 8) {
        return ptr.emplace<Type8>(a, node);
    }
    abort_with("invalid pointer size");
}

art::node_ptr art::resolve_read_node(logical_address address) {
    auto *node = address.get_ap<alloc_pair>().get_nodes().read<node_data>(address);
    node_ptr_storage ptr;
    if (node == nullptr) {
        return node_ptr{nullptr};
    }
    switch (node->type) {
        case node_4:
            return make_node<node4_4, node4_8>(ptr, address, node);
        case node_16:
            return make_node<node16_4, node16_8>(ptr, address, node);
        case node_48:
            return make_node<node48_4, node48_8>(ptr, address, node);
        case node_256:
            return make_node<node256_4, node256_8>(ptr, address, node);
        default:
            abort_with("unknown or invalid node type");
    }
}
namespace art {
    node_ptr resolve_write_node(logical_address address) {
        auto *node = address.get_ap<alloc_pair>().get_nodes().modify<node_data>(address);
        node_ptr_storage ptr;
        switch (node->type) {
            case node_4:
                return make_node<node4_4, node4_8>(ptr, address, node);
            case node_16:
                return make_node<node16_4, node16_8>(ptr, address, node);
            case node_48:
                return make_node<node48_4, node48_8>(ptr, address, node);
            case node_256:
                return make_node<node256_4, node256_8>(ptr, address, node);
            default:
                throw std::runtime_error("Unknown node type");
        }
    }

    node_ptr alloc_node_ptr(alloc_pair& alloc, unsigned ptrsize, unsigned nt, const art::children_t &c) {
        if (ptrsize == 8) return alloc_8_node_ptr(alloc, nt);

        node_ptr_storage ptr;
        switch (nt) {
            case node_4:
                return ptr.emplace<node4_4>()->create(alloc).expand_pointers(c);
            case node_16:
                return ptr.emplace<node16_4>()->create(alloc).expand_pointers(c);
            case node_48:
                return ptr.emplace<node48_4>()->create(alloc).expand_pointers(c);
            case node_256:
                return ptr.emplace<node256_4>()->create(alloc).expand_pointers(c);
            default:
                throw std::runtime_error("Unknown node type");
        }
    }

    node_ptr tree::alloc_node_ptr(unsigned ptrsize, unsigned nt, const children_t &c) {
        return art::alloc_node_ptr(*this, ptrsize, nt, c);
    }
    node_ptr alloc_8_node_ptr(alloc_pair& alloc, unsigned nt) {
        node_ptr_storage ptr;
        switch (nt) {
            case node_4:
                return ptr.emplace<node4_8>()->create_node(alloc);
            case node_16:
                return ptr.emplace<node16_8>()->create_node(alloc);
            case node_48:
                return ptr.emplace<node48_8>()->create_node(alloc);
            case node_256:
                return ptr.emplace<node256_8>()->create_node(alloc);
            default:
                throw std::runtime_error("Unknown node type");
        }
    }

    node_ptr art::tree::alloc_8_node_ptr(unsigned nt) {
        return art::alloc_8_node_ptr(*this, nt);
    }
}



/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */


unsigned art::node::check_prefix(const unsigned char *key, unsigned key_len, unsigned depth) const {
    auto &d = data();
    unsigned max_cmp = std::min<int>(std::min<int>(d.partial_len, max_prefix_llength),
                                     (int) key_len - (int) depth);
    unsigned idx;

    for (idx = 0; idx < max_cmp; idx++) {
        if (d.partial[idx] != key[depth + idx])
            return idx;
    }
    return idx;
}

