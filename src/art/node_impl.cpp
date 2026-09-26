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
    /*
     * Stamps the leaf's LRU bit on access: the write a true LRU costs on the
     * read path, paid only when a policy asked for one. It runs under nothing
     * stronger than a shared lock, concurrently with readers testing the other
     * bits of the same byte, which is why `leaf::flags` is atomic and this is a
     * relaxed fetch_or rather than a plain OR - see DONE 296.
     *
     * Only the key the caller asked for reaches here. Everything else looks at
     * a leaf through `peek_leaf()` instead - candidates compared on the way to
     * an answer, and every scan, range, glob and iterator walk - so nothing
     * marks a key as read except a lookup of that key. See DONE 297 and 298.
     */
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
        // the same predicate the leaf constructor uses. make_size took any non-zero ttl
        // as one needing eight bytes reserved while the constructor only records an expiry
        // when it is positive, so a negative deadline - which an overflowed EX produced -
        // sized the leaf one way and built it another, and the check below aborted. The
        // commands refuse such an expiry now (DONE 53), and this makes it unreachable
        // rather than merely unlikely
        size_t leaf_size = leaf::make_size(key_len,val_len,ttl > 0,is_volatile);
        // NB the + 1 is for a hidden 0 byte ay the end of the key not reflected by length()
        logical_address logical{&alloc};
        auto ldata = alloc.get_leaves().new_address(logical, leaf_size);
        auto *l = new(ldata) leaf(key_len, val_len, ttl, is_volatile, is_compressed);
        if (alloc.is_debug) {
            barch::log({"allocated leaf at", logical.address(),"size", leaf_size});
        }
        ++statistics::leaf_nodes;
        ++alloc.owned.leaves;
        l->set_key(key);
        l->set_value(v);
        /*
         * No LRU stamp here. The bit means "this key was read" - `l()` and
         * `const_leaf()` are what set it, and DONE 297 and 298 went to some
         * trouble to make sure only a real lookup of that key does. Creation
         * was the one place left setting it for something that is not a read,
         * which made a key nobody had ever read look read: the background
         * compressor could not find a cold key, and every key it rewrote came
         * back stamped. See TODO 307.
         *
         * A new key therefore starts cold and is a candidate on the next sweep
         * of its page. That only matters over the pre-eviction threshold, since
         * that is the only time the sweep runs at all.
         */
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
    --ap.owned.leaves;
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
    unsigned max_cmp = std::min<int>(std::min<int>(d.prefix_len(), max_prefix_llength),
                                     (int) key_len - (int) depth);
    unsigned idx;

    for (idx = 0; idx < max_cmp; idx++) {
        if (d.partial[idx] != key[depth + idx])
            return idx;
    }
    return idx;
}


#include "configuration.h"
#include <functional>
#include "nodes.h"
static void page_iterator_ptr_(const uint8_t* page_data, unsigned size, unsigned from, std::function<bool(const art::leaf *, uint32_t pos)> cb) {
    if (!size) return;

    auto e = page_data + size;
    size_t deleted = 0;
    size_t pos = from;
    for (auto i = page_data+from; i < e;) {
        const art::leaf *l = (art::leaf *) i;
        if (l->deleted()) {
            deleted++;
        } else {
            if (!cb(l,pos)) {
                return;
            }
        }
        pos += l->next_leaf();
        i += l->next_leaf();
    }
}
void art::page_iterator_ptr(const uint8_t* page_data, unsigned size, std::function<bool(const leaf *, uint32_t pos)> cb, unsigned from) {
    if (from >= size ) return;
    page_iterator_ptr_(page_data, size, from, cb);
}
void art::page_iterator(const heap::buffer<uint8_t> &page_data, unsigned size, std::function<bool(const leaf *, uint32_t pos)> cb) {
    return page_iterator_ptr(page_data.data(), size, std::move(cb));
}
