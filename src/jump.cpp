//
// Created by teejip on 8/19/25.
//

#include "jump.h"
#include "sastam.h"
#include "constants.h"
namespace jump {
    unused(
    static uint64_t fnv_hash_1a_64 ( const uint8_t *key, int len ) {
        const uint8_t *p = key;
        uint64_t h = 0xcbf29ce484222325ULL;
        int i;

        for ( i = 0; i < len; i++ )
            h = ( h ^ p[i] ) * 0x100000001b3ULL;

        return h;
    }
    )
    static size_t hash_(art::value_type v) {
        unused(
        return fnv_hash_1a_64(v.bytes, v.length());
        )
        return ankerl::unordered_dense::detail::wyhash::hash(v.bytes,v.length());
    }
    art::node_ptr hash_find(void* t, heap::vector<uint32_t>& jump, art::value_type k) {
        if (jump.empty()) return nullptr;
        size_t at = hash_(k) % jump.size();
        for (int i = 0; i < max_jump_probe; ++i) {
            auto addr = jump[at];
            if ( addr ) {
                art::node_ptr l = logical_address(addr, (void*)t);
                auto cl = l.const_leaf();
                if (!cl->deleted() && !cl->expired() && cl->compare(k)==0) return l;
            }
            at = (at + 1) % jump.size();
        }
        return nullptr;
    }

    bool hash_insert(void* t, heap::vector<uint32_t>& jump, art::value_type key, const art::node_ptr& leaf) {
        if (jump.empty()) return false;
        if (leaf.logical.address() > std::numeric_limits<uint32_t>::max()) return false;

        size_t at = hash_(key) % jump.size();
        for (int i = 0; i < max_jump_probe && jump[at]; ++i) {
            auto addr = jump[at];
            if ( addr ) {
                art::node_ptr l = logical_address(addr, t);
                auto cl = l.const_leaf();
                if (cl->compare(key)==0) {
                    if (addr != l.logical.address()) {
                        jump[at] = l.logical.address();
                    }

                    return false;
                }
            }else {
                break;
            }
            at = (at + 1) % jump.size();
        }
        bool r = jump[at] == 0;
        if (r)
            jump[at] = leaf.logical.address();
        return r;
    }

    bool hash_insert(void* t, heap::vector<uint32_t>& jump, const art::node_ptr& leaf) {
        return hash_insert(t, jump, leaf.const_leaf()->get_key(), leaf);
    }
    bool hash_remove(void* t, heap::vector<uint32_t>& jump, art::value_type k) {
        if (jump.empty()) return false;
        size_t at = hash_(k) % jump.size();
        for (int i = 0; i < max_jump_probe; ++i) {
            auto addr = jump[at];
            if ( addr ) {
                art::node_ptr l = logical_address(addr, t);
                auto cl = l.const_leaf();
                if (!cl->deleted() && cl->compare(k)==0) {
                    jump[at] = 0;
                    return true;
                }
            }
            at = (at + 1) % jump.size();
        }
        return false;
    }
}
