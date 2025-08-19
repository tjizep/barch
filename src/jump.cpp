//
// Created by teejip on 8/19/25.
//

#include "jump.h"
#include "sastam.h"
#include "constants.h"
#include "configuration.h"
#include "asio/detail/thread_info_base.hpp"
typedef std::unique_lock<std::mutex> lock;
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
art::node_ptr jump::find(art::value_type k) const {
    //lock l(latch);
    auto d = data;
    if (d->empty()) return nullptr;
    size_t at = hash_(k) % d->size();
    for (int i = 0; i < max_jump_probe; ++i) {
        auto addr = (*d)[at];
        if ( addr ) {
            art::node_ptr l = logical_address(addr, t);
            auto cl = l.const_leaf();
            if (!cl->deleted() && !cl->expired() && cl->compare(k)==0) return l;
        }
        at = (at + 1) % d->size();
    }
    return nullptr;
}

static bool insert(alloc_pair* t, std::shared_ptr<jump::d_t>& data, art::value_type key, const art::node_ptr& leaf) {
    auto d = data;
    if (leaf.logical.address() > std::numeric_limits<uint32_t>::max()) return false;
    if (d->empty()) {
        d->resize(31*art::get_jump_factor());
    }
    size_t at = hash_(key) % d->size();
    for (int i = 0; i < max_jump_probe && (*d)[at]; ++i) {
        auto addr = (*d)[at];
        if ( addr ) {
            art::node_ptr l = logical_address(addr, t);
            auto cl = l.const_leaf();
            if (cl->compare(key)==0) {
                if (addr != l.logical.address()) {
                    (*d)[at] = l.logical.address();
                }
                return false;
            }
        }else {
            break;
        }
        at = (at + 1) % d->size();
    }
    bool r = (*d)[at] == 0;
    if (r)
        (*d)[at] = leaf.logical.address();
    return r;
}

bool jump::insert(const art::node_ptr& leaf) {
    auto d = data;
    if (d->empty()) {
        rehash(31);
    }
    if (::insert(t, d, leaf.const_leaf()->get_key(), leaf)) {
        ++jump_size;

        return true;
    }
    return false;
}
bool jump::remove(art::value_type k) {
    //lock l(latch);
    auto d = data;
    if (d->empty()) return false;
    size_t at = hash_(k) % d->size();
    for (int i = 0; i < max_jump_probe; ++i) {
        auto addr = (*d)[at];
        if ( addr ) {
            art::node_ptr l = logical_address(addr, t);
            auto cl = l.const_leaf();
            if (!cl->deleted() && cl->compare(k)==0) {
                (*d)[at] = 0;
                --jump_size;
                return true;
            }
        }
        at = (at + 1) % d->size();
        if (at >= d->size()) {
            abort_with("size+error");
        };
    }
    return false;
}
uint64_t jump::size() const {
    return jump_size;
}
bool jump::empty() const {
    return jump_size > 0;
}
void jump::clear() {
    lock l(latch);
    auto d = data;
    jump_size = 0;
    auto v = std::make_shared<d_t>(31);
    data = v;
}
void jump::rehash() {
    auto d = data;
    if (jump_size > d->size()*75/100) {
        lock l(latch);
        auto oldsize = jump_size;
        if (jump_size > d->size()*75/100) {
            rehash_(d->size() * art::get_jump_factor());
            if (jump_size != oldsize) {
                //art::std_log("resize failed");
            }
        }
    }
}
void jump::rehash(size_t new_size) {
    lock l(latch);
    rehash_(new_size);
}
void jump::rehash_(size_t new_size) {
    uint64_t njs = new_size;
    auto d = data;
    if (new_size < d->size()) {
        return;
    }

    auto new_jump = std::make_shared<d_t>(njs);
    jump_size = 0;
    if (new_size > 0) {
        for (auto addr : *d) {
            if (addr) {
                art::node_ptr l = logical_address(addr, t);
                auto cl = l.const_leaf();
                if (!cl->deleted() && !cl->expired()) {
                    if (::insert(t, new_jump, cl->get_key(), l)) {
                        ++jump_size;
                    }
                }
            }
        }
    }
    data = new_jump;
}

void jump::erase_page(size_t page) {
    lock l(latch);
    auto d = data;

    for (auto& addr : *d) {
        if (addr) {
            logical_address lad(addr, t);
            if (lad.page() == page) {
                addr = 0;
            }
        }
    }

}