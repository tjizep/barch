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
    auto d = data; // data can be reallocated now it wont affect this function
    if (d->empty()) return nullptr;
    size_t at = hash_(k) % d->size();
    for (int i = 0; i < max_jump_probe; ++i) {
        auto addr = (*d)[at].load();
        if ( addr ) { // we dont really care if the addr changes while this runs
            art::node_ptr l = logical_address(addr, t);
            auto cl = l.const_leaf();
            if (!cl->deleted() && !cl->expired() && cl->compare(k)==0) return l;
        }
        at = (at + 1) % d->size();
    }
    return nullptr;
}

static bool insert(alloc_pair* t,std::atomic<uint64_t>& jump_size, std::shared_ptr<jump::d_t>& data, art::value_type key, const art::node_ptr& leaf) {
    auto d = data;
    if (leaf.logical.address() > std::numeric_limits<uint32_t>::max()) return false;
    if (d->empty()) return false;
    size_t at = hash_(key) % d->size();
    for (int i = 0; i < max_jump_probe ; ++i) {
        auto &addr = (*d)[at];
        uint32_t prev = addr.load();
        if ( prev ) {
            art::node_ptr l = logical_address(prev, t);
            auto cl = l.const_leaf();
            if (cl->compare(key) == 0) {
                // only change to the possible new value if it's the same as prev
                // otherwise we give up and return false
                return addr.compare_exchange_strong(prev, leaf.logical.address());
            }
        }else {
            break;
        }
        at = (at + 1) % d->size();
    }
    uint32_t zero = 0;
    // if its still zero then, fine, we add it
    auto addr = leaf.logical.address();
    if ( (*d)[at].compare_exchange_strong(zero, addr)) {
        ++jump_size;
        return true;
    }
    return false;

}

bool jump::insert(const art::node_ptr& leaf) {
    auto d = data; // data can be reallocated now it wont affect this function
    if (d->empty()) {
        lock l(latch);
        if (d->empty()) {
            rehash(31);
        }
    }
    return ::insert(t, jump_size, d, leaf.const_leaf()->get_key(), leaf);
}
bool jump::remove(art::value_type k) {
    //lock l(latch);
    auto d = data; // data can be reallocated now it wont affect this function
    if (d->empty()) return false;
    size_t at = hash_(k) % d->size();
    for (int i = 0; i < max_jump_probe; ++i) {
        auto& addr = (*d)[at];
        uint32_t prev = addr.load();
        if ( prev ) {
            // some other thread can add to this same index
            art::node_ptr l = logical_address(prev, t);
            auto cl = l.const_leaf();
            if (!cl->deleted() && cl->compare(k)==0) {
                if (addr.compare_exchange_strong(prev, zero)) {
                    --jump_size;
                }
                return true;
            }
        }
        at = (at + 1) % d->size();
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
        auto oldsize = jump_size.load();
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
    std::atomic<uint64_t> r_jump_size = 0;
    if (new_size > 0) {
        for (auto& addr : *d) {
            if (addr) {
                art::node_ptr l = logical_address(addr, t);
                auto cl = l.const_leaf();
                if (!cl->deleted() && !cl->expired()) {
                    ::insert(t, r_jump_size, new_jump, cl->get_key(), l);
                }
            }
        }
    }
    jump_size = r_jump_size.load();
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