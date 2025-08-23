//
// Created by teejip on 8/19/25.
//

#include "jump.h"
#include "sastam.h"
#include "constants.h"
#include "configuration.h"
#include "asio/detail/thread_info_base.hpp"

typedef std::unique_lock<std::mutex> lock;
unused()
static uint64_t fnv_hash_1a_64 ( const uint8_t *key, int len ) {
    const uint8_t *p = key;
    uint64_t h = 0xcbf29ce484222325ULL;
    int i;

    for ( i = 0; i < len; i++ )
        h = ( h ^ p[i] ) * 0x100000001b3ULL;

    return h;
}

static size_t hash_(art::value_type v) {
    return fnv_hash_1a_64(v.bytes, v.length()-1);
    unused(
    return ankerl::unordered_dense::detail::wyhash::hash(v.bytes,v.length()-1);
    )

}
bool jump::may_contain(art::value_type k) const {
    return false;
}
art::node_ptr jump::find(art::value_type k) const {
    return nullptr;
}


int jump::insert(const art::node_ptr& leaf, bool replace) {


}
bool jump::remove(art::value_type k) {
    //lock l(latch);

    return false;
}
size_t jump::capacity() const {
    return data.size();
}
uint64_t jump::size() const {
    return data.size();
}
bool jump::empty() const {
    return true;
}
void jump::clear() {

}
void jump::rehash(const spill_function& spill) {
}
void jump::rehash(size_t new_size,const spill_function& spill) {
}
void jump::rehash_(size_t new_size, const spill_function& spill) {
}

size_t jump::erase_page(size_t page) {

}