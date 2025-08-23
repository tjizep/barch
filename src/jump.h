//
// Created by teejip on 8/19/25.
//

#ifndef BARCH_JUMP_H
#define BARCH_JUMP_H
#include "sastam.h"
#include "value_type.h"
#include "nodes.h"
#include "logical_allocator.h"
#include <atomic>

/**
 * the jump cache is a lossy sometimes lock free and wait-free hash table
 * its used to determine if some ops are doable in a shared lock
 * environment.
 * where insertion and removal is not guaranteed,
 * it will respond on insert/remove ops with true or
 * false if it succeeded
 * the size should always be consistent though.
 */
struct jump {

    typedef std::function<void (const art::node_ptr& ptr)> spill_function;
    uint32_t zero = 0;
    uint32_t exchanged{};
    alloc_pair* t;
    heap::set<uint32_t> data{};
    jump() = default;
    jump(const jump&) = delete;
    jump& operator=(const jump&) = delete;
    void clear();
    bool empty() const;
    size_t capacity() const;
    uint64_t size() const;
    jump(alloc_pair* t) : t(t) {}
    art::node_ptr find(art::value_type k) const;
    // may_contain is the only truly lock free function
    // in this hash
    bool may_contain(art::value_type k) const;
    // the insert and remove functions are'nt guaranteed to succeed
    // mostly because I can't write a lock free version
    // but they dont have to
    enum {
        ok = 0,
        no_space,
        no_time,
        replaced,
        exist
    };
    int insert(const art::node_ptr& leaf, bool replace);
    bool remove(art::value_type k);
    void rehash(size_t new_size, const spill_function& spill);
    void rehash(const spill_function& spill);
    void rehash_(size_t new_size, const spill_function& spill);
    size_t erase_page(size_t page);
};


#endif //BARCH_JUMP_H