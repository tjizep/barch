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
 * the jump cache is a lossy lock free wait-free hash table
 * where insertion and removal is not guaranteed,
 * it will respond on insert/remove ops with true or
 * false if it succeeded
 * the size should always be consistent though.
 */
struct jump {
    typedef heap::vector<std::atomic<uint32_t>> d_t;
    uint32_t zero = 0;
    alloc_pair* t;
    std::atomic<uint64_t> jump_size{};
    mutable std::mutex latch{};
    jump() = default;
    jump(const jump&) = delete;
    jump& operator=(const jump&) = delete;
    void clear();
    bool empty() const;
    uint64_t size() const;
    jump(alloc_pair* t) : t(t) {}
    std::shared_ptr<d_t> data = std::make_shared<d_t>();
    art::node_ptr find(art::value_type k) const;
    // the insert and remove functions are'nt guaranteed to succeed
    // mostly because I can't write a lock free version
    // but they dont have to
    bool insert(const art::node_ptr& leaf);
    bool remove(art::value_type k);
    void rehash(size_t new_size);
    void rehash();
    void rehash_(size_t new_size);
    void erase_page(size_t page);
};


#endif //BARCH_JUMP_H