//
// Created by teejip on 8/19/25.
//

#ifndef BARCH_JUMP_H
#define BARCH_JUMP_H
#include "sastam.h"
#include "value_type.h"
#include "nodes.h"
#include "logical_allocator.h"

struct jump {
    typedef heap::vector<uint32_t> d_t;
    alloc_pair* t;
    uint64_t jump_size{};
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
    bool insert(art::value_type key, const art::node_ptr& leaf);
    bool insert(const art::node_ptr& leaf);
    bool remove(art::value_type k);
    void rehash(size_t new_size);
    void rehash();
    void rehash_(size_t new_size);
    void erase_page(size_t page);
};


#endif //BARCH_JUMP_H