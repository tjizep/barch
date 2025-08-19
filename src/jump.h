//
// Created by teejip on 8/19/25.
//

#ifndef BARCH_JUMP_H
#define BARCH_JUMP_H
#include "sastam.h"
#include "value_type.h"
#include "nodes.h"
namespace jump {
    art::node_ptr hash_find(void* t, heap::vector<uint32_t>& jump, art::value_type k);
    bool hash_insert(void* t, heap::vector<uint32_t>& jump, art::value_type key, const art::node_ptr& leaf);
    bool hash_insert(void* t, heap::vector<uint32_t>& jump, const art::node_ptr& leaf);
    bool hash_remove(void* t, heap::vector<uint32_t>& jump, art::value_type k);
}


#endif //BARCH_JUMP_H