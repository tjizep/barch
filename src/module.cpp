//
// Created by teejip on 4/9/25.
//

#include "module.h"

art::tree *ad{};
static std::shared_mutex shared{};
constants Constants{};

std::shared_mutex &get_lock() {
    return shared;
}

art::tree *get_art() {
    if (ad == nullptr) {
        ad = new(heap::allocate<art::tree>(1)) art::tree(nullptr, 0);
    }
    return ad;
}
