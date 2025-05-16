//
// Created by linuxlite on 3/27/25.
//

#ifndef STORAGE_H
#define STORAGE_H
#include <cinttypes>
#include <list>
#include "sastam.h"
typedef std::list<size_t, heap::allocator<size_t> > lru_list;

struct storage {
    storage() {
    }

    storage(storage &&other) noexcept {
        *this = std::move(other);
    }

    storage(const storage &other) {
        *this = other;
    }

    storage &operator=(const storage &other) {
        if (&other == this) return *this;
        write_position = other.write_position;
        //modifications = other.modifications;
        size = other.size;
        lru = other.lru;
        ticker = other.ticker;
        fragmentation = other.fragmentation;
        return *this;
    }

    void clear() {
        write_position = 0;
        //modifications = 0;
        size = 0;
        ticker = 0;
        fragmentation = 0;
        lru = lru_list::iterator();
    }

    storage &operator=(storage &&other) noexcept {
        if (&other == this) return *this;
        write_position = other.write_position;
        //modifications = other.modifications;
        size = other.size;
        lru = other.lru;
        ticker = other.ticker;
        fragmentation = other.fragmentation;
        other.clear();
        return *this;
    }

    [[nodiscard]] bool empty() const {
        return size == 0 && write_position == 0;
    }

    uint32_t write_position = 0;
    uint32_t size = 0;
    //uint32_t modifications = 0;
    lru_list::iterator lru{};
    uint64_t ticker = 0;
    uint64_t physical = 0;
    uint64_t logical = 0;
    uint32_t fragmentation = 0;
};

#endif //STORAGE_H
