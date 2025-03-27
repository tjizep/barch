//
// Created by linuxlite on 3/27/25.
//

#ifndef STORAGE_H
#define STORAGE_H
#include <cinttypes>
#include <list>
#include "sastam.h"
typedef std::list<size_t, heap::allocator<size_t>> lru_list;
struct storage
{
    storage() : compressed(0), decompressed(0)
    {
    }

    storage(storage&& other) noexcept : compressed(0), decompressed(0)
    {
        *this = std::move(other);
    }

    storage(const storage& other) : compressed(0), decompressed(0)
    {
        *this = other;
    }

    storage& operator=(const storage& other)
    {
        if (&other == this) return *this;
        compressed = other.compressed;
        decompressed = other.decompressed;
        write_position = other.write_position;
        modifications = other.modifications;
        size = other.size;
        lru = other.lru;
        ticker = other.ticker;
        fragmentation = other.fragmentation;
        return *this;
    }

    void clear()
    {
        compressed.release();
        decompressed.release();
        write_position = 0;
        modifications = 0;
        size = 0;
        ticker = 0;
        fragmentation = 0;
        lru = lru_list::iterator();
    }

    storage& operator=(storage&& other) noexcept
    {
        if (&other == this) return *this;
        compressed = std::move(other.compressed);
        decompressed = std::move(other.decompressed);
        write_position = other.write_position;
        modifications = other.modifications;
        size = other.size;
        lru = other.lru;
        ticker = other.ticker;
        fragmentation = other.fragmentation;
        other.clear();
        return *this;
    }

    [[nodiscard]] bool empty() const
    {
        return size == 0 && write_position == 0 && compressed.empty() && decompressed.empty();
    }

    heap::buffer<uint8_t> compressed;
    heap::buffer<uint8_t> decompressed;
    uint16_t write_position = 0;
    uint16_t size = 0;
    uint16_t modifications = 0;
    lru_list::iterator lru{};
    uint64_t ticker = 0;
    uint16_t fragmentation = 0;
};

#endif //STORAGE_H
