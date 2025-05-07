//
// Created by linuxlite on 3/27/25.
//

#ifndef COMPRESSED_ADDRESS_H
#define COMPRESSED_ADDRESS_H
#include <limits>
#include <cstddef>
#include <cstdint>
#include "constants.h"

struct logical_address {
    typedef uint64_t AddressIntType;

    logical_address() = default;

    logical_address(const logical_address &) = default;

    logical_address &operator=(const logical_address &) = default;

    void set_as_ptr(const uint8_t *addr) {
        this->index = (AddressIntType) addr;
    }

    explicit logical_address(size_t index) : index(index) {
    }

    logical_address(size_t p, size_t o) {
        from_page_offset(p, o);
    }

    logical_address &operator =(nullptr_t) {
        index = 0;
        return *this;
    }

    [[nodiscard]] bool null() const {
        return index == 0;
    }

    bool operator==(const logical_address &other) const {
        return index == other.index;
    }

    bool operator!=(const logical_address &other) const {
        return index != other.index;
    }

    bool operator<(const logical_address &other) const {
        return index < other.index;
    }

    [[nodiscard]] static bool is_null_base(size_t page) {
        return (page % reserved_address_base) == 0;
    }

    [[nodiscard]] bool is_null_base() const {
        return is_null_base(page());
    }

    void clear() {
        index = 0;
    }

    void from_page_index(size_t p) {
        index = p * page_size;
    }

    void from_page_offset(size_t p, size_t offset) {
        index = p * page_size + offset;
    }

    [[nodiscard]] size_t offset() const {
        return index % page_size;
    }

    [[nodiscard]] size_t page() const {
        return index / page_size;
    }

    [[nodiscard]] AddressIntType address() const {
        return index;
    }

    void from_address(size_t a) {
        index = a;
    }

    bool operator==(AddressIntType other) const {
        return index == other;
    }

    bool operator!=(AddressIntType other) const {
        return index != other;
    }

    explicit operator size_t() const {
        return index;
    }

private:
    AddressIntType index = 0;
};

#endif //COMPRESSED_ADDRESS_H
