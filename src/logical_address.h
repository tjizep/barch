//
// Created by linuxlite on 3/27/25.
//

#ifndef COMPRESSED_ADDRESS_H
#define COMPRESSED_ADDRESS_H
#include <cstddef>
#include <cstdint>
#include "logger.h"
#include "sastam.h"
#include "constants.h"
#define _CHECK_AP_ 1
struct logical_address {
    typedef uint64_t AddressIntType;

    logical_address() = delete;
    logical_address(void * alloc) : alloc(alloc){};
    logical_address(const logical_address &) = default;

    logical_address &operator=(const logical_address &) = default;

    explicit logical_address(size_t index, void* alloc) : index(index), alloc(alloc) {
        if (alloc && *(int*)alloc != 1<<24) {
            abort_with("invalid allocator pair");
        }
    }

    logical_address(size_t p, size_t o, void * alloc) :alloc(alloc){
        if (alloc && *(int*)alloc != 1<<24) {
            abort_with("invalid allocator pair");
        }
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
    void check_ap() const {
#if _CHECK_AP_
        if (alloc == nullptr) {
            abort_with("allocator pair not set");
        }
        if (*(int*)alloc != 1<<24) {
            abort_with("invalid allocator pair");
        }
#endif

    }
    template<typename AT>
    AT& get_ap() {
        check_ap();
        return *(AT*)alloc;
    }
    template<typename AT>
    const AT& get_ap() const {
        check_ap();
        return *(AT*)alloc;
    }
private:
    AddressIntType index = 0;
    // for better or worse the allocators associated for this addess is taken with the logical address
    // it does have a 3% perf impact in single threaded perf, but hopefully we can have
    // much better multithreaded perf because a seperate tree is allocated
    // for each key shard associated with it's own processing thread
    void * alloc = nullptr;
};

#endif //COMPRESSED_ADDRESS_H
