//
// Created by linuxlite on 3/27/25.
//

#ifndef COMPRESSED_ADDRESS_H
#define COMPRESSED_ADDRESS_H
#include <cstddef>
#include <cstdint>
#include <atomic>
#include "sastam.h"
#include "constants.h"
#define _CHECK_AP_ 0
/**
 * What one shard's tree actually holds.
 *
 * The counters in statistics.h are process globals from when there was a single tree, and
 * nothing recorded which tree an event belonged to. That was invisible while the numbers
 * only ever went up together, and wrong the moment an operation was scoped to one shard:
 * clearing a shard zeroed the counts for every shard, and loading one assigned its saved
 * numbers over the totals of the shards already loaded.
 *
 * These are the same quantities, owned by the allocator pair that did the allocating - one
 * per shard. The globals are still maintained beside them and are still what INFO and the
 * memory checks read, because those want one cheap number rather than a sum over hundreds
 * of shards. What these add is the ability to answer "how much of that total is mine",
 * which is what clear has to subtract and what save has to write.
 */
struct owned_content_stats {
    std::atomic<int64_t> n4{0};
    std::atomic<int64_t> n16{0};
    std::atomic<int64_t> n48{0};
    std::atomic<int64_t> n256{0};
    std::atomic<int64_t> occupants{0};
    std::atomic<int64_t> leaves{0};
    std::atomic<int64_t> logical{0};

    void zero() {
        n4 = 0; n16 = 0; n48 = 0; n256 = 0; occupants = 0; leaves = 0; logical = 0;
    }
};

struct abstract_alloc_pair {
    virtual ~abstract_alloc_pair() = default;

    int sentinel = 1<<24;

    /** this pair's share of the global counters - see owned_content_stats */
    owned_content_stats owned{};
};
struct logical_address {
    typedef uint64_t AddressIntType;

    logical_address() = delete;
    logical_address(abstract_alloc_pair * alloc) : alloc(alloc){};
    logical_address(const logical_address &) = default;

    logical_address &operator=(const logical_address &) = default;

    explicit logical_address(size_t index, abstract_alloc_pair* alloc) : index(index), alloc(alloc) {
        if (alloc && alloc->sentinel != 1<<24) {
            abort_with("invalid allocator pair");
        }
    }

    logical_address(size_t p, size_t o, const abstract_alloc_pair * alloc) :alloc((abstract_alloc_pair *)alloc){
        if (alloc && alloc->sentinel != 1<<24) {
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
        if (alloc->sentinel != 1<<24) {
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
    abstract_alloc_pair * alloc = nullptr;
};
struct abstract_leaf_pair : public abstract_alloc_pair {
    bool opt_all_keys_lru{false};
    bool opt_volatile_keys_lru{false};
    virtual void remove_leaf(const logical_address& at) = 0;
};
#endif //COMPRESSED_ADDRESS_H
