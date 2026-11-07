// a new shard's root is null whatever memory it is built in - TODO 841
#include "shard.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <string>

static int failures = 0;
static void check(bool ok, const std::string& what) {
    std::printf("  %-62s %s\n", what.c_str(), ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

/*
 * Hands out memory filled with 0xa5, the way reused malloc memory can look.
 * heap::allocator zeroes, which hid a root copied from itself.
 */
template <class T>
struct dirty_allocator {
    using value_type = T;
    dirty_allocator() = default;
    template <class U>
    dirty_allocator(const dirty_allocator<U>&) {}
    T* allocate(size_t n) {
        void* p = ::operator new(n * sizeof(T), std::align_val_t{alignof(T)});
        std::memset(p, 0xa5, n * sizeof(T));
        return static_cast<T*>(p);
    }
    void deallocate(T* p, size_t) { ::operator delete(p, std::align_val_t{alignof(T)}); }
    template <class U>
    bool operator==(const dirty_allocator<U>&) const { return true; }
};

/*
 * Reads the root's bytes, not through node_ptr: null() checks is_leaf and
 * aborts on a byte that isn't 0 or 1, which is the failure this looks for.
 */
static void root_is_null(const std::shared_ptr<barch::shard>& s, const std::string& what) {
    const art::tree& t = *s;
    uint8_t is_leaf;
    std::memcpy(&is_leaf, &t.root.is_leaf, 1);
    check(is_leaf == 0 && t.root.storage.size == 0 && t.root.logical.null(), what);
}

int main() {
    std::printf("a new shard's root\n");
    dirty_allocator<barch::shard> alloc;
    root_is_null(std::allocate_shared<barch::shard>(alloc, "s1", barch::shard::scratch_t{}),
                 "scratch shard");
    root_is_null(std::allocate_shared<barch::shard>(alloc, "s2", size_t{3}, barch::shard::scratch_t{}),
                 "numbered scratch shard");
    root_is_null(std::allocate_shared<barch::shard>(alloc, "s3", uint64_t{0}, size_t{0}),
                 "named shard");
    std::printf(failures ? "%d failed\n" : "all passed\n", failures);
    return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
