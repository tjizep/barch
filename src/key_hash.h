#ifndef BARCH_KEY_HASH_H
#define BARCH_KEY_HASH_H
#include <cstddef>
#include <cstdint>
#include "a5hash.h"

namespace barch {
    /**
     * The one hash over a key's bytes - TODO 849. It routes a key to its shard,
     * so it is fixed: a saved shard holds the keys this sent there, and a new
     * hash would look for them in another. The shard's hash set uses it too. The
     * set is never saved, so that costs nothing, and a read that routed a key
     * can hand the hash to the set instead of hashing the bytes again.
     */
    inline uint64_t key_hash(const char* key, size_t size) {
        return a5hash(key, size, 0);
    }
}
#endif
