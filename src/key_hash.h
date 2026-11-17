#ifndef BARCH_KEY_HASH_H
#define BARCH_KEY_HASH_H
#include <cstddef>
#include <cstdint>
#include <random>
#include "a5hash.h"

namespace barch {
    /**
     * The one hash over a key's bytes that routes it to its shard - TODO 849. It is
     * fixed: a saved shard holds the keys this sent there, and a new hash would
     * look for them in another. Every barch that swaps shard files with this one
     * (RETRIEVE, cluster snapshots, split groups) routes the same way. A key picked
     * to collide here only makes one shard bigger, since a shard is a tree.
     */
    inline uint64_t key_hash(const char* key, size_t size) {
        return a5hash(key, size, 0);
    }

    /**
     * This process's seed for the in-memory hash indexes - TODO 912. Random at
     * first use and never saved: nothing hashed with it outlives the process.
     * A function static, so a map built during static init gets the same one.
     */
    inline uint64_t index_seed() {
        static const uint64_t seed = [] {
            std::random_device rd;
            return (uint64_t) rd() << 32 | rd();
        }();
        return seed;
    }

    /**
     * The hash for in-memory indexes: a shard's hash set and the string maps.
     * Seeded, so nobody outside can pick keys that fill one bucket - TODO 912.
     * It is not key_hash, so a read that routed a key hashes it again for the
     * set. TODO 849 measured no gain from sharing it.
     */
    inline uint64_t index_hash(const char* key, size_t size) {
        return a5hash(key, size, index_seed());
    }
}
#endif
