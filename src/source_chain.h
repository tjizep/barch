//
// Created by barch on 25-09-2026.
//
// Locking the chain of shards a dependent shard reads through - TODO 453.
//

#ifndef BARCH_SOURCE_CHAIN_H
#define BARCH_SOURCE_CHAIN_H
#include <cstddef>

namespace barch {
    /**
     * Unlock the first `count` shards of the chain that starts at `first`.
     *
     * The count is how many were locked, so a chain that was only partly locked
     * is only partly unlocked. The chain can't change while any of it is held:
     * relinking a dependency takes the dependent shard unique (KSPACE DEPENDS),
     * and whoever holds this chain holds that shard too.
     */
    template<typename P>
    void unlock_source_chain(const P& first, size_t count) {
        P s = first;
        for (size_t i = 0; i < count && s; ++i) {
            s->unlock_shared();
            s = s->sources();
        }
    }

    /**
     * Lock every shard in the chain that starts at `first` shared, and return how
     * many that was.
     *
     * lock_shared() throws when it times out. If that happens partway along, the
     * ones already taken are let go before the exception goes on. The callers do
     * this from a constructor, and a constructor that throws never runs its
     * destructor, so anything left held here would stay held for good and every
     * later writer to those shards would hang.
     */
    template<typename P>
    size_t lock_source_chain(const P& first) {
        size_t held = 0;
        try {
            for (P s = first; s; s = s->sources()) {
                s->lock_shared();
                ++held;
            }
        } catch (...) {
            unlock_source_chain(first, held);
            throw;
        }
        return held;
    }
}

#endif //BARCH_SOURCE_CHAIN_H
