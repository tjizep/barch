//
// Created by barch on 27-09-2026.
//
#ifndef BARCH_MEMORY_LIMIT_H
#define BARCH_MEMORY_LIMIT_H

namespace barch {
    /**
     * While one of these lives, inserts on this thread aren't refused for going
     * over max_memory - TODO 483. For a change log replay, and for a replica
     * applying its primary's writes (TODO 502): refusing either drops a write for
     * good, or leaves an older value in its place. The first maintenance pass
     * evicts back under the limit instead, and logs what it takes.
     *
     * On its own here, out of shard.h, so the replication commands can use it
     * without everything shard.h brings in.
     */
    struct lift_memory_limit {
        lift_memory_limit();
        ~lift_memory_limit();
        lift_memory_limit(const lift_memory_limit&) = delete;
        lift_memory_limit& operator=(const lift_memory_limit&) = delete;
    private:
        bool was;
    };
}

#endif //BARCH_MEMORY_LIMIT_H
