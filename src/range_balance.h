#pragma once
//
// Where a range sweep starts - TODO 421. Kept apart from range_index.cpp so it
// can be tested over plain sizes, without shards.
//
#include <cstddef>

namespace barch {

/**
 * The shard a sweep should start its cascade from: the largest one that can
 * shed, meaning it has a neighbour more than a key smaller. `n` when none can.
 *
 * The sweep used to start from the first largest shard whatever its neighbours
 * held. Two largest shards side by side at the end of the range, like
 * [1682, 1682, 951, ...], left it on shard 0, whose only neighbour is shard 1.
 * Nothing moves between shards within a key of each other, so the cascade
 * stopped there. It never tried shard 1, which could shed into shard 2, and every
 * tick after made the same choice. Ties like that are common, because shedding
 * half way is what makes neighbours equal.
 */
template<typename SizeAt>
size_t shed_start(size_t n, SizeAt size_at) {
    size_t start = n;
    for (size_t i = 0; i < n; ++i) {
        size_t here = size_at(i);
        bool can = (i + 1 < n && here > size_at(i + 1) + 1) ||
                   (i > 0 && here > size_at(i - 1) + 1);
        if (can && (start == n || here > size_at(start)))
            start = i;
    }
    return start;
}

}
