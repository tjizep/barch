#pragma once
//
// Where a shard reports a write for the indexes over its space - TODO 422, phase 2.
//
// Called under the shard's own latch, so it may only record: nothing that takes
// another latch, nothing that blocks for long. perm_index.cpp's queue appends the key
// behind a mutex of its own and the space's maintenance thread applies it later.
//
#include "value_type.h"

namespace barch {

struct index_sink {
    virtual ~index_sink() = default;
    /** `key` (encoded, as the caller passed it) was written, or erased */
    virtual void changed(art::value_type key, bool erased) = 0;
};

}
