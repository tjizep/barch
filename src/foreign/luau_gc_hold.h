#pragma once

#include "lua.h"

#include <cstddef>

namespace barch::luau_gc {
    /*
     * Pushes a buffer holding a copy of `n` bytes, made under a shard's lock - TODO 960.
     *
     * It has none of the collector checks lua_newbuffer makes, so no finalizer runs
     * under the lock. It can still throw, out of memory or too big.
     *
     * The buffer isn't zeroed first, as lua_newbuffer does: all `n` bytes are copied
     * over before anything can read it. `unzeroed` is the state's allocator without
     * the zeroing, or null. It's used for a buffer big enough to get a block of its
     * own, so no other object's memory comes from it.
     */
    void push_copy_held(lua_State* L, const void* bytes, size_t n, lua_Alloc unzeroed);

    /*
     * The collector checks skipped for `made` buffers. Call with no lock held: a
     * step can run a finalizer. Each is the check lua_newbuffer would have made, so
     * the pacing is the same.
     */
    void step_owed(lua_State* L, size_t made);
}
