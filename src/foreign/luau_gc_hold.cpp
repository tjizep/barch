// The one file that uses Luau's internals: the public API can't make a buffer without
// a collector step or without zeroing it, and can't make that step later. See
// luau_gc_hold.h. Nothing in push_copy_held may check the collector, so a new Luau
// release needs TestLuauGcHold run against it.
#ifdef BARCH_HAS_LUAU

#include "luau_gc_hold.h"

#include "lobject.h"
#include "lstate.h"
#include "lbuffer.h"
#include "ldo.h"
#include "lgc.h"
#include "lmem.h"

#include <cstring>

namespace barch::luau_gc {

namespace {
    // past Luau's size classes (1024 at most), so the block is that buffer's alone
    constexpr size_t own_block = 4096;

    /** swaps the state's allocator, and puts it back on a throw too */
    struct use_allocator {
        global_State* g;
        const lua_Alloc was;
        use_allocator(global_State* g, lua_Alloc now) : g(g), was(g->frealloc) { g->frealloc = now; }
        ~use_allocator() { g->frealloc = was; }
        use_allocator(const use_allocator&) = delete;
        use_allocator& operator=(const use_allocator&) = delete;
    };
}

void push_copy_held(lua_State* L, const void* bytes, size_t n, lua_Alloc unzeroed) {
    global_State* g = L->global;
    if (n > MAX_BUFFER_SIZE)
        luaM_toobig(L);
    lua_pushnil(L); // grows the stack, before the allocator is swapped
    Buffer* b;
    {
        const bool alone = unzeroed && n >= own_block;
        use_allocator from(g, alone ? unzeroed : g->frealloc);
        b = luaM_newgco(L, Buffer, sizebuffer(n), L->activememcat);
    }
    luaC_init(L, b, LUA_TBUFFER);
    b->len = unsigned(n);
    if (n)
        memcpy(b->data, bytes, n);
    luaC_threadbarrier(L);
    setbufvalue(L, L->top - 1, b);
}

void step_owed(lua_State* L, size_t made) {
    for (size_t i = 0; i < made; ++i)
        luaC_checkGC(L);
}

} // namespace barch::luau_gc

#endif
