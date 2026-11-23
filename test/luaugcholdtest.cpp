// A buffer made with no collector step - TODO 960, src/foreign/luau_gc_hold.h.
//
// getBuffers and getBufferAt make their buffers under shard locks. This checks:
//   - no finalizer runs while they're made, even with a collection due;
//   - the steps owed collect afterwards, and the finalizers run then;
//   - the pause between cycles is kept: a collection that isn't due isn't started;
//   - the heap grows no more than it does with plain lua_newbuffer;
//   - out of memory and a buffer too big are Lua errors, and the collector runs on;
//   - the bytes are copied whole, and survive collections under way;
//   - the allocator that doesn't zero is used for a big buffer alone, and only then.
#include "foreign/luau_gc_hold.h"

#include "lua.h"
#include "lualib.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

using namespace barch::luau_gc;

static int failures = 0;

static void check(bool ok, const char* what) {
    std::printf("%-72s %s\n", what, ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

static bool refuse = false;
static size_t in_use = 0;
static size_t most_used = 0;

static void* alloc(void*, void* p, size_t osize, size_t nsize) {
    if (nsize == 0) {
        std::free(p);
        in_use -= osize;
        return nullptr;
    }
    if (refuse && nsize > osize)
        return nullptr;
    void* r = std::realloc(p, nsize);
    if (r) {
        in_use += nsize;
        in_use -= osize;
        most_used = std::max(most_used, in_use);
    }
    return r;
}

static size_t freed = 0;

static size_t dirty_calls = 0;

// as the allocator that doesn't zero can be: full of something else
static void* dirty(void* ud, void* p, size_t osize, size_t nsize) {
    void* r = alloc(ud, p, osize, nsize);
    if (r) {
        ++dirty_calls;
        std::memset(r, 0xa5, nsize);
    }
    return r;
}

static std::vector<unsigned char> source(3 << 20, 0x5c);

static void held_buffer(lua_State* L, size_t n) {
    push_copy_held(L, source.data(), n, dirty);
}

static void garbage(lua_State* L, size_t n) {
    for (size_t i = 0; i < n; ++i) {
        lua_newuserdatadtor(L, 8, [](void*) { ++freed; });
        lua_pop(L, 1);
    }
}

static void no_finalizer_while_held() {
    lua_State* L = lua_newstate(alloc, nullptr);
    // from the pause: garbage made during a sweep waits for the cycle after
    lua_gc(L, LUA_GCCOLLECT, 0);
    garbage(L, 64);
    freed = 0;
    // far past the threshold: plain lua_newbuffer collects several times over
    for (int i = 0; i < 400; ++i) {
        held_buffer(L, 64 * 1024);
        lua_pop(L, 1);
    }
    check(freed == 0, "no finalizer runs while buffers are made");
    const size_t owing = lua_totalbytes(L, -1);
    step_owed(L, 400);
    check(lua_totalbytes(L, -1) < owing, "the steps owed collect afterwards");
    // the debt is paid part way through the sweep, and later calls' steps end it
    for (int i = 0; i < 400 && freed == 0; ++i) {
        held_buffer(L, 64 * 1024);
        lua_pop(L, 1);
        step_owed(L, 1);
    }
    check(freed > 0, "and the finalizers run then");
    lua_close(L);
}

static void pause_kept() {
    lua_State* L = lua_newstate(alloc, nullptr);
    // live, so the next cycle is megabytes away
    lua_newbuffer(L, 8 << 20);
    lua_gc(L, LUA_GCCOLLECT, 0);
    garbage(L, 64);
    freed = 0;
    for (int i = 0; i < 64; ++i) {
        held_buffer(L, 16);
        lua_pop(L, 1);
        step_owed(L, 1);
    }
    check(freed == 0, "a collection that isn't due isn't started");
    check(lua_gc(L, LUA_GCISRUNNING, 0) == 1, "the collector is running");
    lua_close(L);
}

// the heap's peak over many batches of 16 blocks, each batch dropped
static size_t peak(bool held) {
    in_use = most_used = 0;
    lua_State* L = lua_newstate(alloc, nullptr);
    lua_newbuffer(L, 1 << 20);
    for (int batch = 0; batch < 5000; ++batch) {
        lua_createtable(L, 16, 0);
        for (int i = 1; i <= 16; ++i) {
            if (held)
                held_buffer(L, 7000);
            else
                lua_newbuffer(L, 7000);
            lua_rawseti(L, -2, i);
        }
        if (held)
            step_owed(L, 16);
        lua_pop(L, 1);
    }
    lua_close(L);
    return most_used;
}

static void errors_put_it_back() {
    lua_State* L = lua_newstate(alloc, nullptr);
    lua_pushcfunction(L, [](lua_State* l) -> int {
        refuse = true;
        held_buffer(l, 1 << 20);
        return 0;
    }, "no memory");
    int st = lua_pcall(L, 0, 0, 0);
    refuse = false;
    check(st == LUA_ERRMEM, "out of memory is a Lua error");
    dirty_calls = 0;
    lua_newbuffer(L, 100000);
    check(dirty_calls == 0, "and the state's own allocator is back");
    lua_pop(L, 1);
    check(lua_gc(L, LUA_GCISRUNNING, 0) == 1, "and the collector runs on");
    lua_pop(L, 1);

    lua_pushcfunction(L, [](lua_State* l) -> int {
        // refused on its size, so the source is never read
        push_copy_held(l, source.data(), size_t(3) << 30, dirty);
        return 0;
    }, "too big");
    st = lua_pcall(L, 0, 0, 0);
    check(st != LUA_OK, "a buffer too big is a Lua error");
    check(lua_gc(L, LUA_GCISRUNNING, 0) == 1, "and the collector runs on");
    lua_close(L);
}

static void copies() {
    lua_State* L = lua_newstate(alloc, nullptr);
    for (size_t i = 0; i < source.size(); ++i)
        source[i] = (unsigned char) (i % 251);
    const size_t sizes[] = {0, 1, 7, 8, 9, 1000, 1024, 1025, 4095, 4096, 7090, 100000};
    const int kinds = sizeof(sizes) / sizeof(sizes[0]);
    const int count = kinds * 200;
    lua_createtable(L, count, 0);
    dirty_calls = 0;
    for (int i = 0; i < count; ++i) {
        push_copy_held(L, source.data() + i, sizes[i % kinds], dirty);
        lua_rawseti(L, -2, i + 1);
        step_owed(L, 1);
        lua_gc(L, LUA_GCSTEP, 1); // some are made with marking part done, some in a sweep
    }
    check(dirty_calls == 200 * 3, "the allocator that doesn't zero gets only the big ones");
    lua_gc(L, LUA_GCCOLLECT, 0);
    bool whole = true;
    for (int i = 0; i < count; ++i) {
        lua_rawgeti(L, -1, i + 1);
        size_t n = 0;
        const void* b = lua_tobuffer(L, -1, &n);
        whole = whole && b && n == sizes[i % kinds] && !std::memcmp(b, source.data() + i, n);
        lua_pop(L, 1);
    }
    check(whole, "every buffer holds its bytes after collections");
    dirty_calls = 0;
    lua_newbuffer(L, 100000);
    size_t n = 0;
    const auto* b = static_cast<const unsigned char*>(lua_tobuffer(L, -1, &n));
    check(dirty_calls == 0 && std::all_of(b, b + n, [](unsigned char c) { return c == 0; }),
          "the state's own allocator is back");
    lua_close(L);
}

int main() {
    no_finalizer_while_held();
    pause_kept();
    const size_t plain = peak(false), held = peak(true);
    std::printf("heap peak: plain %zu, held %zu\n", plain, held);
    check(held <= plain + plain / 4, "the heap grows no more than with lua_newbuffer");
    errors_put_it_back();
    copies();
    std::printf("%s\n", failures ? "FAILED" : "all passed");
    return failures ? 1 : 0;
}
