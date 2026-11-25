#pragma once

#include <cstddef>
#include <cstdint>

struct lua_State;

/** Pushes barch.vec: native kernels that filter, group and fold typed column
 *  blocks held in Luau buffers. lent's lent/vec.luau is the reference. */
void push_vec_table(lua_State* L);

/**
 * How barch.vec reads a store cursor's value in place - TODO 969. The host sets it
 * once, before any state pushes barch.vec. Without it, a cursor is no buffer and
 * barch.vec.cursors stays unset.
 */
struct vec_cursor_source {
    enum state : int { not_cursor = 0, has_value, no_value, ended };
    /** whether idx holds a cursor and what it holds. With a value, n is its size */
    state (*size)(lua_State* L, int idx, size_t* n);
    /**
     * Runs f(arg, bytes) once on the value, under the store's latch, and answers true.
     * It answers false, having run nothing, when the value is gone or its size isn't n.
     * f may not touch the Luau state or allocate.
     */
    bool (*read)(lua_State* L, int idx, size_t n, void (*f)(void* arg, const uint8_t* p), void* arg);
};

void set_vec_cursor_source(const vec_cursor_source* s);
