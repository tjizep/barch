#pragma once

struct lua_State;

/** Pushes barch.vec: native kernels that filter, group and fold typed column
 *  blocks held in Luau buffers. lent's lent/vec.luau is the reference. */
void push_vec_table(lua_State* L);
