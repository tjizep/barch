#pragma once

struct lua_State;

/** NumKong scalars, vectors and matrices for stored functions: nk.f32, nkf32, nkf32vector, nkf32matrix, … */
void luaopen_nk(lua_State* L);
