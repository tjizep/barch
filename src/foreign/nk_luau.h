#pragma once

struct lua_State;

/** NumKong scalars and vectors for stored functions: nk.f32, nkf32, nkf32vector, … */
void luaopen_nk(lua_State* L);
