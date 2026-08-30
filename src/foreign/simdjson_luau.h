#pragma once

struct lua_State;

/** simdjson parse / open / encode for stored functions, as the simdjson global. */
void luaopen_simdjson(lua_State* L);
