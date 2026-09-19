#pragma once

struct lua_State;

/**
 * Install the `resp` table: a RESP client for stored functions. TODO 379.
 *
 *     local r = resp.connect("10.0.0.5", 6379, {password_from = "cache.password"})
 *     local v = r:call("GET", "k")
 *     local out = r:pipeline({{"SET", "a", "1"}, {"GET", "a"}})
 *
 * `call` and `pipeline` suspend the calling function while the client's own thread
 * does the work, the way http.request does, and wait inline in an HTTP route handler.
 * A handle holds no connection: each operation takes one from a pool and gives it
 * back, so nothing is left open when the function returns.
 */
void luaopen_resp(lua_State* L);
