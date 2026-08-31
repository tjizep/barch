#pragma once

struct lua_State;

/**
 * Install the `http` table: an outbound HTTP client for stored functions.
 *
 *     local r = http.request(url):timeout(2000):get()
 *     if r.ok then return r.body else return r.error end
 *
 * The verb suspends the calling function rather than blocking the worker it is
 * on, so a slow server costs a socket and not a thread. Nothing is installed
 * when the build has no cofetch.
 */
void luaopen_fetch(lua_State* L);
