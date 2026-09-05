#pragma once

#include "driver.h"

struct lua_State;

/** request / response userdata for HTTP method handlers */
void luaopen_crowhttp(lua_State* L);

/** Read a transport() table at `idx` into `out`. False fills err. */
bool crow_read_transport(lua_State* L, int idx, barch::foreign::http_route& out,
                         std::string& err);

/** Push userdata wrapping a live crow::request / crow::response. */
void crow_push_request(lua_State* L, const void* req);
void crow_push_response(lua_State* L, void* res);
/**
 * Push the two tables a templated route's handler gets after req and res:
 * the `{name}` bindings matched out of the path, and the query string. A null
 * `params` pushes an empty table, so an untemplated route's handler still sees
 * four arguments and can ignore the last two. See TODO 222.
 */
void crow_push_params(lua_State* L, const std::vector<barch::foreign::http_binding>* params);
void crow_push_query(lua_State* L, const void* req);
/** Invalidate userdata from the last handler so a stored req/res is dead. */
void crow_http_end_call();
