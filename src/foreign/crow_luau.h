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
/** Invalidate userdata from the last handler so a stored req/res is dead. */
void crow_http_end_call();
