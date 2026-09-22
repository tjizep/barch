#pragma once

#include <cstddef>
#include <string>

struct lua_State;

/** simdjson parse / open / encode for stored functions, as the simdjson global. */
void luaopen_simdjson(lua_State* L);

/**
 * Push what simdjson.parse would for these bytes, without raising: false
 * pushes nothing and leaves the reason in `why`. getJson's half - bytes that
 * aren't JSON are an answer there, not an error. See TODO 408.
 */
bool simdjson_push_parsed(lua_State* L, const char* data, size_t len, std::string& why);

/**
 * Push one value per name for the JSON object in these bytes - the names are
 * the `count` strings from stack slot `first` on - with nil where a name isn't
 * a top-level field. Doesn't raise: false pushes nothing and leaves the reason
 * in `why`, which is bytes that aren't JSON or JSON that isn't an object.
 * parseJson's half. See TODO 409.
 */
bool simdjson_push_fields(lua_State* L, const char* data, size_t len, int first, int count,
                          std::string& why);

/**
 * Encode the value at `idx` the way simdjson.encode does. Raises on what
 * encode refuses - a cycle, a function, NaN. setJson's half.
 */
void simdjson_encode_at(lua_State* L, int idx, std::string& out);
