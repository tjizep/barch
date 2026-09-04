// simdjson for stored Luau. The surface follows lua-simdjson: parse to a
// table, open + atPointer for a JSON pointer, encode back to a string.
// No parseFile / openFile — stored functions do not read the disk.

#include "simdjson_luau.h"

#ifdef BARCH_HAS_LUAU

#include "lua.h"
#include "lualib.h"

#include <simdjson.h>

#include <climits>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <vector>

using namespace simdjson;

namespace {

constexpr int max_encode_depth = 128;
const char* parsed_meta = "simdjson.parsed";

thread_local ondemand::parser ondemand_parser;
thread_local std::unique_ptr<char[]> parse_buffer;
thread_local size_t parse_buffer_capacity = 0;

padded_string_view copy_padded(lua_State* L, const char* data, size_t length) {
    if (length > std::numeric_limits<size_t>::max() - SIMDJSON_PADDING)
        luaL_error(L, "JSON input is too large");
    size_t need = length + SIMDJSON_PADDING;
    if (parse_buffer_capacity < need) {
        char* p = new (std::nothrow) char[need];
        if (!p)
            luaL_error(L, "failed to allocate JSON parse buffer");
        parse_buffer.reset(p);
        parse_buffer_capacity = need;
    }
    std::memcpy(parse_buffer.get(), data, length);
    return padded_string_view(parse_buffer.get(), length, parse_buffer_capacity);
}

bool json_bytes(lua_State* L, int idx, const char*& data, size_t& len) {
    if (void* b = lua_tobuffer(L, idx, &len)) {
        data = static_cast<const char*>(b);
        return true;
    }
    if (lua_isstring(L, idx)) {
        data = lua_tolstring(L, idx, &len);
        return data != nullptr;
    }
    return false;
}

void push_json_null(lua_State* L) {
    lua_pushlightuserdata(L, nullptr);
}

void push_int64(lua_State* L, int64_t v) {
    if (v >= (int64_t) INT_MIN && v <= (int64_t) INT_MAX)
        lua_pushinteger(L, (int) v);
    else
        lua_pushinteger64(L, v);
}

template<typename T>
void convert_element(lua_State* L, T& element) {
    switch (element.type()) {
        case ondemand::json_type::array: {
            lua_newtable(L);
            int i = 1;
            for (ondemand::value child : element.get_array()) {
                convert_element(L, child);
                lua_rawseti(L, -2, i++);
            }
            break;
        }
        case ondemand::json_type::object: {
            lua_newtable(L);
            for (ondemand::field field : element.get_object()) {
                std::string_view key = field.unescaped_key();
                lua_pushlstring(L, key.data(), key.size());
                auto val = field.value();
                convert_element(L, val);
                lua_settable(L, -3);
            }
            break;
        }
        case ondemand::json_type::number: {
            ondemand::number number = element.get_number();
            switch (number.get_number_type()) {
                case ondemand::number_type::floating_point_number:
                    lua_pushnumber(L, element.get_double());
                    break;
                case ondemand::number_type::signed_integer:
                    push_int64(L, element.get_int64());
                    break;
                case ondemand::number_type::unsigned_integer: {
                    uint64_t u = element.get_uint64();
                    if (u > (uint64_t) std::numeric_limits<int64_t>::max())
                        lua_pushnumber(L, (double) u);
                    else
                        push_int64(L, (int64_t) u);
                    break;
                }
                default:
                    lua_pushnumber(L, element.get_double());
                    break;
            }
            break;
        }
        case ondemand::json_type::string: {
            std::string_view s = element.get_string();
            lua_pushlstring(L, s.data(), s.size());
            break;
        }
        case ondemand::json_type::boolean:
            lua_pushboolean(L, element.get_bool());
            break;
        case ondemand::json_type::null:
            push_json_null(L);
            break;
        default:
            luaL_error(L, "unsupported JSON type");
            break;
    }
}

int parse(lua_State* L) {
    const char* data = nullptr;
    size_t len = 0;
    if (!json_bytes(L, 1, data, len))
        luaL_error(L, "simdjson.parse expects a string or buffer");
    try {
        ondemand::document doc = ondemand_parser.iterate(copy_padded(L, data, len));
        convert_element(L, doc);
    } catch (const simdjson_error& e) {
        luaL_error(L, "%s", e.what());
    }
    return 1;
}

struct parsed_ud {
    padded_string json;
    ondemand::parser parser;
    ondemand::document doc;

    parsed_ud(const char* s, size_t n) : json(s, n) {
        doc = parser.iterate(json);
    }
};

// Unique_ptr so a throwing constructor cannot leave the userdata
// destructor running ~parsed_ud on raw memory.
struct parsed_box {
    std::unique_ptr<parsed_ud> inner;
};

parsed_ud* check_parsed(lua_State* L, int idx) {
    auto* box = static_cast<parsed_box*>(luaL_checkudata(L, idx, parsed_meta));
    if (!box || !box->inner)
        luaL_error(L, "invalid simdjson document");
    return box->inner.get();
}

int parsed_at_pointer(lua_State* L) {
    auto* p = check_parsed(L, 1);
    const char* pointer = luaL_checkstring(L, 2);
    try {
        p->doc.rewind();
        ondemand::value v = p->doc.at_pointer(pointer);
        convert_element(L, v);
    } catch (const simdjson_error& e) {
        luaL_error(L, "%s", e.what());
    }
    return 1;
}

int json_open(lua_State* L) {
    const char* data = nullptr;
    size_t len = 0;
    if (!json_bytes(L, 1, data, len))
        luaL_error(L, "simdjson.open expects a string or buffer");
    auto* box = static_cast<parsed_box*>(lua_newuserdatadtor(L, sizeof(parsed_box),
        [](void* u) { static_cast<parsed_box*>(u)->~parsed_box(); }));
    new (box) parsed_box();
    try {
        box->inner = std::make_unique<parsed_ud>(data, len);
    } catch (const simdjson_error& e) {
        luaL_error(L, "%s", e.what());
    }
    luaL_getmetatable(L, parsed_meta);
    lua_setmetatable(L, -2);
    return 1;
}

int active_implementation(lua_State* L) {
    const auto& impl = simdjson::get_active_implementation();
    std::string s = impl->name();
    s += " (";
    s += impl->description();
    s += ")";
    lua_pushlstring(L, s.data(), s.size());
    return 1;
}

int absindex(lua_State* L, int idx) {
    if (idx > 0 || idx <= LUA_REGISTRYINDEX)
        return idx;
    return lua_gettop(L) + idx + 1;
}

int table_array_size(lua_State* L, int idx) {
    idx = absindex(L, idx);
    int hint = (int) lua_objlen(L, idx);
    if (hint == 0) {
        lua_pushnil(L);
        if (lua_next(L, idx) == 0)
            return 0;
        lua_pop(L, 2);
        return -1;
    }
    int n = 0;
    lua_pushnil(L);
    while (lua_next(L, idx) != 0) {
        if (!lua_isnumber(L, -2)) {
            lua_pop(L, 2);
            return -1;
        }
        double k = lua_tonumber(L, -2);
        if (k != (double) (int) k || k < 1 || k > (double) hint) {
            lua_pop(L, 2);
            return -1;
        }
        ++n;
        lua_pop(L, 1);
    }
    return n == hint ? hint : -1;
}

void encode_value(lua_State* L, int idx, std::string& out, std::vector<const void*>& stack);

void encode_string(lua_State* L, int idx, std::string& out) {
    size_t n = 0;
    const char* s = lua_tolstring(L, idx, &n);
    out.push_back('"');
    for (size_t i = 0; i < n; ++i) {
        unsigned char c = (unsigned char) s[i];
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof buf, "\\u%04x", c);
                    out += buf;
                } else {
                    out.push_back((char) c);
                }
        }
    }
    out.push_back('"');
}

void encode_number(lua_State* L, int idx, std::string& out) {
    if (lua_type(L, idx) == LUA_TINTEGER) {
        char buf[32];
        std::snprintf(buf, sizeof buf, "%lld", (long long) lua_tointeger64(L, idx, nullptr));
        out += buf;
        return;
    }
    double v = lua_tonumber(L, idx);
    if (!std::isfinite(v))
        luaL_error(L, "cannot encode NaN or infinity as JSON");
    char buf[64];
    if (v == (double) (int64_t) v && v >= -9.2233720368547758e18 && v <= 9.2233720368547758e18)
        std::snprintf(buf, sizeof buf, "%lld", (long long) v);
    else
        std::snprintf(buf, sizeof buf, "%.17g", v);
    out += buf;
}

void encode_value(lua_State* L, int idx, std::string& out, std::vector<const void*>& stack) {
    idx = absindex(L, idx);
    switch (lua_type(L, idx)) {
        case LUA_TSTRING:
            encode_string(L, idx, out);
            break;
        case LUA_TNUMBER:
        case LUA_TINTEGER:
            encode_number(L, idx, out);
            break;
        case LUA_TBOOLEAN:
            out += lua_toboolean(L, idx) ? "true" : "false";
            break;
        case LUA_TNIL:
            out += "null";
            break;
        case LUA_TLIGHTUSERDATA:
            if (lua_touserdata(L, idx) == nullptr)
                out += "null";
            else
                luaL_error(L, "unsupported lightuserdata for JSON encode");
            break;
        case LUA_TTABLE: {
            const void* id = lua_topointer(L, idx);
            for (const void* seen : stack) {
                if (seen == id)
                    luaL_error(L, "cannot encode a cyclic table");
            }
            if ((int) stack.size() >= max_encode_depth)
                luaL_error(L, "maximum nesting depth exceeded");
            stack.push_back(id);
            int n = table_array_size(L, idx);
            if (n > 0) {
                out.push_back('[');
                for (int i = 1; i <= n; ++i) {
                    if (i > 1)
                        out.push_back(',');
                    lua_rawgeti(L, idx, i);
                    encode_value(L, -1, out, stack);
                    lua_pop(L, 1);
                }
                out.push_back(']');
            } else {
                out.push_back('{');
                bool first = true;
                lua_pushnil(L);
                while (lua_next(L, idx) != 0) {
                    if (!first)
                        out.push_back(',');
                    first = false;
                    int kt = lua_type(L, -2);
                    if (kt == LUA_TSTRING) {
                        encode_string(L, -2, out);
                    } else if (kt == LUA_TNUMBER || kt == LUA_TINTEGER) {
                        out.push_back('"');
                        encode_number(L, -2, out);
                        out.push_back('"');
                    } else {
                        luaL_error(L, "unsupported key type for JSON encode");
                    }
                    out.push_back(':');
                    encode_value(L, -1, out, stack);
                    lua_pop(L, 1);
                }
                out.push_back('}');
            }
            stack.pop_back();
            break;
        }
        default:
            luaL_error(L, "unsupported type for JSON encode");
    }
}

void encode_to(lua_State* L, std::string& out) {
    out.reserve(256);
    std::vector<const void*> stack;
    encode_value(L, 1, out, stack);
}

int encode(lua_State* L) {
    std::string out;
    encode_to(L, out);
    lua_pushlstring(L, out.data(), out.size());
    return 1;
}

/*
 * encode, but into a luau buffer rather than a lua string.
 *
 * The input side already took either - json_bytes() tries lua_tobuffer before
 * lua_isstring, so parse() and open() have always accepted a buffer. The output
 * side only made strings, so anything holding json as bytes had to go through
 * an interned lua string on the way out and another on the way back in. This is
 * the other half: what setBufferAt wants, and what parse() will take straight
 * back. See TODO 215.
 */
int encode_buffer(lua_State* L) {
    std::string out;
    encode_to(L, out);
    void* b = lua_newbuffer(L, out.size());
    if (!out.empty())
        memcpy(b, out.data(), out.size());
    return 1;
}

void make_parsed_meta(lua_State* L) {
    luaL_newmetatable(L, parsed_meta);
    lua_newtable(L);
    lua_pushcfunction(L, parsed_at_pointer, "atPointer");
    lua_setfield(L, -2, "atPointer");
    lua_pushcfunction(L, parsed_at_pointer, "at");
    lua_setfield(L, -2, "at");
    lua_setreadonly(L, -1, true);
    lua_setfield(L, -2, "__index");
    lua_setreadonly(L, -1, true);
    lua_pop(L, 1);
}

} // namespace

void luaopen_simdjson(lua_State* L) {
    make_parsed_meta(L);
    lua_newtable(L);
    lua_pushcfunction(L, parse, "parse");
    lua_setfield(L, -2, "parse");
    lua_pushcfunction(L, json_open, "open");
    lua_setfield(L, -2, "open");
    lua_pushcfunction(L, encode, "encode");
    lua_setfield(L, -2, "encode");
    lua_pushcfunction(L, encode_buffer, "encodeBuffer");
    lua_setfield(L, -2, "encodeBuffer");
    lua_pushcfunction(L, active_implementation, "activeImplementation");
    lua_setfield(L, -2, "activeImplementation");
    push_json_null(L);
    lua_setfield(L, -2, "null");
    lua_setreadonly(L, -1, true);
    lua_setglobal(L, "simdjson");
}

#else

void luaopen_simdjson(lua_State*) {}

#endif
