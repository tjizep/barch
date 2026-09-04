// Crow request/response userdata and transport() table reader for stored Luau.

#include "crow_luau.h"

#ifdef BARCH_HAS_LUAU

#include "lua.h"
#include "lualib.h"

#include <cctype>
#include <cstring>
#include <string>

#ifdef BARCH_HAS_CROW
#include <crow.h>
#endif

namespace {

thread_local uint32_t call_gen = 1;

const char* req_meta = "crow.request";
const char* res_meta = "crow.response";

#ifdef BARCH_HAS_CROW
struct req_box {
    const crow::request* req{nullptr};
    uint32_t gen{0};
};

struct res_box {
    crow::response* res{nullptr};
    uint32_t gen{0};
};

req_box* check_req(lua_State* L, int idx) {
    auto* p = static_cast<req_box*>(luaL_checkudata(L, idx, req_meta));
    if (!p || !p->req || p->gen != call_gen)
        luaL_error(L, "HTTP request is no longer valid");
    return p;
}

res_box* check_res(lua_State* L, int idx) {
    auto* p = static_cast<res_box*>(luaL_checkudata(L, idx, res_meta));
    if (!p || !p->res || p->gen != call_gen)
        luaL_error(L, "HTTP response is no longer valid");
    return p;
}

int req_header(lua_State* L) {
    auto* p = check_req(L, 1);
    const char* name = luaL_checkstring(L, 2);
    const std::string& v = p->req->get_header_value(name);
    lua_pushlstring(L, v.data(), v.size());
    return 1;
}

int req_param(lua_State* L) {
    auto* p = check_req(L, 1);
    const char* name = luaL_checkstring(L, 2);
    const char* v = p->req->url_params.get(name);
    if (!v)
        lua_pushnil(L);
    else
        lua_pushstring(L, v);
    return 1;
}

int req_cookie(lua_State* L) {
    auto* p = check_req(L, 1);
    const char* name = luaL_checkstring(L, 2);
    const std::string& h = p->req->get_header_value("Cookie");
    size_t nlen = std::strlen(name);
    size_t i = 0;
    while (i < h.size()) {
        while (i < h.size() && (h[i] == ' ' || h[i] == ';'))
            ++i;
        if (i + nlen <= h.size() && h.compare(i, nlen, name) == 0 &&
            i + nlen < h.size() && h[i + nlen] == '=') {
            size_t v = i + nlen + 1;
            size_t e = h.find(';', v);
            if (e == std::string::npos)
                e = h.size();
            lua_pushlstring(L, h.data() + v, e - v);
            return 1;
        }
        i = h.find(';', i);
        if (i == std::string::npos)
            break;
        ++i;
    }
    lua_pushnil(L);
    return 1;
}

/*
 * Bytes from a lua string or a luau buffer, whichever was passed.
 *
 * The server used to be strings only in both directions, so a handler holding
 * bytes - what simdjson.encodeBuffer or the store's getBufferAt hand back - had
 * to go out through an interned lua string, and a binary request body arrived
 * as one. See TODO 216.
 */
bool body_bytes(lua_State* L, int idx, const char*& data, size_t& len) {
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

void push_buffer(lua_State* L, const char* data, size_t len) {
    void* b = lua_newbuffer(L, len);
    if (len)
        memcpy(b, data, len);
}

int req_index(lua_State* L) {
    auto* p = check_req(L, 1);
    const char* k = luaL_checkstring(L, 2);
    if (std::strcmp(k, "body") == 0) {
        lua_pushlstring(L, p->req->body.data(), p->req->body.size());
        return 1;
    }
    if (std::strcmp(k, "bodyBuffer") == 0) {
        push_buffer(L, p->req->body.data(), p->req->body.size());
        return 1;
    }
    if (std::strcmp(k, "url") == 0) {
        lua_pushlstring(L, p->req->url.data(), p->req->url.size());
        return 1;
    }
    if (std::strcmp(k, "raw_url") == 0) {
        lua_pushlstring(L, p->req->raw_url.data(), p->req->raw_url.size());
        return 1;
    }
    if (std::strcmp(k, "method") == 0) {
        auto m = crow::method_name(p->req->method);
        lua_pushlstring(L, m.data(), m.size());
        return 1;
    }
    if (std::strcmp(k, "remote") == 0) {
        lua_pushlstring(L, p->req->remote_ip_address.data(),
                        p->req->remote_ip_address.size());
        return 1;
    }
    if (std::strcmp(k, "header") == 0) {
        lua_pushcfunction(L, req_header, "header");
        return 1;
    }
    if (std::strcmp(k, "param") == 0) {
        lua_pushcfunction(L, req_param, "param");
        return 1;
    }
    if (std::strcmp(k, "cookie") == 0) {
        lua_pushcfunction(L, req_cookie, "cookie");
        return 1;
    }
    return 0;
}

int res_header(lua_State* L) {
    auto* p = check_res(L, 1);
    const char* name = luaL_checkstring(L, 2);
    if (lua_gettop(L) < 3) {
        const std::string& v = p->res->get_header_value(name);
        lua_pushlstring(L, v.data(), v.size());
        return 1;
    }
    size_t n = 0;
    const char* v = lua_tolstring(L, 3, &n);
    if (!v)
        luaL_error(L, "header value must be a string");
    p->res->set_header(name, std::string(v, n));
    return 0;
}

int res_cookie(lua_State* L) {
    auto* p = check_res(L, 1);
    const char* name = luaL_checkstring(L, 2);
    size_t vn = 0;
    const char* val = lua_tolstring(L, 3, &vn);
    if (!val)
        luaL_error(L, "cookie value must be a string");
    std::string cookie = std::string(name) + "=" + std::string(val, vn);
    if (lua_istable(L, 4)) {
        lua_getfield(L, 4, "path");
        if (lua_isstring(L, -1)) {
            cookie += "; Path=";
            cookie += lua_tostring(L, -1);
        }
        lua_pop(L, 1);
        lua_getfield(L, 4, "max_age");
        if (lua_isnumber(L, -1)) {
            cookie += "; Max-Age=";
            cookie += std::to_string((long long) lua_tointeger(L, -1));
        }
        lua_pop(L, 1);
        lua_getfield(L, 4, "httponly");
        if (lua_toboolean(L, -1))
            cookie += "; HttpOnly";
        lua_pop(L, 1);
        lua_getfield(L, 4, "secure");
        if (lua_toboolean(L, -1))
            cookie += "; Secure";
        lua_pop(L, 1);
        lua_getfield(L, 4, "samesite");
        if (lua_isstring(L, -1)) {
            cookie += "; SameSite=";
            cookie += lua_tostring(L, -1);
        }
        lua_pop(L, 1);
    } else {
        cookie += "; Path=/; HttpOnly";
    }
    p->res->add_header("Set-Cookie", cookie);
    return 0;
}

int res_write(lua_State* L) {
    auto* p = check_res(L, 1);
    size_t n = 0;
    const char* s = nullptr;
    if (!body_bytes(L, 2, s, n))
        luaL_error(L, "write expects a string or buffer");
    p->res->write(std::string(s, n));
    return 0;
}

int res_redirect(lua_State* L) {
    auto* p = check_res(L, 1);
    p->res->redirect(luaL_checkstring(L, 2));
    return 0;
}

int res_index(lua_State* L) {
    auto* p = check_res(L, 1);
    const char* k = luaL_checkstring(L, 2);
    if (std::strcmp(k, "body") == 0) {
        lua_pushlstring(L, p->res->body.data(), p->res->body.size());
        return 1;
    }
    if (std::strcmp(k, "bodyBuffer") == 0) {
        push_buffer(L, p->res->body.data(), p->res->body.size());
        return 1;
    }
    if (std::strcmp(k, "code") == 0) {
        lua_pushinteger(L, p->res->code);
        return 1;
    }
    if (std::strcmp(k, "header") == 0) {
        lua_pushcfunction(L, res_header, "header");
        return 1;
    }
    if (std::strcmp(k, "write") == 0) {
        lua_pushcfunction(L, res_write, "write");
        return 1;
    }
    if (std::strcmp(k, "redirect") == 0) {
        lua_pushcfunction(L, res_redirect, "redirect");
        return 1;
    }
    if (std::strcmp(k, "cookie") == 0) {
        lua_pushcfunction(L, res_cookie, "cookie");
        return 1;
    }
    return 0;
}

int res_newindex(lua_State* L) {
    auto* p = check_res(L, 1);
    const char* k = luaL_checkstring(L, 2);
    if (std::strcmp(k, "body") == 0 || std::strcmp(k, "bodyBuffer") == 0) {
        size_t n = 0;
        const char* s = nullptr;
        if (!body_bytes(L, 3, s, n))
            luaL_error(L, "body must be a string or buffer");
        p->res->body.assign(s, n);
        return 0;
    }
    if (std::strcmp(k, "code") == 0) {
        p->res->code = (int) luaL_checkinteger(L, 3);
        return 0;
    }
    luaL_error(L, "HTTP response has no field '%s'", k);
    return 0;
}

void make_req_meta(lua_State* L) {
    luaL_newmetatable(L, req_meta);
    lua_pushcfunction(L, req_index, "__index");
    lua_setfield(L, -2, "__index");
    lua_setreadonly(L, -1, true);
    lua_pop(L, 1);
}

void make_res_meta(lua_State* L) {
    luaL_newmetatable(L, res_meta);
    lua_pushcfunction(L, res_index, "__index");
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, res_newindex, "__newindex");
    lua_setfield(L, -2, "__newindex");
    lua_setreadonly(L, -1, true);
    lua_pop(L, 1);
}
#endif

int absindex(lua_State* L, int idx) {
    if (idx > 0 || idx <= LUA_REGISTRYINDEX)
        return idx;
    return lua_gettop(L) + idx + 1;
}

bool field(lua_State* L, int idx, const char* name) {
    lua_getfield(L, idx, name);
    if (lua_isnil(L, -1) || lua_isnone(L, -1)) {
        lua_pop(L, 1);
        return false;
    }
    return true;
}

bool as_string(lua_State* L, int idx, std::string& out, std::string& err, const char* what) {
    if (lua_isfunction(L, idx)) {
        lua_pushvalue(L, idx);
        if (lua_pcall(L, 0, 1, 0) != 0) {
            err = lua_tostring(L, -1) ? lua_tostring(L, -1) : "transport ssl callback failed";
            lua_pop(L, 1);
            return false;
        }
        if (!lua_isstring(L, -1)) {
            lua_pop(L, 1);
            err = std::string(what) + " callback must return a string";
            return false;
        }
        size_t n = 0;
        const char* s = lua_tolstring(L, -1, &n);
        out.assign(s, n);
        lua_pop(L, 1);
        return true;
    }
    if (!lua_isstring(L, idx) && lua_type(L, idx) != LUA_TNUMBER) {
        err = std::string(what) + " must be a string";
        return false;
    }
    size_t n = 0;
    const char* s = lua_tolstring(L, idx, &n);
    out.assign(s ? s : "", n);
    return true;
}

void fold_verb(std::string& v) {
    for (auto& ch : v)
        ch = (char) toupper((unsigned char) ch);
}

} // namespace

void luaopen_crowhttp(lua_State* L) {
#ifdef BARCH_HAS_CROW
    make_req_meta(L);
    make_res_meta(L);
#else
    (void) L;
#endif
}

bool crow_read_transport(lua_State* L, int idx, barch::foreign::http_route& out,
                         std::string& err) {
    idx = absindex(L, idx);
    if (!lua_istable(L, idx)) {
        err = "transport() must return a table";
        return false;
    }
    out.has_transport = true;
    if (field(L, idx, "kind")) {
        if (!as_string(L, -1, out.kind, err, "kind")) {
            lua_pop(L, 1);
            return false;
        }
        lua_pop(L, 1);
        for (auto& ch : out.kind)
            ch = (char) std::tolower((unsigned char) ch);
        if (out.kind != "http" && out.kind != "resource") {
            err = "transport() kind must be http or resource";
            return false;
        }
    }
    if (field(L, idx, "route")) {
        size_t n = 0;
        const char* s = lua_tolstring(L, -1, &n);
        if (s)
            out.route.assign(s, n);
        lua_pop(L, 1);
        if (!out.route.empty() && out.route[0] != '/')
            out.route.insert(out.route.begin(), '/');
        out.has_route = !out.route.empty();
    }
    if (field(L, idx, "accept")) {
        if (!as_string(L, -1, out.accept, err, "accept")) {
            lua_pop(L, 1);
            return false;
        }
        lua_pop(L, 1);
    }
    if (field(L, idx, "send")) {
        if (!as_string(L, -1, out.send, err, "send")) {
            lua_pop(L, 1);
            return false;
        }
        lua_pop(L, 1);
    }
    if (field(L, idx, "cors")) {
        if (!as_string(L, -1, out.cors, err, "cors")) {
            lua_pop(L, 1);
            return false;
        }
        lua_pop(L, 1);
    }
    if (field(L, idx, "bind")) {
        if (!as_string(L, -1, out.bind, err, "bind")) {
            lua_pop(L, 1);
            return false;
        }
        lua_pop(L, 1);
    }
    if (field(L, idx, "port")) {
        if (lua_isnumber(L, -1))
            out.port = (uint16_t) lua_tointeger(L, -1);
        lua_pop(L, 1);
    }
    if (field(L, idx, "methods")) {
        if (!lua_istable(L, -1)) {
            lua_pop(L, 1);
            err = "methods must be a table";
            return false;
        }
        int mt = lua_gettop(L);
        lua_pushnil(L);
        while (lua_next(L, mt) != 0) {
            barch::foreign::http_method m;
            if (lua_type(L, -2) == LUA_TSTRING) {
                size_t n = 0;
                const char* s = lua_tolstring(L, -2, &n);
                m.verb.assign(s, n);
                fold_verb(m.verb);
            }
            if (lua_isfunction(L, -1)) {
                lua_pushvalue(L, -1);
                m.fn_ref = lua_ref(L, -1);
                lua_pop(L, 1);
            } else if (lua_isstring(L, -1)) {
                const char* name = lua_tostring(L, -1);
                lua_getglobal(L, name);
                if (!lua_isfunction(L, -1)) {
                    err = "methods." + m.verb + " is not a function";
                    return false;
                }
                m.fn_ref = lua_ref(L, -1);
                lua_pop(L, 1);
            } else {
                err = "methods." + m.verb + " is not a function";
                return false;
            }
            if (!m.verb.empty())
                out.methods.push_back(std::move(m));
            lua_pop(L, 1);
        }
        lua_pop(L, 1);
        if (!out.methods.empty())
            out.has_route = out.has_route || !out.route.empty();
    }
    if (field(L, idx, "keys")) {
        if (lua_istable(L, -1)) {
            int kt = lua_gettop(L);
            int n = (int) lua_objlen(L, kt);
            for (int i = 1; i <= n; ++i) {
                lua_rawgeti(L, kt, i);
                if (lua_isstring(L, -1)) {
                    size_t len = 0;
                    const char* s = lua_tolstring(L, -1, &len);
                    std::string name(s, len);
                    fold_verb(name);
                    out.extra_keys.push_back(std::move(name));
                }
                lua_pop(L, 1);
            }
        }
        lua_pop(L, 1);
    }
    if (field(L, idx, "ssl")) {
        if (lua_istable(L, -1)) {
            int st = lua_gettop(L);
            if (field(L, st, "cert")) {
                if (!as_string(L, -1, out.ssl_cert, err, "ssl.cert")) {
                    lua_pop(L, 2);
                    return false;
                }
                lua_pop(L, 1);
            }
            if (field(L, st, "key")) {
                if (!as_string(L, -1, out.ssl_key, err, "ssl.key")) {
                    lua_pop(L, 2);
                    return false;
                }
                lua_pop(L, 1);
            }
            if (field(L, st, "proto")) {
                if (!as_string(L, -1, out.ssl_proto, err, "ssl.proto")) {
                    lua_pop(L, 2);
                    return false;
                }
                lua_pop(L, 1);
            }
        }
        lua_pop(L, 1);
    }
    if (out.kind == "http")
        out.has_route = false;
    else if (out.kind == "resource") {
        if (out.route.empty()) {
            err = "kind=resource needs a route";
            return false;
        }
        out.has_route = true;
    }
    if (out.has_route && out.route.empty())
        out.has_route = false;
    if (out.has_route && out.methods.empty()) {
        err = "transport() route has no methods";
        return false;
    }
    return true;
}

void crow_push_request(lua_State* L, const void* req) {
#ifdef BARCH_HAS_CROW
    auto* p = static_cast<req_box*>(lua_newuserdata(L, sizeof(req_box)));
    p->req = static_cast<const crow::request*>(req);
    p->gen = call_gen;
    luaL_getmetatable(L, req_meta);
    lua_setmetatable(L, -2);
#else
    (void) req;
    lua_pushnil(L);
#endif
}

void crow_push_response(lua_State* L, void* res) {
#ifdef BARCH_HAS_CROW
    auto* p = static_cast<res_box*>(lua_newuserdata(L, sizeof(res_box)));
    p->res = static_cast<crow::response*>(res);
    p->gen = call_gen;
    luaL_getmetatable(L, res_meta);
    lua_setmetatable(L, -2);
#else
    (void) res;
    lua_pushnil(L);
#endif
}

void crow_http_end_call() {
    ++call_gen;
    if (call_gen == 0)
        call_gen = 1;
}

#else

void luaopen_crowhttp(lua_State*) {}

bool crow_read_transport(lua_State*, int, barch::foreign::http_route&, std::string& err) {
    err = "luau not built";
    return false;
}

void crow_push_request(lua_State*, const void*) {}
void crow_push_response(lua_State*, void*) {}
void crow_http_end_call() {}

#endif
