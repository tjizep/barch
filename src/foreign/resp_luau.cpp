#include "resp_luau.h"

#include "driver.h"
#include "../resp_client.h"
#include "swig_api.h"

#include <cmath>
#include <condition_variable>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "lua.h"
#include "lualib.h"

/*
 * resp.connect - the Luau side of the RESP client, TODO 379.
 *
 * Nothing here touches a socket and nothing on the client's thread touches a Lua
 * state. What crosses is owned data: the endpoint, the settings and the commands go
 * out as copies, the replies come back as Variables, and they are pushed onto the Lua
 * stack only on the thread that resumes the call - complete_call's push function runs
 * there, never on the client's thread.
 *
 * A handle is only the endpoint and the settings. It holds no connection, so there
 * is nothing to close and nothing to leak when a function returns or errors: each
 * call takes a connection from the pool and the client puts it back when the
 * exchange is over.
 */

namespace {

using barch::resp_client::command;
using barch::resp_client::endpoint;
using barch::resp_client::settings;

constexpr const char* handle_mt = "barch.resp.connection";

struct handle {
    endpoint ep;
    settings set;
};

void require_outbound(lua_State* L, const char* what) {
    const auto* acc = barch::foreign::current_access(L);
    if (acc && !acc->may_reach_out)
        luaL_error(L, "FUNCTION %s needs the outbound category", what);
}

void refuse_locked(lua_State* L, const char* what) {
    // a remote that takes its time would hold a shard lock for as long as it takes,
    // and reading the settings takes latches of its own
    if (barch::foreign::in_locked_region(L))
        luaL_error(L, "FUNCTION %s is not allowed inside a locked region", what);
}

uint64_t conf_u64(KeyValue& conf, const char* key, uint64_t fallback) {
    auto v = conf.get(key);
    if (v.empty())
        return fallback;
    char* end = nullptr;
    unsigned long long n = strtoull(v.c_str(), &end, 10);
    return (end && *end == 0 && n > 0) ? (uint64_t) n : fallback;
}

settings read_settings() {
    settings s;
    KeyValue conf("configuration");
    s.connect_timeout_ms = (uint32_t) conf_u64(conf, "resp.connect_timeout_ms", s.connect_timeout_ms);
    s.timeout_ms = (uint32_t) conf_u64(conf, "resp.timeout_ms", s.timeout_ms);
    s.max_connections = (size_t) conf_u64(conf, "resp.max_connections", s.max_connections);
    s.max_idle = (size_t) conf_u64(conf, "resp.max_idle", s.max_idle);
    s.idle_ms = conf_u64(conf, "resp.idle_ms", s.idle_ms);
    s.lim.max_reply = (size_t) conf_u64(conf, "resp.max_reply_bytes", s.lim.max_reply);
    s.lim.max_bulk = (size_t) conf_u64(conf, "resp.max_bulk_bytes", s.lim.max_bulk);
    return s;
}

/** one argument the way barch.call writes it: a number as the wire would carry it */
std::string word_at(lua_State* L, int i, const char* what) {
    if (lua_type(L, i) == LUA_TNUMBER) {
        double d = lua_tonumber(L, i);
        double whole = 0;
        char buf[40];
        if (std::modf(d, &whole) == 0.0 && std::fabs(d) < 9.2e18)
            snprintf(buf, sizeof buf, "%lld", (long long) d);
        else
            snprintf(buf, sizeof buf, "%.17g", d);
        return buf;
    }
    size_t n = 0;
    const char* s = lua_tolstring(L, i, &n);
    if (!s || lua_type(L, i) != LUA_TSTRING)
        luaL_error(L, "FUNCTION %s takes strings and numbers", what);
    return std::string(s, n);
}

std::string opt_string(lua_State* L, int t, const char* field) {
    lua_getfield(L, t, field);
    std::string out;
    if (lua_isstring(L, -1)) {
        size_t n = 0;
        const char* s = lua_tolstring(L, -1, &n);
        out.assign(s, n);
    } else if (!lua_isnil(L, -1)) {
        luaL_error(L, "FUNCTION resp.connect %s must be a string", field);
    }
    lua_pop(L, 1);
    return out;
}

int64_t opt_int(lua_State* L, int t, const char* field, int64_t fallback) {
    lua_getfield(L, t, field);
    int64_t out = fallback;
    if (lua_isnumber(L, -1))
        out = (int64_t) lua_tonumber(L, -1);
    else if (!lua_isnil(L, -1))
        luaL_error(L, "FUNCTION resp.connect %s must be a number", field);
    lua_pop(L, 1);
    return out;
}

/*
 * resp.connect(host, port [, opts]) - a handle; no network yet.
 *
 * opts: user, password, password_from (a configuration key the password is read
 * from, so a script never holds it - keys ending in "password" are hidden from
 * scripts, see hide_secrets), db, protocol (2 or 3), timeout, connect_timeout.
 */
int resp_connect(lua_State* L) {
    require_outbound(L, "resp.connect");
    refuse_locked(L, "resp.connect");
    size_t hn = 0;
    const char* host = luaL_checklstring(L, 1, &hn);
    int64_t port = (int64_t) luaL_checknumber(L, 2);
    if (hn == 0)
        luaL_error(L, "FUNCTION resp.connect needs a host");
    if (port < 1 || port > 65535)
        luaL_error(L, "FUNCTION resp.connect port must be 1 to 65535");

    endpoint ep;
    ep.host.assign(host, hn);
    ep.port = (uint16_t) port;
    settings set = read_settings();
    if (!lua_isnoneornil(L, 3)) {
        luaL_checktype(L, 3, LUA_TTABLE);
        ep.user = opt_string(L, 3, "user");
        ep.password = opt_string(L, 3, "password");
        auto from = opt_string(L, 3, "password_from");
        if (!from.empty()) {
            if (!ep.password.empty())
                luaL_error(L, "FUNCTION resp.connect takes password or password_from, not both");
            KeyValue conf("configuration");
            ep.password = conf.get(from);
            if (ep.password.empty())
                luaL_error(L, "FUNCTION resp.connect found no password at configuration key %s",
                           from.c_str());
        }
        ep.db = opt_int(L, 3, "db", -1);
        if (ep.db < -1)
            luaL_error(L, "FUNCTION resp.connect db must not be negative");
        ep.protocol = (int) opt_int(L, 3, "protocol", 2);
        if (ep.protocol != 2 && ep.protocol != 3)
            luaL_error(L, "FUNCTION resp.connect protocol is 2 or 3");
        auto t = opt_int(L, 3, "timeout", 0);
        auto ct = opt_int(L, 3, "connect_timeout", 0);
        // lowered or raised, never switched off: there is no safe unbounded wait here
        if (t < 0 || ct < 0)
            luaL_error(L, "FUNCTION resp.connect timeouts must be positive");
        if (t > 0)
            set.timeout_ms = (uint32_t) t;
        if (ct > 0)
            set.connect_timeout_ms = (uint32_t) ct;
    }

    auto* h = static_cast<handle*>(lua_newuserdatadtor(L, sizeof(handle),
        [](void* p) { static_cast<handle*>(p)->~handle(); }));
    new (h) handle{std::move(ep), set};
    luaL_getmetatable(L, handle_mt);
    lua_setmetatable(L, -2);
    return 1;
}

/*
 * What a finished exchange leaves on the stack for the continuation: a flag and a
 * value. true and the reply, or false and the message to raise.
 */
int push_outcome(lua_State* L, const std::vector<Variable>& replies, const std::string& err,
                 bool single) {
    if (!err.empty()) {
        lua_pushboolean(L, false);
        lua_pushlstring(L, err.data(), err.size());
        return 2;
    }
    if (single) {
        const Variable& v = replies.at(0);
        if (v.index() == var_error) {
            // raised the way barch.call raises a refused command
            lua_pushboolean(L, false);
            lua_pushstring(L, std::get<error>(v).what());
            return 2;
        }
        lua_pushboolean(L, true);
        barch::foreign::push_reply(L, v);
        return 2;
    }
    // a pipeline hands everything back: one failed command doesn't cost the others'
    // replies, and an error reply is a table {err = message}, the redis-lua shape
    lua_pushboolean(L, true);
    lua_createtable(L, (int) replies.size(), 0);
    int at = 1;
    for (const auto& v : replies) {
        if (v.index() == var_error) {
            lua_createtable(L, 0, 1);
            lua_pushstring(L, std::get<error>(v).what());
            lua_setfield(L, -2, "err");
        } else {
            barch::foreign::push_reply(L, v);
        }
        lua_rawseti(L, -2, at++);
    }
    return 2;
}

/** runs after the resume, where it can raise: the push function can only push */
int finish(lua_State* L, int) {
    if (!lua_toboolean(L, -2)) {
        size_t n = 0;
        const char* msg = lua_tolstring(L, -1, &n);
        lua_pushlstring(L, msg ? msg : "resp: failed", msg ? n : 12);
        lua_error(L);
    }
    return 1;
}

struct wait_box {
    std::mutex mu;
    std::condition_variable cv;
    bool done{false};
    std::vector<Variable> replies;
    std::string err;
};

int exchange(lua_State* L, const handle* h, std::vector<command> cmds, bool single) {
    if (auto parked = barch::foreign::park_call(L)) {
        barch::resp_client::run(h->ep, h->set, std::move(cmds),
            [parked, single](std::vector<Variable> replies, std::string err) {
                // on the client's thread: nothing Lua here. The push runs where the
                // call resumes
                barch::foreign::complete_call(parked,
                    [replies = std::move(replies), err = std::move(err), single](lua_State* T) {
                        return push_outcome(T, replies, err, single);
                    });
            });
        return lua_yield(L, 0);
    }
    /*
     * An HTTP route handler can't yield - see park_call - so it waits here, holding
     * its thread for as long as the timeout allows. The client's thread never waits
     * on anything, so this can't be waiting on itself.
     */
    // the wait comes off the deadline, and the timeouts are cut down to what's
    // left before the wall ceiling - TODO 435
    auto set = h->set;
    if (uint64_t cap = barch::foreign::blocking_wait_cap(L)) {
        if (cap < set.timeout_ms) set.timeout_ms = (uint32_t) cap;
        if (cap < set.connect_timeout_ms) set.connect_timeout_ms = (uint32_t) cap;
    }
    const int64_t started = barch::foreign::blocking_wait_start(L);
    auto box = std::make_shared<wait_box>();
    barch::resp_client::run(h->ep, set, std::move(cmds),
        [box](std::vector<Variable> replies, std::string err) {
            {
                std::lock_guard<std::mutex> lk(box->mu);
                box->replies = std::move(replies);
                box->err = std::move(err);
                box->done = true;
            }
            box->cv.notify_one();
        });
    std::unique_lock<std::mutex> lk(box->mu);
    box->cv.wait(lk, [&] { return box->done; });
    auto replies = std::move(box->replies);
    auto err = std::move(box->err);
    lk.unlock();
    barch::foreign::blocking_wait_end(L, started);
    push_outcome(L, replies, err, single);
    return finish(L, 0);
}

handle* check_handle(lua_State* L) {
    return static_cast<handle*>(luaL_checkudata(L, 1, handle_mt));
}

/* r:call(cmd, ...) - one command, its reply, or an error raised */
int resp_call(lua_State* L) {
    auto* h = check_handle(L);
    require_outbound(L, "resp call");
    refuse_locked(L, "resp call");
    int n = lua_gettop(L);
    if (n < 2)
        luaL_error(L, "FUNCTION resp call needs a command name");
    command c;
    c.reserve(n - 1);
    for (int i = 2; i <= n; ++i)
        c.push_back(word_at(L, i, "resp call"));
    auto why = barch::resp_client::refused(c);
    if (!why.empty())
        luaL_error(L, "FUNCTION %s", why.c_str());
    std::vector<command> cmds;
    cmds.push_back(std::move(c));
    return exchange(L, h, std::move(cmds), true);
}

/* r:pipeline({{cmd, ...}, ...}) - several commands in one round trip */
int resp_pipeline(lua_State* L) {
    auto* h = check_handle(L);
    require_outbound(L, "resp pipeline");
    refuse_locked(L, "resp pipeline");
    luaL_checktype(L, 2, LUA_TTABLE);
    int count = lua_objlen(L, 2);
    std::vector<command> cmds;
    cmds.reserve(count);
    for (int i = 1; i <= count; ++i) {
        lua_rawgeti(L, 2, i);
        if (!lua_istable(L, -1))
            luaL_error(L, "FUNCTION resp pipeline takes a list of commands, each a list of words");
        int words = lua_objlen(L, -1);
        command c;
        c.reserve(words);
        for (int w = 1; w <= words; ++w) {
            lua_rawgeti(L, -1, w);
            c.push_back(word_at(L, lua_gettop(L), "resp pipeline"));
            lua_pop(L, 1);
        }
        lua_pop(L, 1);
        auto why = barch::resp_client::refused(c);
        if (!why.empty())
            luaL_error(L, "FUNCTION %s", why.c_str());
        cmds.push_back(std::move(c));
    }
    if (cmds.empty()) {
        lua_newtable(L);
        return 1;
    }
    return exchange(L, h, std::move(cmds), false);
}

}

void luaopen_resp(lua_State* L) {
    luaL_newmetatable(L, handle_mt);
    lua_newtable(L);
    // with a continuation, so an error reply can be raised once the call resumes
    lua_pushcclosurek(L, resp_call, "call", 0, finish);
    lua_setfield(L, -2, "call");
    lua_pushcclosurek(L, resp_pipeline, "pipeline", 0, finish);
    lua_setfield(L, -2, "pipeline");
    lua_setfield(L, -2, "__index");
    lua_pushstring(L, "locked");
    lua_setfield(L, -2, "__metatable");
    lua_pop(L, 1);

    lua_newtable(L);
    lua_pushcfunction(L, resp_connect, "connect");
    lua_setfield(L, -2, "connect");
    lua_setglobal(L, "resp");
}
