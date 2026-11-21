#include "luau_internal.h"

#ifdef BARCH_HAS_LUAU

#include "queue_service.h"
#include "../fs.h"
#include "../fs_api.h"
#include "../graph.h"
#include "../ids.h"
#include "../uuid_text.h"
#include "auth_api.h"
#include "key_space.h"
#include "configuration.h"
#include "art/nodes.h"
#include "abstract_shard.h"
#include "sastam.h"
#include "sql.h"
#include "luacode.h"
#include "simdjson_luau.h"
#include "vec_luau.h"
#include "luau_protect.h"
#include "meta_keys.h"
#include <fast_float/fast_float.h>

#include <cerrno>
#include <climits>
#include <cmath>
#include <cstring>
#include <limits>

namespace barch {
namespace foreign {

namespace detail {

static int barch_call(lua_State* L);
static int run_script_command(lua_State* L, int first, const std::string& space,
                              const char* what);

int sql_query(lua_State* L) {
    refuse_from_log(L, "sql.query");               // TODO 673
    if (auto* rc = static_cast<run_ctx*>(lua_callbacks(L)->userdata); rc && rc->locked)
        luaL_error(L, "FUNCTION sql.query is not allowed inside a locked region");
    /*
     * A stored function carries its space on the coroutine, because one state serves
     * every space the session uses. The foreign fill path still has a state of its own
     * with the space in the registry, so it falls through to that - TODO 150.
     */
    std::string running = current_space(L);
    if (running.empty()) {
        lua_getfield(L, LUA_REGISTRYINDEX, "barch.foreign.space");
        const char* reg = lua_tostring(L, -1);
        if (reg) running = reg;
        lua_pop(L, 1);
    }
    const char* sp = running.empty() ? nullptr : running.c_str();
    auto ks = sp ? get_keyspace(sp) : nullptr;
    if (!ks || !ks->sql)
        luaL_error(L, "FOREIGN no sql driver");
    size_t qn = 0, kn = 0;
    const char* q = luaL_checklstring(L, 1, &qn);
    const char* k = luaL_checklstring(L, 2, &kn);
    // waiting on the database is not running: taken off the deadline the way a park
    // is, and bounded by the query's own timeout instead - TODO 434
    const int64_t asked = art::now();
    auto r = ks->sql->query({q, qn}, {k, kn}, ks->foreign_query_timeout_ms);
    // unless the slice is measured, which covers it already - TODO 445
    if (auto* rc = static_cast<run_ctx*>(lua_callbacks(L)->userdata))
        rc->credit_wait(art::now() - asked);
    if (r.status == result::status::error)
        luaL_error(L, "%s", r.payload.empty() ? "FOREIGN sql" : r.payload.c_str());
    if (r.status == result::status::missing) {
        lua_pushnil(L);
        return 1;
    }
    lua_createtable(L, 1, 0);
    lua_pushlstring(L, r.payload.data(), r.payload.size());
    lua_rawseti(L, -2, 1);
    return 1;
}

/*
 * A command's reply, as a Luau value. The inverse of to_variable below, and it has to
 * stay the inverse: a script that writes a value with barch.call and reads it back
 * should see what it wrote.
 */
void push_variable(lua_State* L, const Variable& v) {
    switch (v.index()) {
        case var_bool:
            lua_pushboolean(L, std::get<bool>(v));
            return;
        case var_int64:
            // pushed as a 64 bit integer, not a double: a script that reads a counter
            // back through barch.call should get every bit of it
            lua_pushinteger64(L, std::get<int64_t>(v));
            return;
        case var_uint64:
            lua_pushinteger64(L, (int64_t) std::get<uint64_t>(v));
            return;
        case var_double:
            lua_pushnumber(L, std::get<double>(v));
            return;
        case var_string: {
            /*
             * Through bulk_vt, not raw. A bulk string is held in `results` as `$`
             * followed by the value - the marker is how one is told from a simple
             * string, since both are var_string - and every other reader strips it:
             * `Variable::to_string` does, and so does the wire writer. This did not,
             * so `barch.call('GET', k)` handed a script `$value` while
             * `barch.store.get(k)` handed it `value`, and the difference only showed
             * up when something looked at the bytes: simdjson refusing a document
             * that began `${"asin":`. See TODO 261.
             */
            const auto& s = std::get<std::string>(v);
            auto body = v.bulk_vt(s);
            lua_pushlstring(L, body.chars(), body.size);
            return;
        }
        case var_verbatim: {
            const auto& vb = std::get<verbatim_t>(v);
            lua_pushlstring(L, vb.text.data(), vb.text.size());
            return;
        }
        case var_array:
        case var_map:
        case var_set: {
            const heap::vector<wrapped_variable_t>* items = nullptr;
            if (v.index() == var_array)
                items = &std::get<heap::vector<wrapped_variable_t>>(v);
            else if (v.index() == var_map)
                items = &std::get<map_t>(v).items;
            else
                items = &std::get<set_t>(v).items;
            lua_createtable(L, (int) items->size(), 0);
            int at = 1;
            for (const auto& item : *items) {
                const Variable& e = static_cast<const variable_t&>(item);
                push_variable(L, e);
                lua_rawseti(L, -2, at++);
            }
            return;
        }
        default:
            // null, and an error that was not turned into a Lua error before we got here
            lua_pushnil(L);
            return;
    }
}

/*
 * barch.store - reads of the space the function is running against.
 *
 * Every one of these takes what it needs and gives the lock back before a single line
 * of Luau runs again, which is the rule in TODO 98 F. Nothing here calls into the
 * script from inside a locked scope.
 */
/** whether the running function said `"write": false` - TODO 672 */
static bool running_read_only(lua_State* L) {
    auto* rc = static_cast<run_ctx*>(lua_callbacks(L)->userdata);
    return rc && rc->read_only;
}

/** a write through `s`: refused without the rights, or in a read-only function - TODO 672 */
void check_may_write(lua_State* L, const store_access* s) {
    if (running_read_only(L))
        luaL_error(L, "FUNCTION declared \"write\": false in its --@barch header, so it can't write");
    if (!s->may_write)
        luaL_error(L, "FUNCTION not authorized to write there");
}

const store_access* store_of(lua_State* L, const char* what, bool writing) {
    space_state* st = state_of(L);
    if (!st || !st->store())
        luaL_error(L, "FUNCTION barch.store.%s is not available here", what);
    // the script runs as whoever called it. Reading the store directly must not be a
    // way round the rights a client would have been refused at the command - the same
    // check barch.call makes, one layer down where there is no command to read it off
    if (writing)
        check_may_write(L, st->store());
    else if (!st->store()->may_read)
        luaL_error(L, "FUNCTION not authorized to read here");
    return st->store();
}

/*
 * `barch.tomb` - what a cached source miss reads as.
 *
 * A foreign space has three states for a key and a script could see only two: nil for
 * both "nobody has looked" and "looked, and the source had nothing". The second is a
 * fact worth acting on - it is why the source will not be asked again - so it gets a
 * value of its own rather than being folded into nil. See TODO 148.
 *
 * A table with an identity rather than a boolean, because `false` collapses under the
 * `if not v` that everyone writes, and the point is that a script has to say which
 * case it means.
 */
static void push_tomb(lua_State* L) {
    lua_getfield(L, LUA_REGISTRYINDEX, "barch.tomb");
}

static int tomb_tostring(lua_State* L) {
    lua_pushstring(L, "barch.tomb");
    return 1;
}

static void make_tomb(lua_State* L) {
    lua_newtable(L);
    lua_newtable(L);
    lua_pushcfunction(L, tomb_tostring, "__tostring");
    lua_setfield(L, -2, "__tostring");
    lua_pushstring(L, "tomb");
    lua_setfield(L, -2, "__type");
    lua_pushboolean(L, true);
    lua_setfield(L, -2, "__metatable");     // not rewritable from a script
    lua_setmetatable(L, -2);
    lua_setfield(L, LUA_REGISTRYINDEX, "barch.tomb");
}

/**
 * `fetch` - a read that is allowed to go and get it.
 *
 * A key space configured `foreign` fills a missing key from its source, and until
 * now only a client could make that happen: the miss path parks the connection that
 * asked, and inside a script there is no connection to park. So a script reading a
 * foreign space saw whatever was already cached and nothing else - see TODO 259.
 *
 * It is a separate call rather than something `get` does quietly, because the two
 * are not the same operation. This one can take the source's latency and the
 * space's timeout, and in an HTTP handler it holds the VM slot while it does. That
 * should be visible at the call, not a property of the key space that the reader
 * cannot see.
 *
 * Returns the value, or nil and the reason.
 */
static int push_store_fetch(lua_State* L, const store_access* s, art::value_type key) {
    refuse_from_log(L, "fetch");                    // asks the space's source - TODO 673
    if (!s->fetch) {
        lua_pushnil(L);
        lua_pushstring(L, "not a foreign key space");
        return 2;
    }
    std::string value;
    std::string err;
    auto state = s->fetch(std::string(key.chars(), key.size), value, err);
    if (state == store_access::read_state::present) {
        lua_pushlstring(L, value.data(), value.size());
        return 1;
    }
    lua_pushnil(L);
    if (state == store_access::read_state::tombed) {
        lua_pushstring(L, "the source has no such key");
        return 2;
    }
    if (!err.empty()) {
        lua_pushlstring(L, err.data(), err.size());
        return 2;
    }
    return 1;
}

/*
 * A function run from a Raft log uses only the space it runs in - TODO 673. Another
 * space is in another group's log, or in none, so it isn't where it is on every
 * member at this entry, and a write to it would land outside its own log.
 */
void refuse_other_space(lua_State* L, const std::string& name) {
    auto* rc = static_cast<run_ctx*>(lua_callbacks(L)->userdata);
    if (!rc || !rc->from_log)
        return;
    space_state* st = state_of(L);
    const std::string running = st && st->call && st->call->iface ? st->call->iface->running_in : std::string();
    std::string canonical = name;
    if (barch::keyspace_exists(name))
        canonical = barch::get_keyspace(name)->canonical();
    if (canonical != running)
        luaL_error(L, "FUNCTION a function run from the log uses only the space it runs in, not %s - "
                      "give it --@barch {\"replicate\": \"effects\"} to reach other spaces", name.c_str());
}

static int store_fetch(lua_State* L) {
    size_t n = 0;
    const char* k = luaL_checklstring(L, 1, &n);
    const auto* s = store_of(L, "fetch");
    return push_store_fetch(L, s, {k, n});
}

static int push_store_get(lua_State* L, const store_access* s, const std::string& key, bool for_update);

// barch.store.get(k [, for_update]) - a true second argument is GETFORUPDATE (TODO 732)
static int store_get(lua_State* L) {
    size_t n = 0;
    const char* k = luaL_checklstring(L, 1, &n);
    return push_store_get(L, store_of(L, "get"), {k, n}, lua_toboolean(L, 2));
}

/*
 * barch.store.set(k, v) and barch.store.remove(k).
 *
 * The reads landed first and the write half was reached only through the space value,
 * `sp[k] = v`, which meant the two halves of the same interface disagreed about what
 * a script could do. They are the same calls underneath - `store_of` has taken a
 * `writing` flag since the rights went in, and this is what it was for. See 98 I.6.
 *
 * A nil value removes, so `set(k, nil)` and `remove(k)` are the same thing, which is
 * how `sp[k] = nil` already behaves. Keeping the two forms consistent matters more
 * than making one of them an error.
 */
static int store_set(lua_State* L) {
    size_t n = 0;
    const char* k = luaL_checklstring(L, 1, &n);
    const auto* s = store_of(L, "set", true);
    if (lua_isnoneornil(L, 2)) {
        s->remove({k, n});
        return 0;
    }
    size_t vn = 0;
    const char* v = nullptr;
    if (void* b = lua_tobuffer(L, 2, &vn))
        v = static_cast<const char*>(b);
    else
        v = lua_tolstring(L, 2, &vn);
    if (!v)
        luaL_error(L, "FUNCTION a key space holds strings, numbers, and buffers");
    std::string err;
    if (!s->set({k, n}, {v, vn}, err))
        luaL_error(L, "%s", err.empty() ? "FUNCTION write refused" : err.c_str());
    // a script write does not republish: there is no RELOAD to carry on one, and a
    // write that goes live the moment it lands is the thing TODO 245 took out. A
    // module deployed this way is picked up by connections that have not compiled it
    // and by everything else on the next RELOAD.
    return 0;
}

/*
 * getJson(k) and setJson(k, t) - on barch.store and on every space handle. A
 * shortcut for simdjson.parse(get(k)) and set(k, simdjson.encode(t)). See TODO 408.
 *
 * getJson answers nil for a missing key, and nil plus the reason for bytes that
 * aren't JSON: a key holding something else is an answer, not an error, and the
 * reason is there for a caller that wants to tell it apart from "missing". A
 * JSON null comes back as simdjson.null and a foreign tomb as barch.tomb,
 * as get and parse would give them. Rights still raise, since a refused read
 * that came back nil would pass for an empty key.
 *
 * setJson raises when the value won't encode or the write is refused - a write
 * that fails quietly is the one nobody finds - and nil removes, the way set does.
 */
static int push_store_get_json(lua_State* L, const store_access* s, const std::string& key) {
    if (!s->may_read)
        luaL_error(L, "FUNCTION not authorized to read there");
    std::string value;
    switch (s->get(key, value)) {
        case store_access::read_state::present: {
            std::string why;
            if (simdjson_push_parsed(L, value.data(), value.size(), why))
                return 1;
            lua_pushnil(L);
            lua_pushlstring(L, why.data(), why.size());
            return 2;
        }
        case store_access::read_state::tombed:
            push_tomb(L);
            return 1;
        default:
            lua_pushnil(L);
            return 1;
    }
}

static int store_set_json_at(lua_State* L, const store_access* s, const std::string& key, int at) {
    check_may_write(L, s);
    if (lua_isnoneornil(L, at)) {
        s->remove(key);
        return 0;
    }
    std::string doc;
    simdjson_encode_at(L, at, doc);
    std::string err;
    if (!s->set(key, doc, err))
        luaL_error(L, "%s", err.empty() ? "FUNCTION write refused" : err.c_str());
    return 0;
}

/*
 * parseJson(k, name1, name2, ...) - the named top-level fields of the JSON object
 * at k, one value per name and nil where a name isn't there. Only those fields
 * become Lua values. See TODO 409.
 *
 * Positions always line up: a missing key is a nil per name, and bytes that
 * aren't a JSON object are a nil per name followed by the reason - so a caller
 * who unpacks by position never shifts, and one extra value means something went
 * wrong. A foreign tomb is that extra value, where getJson would answer it alone.
 */
static int push_store_parse_json(lua_State* L, const store_access* s, const std::string& key,
                                 int first) {
    if (!s->may_read)
        luaL_error(L, "FUNCTION not authorized to read there");
    const int count = lua_gettop(L) - first + 1;
    if (count < 1)
        luaL_error(L, "FUNCTION parseJson takes a key and at least one name");
    for (int i = 0; i < count; ++i) {
        if (lua_type(L, first + i) != LUA_TSTRING)
            luaL_error(L, "FUNCTION parseJson names are strings");
    }
    luaL_checkstack(L, count + 8, "FUNCTION too many names for parseJson");
    auto nils = [&]() {
        for (int i = 0; i < count; ++i)
            lua_pushnil(L);
    };
    std::string value;
    switch (s->get(key, value)) {
        case store_access::read_state::present: {
            std::string why;
            if (simdjson_push_fields(L, value.data(), value.size(), first, count, why))
                return count;
            nils();
            lua_pushlstring(L, why.data(), why.size());
            return count + 1;
        }
        case store_access::read_state::tombed:
            nils();
            push_tomb(L);
            return count + 1;
        default:
            nils();
            return count;
    }
}

static int store_parse_json(lua_State* L) {
    size_t n = 0;
    const char* k = luaL_checklstring(L, 1, &n);
    return push_store_parse_json(L, store_of(L, "parseJson"), {k, n}, 2);
}

static int store_get_json(lua_State* L) {
    size_t n = 0;
    const char* k = luaL_checklstring(L, 1, &n);
    return push_store_get_json(L, store_of(L, "getJson"), {k, n});
}

static int store_set_json(lua_State* L) {
    size_t n = 0;
    const char* k = luaL_checklstring(L, 1, &n);
    return store_set_json_at(L, store_of(L, "setJson", true), {k, n}, 2);
}

static int store_remove(lua_State* L) {
    size_t n = 0;
    const char* k = luaL_checklstring(L, 1, &n);
    lua_pushboolean(L, store_of(L, "remove", true)->remove({k, n}));
    return 1;
}

/*
 * A script callback run from inside a C++ walk (save, load, pages, locked) - TODO 734.
 * Luau counts a Lua function called from a C function that has a continuation as
 * yieldable, and since TODO 731 the space handle's methods and several barch.store
 * calls have one. Called straight from there, the slice interrupt could yield inside
 * the callback, and a Luau yield unwinds by returning: lua_pcall would come back
 * "fine" with the stack still on the callback's frame, and the walk would go on over
 * it. Calling it through this plain C function, which has no continuation, keeps it
 * from yielding, as it was before.
 */
static int unyielding_call(lua_State* L) {
    lua_call(L, lua_gettop(L) - 1, LUA_MULTRET);
    return lua_gettop(L);
}

// lua_pcall for the function and `nargs` arguments on top, that never yields
static int callback_pcall(lua_State* L, int nargs, int nresults) {
    lua_pushcfunction(L, unyielding_call, "callback");
    lua_insert(L, -(nargs + 2));
    return lua_pcall(L, nargs + 1, nresults, 0);
}

/*
 * barch.store.locked(key, fn) - run fn with the shard that owns `key` write locked,
 * or with the whole space locked when key is nil. See TODO 98 F6.
 *
 * Scoped rather than a lock and an unlock a script pairs up itself. A pair leaks the
 * shard lock the first time a script errors between the two halves, and errors are
 * exactly what a region with a hard instruction cap produces - so the guard lives on
 * the C++ side of the boundary and the lock goes back whether fn returns, raises, or
 * is cut off.
 *
 * What fn returns is what this returns, so a region can hand its result out.
 */
static int store_locked(lua_State* L) {
    std::string key;
    int fn_at = 1;
    if (lua_isstring(L, 1) && !lua_isnoneornil(L, 2)) {
        size_t n = 0;
        const char* k = luaL_checklstring(L, 1, &n);
        key.assign(k, n);
        fn_at = 2;
    }
    luaL_checktype(L, fn_at, LUA_TFUNCTION);
    const auto* s = store_of(L, "locked", true);
    if (!s->locked)
        luaL_error(L, "FUNCTION barch.store.locked is not available here");

    auto* rc = static_cast<run_ctx*>(lua_callbacks(L)->userdata);
    int base = lua_gettop(L);
    int raised = 0;
    std::string err;

    // puts rc->locked back on every way out of the region. callback_pcall pushes
    // before its pcall, and an allocation failure there throws past a plain reset,
    // which left the call unable to slice or park after the shard was given back -
    // TODO 877
    struct locked_flag {
        run_ctx* rc;
        bool was;
        explicit locked_flag(run_ctx* rc) : rc(rc), was(rc && rc->locked) {
            if (rc)
                rc->locked = true;
        }
        ~locked_flag() {
            if (rc)
                rc->locked = was;
        }
        locked_flag(const locked_flag&) = delete;
        locked_flag& operator=(const locked_flag&) = delete;
    };

    bool ok = s->locked(key, [&]() -> bool {
        locked_flag flag(rc);
        if (rc) {
            // cannot yield while the shard is held, so the usual slice is a hard
            // stop here. function_slice_insns is the count; function_deadline_ms
            // is already on ctx.deadline and still applies
            rc->locked_left = rc->slice ? rc->slice : get_function_slice_insns();
        }
        // pcall rather than a straight call: an error has to come back through here so
        // the lock is given up on this side before it is re-raised to the script
        lua_pushvalue(L, fn_at);
        raised = callback_pcall(L, 0, 1);
        return raised == LUA_OK;
    }, err);

    if (!ok && raised == 0) {
        // the region never ran - no rights, no shard, or already inside one
        luaL_error(L, "%s", err.empty() ? "FUNCTION locked region refused" : err.c_str());
    }
    if (raised != LUA_OK) {
        // re-raise what the body raised, now that the lock is back
        lua_error(L);
    }
    return lua_gettop(L) > base ? 1 : 0;
}

/*
 * barch.store.shardNumber(k) and barch.store.hasLock(k).
 *
 * A locked region may hold one shard or the whole space and never two, so a script
 * that wants two keys together needs to be able to ask whether they are on one shard
 * rather than finding out from the abort. And a helper called from both inside and
 * outside a region can ask whether the lock is already there instead of being passed
 * a flag. See TODO 98 F6.
 */
static int store_shard_number(lua_State* L) {
    size_t n = 0;
    const char* k = luaL_checklstring(L, 1, &n);
    const auto* s = store_of(L, "shardNumber");
    if (!s->shard_number)
        luaL_error(L, "FUNCTION barch.store.shardNumber is not available here");
    lua_pushinteger(L, (int) s->shard_number(std::string(k, n)));
    return 1;
}

static int store_has_lock(lua_State* L) {
    size_t n = 0;
    const char* k = luaL_checklstring(L, 1, &n);
    const auto* s = store_of(L, "hasLock");
    lua_pushboolean(L, s->has_lock && s->has_lock(std::string(k, n)));
    return 1;
}

/*
 * barch.ws.send(id, data[, binary]) and barch.ws.close(id[, code[, reason]]) - TODO 909.
 * A push to one of the space's open websockets from anywhere a function runs: an HTTP
 * handler, a queue consumer, a cron job. True is done, false is no such socket open
 * here. Not a store read or write, so it asks for neither right.
 */
static std::string ws_id_arg(lua_State* L) {
    if (lua_type(L, 1) == LUA_TNUMBER) {
        const double d = lua_tonumber(L, 1);
        if (!(d >= 0 && d < 9007199254740992.0) || d != (double) (uint64_t) d)
            luaL_error(L, "FUNCTION barch.ws: not a websocket id: %f", d);
        return std::to_string((uint64_t) d);
    }
    size_t n = 0;
    const char* id = luaL_checklstring(L, 1, &n);
    return std::string(id, n);
}

static const store_access* ws_store(lua_State* L, const char* what) {
    space_state* st = state_of(L);
    const store_access* s = st ? st->store() : nullptr;
    if (!s || !s->ws_send || !s->ws_close)
        luaL_error(L, "FUNCTION barch.ws.%s is not available here", what);
    return s;
}

static int ws_send_frame(lua_State* L) {
    const std::string id = ws_id_arg(L);
    size_t n = 0;
    const char* data = luaL_checklstring(L, 2, &n);
    const bool binary = lua_toboolean(L, 3);
    const auto* s = ws_store(L, "send");
    std::string err;
    const int done = s->ws_send(id, std::string(data, n), binary, err);
    if (done < 0)
        luaL_error(L, "FUNCTION barch.ws.send: %s", err.c_str());
    lua_pushboolean(L, done > 0);
    return 1;
}

static int ws_close_socket(lua_State* L) {
    const std::string id = ws_id_arg(L);
    const int code = lua_isnoneornil(L, 2) ? 1000 : (int) luaL_checkinteger(L, 2);
    size_t n = 0;
    const char* reason = lua_isnoneornil(L, 3) ? "" : luaL_checklstring(L, 3, &n);
    const auto* s = ws_store(L, "close");
    std::string err;
    const int done = s->ws_close(id, code, std::string(reason, n), err);
    if (done < 0)
        luaL_error(L, "FUNCTION barch.ws.close: %s", err.c_str());
    lua_pushboolean(L, done > 0);
    return 1;
}

static size_t check_buf_offset(lua_State* L, int idx) {
    if (lua_isnoneornil(L, idx))
        return 0;
    lua_Integer n = luaL_checkinteger(L, idx);
    if (n < 0)
        luaL_error(L, "FUNCTION offset is out of range");
    return (size_t) n;
}

static void write_le32(uint8_t* p, int32_t v) {
    uint32_t u = (uint32_t) v;
    p[0] = (uint8_t) u;
    p[1] = (uint8_t) (u >> 8);
    p[2] = (uint8_t) (u >> 16);
    p[3] = (uint8_t) (u >> 24);
}

static int32_t read_le32(const uint8_t* p) {
    uint32_t u = (uint32_t) p[0] | ((uint32_t) p[1] << 8) |
                 ((uint32_t) p[2] << 16) | ((uint32_t) p[3] << 24);
    return (int32_t) u;
}

static void write_le64(uint8_t* p, int64_t v) {
    uint64_t u = (uint64_t) v;
    for (int i = 0; i < 8; ++i)
        p[i] = (uint8_t) (u >> (8 * i));
}

static int64_t read_le64(const uint8_t* p) {
    uint64_t u = 0;
    for (int i = 0; i < 8; ++i)
        u |= (uint64_t) p[i] << (8 * i);
    return (int64_t) u;
}

/** CALLF arguments are strings, so an integer helper has to accept those too */
static int64_t check_int64_arg(lua_State* L, int idx) {
    int isnum = 0;
    int64_t v = lua_tointeger64(L, idx, &isnum);
    if (isnum)
        return v;
    size_t len = 0;
    const char* s = lua_tolstring(L, idx, &len);
    if (!s || len == 0)
        luaL_error(L, "FUNCTION expected an integer");
    errno = 0;
    const char* end = s+len;
    int64_t parsed;
    auto ianswer = fast_float::from_chars(s, end, parsed); // check if it's an integer first

    if (ianswer.ec == std::errc() && ianswer.ptr == end)
        return parsed;

    luaL_error(L, "FUNCTION expected an integer");
    return parsed;
}

static int push_copied_buffer(lua_State* L, const store_access* s, const std::string& key,
                              size_t offset) {
    if (!s->getBufferAt)
        luaL_error(L, "FUNCTION getBufferAt is not available here");
    std::string copy;
    switch (s->getBufferAt(key, offset, [&](const void* p, size_t n) {
        copy.assign(static_cast<const char*>(p), n);
    })) {
        case store_access::read_state::present: {
            void* b = lua_newbuffer(L, copy.size());
            if (!copy.empty())
                memcpy(b, copy.data(), copy.size());
            break;
        }
        case store_access::read_state::tombed:
            push_tomb(L);
            break;
        default:
            lua_pushnil(L);
    }
    return 1;
}

/*
 * getBuffers(keys [, offset]) - TODO 910. One slot per key, so the array keeps #keys:
 * a buffer, barch.tomb, or false where getBufferAt answers nil. The bytes are copied
 * out under the read lock and the buffers made after it, as push_copied_buffer does:
 * making a Lua buffer can run the collector, and a finalizer must not run under a
 * shard's lock. A store without getBuffers is read a key at a time.
 */
static int push_copied_buffers(lua_State* L, const store_access* s, int keys_idx, size_t offset) {
    if (!s->getBuffers && !s->getBufferAt)
        luaL_error(L, "FUNCTION getBuffers is not available here");
    luaL_checktype(L, keys_idx, LUA_TTABLE);
    const int n = lua_objlen(L, keys_idx);
    std::vector<std::string> keys;
    keys.reserve((size_t) n);
    for (int i = 1; i <= n; ++i) {
        lua_rawgeti(L, keys_idx, i);
        size_t kn = 0;
        const char* k = lua_type(L, -1) == LUA_TSTRING || lua_type(L, -1) == LUA_TNUMBER
            ? lua_tolstring(L, -1, &kn) : nullptr;
        if (!k)
            luaL_error(L, "FUNCTION getBuffers wants an array of keys; key %d isn't a string", i);
        keys.emplace_back(k, kn);
        lua_pop(L, 1);
    }
    struct slot {
        store_access::read_state state{store_access::read_state::absent};
        std::string bytes;
    };
    std::vector<slot> slots((size_t) n);
    auto take = [&](size_t i, store_access::read_state st, const void* p, size_t len) {
        slots[i].state = st;
        if (st == store_access::read_state::present)
            slots[i].bytes.assign(static_cast<const char*>(p), len);
    };
    if (s->getBuffers) {
        s->getBuffers(keys, offset, take);
    } else {
        for (size_t i = 0; i < keys.size(); ++i) {
            bool handed = false;
            auto st = s->getBufferAt(keys[i], offset, [&](const void* p, size_t len) {
                handed = true;
                take(i, store_access::read_state::present, p, len);
            });
            if (!handed)
                take(i, st, nullptr, 0);
        }
    }
    lua_createtable(L, n, 0);
    for (int i = 0; i < n; ++i) {
        slot& sl = slots[(size_t) i];
        if (sl.state == store_access::read_state::present) {
            void* b = lua_newbuffer(L, sl.bytes.size());
            if (!sl.bytes.empty())
                memcpy(b, sl.bytes.data(), sl.bytes.size());
            std::string().swap(sl.bytes);   // let go as it goes, not all at the end
        } else if (sl.state == store_access::read_state::tombed) {
            push_tomb(L);
        } else {
            lua_pushboolean(L, false);
        }
        lua_rawseti(L, -2, i + 1);
    }
    return 1;
}

static int do_set_buffer_at(lua_State* L, const store_access* s, int key_idx) {
    check_may_write(L, s);
    if (!s->setBufferAt)
        luaL_error(L, "FUNCTION setBufferAt is not available here");
    size_t n = 0;
    const char* k = luaL_checklstring(L, key_idx, &n);
    size_t vn = 0;
    void* b = lua_tobuffer(L, key_idx + 1, &vn);
    if (!b && lua_type(L, key_idx + 1) != LUA_TBUFFER)
        luaL_error(L, "FUNCTION setBufferAt wants a buffer");
    size_t offset = check_buf_offset(L, key_idx + 2);
    std::string err;
    if (!s->setBufferAt({k, n}, offset, b, vn, err))
        luaL_error(L, "%s", err.empty() ? "FUNCTION write refused" : err.c_str());
    return 0;
}

static int do_get_int_at(lua_State* L, const store_access* s, int key_idx, int width) {
    if (!s->may_read)
        luaL_error(L, "FUNCTION not authorized to read there");
    if (!s->getBufferAt)
        luaL_error(L, "FUNCTION getBufferAt is not available here");
    size_t n = 0;
    const char* k = luaL_checklstring(L, key_idx, &n);
    size_t offset = check_buf_offset(L, key_idx + 1);
    uint8_t tmp[8]{};
    size_t got = 0;
    auto st = s->getBufferAt({k, n}, offset, [&](const void* p, size_t len) {
        got = len;
        if (len >= (size_t) width)
            memcpy(tmp, p, (size_t) width);
    });
    if (st != store_access::read_state::present || got < (size_t) width) {
        lua_pushnil(L);
        return 1;
    }
    if (width == 4)
        lua_pushinteger(L, read_le32(tmp));
    else
        lua_pushinteger64(L, read_le64(tmp));
    return 1;
}

static int do_set_int_at(lua_State* L, const store_access* s, int key_idx, int width) {
    check_may_write(L, s);
    if (!s->setBufferAt)
        luaL_error(L, "FUNCTION setBufferAt is not available here");
    size_t n = 0;
    const char* k = luaL_checklstring(L, key_idx, &n);
    int64_t v = check_int64_arg(L, key_idx + 1);
    if (width == 4 && (v < INT32_MIN || v > INT32_MAX))
        luaL_error(L, "FUNCTION int32 out of range");
    size_t offset = check_buf_offset(L, key_idx + 2);
    uint8_t tmp[8]{};
    if (width == 4)
        write_le32(tmp, (int32_t) v);
    else
        write_le64(tmp, v);
    std::string err;
    if (!s->setBufferAt({k, n}, offset, tmp, (size_t) width, err))
        luaL_error(L, "%s", err.empty() ? "FUNCTION write refused" : err.c_str());
    return 0;
}

/**
 * incrInt32ByAt(k, by [, init [, offset]]) and the int64 one - TODO 661.
 *
 * Adds `by` to the integer at `offset` and answers the sum. A missing key, or one
 * that ends before the number would, is set to `init` (0 if not given) and `by`
 * isn't added - memcached's rule - so the first incrInt64ByAt(k, 1, 1) answers 1.
 * The key is created or grown the way setInt64At would. The read and write share one write lock, so concurrent
 * callers never lose an increment. Overflow raises and leaves the value alone.
 */
static int do_incr_int_at(lua_State* L, const store_access* s, int key_idx, int width) {
    check_may_write(L, s);
    if (!s->incrIntAt)
        luaL_error(L, "FUNCTION incrInt%dByAt is not available here", width * 8);
    size_t n = 0;
    const char* k = luaL_checklstring(L, key_idx, &n);
    int64_t by = check_int64_arg(L, key_idx + 1);
    int64_t init = lua_isnoneornil(L, key_idx + 2) ? 0 : check_int64_arg(L, key_idx + 2);
    if (width == 4 && (by < INT32_MIN || by > INT32_MAX || init < INT32_MIN || init > INT32_MAX))
        luaL_error(L, "FUNCTION int32 out of range");
    size_t offset = check_buf_offset(L, key_idx + 3);
    int64_t out = 0;
    std::string err;
    if (!s->incrIntAt({k, n}, offset, width, by, init, out, err))
        luaL_error(L, "%s", err.empty() ? "FUNCTION write refused" : err.c_str());
    if (width == 4)
        lua_pushinteger(L, (int) out);
    else
        lua_pushinteger64(L, out);
    return 1;
}

/**
 * A float read in place, little-endian like the int helpers. nil when the key isn't
 * there or doesn't reach `width` bytes past the offset, the same as getInt32At.
 */
static int do_get_float_at(lua_State* L, const store_access* s, int key_idx, int width) {
    if (!s->may_read)
        luaL_error(L, "FUNCTION not authorized to read there");
    if (!s->getBufferAt)
        luaL_error(L, "FUNCTION getBufferAt is not available here");
    size_t n = 0;
    const char* k = luaL_checklstring(L, key_idx, &n);
    size_t offset = check_buf_offset(L, key_idx + 1);
    uint8_t tmp[8]{};
    size_t got = 0;
    auto st = s->getBufferAt({k, n}, offset, [&](const void* p, size_t len) {
        got = len;
        if (len >= (size_t) width)
            memcpy(tmp, p, (size_t) width);
    });
    if (st != store_access::read_state::present || got < (size_t) width) {
        lua_pushnil(L);
        return 1;
    }
    if (width == 4) {
        uint32_t u = (uint32_t) read_le32(tmp);
        float f = 0;
        memcpy(&f, &u, 4);
        lua_pushnumber(L, f);
    } else {
        uint64_t u = (uint64_t) read_le64(tmp);
        double d = 0;
        memcpy(&d, &u, 8);
        lua_pushnumber(L, d);
    }
    return 1;
}

/**
 * A float written in place, little-endian like the int helpers, so buffer.readf64 and
 * buffer.readf32 read it back too. A finite value too big for a float32 is refused
 * rather than stored as inf.
 */
static int do_set_float_at(lua_State* L, const store_access* s, int key_idx, int width) {
    check_may_write(L, s);
    if (!s->setBufferAt)
        luaL_error(L, "FUNCTION setBufferAt is not available here");
    size_t n = 0;
    const char* k = luaL_checklstring(L, key_idx, &n);
    double v = luaL_checknumber(L, key_idx + 1);
    size_t offset = check_buf_offset(L, key_idx + 2);
    uint8_t tmp[8]{};
    if (width == 4) {
        float f = (float) v;
        if (std::isfinite(v) && !std::isfinite(f))
            luaL_error(L, "FUNCTION float32 out of range");
        uint32_t u = 0;
        memcpy(&u, &f, 4);
        write_le32(tmp, (int32_t) u);
    } else {
        uint64_t u = 0;
        memcpy(&u, &v, 8);
        write_le64(tmp, (int64_t) u);
    }
    std::string err;
    if (!s->setBufferAt({k, n}, offset, tmp, (size_t) width, err))
        luaL_error(L, "%s", err.empty() ? "FUNCTION write refused" : err.c_str());
    return 0;
}

static int store_get_buffer_at(lua_State* L) {
    size_t n = 0;
    const char* k = luaL_checklstring(L, 1, &n);
    size_t offset = check_buf_offset(L, 2);
    return push_copied_buffer(L, store_of(L, "getBufferAt"), {k, n}, offset);
}

static int store_get_buffers(lua_State* L) {
    size_t offset = check_buf_offset(L, 2);
    return push_copied_buffers(L, store_of(L, "getBuffers"), 1, offset);
}

static int store_set_buffer_at(lua_State* L) {
    return do_set_buffer_at(L, store_of(L, "setBufferAt", true), 1);
}

static int store_get_int32_at(lua_State* L) {
    return do_get_int_at(L, store_of(L, "getInt32At"), 1, 4);
}

static int store_set_int32_at(lua_State* L) {
    return do_set_int_at(L, store_of(L, "setInt32At", true), 1, 4);
}

static int store_get_int64_at(lua_State* L) {
    return do_get_int_at(L, store_of(L, "getInt64At"), 1, 8);
}

static int store_set_int64_at(lua_State* L) {
    return do_set_int_at(L, store_of(L, "setInt64At", true), 1, 8);
}

static int store_incr_int32_by_at(lua_State* L) {
    return do_incr_int_at(L, store_of(L, "incrInt32ByAt", true), 1, 4);
}

static int store_incr_int64_by_at(lua_State* L) {
    return do_incr_int_at(L, store_of(L, "incrInt64ByAt", true), 1, 8);
}

static int store_get_f64_at(lua_State* L) {
    return do_get_float_at(L, store_of(L, "getF64At"), 1, 8);
}

static int store_get_f32_at(lua_State* L) {
    return do_get_float_at(L, store_of(L, "getF32At"), 1, 4);
}

static int store_set_f64_at(lua_State* L) {
    return do_set_float_at(L, store_of(L, "setF64At", true), 1, 8);
}

static int store_set_f32_at(lua_State* L) {
    return do_set_float_at(L, store_of(L, "setF32At", true), 1, 4);
}

static int store_size(lua_State* L) {
    auto* s = store_of(L, "size");
    lua_pushnumber(L, s->size ? (double) s->size() : 0);
    return 1;
}

/*
 * barch.store.leaves(f) and barch.store.nodes(f) - every page of the space's leaf or
 * node arena, for backups. TODO 416.
 *
 * `f(page, buf, shard)` once per page: the page number, a buffer with the page's
 * bytes up to its write position, and the shard it is in - page numbers start again
 * in every shard. Returning false stops the walk. The answer is how many pages `f`
 * was handed.
 *
 * Inside a transaction the pages are the ones as they stood at BEGIN, so the way to
 * take a consistent copy while the store carries on is BEGIN, leaves, nodes, COMMIT.
 * A COMMIT or ROLLBACK part way through raises rather than carrying on over a
 * mixture of the two states.
 *
 * `f` runs with no shard lock held, like every other callback here. It is called
 * from C, so it cannot yield, and a long walk is bounded by the function deadline
 * rather than the slice.
 */
static int do_store_pages(lua_State* L, const store_access* s, int fn_at, bool nodes,
                          const char* what) {
    refuse_from_log(L, what);                       // this node's pages - TODO 673
    luaL_checktype(L, fn_at, LUA_TFUNCTION);
    if (!s->may_read)
        luaL_error(L, "FUNCTION not authorized to read there");
    if (!s->pages)
        luaL_error(L, "FUNCTION %s is not available here", what);
    int raised = LUA_OK;
    size_t handed = 0;
    std::string err;
    bool ok = s->pages(nodes, [&](size_t shard, size_t page, const void* data, size_t len) -> bool {
        lua_pushvalue(L, fn_at);
        lua_pushnumber(L, (double) page);
        void* b = lua_newbuffer(L, len);
        if (len)
            memcpy(b, data, len);
        lua_pushnumber(L, (double) shard);
        // pcall, so an error comes back through the walk and out of it before it
        // is raised again
        raised = callback_pcall(L, 3, 1);
        if (raised != LUA_OK)
            return false;           // the error stays on the stack for lua_error
        ++handed;
        bool go_on = !(lua_isboolean(L, -1) && !lua_toboolean(L, -1));
        lua_pop(L, 1);
        return go_on;
    }, err);
    if (raised != LUA_OK)
        lua_error(L);
    if (!ok)
        luaL_error(L, "%s", err.c_str());
    lua_pushnumber(L, (double) handed);
    return 1;
}

static void push_bytes_buffer(lua_State* L, const std::string& bytes) {
    void* b = lua_newbuffer(L, bytes.size());
    if (!bytes.empty())
        memcpy(b, bytes.data(), bytes.size());
}

/*
 * barch.store.freeList(f) and barch.store.stats(f) - the rest of each shard's file,
 * for a backup that has the pages from leaves and nodes. TODO 416.
 *
 * freeList calls `f(leaves, nodes, shard)` with each arena's allocator state - its
 * free list, counters and page table - and stats calls `f(stats, shard)` with the
 * shard's counters, root and size. Buffers, once per shard, false stops. Inside a
 * transaction both are as they were at BEGIN, matching the pages.
 */
static int do_store_shard_state(lua_State* L, const store_access* s, int fn_at, bool free_lists,
                                const char* what) {
    refuse_from_log(L, what);                       // this node's storage - TODO 673
    luaL_checktype(L, fn_at, LUA_TFUNCTION);
    if (!s->may_read)
        luaL_error(L, "FUNCTION not authorized to read there");
    if (!s->shard_state)
        luaL_error(L, "FUNCTION %s is not available here", what);
    int raised = LUA_OK;
    size_t handed = 0;
    std::string err;
    bool ok = s->shard_state(free_lists, !free_lists,
        [&](size_t shard, const std::string& leaves, const std::string& nodes,
            const std::string& stats) -> bool {
        lua_pushvalue(L, fn_at);
        int args = 0;
        if (free_lists) {
            push_bytes_buffer(L, leaves);
            push_bytes_buffer(L, nodes);
            args = 2;
        } else {
            push_bytes_buffer(L, stats);
            args = 1;
        }
        lua_pushnumber(L, (double) shard);
        raised = callback_pcall(L, args + 1, 1);
        if (raised != LUA_OK)
            return false;
        ++handed;
        bool go_on = !(lua_isboolean(L, -1) && !lua_toboolean(L, -1));
        lua_pop(L, 1);
        return go_on;
    }, err);
    if (raised != LUA_OK)
        lua_error(L);
    if (!ok)
        luaL_error(L, "%s", err.c_str());
    lua_pushnumber(L, (double) handed);
    return 1;
}

/*
 * barch.store.save(f) and barch.store.load(f) - the whole space as a stream of
 * blocks, for backups. TODO 418.
 *
 * save calls `f(buf, block, shard)` for every block of every shard, in shard order,
 * block numbers from 0 in each shard, each a buffer of up to 64K. What it saves is
 * the space as it stood at BEGIN, and the transaction is the caller's to open and end
 * so metadata can be taken from the same moment after it - TODO 424. Refused with no
 * transaction open. The answer is how many blocks f got.
 *
 * load calls `f(block, shard)` for block 0, 1, ... of shard 0, then shard 1 and on,
 * and wants each block back as a buffer or a string, or nil when that shard has no
 * more. A shard is collected whole and replaced only if the blocks are a complete
 * stream for that shard of a space with this many shards. Refused inside a
 * transaction. The answer is how many blocks it read.
 *
 * Like leaves and nodes, `f` runs with no lock held and can't yield.
 */
static int do_store_save(lua_State* L, const store_access* s, int fn_at, const char* what) {
    refuse_from_log(L, what);                       // a save freezes the space it runs in - TODO 673
    luaL_checktype(L, fn_at, LUA_TFUNCTION);
    if (!s->may_read)
        luaL_error(L, "FUNCTION not authorized to read there");
    if (!s->save_stream)
        luaL_error(L, "FUNCTION %s is not available here", what);
    int raised = LUA_OK;
    size_t handed = 0;
    std::string err;
    bool ok = s->save_stream([&](const char* data, size_t len, uint64_t block, size_t shard) -> bool {
        lua_pushvalue(L, fn_at);
        void* b = lua_newbuffer(L, len);
        if (len)
            memcpy(b, data, len);
        lua_pushnumber(L, (double) block);
        lua_pushnumber(L, (double) shard);
        raised = callback_pcall(L, 3, 0);
        if (raised != LUA_OK)
            return false;           // the error stays on the stack for lua_error
        ++handed;
        return true;
    }, err);
    if (raised != LUA_OK)
        lua_error(L);
    if (!ok)
        luaL_error(L, "%s", err.c_str());
    lua_pushnumber(L, (double) handed);
    return 1;
}

static int do_store_load(lua_State* L, const store_access* s, int fn_at, const char* what) {
    refuse_from_log(L, what);                       // TODO 673
    luaL_checktype(L, fn_at, LUA_TFUNCTION);
    check_may_write(L, s);
    if (!s->load_stream)
        luaL_error(L, "FUNCTION %s is not available here", what);
    int raised = LUA_OK;
    bool bad_value = false;
    size_t read = 0;
    std::string err;
    bool ok = false;
    // a raised error or a bad value stops the load by throwing this out of the
    // walk, which holds no latch while it collects blocks; caught here, then
    // raised. Its own type, so a store error is not mistaken for it - TODO 877
    struct load_stop {};
    try {
    ok = s->load_stream([&](uint64_t block, size_t shard, std::string& out) -> bool {
        lua_pushvalue(L, fn_at);
        lua_pushnumber(L, (double) block);
        lua_pushnumber(L, (double) shard);
        raised = callback_pcall(L, 2, 1);
        if (raised != LUA_OK)
            throw load_stop{};                  // unwound below; the error is on the stack
        bool more = true;
        if (lua_isnil(L, -1)) {
            more = false;
        } else if (lua_type(L, -1) == LUA_TBUFFER) {
            size_t n = 0;
            void* b = lua_tobuffer(L, -1, &n);
            out.assign(static_cast<const char*>(b), n);
        } else if (lua_type(L, -1) == LUA_TSTRING) {
            size_t n = 0;
            const char* p = lua_tolstring(L, -1, &n);
            out.assign(p, n);
        } else {
            bad_value = true;
            lua_pop(L, 1);
            throw load_stop{};
        }
        lua_pop(L, 1);
        if (more)
            ++read;
        return more;
    }, err);
    } catch (const load_stop&) {
        ok = false;
    } catch (const std::runtime_error& e) {
        // the store's own. Luau's errors are not runtime_errors, so they pass
        ok = false;
        err = e.what();
    }
    if (raised != LUA_OK)
        lua_error(L);
    if (bad_value)
        luaL_error(L, "FUNCTION %s wants a buffer, a string or nil back", what);
    if (!ok)
        luaL_error(L, "%s", err.empty() ? "FUNCTION load failed" : err.c_str());
    lua_pushnumber(L, (double) read);
    return 1;
}

static int store_save(lua_State* L) {
    return do_store_save(L, store_of(L, "save"), 1, "barch.store.save");
}

static int store_load(lua_State* L) {
    return do_store_load(L, store_of(L, "load", true), 1, "barch.store.load");
}

/*
 * barch.store.getDictionary() / setDictionary(buffer), and the same on a space
 * handle - TODO 458. The DICTIONARY command's pair, so a backup taken from a
 * script can carry the dictionary a compressed value needs. nil when
 * compression is off, which is the one outcome a script can act on without
 * having to catch an error.
 */
static int push_store_dictionary(lua_State* L, const store_access* s) {
    if (!barch::get_compression_enabled()) {
        lua_pushnil(L);
        return 1;
    }
    if (!s->may_read)
        luaL_error(L, "FUNCTION not authorized to read there");
    heap::vector<uint8_t> d;
    if (!s->get_dictionary || !s->get_dictionary(d)) {
        lua_pushnil(L);
        return 1;
    }
    void* b = lua_newbuffer(L, d.size());
    if (d.size())
        std::memcpy(b, d.data(), d.size());
    return 1;
}

static int set_store_dictionary_at(lua_State* L, const store_access* s, int at) {
    if (!barch::get_compression_enabled()) {
        lua_pushnil(L);
        return 1;
    }
    check_may_write(L, s);
    size_t n = 0;
    const char* p = nullptr;
    if (lua_type(L, at) == LUA_TBUFFER)
        p = static_cast<const char*>(lua_tobuffer(L, at, &n));
    else
        p = lua_tolstring(L, at, &n);
    std::string err;
    if (!s->set_dictionary || !s->set_dictionary(std::string(p ? p : "", n), err))
        luaL_error(L, "%s", err.empty() ? "FUNCTION setDictionary refused" : err.c_str());
    lua_pushboolean(L, true);
    return 1;
}

static int store_get_dictionary(lua_State* L) {
    return push_store_dictionary(L, store_of(L, "getDictionary"));
}

static int store_set_dictionary(lua_State* L) {
    return set_store_dictionary_at(L, store_of(L, "setDictionary", true), 1);
}

static int store_free_list(lua_State* L) {
    return do_store_shard_state(L, store_of(L, "freeList"), 1, true, "barch.store.freeList");
}

static int store_stats(lua_State* L) {
    return do_store_shard_state(L, store_of(L, "stats"), 1, false, "barch.store.stats");
}

static int store_leaves(lua_State* L) {
    return do_store_pages(L, store_of(L, "leaves"), 1, false, "barch.store.leaves");
}

static int store_nodes(lua_State* L) {
    return do_store_pages(L, store_of(L, "nodes"), 1, true, "barch.store.nodes");
}

static int store_exists(lua_State* L) {
    size_t n = 0;
    const char* k = luaL_checklstring(L, 1, &n);
    lua_pushboolean(L, store_of(L, "exists")->exists({k, n}));
    return 1;
}

static int store_count(lua_State* L) {
    size_t ln = 0, hn = 0;
    const char* lo = luaL_checklstring(L, 1, &ln);
    const char* hi = luaL_checklstring(L, 2, &hn);
    lua_pushnumber(L, (double) store_of(L, "count")->count({lo, ln}, {hi, hn}));
    return 1;
}

/* barch.store.looseCount(lo, hi) - the keys in [lo, hi) of the space as it is now, with
 * no lock and no read logged, even in a transaction - TODO 810. */
static int push_loose_count(lua_State* L, const store_access* s, int at) {
    size_t ln = 0, hn = 0;
    const char* lo = luaL_checklstring(L, at, &ln);
    const char* hi = luaL_checklstring(L, at + 1, &hn);
    if (!s->loose_count)
        luaL_error(L, "FUNCTION looseCount is not available here");
    lua_pushnumber(L, (double) s->loose_count({lo, ln}, {hi, hn}));
    return 1;
}

static int store_loose_count(lua_State* L) {
    return push_loose_count(L, store_of(L, "looseCount"), 1);
}

/* barch.store.range(lo, hi, limit [, offset]) - the keys in [lo, hi), at most limit of
 * them, after leaving out the first `offset`.
 *
 * The limit is required and capped. An unbounded walk would copy the whole space into
 * one Luau table, which is the thing KEYS was made asynchronous to avoid. The offset is
 * how a script pages through a big range without reading every page before the one it
 * wants: the store jumps to it by node counts - TODO 369 and 372. */
enum { max_range_batch = 10000 };

/** the keys of one page, into `keys`; the offset argument sits at `at` */
static void range_page(lua_State* L, const barch::foreign::store_access* s, int at,
                       const char* lo, size_t ln, const char* hi, size_t hn, int64_t limit,
                       heap::vector<std::string>& keys) {
    double off = luaL_optnumber(L, at, 0);
    if (off < 0)
        luaL_error(L, "FUNCTION range offset must not be negative");
    auto offset = (int64_t) off;
    if (offset > 0 && s->range_from)
        s->range_from({lo, ln}, {hi, hn}, limit, offset, keys);
    else if (offset > 0)
        luaL_error(L, "FUNCTION range offset is not available here");
    else
        s->range({lo, ln}, {hi, hn}, limit, keys);
}

static int store_range(lua_State* L) {
    size_t ln = 0, hn = 0;
    const char* lo = luaL_checklstring(L, 1, &ln);
    const char* hi = luaL_checklstring(L, 2, &hn);
    int64_t limit = (int64_t) luaL_checknumber(L, 3);
    if (limit <= 0 || limit > max_range_batch)
        limit = max_range_batch;
    heap::vector<std::string> keys;
    range_page(L, store_of(L, "range"), 4, lo, ln, hi, hn, limit, keys);
    lua_createtable(L, (int) keys.size(), 0);
    int at = 1;
    for (const auto& k : keys) {
        lua_pushlstring(L, k.data(), k.size());
        lua_rawseti(L, -2, at++);
    }
    return 1;
}

/* barch.store.rangeValues(lo, hi, limit [, offset]) - range, and each key's value with
 * it from the same walk: two tables, the keys and the values at the same positions. A
 * scan that reads the rows then needs no get for each - TODO 764. In a transaction it
 * reads and locks as range does, and a value it answers that changes before COMMITTX
 * is a conflict, as a range's keys are. */
static int push_range_values(lua_State* L, const store_access* s, int at) {
    if (!s->may_read)
        luaL_error(L, "FUNCTION not authorized to read there");
    if (!s->range_values)
        luaL_error(L, "FUNCTION rangeValues is not available here");
    size_t ln = 0, hn = 0;
    const char* lo = luaL_checklstring(L, at, &ln);
    const char* hi = luaL_checklstring(L, at + 1, &hn);
    auto limit = (int64_t) luaL_checknumber(L, at + 2);
    if (limit <= 0 || limit > max_range_batch)
        limit = max_range_batch;
    const double off = luaL_optnumber(L, at + 3, 0);
    if (off < 0)
        luaL_error(L, "FUNCTION range offset must not be negative");
    heap::vector<std::pair<std::string, std::string>> rows;
    s->range_values({lo, ln}, {hi, hn}, limit, (int64_t) off, rows);
    lua_createtable(L, (int) rows.size(), 0);
    lua_createtable(L, (int) rows.size(), 0);
    int i = 1;
    for (const auto& [k, v] : rows) {
        lua_pushlstring(L, k.data(), k.size());
        lua_rawseti(L, -3, i);
        lua_pushlstring(L, v.data(), v.size());
        lua_rawseti(L, -2, i);
        ++i;
    }
    return 2;
}

static int store_range_values(lua_State* L) {
    return push_range_values(L, store_of(L, "rangeValues"), 1);
}

static int store_min(lua_State* L) {
    std::string key;
    if (!store_of(L, "min")->min(key)) {
        lua_pushnil(L);
        return 1;
    }
    lua_pushlstring(L, key.data(), key.size());
    return 1;
}

static int store_max(lua_State* L) {
    std::string key;
    if (!store_of(L, "max")->max(key)) {
        lua_pushnil(L);
        return 1;
    }
    lua_pushlstring(L, key.data(), key.size());
    return 1;
}

/* barch.space() - what the key space is configured as, as a table */
static int store_space(lua_State* L) {
    heap::vector<std::pair<std::string, std::string>> settings;
    store_of(L, "space")->config(settings);
    lua_createtable(L, 0, (int) settings.size());
    for (const auto& [k, v] : settings) {
        lua_pushlstring(L, v.data(), v.size());
        lua_setfield(L, -2, k.c_str());
    }
    return 1;
}

/*
 * barch.space.NAME.key - a key space as a value.
 *
 *     barch.space.sp1.key1              read
 *     barch.space.sp1.key1 = "v"        write
 *     barch.space.sp1.key1 = nil        remove
 *     for row in barch.space.sp1 do     walk it
 *
 * Userdata rather than tables all the way down: `__index` on a table only fires when
 * the key is absent, so a table could be written past, while userdata always goes
 * through the metamethods. See TODO 98 F2.
 */
enum { space_tag = 1, row_tag = 2 };

/*
 * Which space a thing a script holds reads through - TODO 430.
 *
 * Handles, containers and cursors are Luau values: a module can keep them for as long
 * as its state lives, far longer than the call that made them. What they point into
 * doesn't last that long. The call interface, and every store_access barch.space
 * opened in it, is replaced when the connection runs a call in another space, and a
 * barch.art() space went when its handle was collected. Raw pointers kept in them
 * were TODO 427 (a module's handle into a freed interface, a crash in hide_secrets'
 * wrapper) and TODO 431 (a walk over barch.art() whose handle had been collected, a
 * segfault on the next page). So a space_ref holds what it names, not where it was:
 *
 *   - `scratch`: a barch.art() space, shared, so everything made from it keeps it
 *   - `current`: barch.current(), the running call's store at every use
 *   - `name`: any other space, with a cache that is only trusted while the interface
 *     it came from is the running one. The weak_ptr makes that check safe even when
 *     a new interface is built where an old one was freed.
 */
struct space_ref {
    std::string name{};
    bool current{false};
    std::shared_ptr<const store_access> scratch{};
    std::weak_ptr<call_interface> from{};
    const store_access* cached{nullptr};
};

/** a barch.art() space and its store_access, owned together */
struct scratch_space {
    barch::key_space_ptr space{};
    store_access access{};
};

struct space_handle {
    /** what it reads through, and for a named space where `sp:call` sends a command */
    space_ref ref{};
};

/** a container reached through a space handle: sp:container("name") */
struct container_handle {
    space_ref ref{};
    std::string name{};
    /** resolved from `ref` by as_container, for the operation in hand only */
    const store_access* store{nullptr};
};

/** where a container walk has got to */
struct member_cursor {
    space_ref ref{};
    std::string name{};
    heap::vector<std::pair<std::string, std::string>> page{};
    size_t at{0};
    std::string after{};
    bool done{false};
};

/** where a walk has got to, and the page it is reading */
struct row_cursor {
    space_ref ref{};
    heap::vector<store_access::row> page{};
    size_t at{0};
    std::string after{};
    bool done{false};
};

/*
 * Each of these is built in memory Luau hands over unconstructed, with its destructor
 * already attached. A constructor that threw would leave the destructor to run on raw
 * bytes at the next collection, so the constructors must not throw - TODO 575.
 */
static_assert(std::is_nothrow_default_constructible_v<row_cursor>);
static_assert(std::is_nothrow_default_constructible_v<member_cursor>);
static_assert(std::is_nothrow_default_constructible_v<space_handle>);
static_assert(std::is_nothrow_default_constructible_v<container_handle>);

/*
 * The tags these handles are made with - TODO 572. Each tag carries its own
 * destructor and metatable, set up once in state_for, and the C functions behind
 * the metatables check the tag rather than trusting their first argument. Those
 * functions used to cast whatever they were handed, and getmetatable gave a
 * script the functions, so another userdata could be read as a space handle.
 * Tag 0 is what plain lua_newuserdata makes, so it isn't one of these.
 */
enum handle_tag : int { tag_space = 1, tag_container = 2, tag_row = 3 };

/** the store `r` names, now, in the running call - see space_ref */
static const store_access* resolve(lua_State* L, space_ref& r) {
    if (r.scratch)
        return r.scratch.get();
    space_state* st = state_of(L);
    if (r.current) {
        if (!st || !st->store())
            luaL_error(L, "FUNCTION barch.current() is only open while a call runs");
        return st->store();
    }
    /*
     * Checked on every use, not when the handle is made: Luau resolves a constant
     * path like `barch.space.sp1` once, when the function loads, outside any run,
     * and keeps the handle - TODO 673.
     */
    refuse_other_space(L, r.name);
    call_interface* running = st ? st->iface() : nullptr;
    if (running && r.cached && r.from.lock().get() == running)
        return r.cached;
    if (!running || !st->opened() || !st->open_space())
        luaL_error(L, "FUNCTION that key space is not open");
    auto have = st->opened()->find(r.name);
    if (have == st->opened()->end()) {
        auto opened = std::make_unique<store_access>();
        if (!(*st->open_space())(r.name, *opened))
            luaL_error(L, "FUNCTION no key space called %s", r.name.c_str());
        have = st->opened()->emplace(r.name, std::move(opened)).first;
    }
    r.cached = have->second.get();
    r.from = st->call->iface;
    return r.cached;
}

static const store_access* handle_store(lua_State* L, int idx) {
    auto* h = static_cast<space_handle*>(lua_touserdatatagged(L, idx, tag_space));
    if (!h)
        luaL_error(L, "FUNCTION that key space is not open");
    return resolve(L, h->ref);
}

static int space_read(lua_State* L) {
    const store_access* s = handle_store(L, 1);
    if (!s->may_read)
        luaL_error(L, "FUNCTION not authorized to read there");
    size_t n = 0;
    const char* k = luaL_checklstring(L, 2, &n);
    std::string value;
    switch (s->get({k, n}, value)) {
        case store_access::read_state::present:
            lua_pushlstring(L, value.data(), value.size());
            break;
        case store_access::read_state::tombed:
            push_tomb(L);
            break;
        default:
            lua_pushnil(L);
    }
    return 1;
}

static int space_write(lua_State* L) {
    const store_access* s = handle_store(L, 1);
    check_may_write(L, s);
    size_t n = 0;
    const char* k = luaL_checklstring(L, 2, &n);
    // assigning nil removes, which is how a table behaves and so how this should
    if (lua_isnoneornil(L, 3)) {
        s->remove({k, n});
        return 0;
    }
    size_t vn = 0;
    const char* v = lua_tolstring(L, 3, &vn);
    if (!v)
        luaL_error(L, "FUNCTION a key space holds strings and numbers");
    std::string err;
    if (!s->set({k, n}, {v, vn}, err))
        luaL_error(L, "%s", err.empty() ? "FUNCTION write refused" : err.c_str());
    return 0;
}

static int row_read(lua_State* L) {
    auto* c = static_cast<row_cursor*>(lua_touserdatatagged(L, 1, tag_row));
    const char* f = lua_tostring(L, 2);
    if (!c || !f || c->at == 0 || c->at > c->page.size()) {
        lua_pushnil(L);
        return 1;
    }
    const auto& r = c->page[c->at - 1];
    // decoded only when asked for: a filter that reads keys and skips most values is
    // the loop people write, and it is 3.3x faster for not building the rest
    if (!strcmp(f, "key")) {
        if (r.key.empty()) lua_pushnil(L);
        else lua_pushlstring(L, r.key.data(), r.key.size());
    } else if (!strcmp(f, "value")) {
        // read now, not when the page was copied: a walk that looks at keys and wants
        // the value of a few should pay for those few, which is the whole point of
        // handing back a row that decodes on demand
        const store_access* rs = r.key.empty() ? nullptr : resolve(L, c->ref);
        if (!rs || !rs->get) {
            lua_pushnil(L);
        } else {
            std::string v;
            // a walk never hands out a tomb as a row, so anything but present here
            // is an absent value rather than a cached miss
            if (rs->get(r.key, v) != store_access::read_state::present)
                lua_pushnil(L);
            else lua_pushlstring(L, v.data(), v.size());
        }
    } else if (!strcmp(f, "container")) {
        if (r.container.empty()) lua_pushnil(L);
        else lua_pushlstring(L, r.container.data(), r.container.size());
    } else if (!strcmp(f, "type")) {
        lua_pushlstring(L, r.type.data(), r.type.size());
    } else {
        lua_pushnil(L);
    }
    return 1;
}

/** the step of `for row in space do`: one row, or nil when the walk is over */
static int row_next(lua_State* L) {
    auto* c = static_cast<row_cursor*>(lua_touserdatatagged(L, lua_upvalueindex(1), tag_row));
    if (!c) {
        lua_pushnil(L);
        return 1;
    }
    if (c->at >= c->page.size()) {
        if (c->done) {
            lua_pushnil(L);
            return 1;
        }
        c->page.clear();
        c->at = 0;
        // `after` is the encoded key the last page ended on, kept as the store
        // handed it over. A key erased after this page was copied simply does not
        // appear, which is the promise SCAN already makes
        // resolved per page: a walk kept past its call reads through the call it is
        // in now, and a barch.art() walk keeps its space alive itself - TODO 430
        resolve(L, c->ref)->page(c->after, 256, c->page, c->after);
        if (c->page.empty()) {
            c->done = true;
            lua_pushnil(L);
            return 1;
        }
    }
    ++c->at;
    lua_pushvalue(L, lua_upvalueindex(1));   // the same row object every step
    return 1;
}

static int space_iter(lua_State* L) {
    const store_access* s = handle_store(L, 1);
    if (!s->may_read)
        luaL_error(L, "FUNCTION not authorized to read there");
    auto* c = static_cast<row_cursor*>(lua_newuserdatataggedwithmetatable(L,
        sizeof(row_cursor), tag_row));
    new (c) row_cursor();
    c->ref = static_cast<space_handle*>(lua_touserdatatagged(L, 1, tag_space))->ref;
    lua_pushcclosure(L, row_next, "next", 1);
    return 1;
}

static container_handle* as_container(lua_State* L, int idx) {
    auto* h = static_cast<container_handle*>(lua_touserdatatagged(L, idx, tag_container));
    if (!h)
        luaL_error(L, "FUNCTION that container is not open");
    h->store = resolve(L, h->ref);
    return h;
}

static int container_read(lua_State* L) {
    auto* h = as_container(L, 1);
    size_t n = 0;
    const char* m = luaL_checklstring(L, 2, &n);
    std::string value;
    if (!h->store->container_get || !h->store->container_get(h->name, {m, n}, value)) {
        lua_pushnil(L);
        return 1;
    }
    lua_pushlstring(L, value.data(), value.size());
    return 1;
}

static int container_write(lua_State* L) {
    auto* h = as_container(L, 1);
    size_t n = 0;
    const char* m = luaL_checklstring(L, 2, &n);
    if (lua_isnoneornil(L, 3)) {
        if (h->store->container_del)
            h->store->container_del(h->name, {m, n});
        return 0;                          // assigning nil removes, as a table does
    }
    size_t vn = 0;
    const char* v = lua_tolstring(L, 3, &vn);
    if (!v)
        luaL_error(L, "FUNCTION a container holds strings and numbers");
    std::string err;
    if (!h->store->container_set || !h->store->container_set(h->name, {m, n}, {v, vn}, err))
        luaL_error(L, "%s", err.empty() ? "FUNCTION write refused" : err.c_str());
    return 0;
}

/** the step of `for member, value in container do` */
static int member_next(lua_State* L) {
    auto* c = static_cast<member_cursor*>(lua_touserdata(L, lua_upvalueindex(1)));
    if (!c) {
        lua_pushnil(L);
        return 1;
    }
    if (c->at >= c->page.size()) {
        if (c->done) {
            lua_pushnil(L);
            return 1;
        }
        c->page.clear();
        c->at = 0;
        resolve(L, c->ref)->container_page(c->name, c->after, 128, c->page, c->after);
        if (c->page.empty()) {
            c->done = true;
            lua_pushnil(L);
            return 1;
        }
    }
    const auto& kv = c->page.at(c->at++);
    lua_pushlstring(L, kv.first.data(), kv.first.size());
    lua_pushlstring(L, kv.second.data(), kv.second.size());
    return 2;                              // member first: it is never nil until the end
}

static int container_iter(lua_State* L) {
    auto* h = as_container(L, 1);
    auto* c = static_cast<member_cursor*>(lua_newuserdatadtor(L, sizeof(member_cursor),
        [](void* p) { static_cast<member_cursor*>(p)->~member_cursor(); }));
    new (c) member_cursor();
    c->ref = h->ref;
    c->name = h->name;
    lua_pushcclosure(L, member_next, "next", 1);
    return 1;
}

static int push_store_get(lua_State* L, const store_access* s, const std::string& key, bool for_update) {
    if (!s->may_read)
        luaL_error(L, "FUNCTION not authorized to read there");
    if (for_update && s->for_update)
        s->for_update(key);     // GETFORUPDATE: locked for the write that follows (TODO 732)
    std::string value;
    switch (s->get(key, value)) {
        case store_access::read_state::present:
            lua_pushlstring(L, value.data(), value.size());
            break;
        case store_access::read_state::tombed:
            push_tomb(L);
            break;
        default:
            lua_pushnil(L);
    }
    return 1;
}

static int push_store_bound(lua_State* L, const store_access* s, bool want_min) {
    if (!s->may_read)
        luaL_error(L, "FUNCTION not authorized to read there");
    std::string key;
    bool found = want_min ? s->min(key) : s->max(key);
    if (!found) {
        lua_pushnil(L);
        return 1;
    }
    lua_pushlstring(L, key.data(), key.size());
    return 1;
}

static int space_pop(lua_State* L, const store_access* s, bool want_min) {
    check_may_write(L, s);
    std::string key;
    bool found = want_min ? s->min(key) : s->max(key);
    if (!found) {
        lua_pushnil(L);
        return 1;
    }
    std::string value;
    if (s->get(key, value) == store_access::read_state::present)
        lua_pushlstring(L, value.data(), value.size());
    else
        lua_pushnil(L);
    if (!s->remove(key)) {
        err({"remove failed for",key});
    }
    lua_pushlstring(L, key.data(), key.size());
    lua_insert(L, -2); // key, value
    return 2;
}

/*
 * the store methods plus container/kind - same interface on a live space and on
 * barch.art().
 *
 * Methods are binned by prefix ("get", "set", "incr") and each bin compares only
 * what follows it, so a call isn't checked against every name in the list. A
 * prefix with nothing after it is the plain method of that name - get and set.
 * TODO 661.
 */
static int space_get_method(lua_State* L, const store_access* s, const char* t) {
    if (!*t) {
        size_t n = 0;
        const char* k = luaL_checklstring(L, 2, &n);
        return push_store_get(L, s, {k, n}, lua_toboolean(L, 3));
    }
    if (!strcmp(t, "Json")) {
        size_t n = 0;
        const char* k = luaL_checklstring(L, 2, &n);
        return push_store_get_json(L, s, {k, n});
    }
    if (!strcmp(t, "BufferAt")) {
        if (!s->may_read)
            luaL_error(L, "FUNCTION not authorized to read there");
        size_t n = 0;
        const char* k = luaL_checklstring(L, 2, &n);
        size_t offset = check_buf_offset(L, 3);
        return push_copied_buffer(L, s, {k, n}, offset);
    }
    if (!strcmp(t, "Buffers")) {
        if (!s->may_read)
            luaL_error(L, "FUNCTION not authorized to read there");
        return push_copied_buffers(L, s, 2, check_buf_offset(L, 3));
    }
    if (!strcmp(t, "Int32At"))
        return do_get_int_at(L, s, 2, 4);
    if (!strcmp(t, "Int64At"))
        return do_get_int_at(L, s, 2, 8);
    if (!strcmp(t, "F64At"))
        return do_get_float_at(L, s, 2, 8);
    if (!strcmp(t, "F32At"))
        return do_get_float_at(L, s, 2, 4);
    if (!strcmp(t, "Dictionary"))
        return push_store_dictionary(L, s);
    return -1;
}

static int space_set_method(lua_State* L, const store_access* s, const char* t) {
    if (!*t) {
        check_may_write(L, s);
        size_t n = 0;
        const char* k = luaL_checklstring(L, 2, &n);
        if (lua_isnoneornil(L, 3)) {
            if (!s->remove({k, n})) {
                err({"remove failed for",k});
            }
            return 0;
        }
        size_t vn = 0;
        const char* v = lua_tolstring(L, 3, &vn);
        if (!v)
            luaL_error(L, "FUNCTION a key space holds strings and numbers");
        std::string err;
        if (!s->set({k, n}, {v, vn}, err))
            luaL_error(L, "%s", err.empty() ? "FUNCTION write refused" : err.c_str());
        return 0;
    }
    if (!strcmp(t, "Json")) {
        size_t n = 0;
        const char* k = luaL_checklstring(L, 2, &n);
        return store_set_json_at(L, s, {k, n}, 3);
    }
    if (!strcmp(t, "BufferAt"))
        return do_set_buffer_at(L, s, 2);
    if (!strcmp(t, "Int32At"))
        return do_set_int_at(L, s, 2, 4);
    if (!strcmp(t, "Int64At"))
        return do_set_int_at(L, s, 2, 8);
    if (!strcmp(t, "F64At"))
        return do_set_float_at(L, s, 2, 8);
    if (!strcmp(t, "F32At"))
        return do_set_float_at(L, s, 2, 4);
    if (!strcmp(t, "Dictionary"))
        return set_store_dictionary_at(L, s, 2);
    return -1;
}

static int space_incr_method(lua_State* L, const store_access* s, const char* t) {
    if (!strcmp(t, "Int32ByAt"))
        return do_incr_int_at(L, s, 2, 4);
    if (!strcmp(t, "Int64ByAt"))
        return do_incr_int_at(L, s, 2, 8);
    return -1;
}

static int space_namecall_as(lua_State* L, const char* m) {
    const store_access* s = handle_store(L, 1);
    if (!m)
        luaL_error(L, "FUNCTION no such method on a key space");

    // -1 from a bin means no method there by that name
    int r = -1;
    if (!strncmp(m, "get", 3))
        r = space_get_method(L, s, m + 3);
    else if (!strncmp(m, "set", 3))
        r = space_set_method(L, s, m + 3);
    else if (!strncmp(m, "incr", 4))
        r = space_incr_method(L, s, m + 4);
    if (r >= 0)
        return r;

    if (!strcmp(m, "call")) {
        // any command, run in this handle's space with the caller's rights there -
        // what barch.call("images:GET", ...) can't do from a script. TODO 378
        auto* h = static_cast<space_handle*>(lua_touserdatatagged(L, 1, tag_space));
        if (h->ref.scratch)
            luaL_error(L, "FUNCTION a barch.art() space has no name for a command to run in");
        return run_script_command(L, 2, h->ref.name, "sp:call");
    }
    if (!strcmp(m, "parseJson")) {
        size_t n = 0;
        const char* k = luaL_checklstring(L, 2, &n);
        return push_store_parse_json(L, s, {k, n}, 3);
    }
    if (!strcmp(m, "fetch")) {
        if (!s->may_read)
            luaL_error(L, "FUNCTION not authorized to read there");
        size_t n = 0;
        const char* k = luaL_checklstring(L, 2, &n);
        return push_store_fetch(L, s, {k, n});
    }
    if (!strcmp(m, "remove")) {
        check_may_write(L, s);
        size_t n = 0;
        const char* k = luaL_checklstring(L, 2, &n);
        lua_pushboolean(L, s->remove({k, n}));
        return 1;
    }
    if (!strcmp(m, "exists")) {
        size_t n = 0;
        const char* k = luaL_checklstring(L, 2, &n);
        lua_pushboolean(L, s->exists({k, n}));
        return 1;
    }
    if (!strcmp(m, "min"))
        return push_store_bound(L, s, true);
    if (!strcmp(m, "max"))
        return push_store_bound(L, s, false);
    if (!strcmp(m, "size")) {
        lua_pushnumber(L, s->size ? (double) s->size() : 0);
        return 1;
    }
    if (!strcmp(m, "count")) {
        size_t ln = 0, hn = 0;
        const char* lo = luaL_checklstring(L, 2, &ln);
        const char* hi = luaL_checklstring(L, 3, &hn);
        lua_pushnumber(L, (double) s->count({lo, ln}, {hi, hn}));
        return 1;
    }
    if (!strcmp(m, "looseCount"))
        return push_loose_count(L, s, 2);
    if (!strcmp(m, "rangeValues"))
        return push_range_values(L, s, 2);
    if (!strcmp(m, "range")) {
        size_t ln = 0, hn = 0;
        const char* lo = luaL_checklstring(L, 2, &ln);
        const char* hi = luaL_checklstring(L, 3, &hn);
        int64_t limit = (int64_t) luaL_checknumber(L, 4);
        if (limit <= 0 || limit > max_range_batch)
            limit = max_range_batch;
        heap::vector<std::string> keys;
        range_page(L, s, 5, lo, ln, hi, hn, limit, keys);
        lua_createtable(L, (int) keys.size(), 0);
        int at = 1;
        for (const auto& k : keys) {
            lua_pushlstring(L, k.data(), k.size());
            lua_rawseti(L, -2, at++);
        }
        return 1;
    }
    if (!strcmp(m, "popmin"))
        return space_pop(L, s, true);
    if (!strcmp(m, "popmax"))
        return space_pop(L, s, false);
    if (!strcmp(m, "kind")) {
        size_t n = 0;
        const char* raw = luaL_checklstring(L, 2, &n);
        std::string k = s->container_kind ? s->container_kind({raw, n}) : std::string();
        if (k.empty()) lua_pushnil(L);
        else lua_pushlstring(L, k.data(), k.size());
        return 1;
    }
    if (!strcmp(m, "container")) {
        size_t n = 0;
        const char* raw = luaL_checklstring(L, 2, &n);
        std::string name(raw, n);
        if (!s->container_kind || s->container_kind(name).empty())
            luaL_error(L, "FUNCTION no container called %s", name.c_str());
        auto* h = static_cast<container_handle*>(lua_newuserdatataggedwithmetatable(L,
            sizeof(container_handle), tag_container));
        new (h) container_handle();
        h->name = std::move(name);
        // the same space its handle names, resolved the same way - TODO 430
        h->ref = static_cast<space_handle*>(lua_touserdatatagged(L, 1, tag_space))->ref;
        return 1;
    }
    if (!strcmp(m, "leaves"))
        return do_store_pages(L, s, 2, false, "sp:leaves");
    if (!strcmp(m, "nodes"))
        return do_store_pages(L, s, 2, true, "sp:nodes");
    if (!strcmp(m, "freeList"))
        return do_store_shard_state(L, s, 2, true, "sp:freeList");
    if (!strcmp(m, "save"))
        return do_store_save(L, s, 2, "sp:save");
    if (!strcmp(m, "load"))
        return do_store_load(L, s, 2, "sp:load");
    if (!strcmp(m, "stats"))
        return do_store_shard_state(L, s, 2, false, "sp:stats");
    luaL_error(L, "FUNCTION no such method on a key space");
    return 0;
}

/** push a handle that names `ref` */
static space_handle* push_space_handle(lua_State* L, space_ref ref) {
    auto* h = static_cast<space_handle*>(lua_newuserdatataggedwithmetatable(L,
        sizeof(space_handle), tag_space));
    new (h) space_handle();
    h->ref = std::move(ref);
    return h;
}

/*
 * barch.current() - the space this call is running against, as a space handle.
 *
 * `barch.store` is the same data with the older method table. This is the handle
 * `barch.space.NAME` would give you if you already knew the name. See TODO 160.
 */
static int current_space_handle(lua_State* L) {
    space_state* st = state_of(L);
    if (!st || !st->store())
        luaL_error(L, "FUNCTION barch.current is not available here");
    // the running call's store at every use, not this one's - TODO 430
    space_ref ref;
    ref.current = true;
    push_space_handle(L, std::move(ref));
    return 1;
}

/*
 * barch.fs - the file store as paths. See fs.h and TODO 256.
 *
 * Reads go through the same `store_access` the rest of the script uses, so a user
 * with no rights in the space gets nothing. A write checks `may_write` here and then
 * uses the space directly, because `fs::batch` needs the shards to stage a commit and
 * a store_access does not carry them - the check above is what keeps that honest, and
 * it is the same shape `store_of` uses one layer down.
 */
/*
 * Every barch.fs function carries one upvalue: nil for `barch.fs` itself, which
 * works on the space the call is running in, or a space name for the copy that
 * `barch.fs.space(name)` hands out - TODO 374. The name is only ever one that
 * `barch.space` would open, and it's opened the same way, through the same cache,
 * so the rights are the ones a space handle for it would have.
 */
static bool fs_bound(lua_State* L, std::string& name) {
    if (lua_type(L, lua_upvalueindex(1)) != LUA_TSTRING)
        return false;
    size_t n = 0;
    const char* s = lua_tolstring(L, lua_upvalueindex(1), &n);
    name.assign(s, n);
    return true;
}

static const store_access* fs_open(lua_State* L, const std::string& name, const char* what) {
    space_state* st = state_of(L);
    if (!st || !st->open_space() || !st->opened())
        luaL_error(L, "FUNCTION barch.fs.%s is not available here", what);
    refuse_other_space(L, name);
    auto have = st->opened()->find(name);
    if (have == st->opened()->end()) {
        auto opened = std::make_unique<store_access>();
        // an unknown name is not a key space and must not become one
        if (!(*st->open_space())(name, *opened))
            luaL_error(L, "FUNCTION no key space called %s", name.c_str());
        have = st->opened()->emplace(name, std::move(opened)).first;
    }
    return have->second.get();
}

/*
 * What a script can't reach while its session has a transaction open - TODO 708: the
 * parts of the API that write past the transaction's overlay, or that can't be undone
 * if it aborts. barch.store itself goes through the overlay (TODO 707).
 */
void refuse_in_transaction(lua_State* L, const char* what) {
    space_state* st = state_of(L);
    const store_access* s = st ? st->store() : nullptr;
    if (s && s->in_transaction && s->in_transaction())
        luaL_error(L, "FUNCTION %s isn't taken in a transaction yet (TODO 708)", what);
}

static const store_access* fs_store(lua_State* L, const char* what, bool writing) {
    // files are written in batches of their own, past a transaction's overlay.
    // barch.graph doesn't come through here: it runs GRAPH, which is taken - TODO 756
    {
        const std::string named = std::string("barch.fs.") + what;
        refuse_in_transaction(L, named.c_str());
    }
    const store_access* acc = nullptr;
    std::string bound;
    if (fs_bound(L, bound)) {
        acc = fs_open(L, bound, what);
    } else {
        space_state* st = state_of(L);
        if (!st || !st->store())
            luaL_error(L, "FUNCTION barch.fs.%s is not available here", what);
        acc = st->store();
    }
    if (writing)
        check_may_write(L, acc);
    else if (!acc->may_read)
        luaL_error(L, "FUNCTION not authorized to read here");
    return acc;
}

static barch::key_space_ptr fs_space(lua_State* L, const char* what) {
    std::string bound;
    if (fs_bound(L, bound)) {
        (void) fs_open(L, bound, what);    // refuses a name that isn't a space
        return barch::get_keyspace(bound);
    }
    auto* rc = call_of(L);
    if (!rc)
        luaL_error(L, "FUNCTION barch.fs.%s needs a running call", what);
    return barch::get_keyspace(rc->running);
}

static void push_fs_entry(lua_State* L, const barch::fs::entry& e) {
    lua_newtable(L);
    lua_pushlstring(L, e.name.data(), e.name.size());
    lua_setfield(L, -2, "name");
    lua_pushlstring(L, e.path.data(), e.path.size());
    lua_setfield(L, -2, "path");
    lua_pushboolean(L, e.dir);
    lua_setfield(L, -2, "dir");
    lua_pushboolean(L, e.remote);
    lua_setfield(L, -2, "remote");
    if (e.dir || e.remote)
        return;
    lua_pushnumber(L, (double) e.size);
    lua_setfield(L, -2, "size");
    lua_pushnumber(L, (double) e.chunks);
    lua_setfield(L, -2, "chunks");
    lua_pushnumber(L, (double) e.version);
    lua_setfield(L, -2, "version");
    lua_pushlstring(L, e.type.data(), e.type.size());
    lua_setfield(L, -2, "type");
}

/**
 * The optional chunk size fs.put and graph.put take, 0 for the default. Casting a
 * negative or huge number straight to size_t is how CHUNK -1 stored a file with no
 * chunks over RESP (TODO 407), so it's checked the same way parse_chunk does.
 */
static size_t chunk_arg(lua_State* L, int at, const char* who) {
    if (!lua_isnumber(L, at))
        return 0;
    double d = lua_tonumber(L, at);
    if (!(d >= 0 && d <= (double) barch::fs::max_chunk) || d != std::floor(d))
        luaL_error(L, "FUNCTION %s chunk is a whole number of bytes, at most %d", who,
                   (int) barch::fs::max_chunk);
    return (size_t) d;
}

static int fs_put(lua_State* L) {
    size_t pn = 0, bn = 0;
    const char* path = luaL_checklstring(L, 1, &pn);
    const char* body = luaL_checklstring(L, 2, &bn);
    std::string type;
    if (lua_isstring(L, 3)) {
        size_t tn = 0;
        const char* t = lua_tolstring(L, 3, &tn);
        type.assign(t, tn);
    }
    size_t chunk = chunk_arg(L, 4, "barch.fs.put");
    const auto* acc = fs_store(L, "put", true);
    auto space = fs_space(L, "put");
    barch::fs::batch b(space);
    b.write(std::string(path, pn), std::string(body, bn), type, chunk);
    std::string err;
    if (!b.commit(err))
        luaL_error(L, "FUNCTION barch.fs.put %s", err.c_str());
    // the chunk count, which is what a caller sizing a write wants to know. It is
    // the inode that has it, so this is stat_full rather than stat
    barch::fs::entry e;
    lua_pushnumber(L, (double) (barch::fs::stat_full(*acc, std::string(path, pn), e) ? e.chunks : 0));
    return 1;
}

static int fs_get(lua_State* L) {
    size_t pn = 0;
    const char* path = luaL_checklstring(L, 1, &pn);
    const auto* acc = fs_store(L, "get", false);
    std::string body;
    barch::fs::entry meta;
    if (!barch::fs::read(*acc, std::string(path, pn), body, meta)) {
        lua_pushnil(L);
        return 1;
    }
    lua_pushlstring(L, body.data(), body.size());
    return 1;
}

/**
 * `barch.fs.fetch(path)` - a read that may ask the space's file source, where
 * `barch.fs.get` only ever answers from what is already here. See TODO 263, and
 * `barch.store.fetch` for the same distinction one layer down.
 */
static int fs_fetch(lua_State* L) {
    refuse_from_log(L, "barch.fs.fetch");           // asks the space's source - TODO 673
    size_t pn = 0;
    const char* path = luaL_checklstring(L, 1, &pn);
    const auto* acc = fs_store(L, "fetch", false);
    auto space = fs_space(L, "fetch");
    barch::fs::entry meta;
    std::string err;
    if (!barch::fs::fetch(space, std::string(path, pn), meta, err)) {
        lua_pushnil(L);
        lua_pushlstring(L, err.data(), err.size());
        return 2;
    }
    std::string body;
    if (!barch::fs::read(*acc, std::string(path, pn), body, meta)) {
        lua_pushnil(L);
        return 1;
    }
    lua_pushlstring(L, body.data(), body.size());
    return 1;
}

static int fs_stat(lua_State* L) {
    size_t pn = 0;
    const char* path = luaL_checklstring(L, 1, &pn);
    const auto* acc = fs_store(L, "stat", false);
    barch::fs::entry e;
    if (!barch::fs::stat_full(*acc, std::string(path, pn), e)) {
        lua_pushnil(L);
        return 1;
    }
    push_fs_entry(L, e);
    return 1;
}

static int fs_list(lua_State* L) {
    size_t pn = 0;
    const char* path = luaL_checklstring(L, 1, &pn);
    std::string after;
    if (lua_isstring(L, 2)) {
        size_t an = 0;
        const char* a = lua_tolstring(L, 2, &an);
        after.assign(a, an);
    }
    size_t limit = lua_isnumber(L, 3) ? (size_t) lua_tonumber(L, 3) : 0;
    // a fourth argument asks the space's source what else could be there, which is
    // a question that can take as long as the source does - TODO 263
    const bool ask_source = lua_isboolean(L, 4) && lua_toboolean(L, 4);
    // a fifth leaves out that many entries first, for jumping to a page without
    // carrying `after` along - TODO 373
    double off = luaL_optnumber(L, 5, 0);
    if (off < 0)
        luaL_error(L, "FUNCTION fs.list offset must not be negative");
    const auto offset = (size_t) off;
    const auto* acc = fs_store(L, "list", false);
    std::vector<barch::fs::entry> got;
    if (ask_source)
        barch::fs::list_with_source(fs_space(L, "list"), std::string(path, pn), got,
                                    after, limit, offset);
    else
        barch::fs::list(*acc, std::string(path, pn), got, after, limit, offset);
    lua_createtable(L, (int) got.size(), 0);
    int at = 1;
    for (const auto& e : got) {
        push_fs_entry(L, e);
        lua_rawseti(L, -2, at++);
    }
    return 1;
}

static int fs_move(lua_State* L, bool copying) {
    size_t fn = 0, tn = 0;
    const char* from = luaL_checklstring(L, 1, &fn);
    const char* to = luaL_checklstring(L, 2, &tn);
    (void) fs_store(L, copying ? "copy" : "rename", true);
    auto space = fs_space(L, copying ? "copy" : "rename");
    std::string err;
    bool ok = copying ? barch::fs::copy(space, std::string(from, fn), std::string(to, tn), err)
                      : barch::fs::rename(space, std::string(from, fn), std::string(to, tn), err);
    if (!ok) {
        // "exists", "no such file", "into itself" are answers a caller can act on
        lua_pushboolean(L, false);
        lua_pushlstring(L, err.data(), err.size());
        return 2;
    }
    lua_pushboolean(L, true);
    return 1;
}

static int fs_rename(lua_State* L) { return fs_move(L, false); }
static int fs_copy(lua_State* L)   { return fs_move(L, true); }

/**
 * `barch.fs.publish(path)` - say that a module written here should be picked up.
 *
 * A file written by a script is live for a session that has not compiled it yet and
 * stale for one that has, and until now only `LOADFS ... RELOAD` from outside could
 * change that. A script deploying its own module had no way to finish the job - see
 * TODO 246.
 *
 * Separate from `put` on purpose, and opt in, for the reason TODO 245 made
 * publishing opt in on the command: a write is a write, and publishing is the
 * moment every session already running picks the change up. Doing that quietly
 * inside `put` is the sudden behaviour change that was deliberately taken out.
 *
 * The right to do it is the right to write the file. A script that can write the
 * module can already change what the next session compiles; publishing only decides
 * when the ones already running see it, so it grants no reach the writer did not
 * have - what it decides is timing, which is why it is a decision and not a default.
 */
static int fs_publish(lua_State* L) {
    size_t pn = 0;
    const char* path = luaL_checklstring(L, 1, &pn);
    (void) fs_store(L, "publish", true);
    auto space = fs_space(L, "publish");
    std::string clean, err;
    if (!barch::fs::normalise(std::string(path, pn), clean, err)) {
        lua_pushboolean(L, false);
        lua_pushlstring(L, err.data(), err.size());
        return 2;
    }
    if (!space)
        luaL_error(L, "FUNCTION barch.fs.publish has no space");
    barch::functions::publish_compiled(
        barch::functions::compiled_path_key(space->canonical(), clean));
    lua_pushboolean(L, true);
    return 1;
}

static int fs_mkdir(lua_State* L) {
    size_t pn = 0;
    const char* path = luaL_checklstring(L, 1, &pn);
    (void) fs_store(L, "mkdir", true);
    auto space = fs_space(L, "mkdir");
    std::string err;
    if (!barch::fs::mkdir(space, std::string(path, pn), err))
        luaL_error(L, "FUNCTION barch.fs.mkdir %s", err.c_str());
    lua_pushboolean(L, true);
    return 1;
}

static int fs_rmdir(lua_State* L) {
    size_t pn = 0;
    const char* path = luaL_checklstring(L, 1, &pn);
    bool recursive = lua_isboolean(L, 2) && lua_toboolean(L, 2);
    (void) fs_store(L, "rmdir", true);
    auto space = fs_space(L, "rmdir");
    std::string err;
    if (!barch::fs::rmdir(space, std::string(path, pn), recursive, err)) {
        // "not empty" and "no such directory" are answers, not faults: a script
        // asking to remove a directory can reasonably be told no
        lua_pushboolean(L, false);
        lua_pushlstring(L, err.data(), err.size());
        return 2;
    }
    lua_pushboolean(L, true);
    return 1;
}

static int fs_remove(lua_State* L) {
    size_t pn = 0;
    const char* path = luaL_checklstring(L, 1, &pn);
    (void) fs_store(L, "remove", true);
    auto space = fs_space(L, "remove");
    std::string err;
    const bool gone = barch::fs::erase(space, std::string(path, pn), err);
    // false is "no such file"; a remove that was refused raises - TODO 537
    if (!gone && !err.empty())
        luaL_error(L, "FUNCTION barch.fs.remove: %s", err.c_str());
    lua_pushboolean(L, gone);
    return 1;
}

static const luaL_Reg fs_functions[] = {
    {"put", fs_put},       {"get", fs_get},       {"stat", fs_stat},
    {"fetch", fs_fetch},   {"list", fs_list},     {"remove", fs_remove},
    {"mkdir", fs_mkdir},   {"rmdir", fs_rmdir},   {"rename", fs_rename},
    {"copy", fs_copy},     {"publish", fs_publish}, {nullptr, nullptr},
};

static int fs_space_table(lua_State* L);

/**
 * a barch.fs table: every function with the space it's bound to as its upvalue,
 * nil for the running space. Only the unbound one has `space`, so a bound table
 * can't be bound again to something else by accident.
 */
static void push_fs_table(lua_State* L, const char* space, size_t n) {
    lua_newtable(L);
    for (const luaL_Reg* f = fs_functions; f->name; ++f) {
        if (space)
            lua_pushlstring(L, space, n);
        else
            lua_pushnil(L);
        lua_pushcclosure(L, f->func, f->name, 1);
        lua_setfield(L, -2, f->name);
    }
    if (!space) {
        lua_pushcfunction(L, fs_space_table, "space");
        lua_setfield(L, -2, "space");
    }
    lua_setreadonly(L, -1, true);
}

/*
 * barch.fs.space(name) - the file store of another key space, TODO 374.
 *
 * `barch.fs` only ever reaches the space the call is running in, and there was no
 * way round it from a script: `barch.call("images:FS", ...)` is an unknown command
 * in here and USE doesn't move FS anywhere. So a handler in `shop` couldn't list or
 * write the pictures kept in `images`. The space is opened now, not on first use,
 * so a wrong name fails where it's written.
 */
static int fs_space_table(lua_State* L) {
    size_t n = 0;
    const char* name = luaL_checklstring(L, 1, &n);
    (void) fs_open(L, std::string(name, n), "space");
    push_fs_table(L, name, n);
    return 1;
}

/*
 * barch.graph - the graph store as paths. See graph.h and TODO 382.
 *
 * Every function is a GRAPH command - TODO 756. Its arguments become GRAPH's words
 * and the command runs the way barch.call runs it, so a script's graph call gets the
 * rights check GRAPH gets, the same transaction handling (the overlay, the reads a
 * commit checks, a pessimistic transaction's locks, parking on a held one) and the
 * same answers. It used to be a second copy written straight against the graph store,
 * and it missed each of those as GRAPH learned them: inside a transaction it had to be
 * refused. The nodes come back whole, through graph::rows_wanted, not as lines.
 *
 * The verbs match GRAPH's: put, get, stat, list, mkdir, link, unlink (one edge),
 * remove and rm (the node everywhere, RECURSIVE by a true second argument), rename
 * (one edge to another path), copy and cp, bfs and dfs. remove used to be rename under
 * another name, and rm without true unlinked (TODO 407).
 *
 * The upvalue is barch.fs's: nil for the running space, or the name
 * barch.graph.space(name) was given, where the command runs the way `sp:call` runs one.
 */

static void run_script_argv(lua_State* L, const std::string& space, const heap::vector<std::string>& argv,
                            const char* what, Variable& out, bool name_errors);

/** a GRAPH command being put together from a barch.graph call's arguments */
struct graph_command {
    lua_State* L;
    std::string what;
    heap::vector<std::string> argv;
    graph_command(lua_State* L, const char* verb, const char* sub) : L(L), what(std::string("barch.graph.") + verb) {
        argv.emplace_back("GRAPH");
        argv.emplace_back(sub);
    }
    /** a string argument at `at`, as one word */
    graph_command& text(int at) {
        size_t n = 0;
        const char* s = luaL_checklstring(L, at, &n);
        argv.emplace_back(s, n);
        return *this;
    }
    graph_command& word(std::string w) {
        argv.emplace_back(std::move(w));
        return *this;
    }
    /** NAME and a whole number, when there's one at `at`. Anything else there is `why`. */
    graph_command& number(const char* name, int at, const char* why, bool zero_too = true) {
        if (lua_isnoneornil(L, at))
            return *this;
        double d = lua_isnumber(L, at) ? lua_tonumber(L, at) : -1;
        if (d < (zero_too ? 0 : 1) || d != std::floor(d) || d >= 1.8e19)
            luaL_error(L, "FUNCTION %s %s", what.c_str(), why);
        char buf[32];
        snprintf(buf, sizeof buf, "%llu", (unsigned long long) d);
        argv.emplace_back(name);
        argv.emplace_back(buf);
        return *this;
    }
    /** run it; `rows`, when there, gets the nodes LS, STAT, BFS and DFS found */
    Variable run(std::vector<barch::graph::row>* rows = nullptr) {
        std::string bound;
        if (!fs_bound(L, bound))
            bound.clear();
        struct wanted {
            ~wanted() { barch::graph::rows_wanted() = nullptr; }
        } clear;
        barch::graph::rows_wanted() = rows;
        Variable out;
        run_script_argv(L, bound, argv, what.c_str(), out, true);
        return out;
    }
};

static void push_graph_entry(lua_State* L, const barch::graph::row& r) {
    const auto& n = r.n;
    lua_newtable(L);
    lua_pushnumber(L, (double) n.id);
    lua_setfield(L, -2, "id");
    lua_pushlstring(L, r.path.data(), r.path.size());
    lua_setfield(L, -2, "path");
    // a walk hit whose node went before it was read: only what the walk saw
    if (r.bare)
        return;
    // the last segment, the way barch.fs entries carry one - graph.list
    // callers were reaching for e.name and getting nil
    auto slash = r.path.find_last_of('/');
    std::string name = slash == std::string::npos ? r.path : r.path.substr(slash + 1);
    lua_pushlstring(L, name.data(), name.size());
    lua_setfield(L, -2, "name");
    lua_pushboolean(L, n.dir);
    lua_setfield(L, -2, "dir");
    lua_pushnumber(L, (double) n.refs);
    lua_setfield(L, -2, "refs");
    lua_pushnumber(L, (double) r.edge);
    lua_setfield(L, -2, "edge");
    if (n.dir)
        return;
    lua_pushnumber(L, (double) n.size);
    lua_setfield(L, -2, "size");
    lua_pushnumber(L, (double) n.chunk);
    lua_setfield(L, -2, "chunk");
    lua_pushnumber(L, (double) n.chunks);
    lua_setfield(L, -2, "chunks");
    lua_pushnumber(L, (double) n.version);
    lua_setfield(L, -2, "version");
    lua_pushlstring(L, n.type.data(), n.type.size());
    lua_setfield(L, -2, "type");
}

static int push_graph_rows(lua_State* L, const std::vector<barch::graph::row>& rows) {
    lua_createtable(L, (int) rows.size(), 0);
    int at = 1;
    for (const auto& r : rows) {
        push_graph_entry(L, r);
        lua_rawseti(L, -2, at++);
    }
    return 1;
}

/** a write's answer: true, since a refusal has raised by now */
static int graph_done(lua_State* L) {
    lua_pushboolean(L, true);
    return 1;
}

/** put(path, body [, type [, chunk]]) - GRAPH PUT */
static int graph_put(lua_State* L) {
    graph_command g(L, "put", "PUT");
    g.text(1).text(2);
    if (lua_isstring(L, 3))
        g.word("TYPE").text(3);
    if (size_t chunk = chunk_arg(L, 4, "barch.graph.put"))
        g.word("CHUNK").word(std::to_string(chunk));
    g.run();
    return graph_done(L);
}

/** get(path [, from [, len]]) - GRAPH GET: the bytes, or nil */
static int graph_get(lua_State* L) {
    graph_command g(L, "get", "GET");
    g.text(1)
     .number("FROM", 2, "from is a whole number, not negative")
     .number("LEN", 3, "len is a whole number, not negative");
    push_variable(L, g.run());
    return 1;
}

/** stat(path) - GRAPH STAT: the entry, or nil */
static int graph_stat(lua_State* L) {
    graph_command g(L, "stat", "STAT");
    g.text(1);
    std::vector<barch::graph::row> rows;
    g.run(&rows);
    if (rows.empty())
        lua_pushnil(L);
    else
        push_graph_entry(L, rows.front());
    return 1;
}

/*
 * list(path [, after [, limit [, offset]]]) - GRAPH LS. The cursor is the edge id
 * of the last entry of the page before, the way GRAPH LS AFTER takes it: a name
 * can't say which of its edges a page stopped at when it repeats (TODO 407). nil
 * or "" is the first page.
 */
static int graph_list(lua_State* L) {
    graph_command g(L, "list", "LS");
    g.text(1);
    if (!(lua_type(L, 2) == LUA_TSTRING && lua_objlen(L, 2) == 0))
        g.number("AFTER", 2, "after is the edge id of the last entry of a page");
    g.number("LIMIT", 3, "limit must not be negative")
     .number("OFFSET", 4, "offset must not be negative");
    std::vector<barch::graph::row> rows;
    g.run(&rows);
    return push_graph_rows(L, rows);
}

/** rename(from, to [, edge]) - one edge to another path, GRAPH MV */
static int graph_rename(lua_State* L) {
    graph_command g(L, "rename", "MV");
    g.text(1).text(2).number("EDGE", 3, "takes an edge id there", false).run();
    return graph_done(L);
}

/** unlink(path [, edge]) - one edge, and whatever it was the last route to: GRAPH UNLINK */
static int graph_unlink(lua_State* L) {
    graph_command g(L, "unlink", "UNLINK");
    g.text(1).number("EDGE", 2, "takes an edge id there", false).run();
    return graph_done(L);
}

/** remove(path [, recursive [, edge]]) and rm - the node and every edge naming it, GRAPH RM */
static int graph_remove(lua_State* L) {
    graph_command g(L, "remove", "RM");
    g.text(1);
    if (lua_isboolean(L, 2) && lua_toboolean(L, 2))
        g.word("RECURSIVE");
    g.number("EDGE", 3, "takes an edge id there", false).run();
    return graph_done(L);
}

/** copy(from, to [, type [, chunk]]) and cp - a byte copy, GRAPH CP */
static int graph_copy(lua_State* L) {
    graph_command g(L, "copy", "CP");
    g.text(1).text(2);
    if (lua_isstring(L, 3))
        g.word("TYPE").text(3);
    if (size_t chunk = chunk_arg(L, 4, "barch.graph.copy"))
        g.word("CHUNK").word(std::to_string(chunk));
    g.run();
    return graph_done(L);
}

static int graph_mkdir(lua_State* L) {
    graph_command g(L, "mkdir", "MKDIR");
    g.text(1).run();
    return graph_done(L);
}

/** link(id, path) - a second edge onto a node, GRAPH LINK */
static int graph_link(lua_State* L) {
    graph_command g(L, "link", "LINK");
    double id = lua_isnumber(L, 1) ? lua_tonumber(L, 1) : 0;
    if (id < 1 || id != std::floor(id) || id >= 1.8e19)
        luaL_error(L, "FUNCTION barch.graph.link takes a node id and a path");
    char buf[32];
    snprintf(buf, sizeof buf, "%llu", (unsigned long long) id);
    g.word(buf).text(2).run();
    return graph_done(L);
}

/*
 * bfs(start [, depth [, limit [, offset]]]) and dfs - GRAPH BFS and DFS. Each entry
 * is the node, read as the walk met it, so the walk is one call rather than one
 * plus N.
 */
static int graph_walk(lua_State* L, bool breadth) {
    graph_command g(L, breadth ? "bfs" : "dfs", breadth ? "BFS" : "DFS");
    const char* why = "depth, limit and offset must not be negative";
    g.text(1).number("DEPTH", 2, why).number("LIMIT", 3, why).number("OFFSET", 4, why);
    std::vector<barch::graph::row> rows;
    g.run(&rows);
    return push_graph_rows(L, rows);
}

static int graph_bfs(lua_State* L) { return graph_walk(L, true); }
static int graph_dfs(lua_State* L) { return graph_walk(L, false); }

// the lock parking pieces, defined with barch.call's further down
template<int (*F)(lua_State*, bool resumed)>
static int lock_parking_entry(lua_State* L);
template<int (*F)(lua_State*, bool resumed)>
static int lock_parking_cont(lua_State* L, int);
template<lua_CFunction F>
static int plain_call(lua_State* L, bool);

/*
 * Each one parks on a held lock the way barch.call does - TODO 731 - since it is a
 * command that can meet one.
 */
struct graph_function {
    const char* name;
    lua_CFunction entry;
    lua_Continuation cont;
};

template<lua_CFunction F>
constexpr graph_function graph_parking(const char* name) {
    return {name, lock_parking_entry<plain_call<F>>, lock_parking_cont<plain_call<F>>};
}

static const graph_function graph_functions[] = {
    graph_parking<graph_put>("put"),       graph_parking<graph_get>("get"),
    graph_parking<graph_stat>("stat"),     graph_parking<graph_list>("list"),
    graph_parking<graph_remove>("remove"), graph_parking<graph_remove>("rm"),
    graph_parking<graph_mkdir>("mkdir"),   graph_parking<graph_rename>("rename"),
    graph_parking<graph_link>("link"),     graph_parking<graph_unlink>("unlink"),
    graph_parking<graph_copy>("copy"),     graph_parking<graph_copy>("cp"),
    graph_parking<graph_bfs>("bfs"),       graph_parking<graph_dfs>("dfs"),
};

static int graph_space_table(lua_State* L);

/**
 * a barch.graph table: every function with the space it's bound to as its
 * upvalue, nil for the running space. Only the unbound one has `space`, so a
 * bound table can't be bound again to something else by accident - the same
 * rule the fs table follows.
 */
static void push_graph_table(lua_State* L, const char* space, size_t n) {
    lua_newtable(L);
    for (const auto& f : graph_functions) {
        if (space)
            lua_pushlstring(L, space, n);
        else
            lua_pushnil(L);
        lua_pushcclosurek(L, f.entry, f.name, 1, f.cont);
        lua_setfield(L, -2, f.name);
    }
    if (!space) {
        lua_pushcfunction(L, graph_space_table, "space");
        lua_setfield(L, -2, "space");
    }
    lua_setreadonly(L, -1, true);
}

/*
 * barch.graph.space(name) - the graph of another key space. The space is
 * opened now, not on first use, so a wrong name fails where it's written.
 */
static int graph_space_table(lua_State* L) {
    size_t n = 0;
    const char* name = luaL_checklstring(L, 1, &n);
    (void) fs_open(L, std::string(name, n), "space");
    push_graph_table(L, name, n);
    return 1;
}

static int barch_auth(lua_State* L) {
    auto* id = barch::functions::http_ident_tls();
    if (!id)
        luaL_error(L, "FUNCTION barch.auth is only available in HTTP handlers");
    if (id->pinned)
        luaL_error(L, "FUNCTION barch.auth cannot change a pinned route user");
    size_t ul = 0, sl = 0;
    const char* u = luaL_checklstring(L, 1, &ul);
    const char* pw = luaL_checklstring(L, 2, &sl);
    heap::vector<bool> acl;
    if (!authenticate_user(std::string(u, ul), std::string(pw, sl), acl)) {
        lua_pushboolean(L, 0);
        return 1;
    }
    id->user.assign(u, ul);
    id->acl = std::move(acl);
    if (id->sid.empty()) {
        static thread_local std::mt19937_64 rng{std::random_device{}()};
        static const char* hex = "0123456789abcdef";
        id->sid.assign(32, '0');
        for (int i = 0; i < 32; i += 2) {
            auto v = (uint8_t) rng();
            id->sid[i] = hex[v >> 4];
            id->sid[i + 1] = hex[v & 15];
        }
        id->sid_new = true;
    }
    if (id->space) {
        // a meta key, so only this makes a session - TODO 542
        std::string err;
        if (!barch::meta::set(id->space, "http:sess:" + id->sid, id->user, err)) {
            std::string msg = "FUNCTION could not store the HTTP session";
            if (!err.empty()) {
                msg += ": ";
                msg += err;
            }
            luaL_error(L, "%s", msg.c_str());
        }
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int barch_user(lua_State* L) {
    auto* id = barch::functions::http_ident_tls();
    if (!id || id->user.empty())
        lua_pushlstring(L, "", 0);
    else
        lua_pushlstring(L, id->user.data(), id->user.size());
    return 1;
}

/** the Raft log run this call is part of, or null when it isn't one - TODO 763 */
static const barch::functions::from_log* log_run_of(lua_State* L) {
    auto* rc = static_cast<run_ctx*>(lua_callbacks(L)->userdata);
    return rc && rc->from_log ? barch::functions::running_from_log() : nullptr;
}

static void no_time_in_entry(lua_State* L, const char* what) {
    luaL_error(L, "FUNCTION %s has no time to answer in a run from a log entry written before entries "
                  "carried one", what);
}

/**
 * barch.uuid() - a UUIDv7, which sorts by time as a key; see ordered_uuid - TODO 761.
 * In a run from a Raft log it's made from the entry, the same on every member - TODO 763.
 */
static int barch_uuid(lua_State* L) {
    std::string u;
    if (const auto* logged = log_run_of(L)) {
        u = logged->uuid();
        if (u.empty())
            no_time_in_entry(L, "barch.uuid");
    } else {
        u = barch::ordered_uuid();
    }
    lua_pushlstring(L, u.data(), u.size());
    return 1;
}

static void generate_uuid_bytes(lua_State* L, const char* fn, uint8_t out[16]) {
    if (const auto* logged = log_run_of(L)) {
        if (!logged->uuid_bytes(out))
            no_time_in_entry(L, fn);
    } else {
        barch::ordered_uuid_bytes(out);
    }
}

static size_t check_uuid_offset(lua_State* L, const char* fn, int idx) {
    const int t = lua_type(L, idx);
    if (t == LUA_TINTEGER) {
        const int64_t v = lua_tointeger64(L, idx, nullptr);
        if (v < 0)
            luaL_error(L, "FUNCTION %s: offset is out of range", fn);
        return static_cast<size_t>(v);
    }
    if (t == LUA_TNUMBER) {
        const double v = lua_tonumber(L, idx);
        if (std::isfinite(v) && v == std::floor(v) && v >= 0.0 && v <= 9007199254740992.0)
            return static_cast<size_t>(v);
        luaL_error(L, "FUNCTION %s: offset must be a non-negative integer", fn);
    }
    luaL_error(L, "FUNCTION %s: offset must be a number", fn);
    return 0;
}

static void read_uuid_arg(lua_State* L, const char* fn, int idx, int off_idx, uint8_t out[16]) {
    const int t = lua_type(L, idx);
    if (t == LUA_TBUFFER) {
        size_t len = 0;
        const auto* b = static_cast<const uint8_t*>(lua_tobuffer(L, idx, &len));
        if (off_idx > 0 && !lua_isnoneornil(L, off_idx)) {
            const size_t off = check_uuid_offset(L, fn, off_idx);
            if (off > len || len - off < 16)
                luaL_error(L, "FUNCTION %s: buffer too short for uuid at offset", fn);
            std::memcpy(out, b + off, 16);
            return;
        }
        if (len != 16)
            luaL_error(L, "FUNCTION %s: uuid buffer must be 16 bytes", fn);
        std::memcpy(out, b, 16);
        return;
    }
    if (t == LUA_TSTRING) {
        if (off_idx > 0 && !lua_isnoneornil(L, off_idx))
            luaL_error(L, "FUNCTION %s: offset only applies to a buffer", fn);
        size_t len = 0;
        const char* s = lua_tolstring(L, idx, &len);
        if (!barch::uuid_parse(out, s, len))
            luaL_error(L, "FUNCTION %s: invalid uuid string", fn);
        return;
    }
    luaL_error(L, "FUNCTION %s expects a uuid string or buffer", fn);
}

/**
 * barch.uuidAsBuffer([str]) - return a 16-byte Luau buffer.
 * Generates a new UUIDv7 when called with no argument or nil.
 * Parses a 36-char hyphenated or 32-char hex UUID string when given one.
 */
static int barch_uuid_as_buffer(lua_State* L) {
    if (lua_gettop(L) > 1)
        luaL_error(L, "FUNCTION barch.uuidAsBuffer takes at most one argument");
    uint8_t raw[16];
    if (lua_isnoneornil(L, 1)) {
        generate_uuid_bytes(L, "barch.uuidAsBuffer", raw);
    } else {
        if (lua_type(L, 1) != LUA_TSTRING)
            luaL_error(L, "FUNCTION barch.uuidAsBuffer expects a string");
        size_t len = 0;
        const char* s = lua_tolstring(L, 1, &len);
        if (!barch::uuid_parse(raw, s, len))
            luaL_error(L, "FUNCTION barch.uuidAsBuffer: invalid uuid string");
    }
    void* b = lua_newbuffer(L, 16);
    std::memcpy(b, raw, 16);
    return 1;
}

/**
 * barch.uuidToString(buf [, offset]) - format 16 UUID bytes from a buffer as a
 * 36-character lowercase hyphenated string.
 */
static int barch_uuid_to_string(lua_State* L) {
    const int top = lua_gettop(L);
    if (top < 1 || top > 2)
        luaL_error(L, "FUNCTION barch.uuidToString takes a buffer and an optional offset");
    if (lua_type(L, 1) != LUA_TBUFFER)
        luaL_error(L, "FUNCTION barch.uuidToString expects a buffer");
    size_t len = 0;
    const auto* b = static_cast<const uint8_t*>(lua_tobuffer(L, 1, &len));
    size_t off = 0;
    if (!lua_isnoneornil(L, 2)) {
        off = check_uuid_offset(L, "barch.uuidToString", 2);
        if (off > len || len - off < 16)
            luaL_error(L, "FUNCTION barch.uuidToString: buffer too short for uuid at offset");
    } else if (len != 16) {
        luaL_error(L, "FUNCTION barch.uuidToString: uuid buffer must be 16 bytes");
    }
    char out[36];
    barch::uuid_format(out, b + off);
    lua_pushlstring(L, out, 36);
    return 1;
}

/**
 * barch.uuidWrite(buf, offset [, str]) - write 16 UUID bytes into `buf` at `offset`.
 * Generates a new UUIDv7 when `str` is omitted or nil, or parses `str`.
 */
static int barch_uuid_write(lua_State* L) {
    const int top = lua_gettop(L);
    if (top < 2 || top > 3)
        luaL_error(L, "FUNCTION barch.uuidWrite takes a buffer, offset, and optional uuid string");
    if (lua_type(L, 1) != LUA_TBUFFER)
        luaL_error(L, "FUNCTION barch.uuidWrite expects a buffer");
    size_t len = 0;
    auto* b = static_cast<uint8_t*>(lua_tobuffer(L, 1, &len));
    const size_t off = check_uuid_offset(L, "barch.uuidWrite", 2);
    if (off > len || len - off < 16)
        luaL_error(L, "FUNCTION barch.uuidWrite: buffer too short for uuid at offset");
    uint8_t raw[16];
    if (lua_isnoneornil(L, 3)) {
        generate_uuid_bytes(L, "barch.uuidWrite", raw);
    } else {
        if (lua_type(L, 3) != LUA_TSTRING)
            luaL_error(L, "FUNCTION barch.uuidWrite expects a string");
        size_t slen = 0;
        const char* s = lua_tolstring(L, 3, &slen);
        if (!barch::uuid_parse(raw, s, slen))
            luaL_error(L, "FUNCTION barch.uuidWrite: invalid uuid string");
    }
    std::memcpy(b + off, raw, 16);
    return 0;
}

/**
 * barch.uuidTime(uuid [, offset]) - return the Unix timestamp in ms from a UUIDv7
 * string or buffer. Raises if the UUID is not version 7 with RFC 9562 variant.
 */
static int barch_uuid_time(lua_State* L) {
    const int top = lua_gettop(L);
    if (top < 1 || top > 2)
        luaL_error(L, "FUNCTION barch.uuidTime takes a uuid and an optional offset");
    uint8_t raw[16];
    read_uuid_arg(L, "barch.uuidTime", 1, 2, raw);
    uint64_t ms = 0;
    if (!barch::uuid_time_ms(raw, ms))
        luaL_error(L, "FUNCTION barch.uuidTime: not a UUIDv7");
    lua_pushnumber(L, static_cast<double>(ms));
    return 1;
}

/**
 * barch.uuidVersion(uuid [, offset]) - return the 4-bit version number (0..15)
 * of a UUID string or buffer.
 */
static int barch_uuid_version(lua_State* L) {
    const int top = lua_gettop(L);
    if (top < 1 || top > 2)
        luaL_error(L, "FUNCTION barch.uuidVersion takes a uuid and an optional offset");
    uint8_t raw[16];
    read_uuid_arg(L, "barch.uuidVersion", 1, 2, raw);
    lua_pushnumber(L, static_cast<double>(barch::uuid_version(raw)));
    return 1;
}

/**
 * barch.uuidCompare(a, b) - compare two UUIDs (string or 16-byte buffer) in
 * unsigned big-endian byte order. Returns -1, 0, or 1.
 */
static int barch_uuid_compare(lua_State* L) {
    if (lua_gettop(L) != 2)
        luaL_error(L, "FUNCTION barch.uuidCompare takes two uuids");
    uint8_t a[16], b[16];
    read_uuid_arg(L, "barch.uuidCompare", 1, 0, a);
    read_uuid_arg(L, "barch.uuidCompare", 2, 0, b);
    const int cmp = std::memcmp(a, b, 16);
    lua_pushnumber(L, cmp < 0 ? -1.0 : (cmp > 0 ? 1.0 : 0.0));
    return 1;
}

/**
 * barch.time() - the Unix time in ms, as a number - TODO 763. In a run from a Raft log
 * it's the time the entry was logged at, so every member answers the same, and it
 * doesn't move during the run.
 */
static int barch_time(lua_State* L) {
    if (const auto* logged = log_run_of(L)) {
        if (logged->ms < 0)
            no_time_in_entry(L, "barch.time");
        lua_pushnumber(L, (double) logged->ms);
        return 1;
    }
    lua_pushnumber(L, (double) std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    return 1;
}

/** barch.running() - the canonical name of that space, empty for the default */
/*
 * A call that can meet a held key lock, parked on it rather than waiting - TODO 731,
 * see lock_parking in driver.h. `F` runs with the wait on this thread. If a lock it
 * needs is held it unwinds with lock_busy_unwind, having done nothing yet, and the
 * coroutine parks on that lock. Woken, the continuation runs F again from the top with
 * the same arguments, and F parks again or goes on. Once it goes on (or fails), the
 * lock that woke it passes the turn to the next one waiting there.
 *
 * Pushed with lua_pushcclosurek, so the continuation is there to resume into.
 */
template<int (*F)(lua_State*, bool resumed)>
static int lock_parking_call(lua_State* L, bool resumed) {
    auto* rc = call_of(L);
    if (!rc || !rc->owner)
        return F(L, resumed);           // nowhere to park: a foreign fill
    const int args = lua_gettop(L);
    lock_wait wait{L, rc->lock_until};
    lock_wait* was = script_lock_wait();
    script_lock_wait() = &wait;
    struct restore {
        lock_wait* was;
        ~restore() { script_lock_wait() = was; }
    };
    lock_parking_ptr busy;
    try {
        restore r{was};
        int n = F(L, resumed);
        if (rc->lock_woken) {
            rc->lock_woken->went_on();
            rc->lock_woken.reset();
        }
        rc->lock_until = {};
        return n;
    } catch (lock_busy_unwind& b) {
        busy = std::move(b.parking);
    } catch (...) {
        if (rc->lock_woken) {
            rc->lock_woken->went_on();
            rc->lock_woken.reset();
        }
        rc->lock_until = {};
        throw;
    }
    // moved on to another lock: the one that woke it isn't waited on any more, and the
    // next one there may well be able to go on (a reader behind a reader, say). Or
    // in the way of a parked writer on the same lock, which may go on - TODO 869
    if (rc->lock_woken && (rc->lock_woken->key() != busy->key() || busy->behind_parked()))
        rc->lock_woken->went_on();
    rc->lock_woken = busy;
    rc->lock_until = wait.until;
    auto parked = park_call(L);
    if (!parked)
        luaL_error(L, "FUNCTION a lock wait couldn't park");     // can_park said it could
    lua_settop(L, args);
    busy->park([parked] { complete_call(parked, nullptr); });
    return park_yield(L);
}

template<lua_CFunction F>
static int plain_call(lua_State* L, bool) {
    return F(L);
}

template<int (*F)(lua_State*, bool resumed)>
static int lock_parking_entry(lua_State* L) {
    return lock_parking_call<F>(L, false);
}

template<int (*F)(lua_State*, bool resumed)>
static int lock_parking_cont(lua_State* L, int) {
    return lock_parking_call<F>(L, true);
}

// `F` as a C function that parks on a held lock
template<lua_CFunction F>
static void push_lock_parking(lua_State* L, const char* name) {
    lua_pushcclosurek(L, lock_parking_entry<plain_call<F>>, name, 0, lock_parking_cont<plain_call<F>>);
}

// a space handle's method: what it was called as is kept for the continuation
static int space_namecall_parking(lua_State* L, bool resumed) {
    auto* rc = call_of(L);
    const char* m = nullptr;
    if (resumed && rc)
        m = rc->method;
    else {
        m = lua_namecallatom(L, nullptr);
        if (rc)
            rc->method = m;     // an atom: the string lives as long as the code naming it
    }
    if (!m)
        luaL_error(L, "FUNCTION no such method on a key space");
    return space_namecall_as(L, m);
}



/*
 * barch.tx - transactions from a stored function, TODO 732. begin, commit and abort
 * are BEGINTX, COMMITTX and ABORTTX through barch.call, so they behave exactly as the
 * commands do (and yield the same way). open() says whether one is open on the
 * session. run(fn, pessimistic, ...) is fn as one transaction, run again from the top
 * on TXDEADLOCK, TXLOCKTIMEOUT or TXCONFLICT. savepoint(fn, ...) is fn inside one
 * that's open, with what it wrote taken back if it raises (TODO 766).
 *
 * It's written in Luau because run calls back into the script, and a yield in there
 * has to get back out through it. A C function in the middle would stop that.
 */
static int tx_is_open(lua_State* L) {
    space_state* st = state_of(L);
    const store_access* s = st ? st->store() : nullptr;
    lua_pushboolean(L, s && s->in_transaction && s->in_transaction());
    return 1;
}

// the BEGINTX tx.run is about to make, marked as run's before it opens - TODO 743, 758
static int tx_mark_run(lua_State* L) {
    space_state* st = state_of(L);
    const store_access* s = st ? st->store() : nullptr;
    if (s && s->mark_run)
        s->mark_run(lua_toboolean(L, 1) != 0);
    return 0;
}

// a savepoint in the open transaction: its depth, or 0 when none is open - TODO 766
static int tx_savepoint_open(lua_State* L) {
    space_state* st = state_of(L);
    const store_access* s = st ? st->store() : nullptr;
    lua_pushinteger(L, (int) (s && s->savepoint_open ? s->savepoint_open() : 0));
    return 1;
}

// the savepoint at depth (1) ends: kept when (2) is true, otherwise taken back. Nil, or
// why it couldn't be
static int tx_savepoint_close(lua_State* L) {
    space_state* st = state_of(L);
    const store_access* s = st ? st->store() : nullptr;
    const auto depth = (size_t) luaL_checkinteger(L, 1);
    std::string err;
    if (s && s->savepoint_close && !s->savepoint_close(depth, lua_toboolean(L, 2) != 0, err)) {
        lua_pushlstring(L, err.data(), err.size());
        return 1;
    }
    lua_pushnil(L);
    return 1;
}

static const char* tx_source = R"luau(
local barch, is_open, mark_run, savepoint_open, savepoint_close = ...
local call, pcall, error, tostring, find = barch.call, pcall, error, tostring, string.find
local pack, unpack = table.pack, table.unpack
local TRIES = 10

local tx = {}

function tx.begin(pessimistic)
    if pessimistic then
        return call("BEGINTX", "PESSIMISTIC")
    end
    return call("BEGINTX")
end

function tx.commit()
    return call("COMMITTX")
end

function tx.abort()
    return call("ABORTTX")
end

function tx.open()
    return is_open()
end

local function retryable(e)
    e = tostring(e)
    return find(e, "TXDEADLOCK", 1, true) or find(e, "TXLOCKTIMEOUT", 1, true)
        or find(e, "TXCONFLICT", 1, true)
end

--[[
fn(...) as one transaction, and what it returned once committed. If fn ends the
transaction itself (abort, say), what it returned comes back without a commit. Any
error that isn't worth another try is raised as it came, after an abort.

Inside a transaction someone else opened (a client's BEGINTX, say) fn just runs in
it: the commit, and any retry, are theirs.
]]
function tx.run(fn, pessimistic, ...)
    if is_open() then
        return fn(...)
    end
    for _ = 1, TRIES do
        -- a timeout can stop run before its abort below; the server aborts it then.
        -- Marked before BEGINTX, so there's no moment it's open and not run's
        mark_run(true)
        local began, e = pcall(tx.begin, pessimistic)
        mark_run(false)
        if not began then
            error(e, 0)
        end
        local ok, result = pcall(fn, ...)
        if ok then
            if not is_open() then
                return result
            end
            local committed, why = pcall(call, "COMMITTX")
            if committed then
                return result
            end
            result = why
        end
        if is_open() then
            pcall(call, "ABORTTX")
        end
        if not retryable(result) then
            error(result, 0)
        end
    end
    error("TXRETRY gave up after " .. TRIES .. " tries: call again after a pause", 0)
end

--[[
fn(...) as a savepoint in the open transaction - TODO 766: what fn returned. When fn
raises, every key it wrote goes back to what the transaction held for it before, and
the error is raised again as it came. What fn read stays read, so COMMITTX still checks
it. Nested ones work the same way: a kept inner one is taken back with the outer one.
]]
function tx.savepoint(fn, ...)
    local depth = savepoint_open()
    if depth == 0 then
        error("ERR barch.tx.savepoint needs an open transaction", 2)
    end
    local result = pack(pcall(fn, ...))
    local failed = savepoint_close(depth, result[1])
    if failed then
        error(failed, 0)
    end
    if not result[1] then
        error(result[2], 0)
    end
    return unpack(result, 2, result.n)
end

return tx
)luau";

// barch.tx, built from tx_source and left on the stack. The barch table is at `at`
static void push_tx_table(lua_State* L, int at) {
    static const std::string bytecode = [] {
        size_t n = 0;
        char* bc = luau_compile(tx_source, strlen(tx_source), nullptr, &n);
        std::string out(bc, n);
        free(bc);
        return out;
    }();
    at = lua_absindex(L, at);
    if (luau_load(L, "=barch.tx", bytecode.data(), bytecode.size(), 0) != 0)
        lua_error(L);
    lua_pushvalue(L, at);
    lua_pushcfunction(L, tx_is_open, "open");
    lua_pushcfunction(L, tx_mark_run, "mark_run");
    lua_pushcfunction(L, tx_savepoint_open, "savepoint_open");
    lua_pushcfunction(L, tx_savepoint_close, "savepoint_close");
    lua_call(L, 5, 1);
    lua_setreadonly(L, -1, true);
}

static int running_space_name(lua_State* L) {
    auto* rc = call_of(L);
    if (!rc) {
        lua_pushlstring(L, "", 0);
        return 1;
    }
    lua_pushlstring(L, rc->running.data(), rc->running.size());
    return 1;
}

/** barch.space.NAME - the handle, built once per space per call */
static int space_open(lua_State* L) {
    size_t n = 0;
    const char* raw = luaL_checklstring(L, 2, &n);
    std::string name(raw, n);
    space_state* st = state_of(L);
    if (!st || !st->open_space())
        luaL_error(L, "FUNCTION barch.space is not available here");
    if (!st->opened())
        luaL_error(L, "FUNCTION barch.space is not available here");
    refuse_other_space(L, name);
    auto have = st->opened()->find(name);
    if (have == st->opened()->end()) {
        auto opened = std::make_unique<store_access>();
        // an unknown name is not a key space and must not become one
        if (!(*st->open_space())(name, *opened))
            luaL_error(L, "FUNCTION no key space called %s", name.c_str());
        have = st->opened()->emplace(name, std::move(opened)).first;
    }
    space_ref ref;
    ref.name = name;
    ref.cached = have->second.get();
    ref.from = st->call->iface;
    push_space_handle(L, std::move(ref));
    return 1;
}

/*
 * barch.art() - a private one-shard space, same store_access as barch.store.
 *
 * Not named, not saved, not in the space map. The handle owns it; when Luau
 * collects the userdata the shard goes with it. min/max/range are the ordered
 * operations a working set (an HNSW candidate queue, a priority queue) needs
 * without walking the live store.
 */
static int art_open(lua_State* L) {
    // shared, not owned by the handle: a walk or a container made from it keeps it
    // alive after the handle itself is collected - TODO 431
    auto held = std::make_shared<scratch_space>();
    held->space = barch::key_space::make_scratch();
    held->access = barch::functions::store_for_owner(held->space);
    space_ref ref;
    ref.scratch = std::shared_ptr<const store_access>(held, &held->access);
    push_space_handle(L, std::move(ref));
    return 1;
}

/*
 * barch.call("GET", "k") - run an ordinary command and hand back its reply.
 *
 * Named on the barch table rather than as a bare `call`, which is the name a script
 * gives its own entry point. A refusal or a failed command is raised as a Lua error,
 * so a script that wants to survive one wraps it in pcall - the same split redis has
 * between redis.call and redis.pcall.
 */
/*
 * barch.queue(name, message) - put a message on a declared queue, TODO 366.
 *
 * Answers the sequence the message was given, which is the same number the
 * handler is told, so a publisher and a handler can talk about the same message
 * even though delivery is at-least-once.
 *
 * It raises rather than answering false on a failure - no such queue, nowhere
 * to write, the write itself failing. The whole promise of a queue is that an
 * accepted message survives the process, so a publish that did not happen must
 * not be something a script can walk past without noticing. A script that wants
 * to decide for itself wraps it in pcall.
 *
 * Refused inside a locked region for the same reason barch.call is, and it is
 * not only consistency: publishing to a queue nothing has opened yet reads the
 * declarations out of the configuration space, which takes shard latches, and
 * taking those while a region is locked is how a deadlock starts.
 */
static int barch_queue(lua_State* L) {
    // a run from the log may push to a queue on its own space, which mq::publish
    // decides: every member writes the same message to its copy - TODO 682
    if (auto* rc = static_cast<run_ctx*>(lua_callbacks(L)->userdata); rc && rc->locked)
        luaL_error(L, "FUNCTION barch.queue is not allowed inside a locked region");
    // a message is handled later, by another function in its own context, so it can't be
    // part of the transaction; pushing it at COMMITTX is the way to take it later
    refuse_in_transaction(L, "barch.queue");
    size_t nn = 0;
    const char* name = luaL_checklstring(L, 1, &nn);
    size_t dn = 0;
    // checklstring, so a number is accepted and written as its digits, the same
    // as barch.call does with its arguments
    const char* data = luaL_checklstring(L, 2, &dn);
    if (!name || !data)
        luaL_error(L, "FUNCTION barch.queue takes a queue name and a message");
    uint64_t sequence = 0;
    std::string err;
    if (!barch::mq::publish(std::string(name, nn), std::string(data, dn), sequence, err))
        luaL_error(L, "%s", err.c_str());
    lua_pushinteger64(L, (int64_t) sequence);
    return 1;
}

/*
 * Run the command whose name is at stack slot `first`, with the rest as its
 * arguments, in `space` - empty for the function's own - and push the reply. What
 * barch.call is, and what `sp:call` on a barch.space handle is with the handle's
 * space - TODO 378. `what` names the caller in the messages.
 */
static int run_script_command(lua_State* L, int first, const std::string& space,
                              const char* what) {
    if (auto* rc = static_cast<run_ctx*>(lua_callbacks(L)->userdata); rc && rc->locked)
        luaL_error(L, "FUNCTION %s is not allowed inside a locked region", what);
    int n = lua_gettop(L);
    if (n < first)
        luaL_error(L, "FUNCTION %s needs a command name", what);
    space_state* st = state_of(L);
    if (!st || !st->run_command())
        luaL_error(L, "FUNCTION %s is not available here", what);
    heap::vector<std::string> argv;
    argv.reserve(n - first + 1);
    for (int i = first; i <= n; ++i) {
        // numbers are accepted and written the way the wire would carry them, so
        // barch.call("SET", "k", 1) does not have to say tostring(1)
        if (lua_type(L, i) == LUA_TNUMBER) {
            double d = lua_tonumber(L, i);
            double whole = 0;
            char buf[40];
            if (std::modf(d, &whole) == 0.0)
                snprintf(buf, sizeof buf, "%lld", (long long) d);
            else
                snprintf(buf, sizeof buf, "%.17g", d);
            argv.emplace_back(buf);
            continue;
        }
        size_t len = 0;
        const char* s = lua_tolstring(L, i, &len);
        if (!s)
            luaL_error(L, "FUNCTION %s takes strings and numbers", what);
        argv.emplace_back(s, len);
    }
    Variable out;
    run_script_argv(L, space, argv, what, out, false);
    push_variable(L, out);
    return 1;
}

/*
 * Run `argv` the way barch.call does, or raise. barch.graph comes through here too,
 * having turned its arguments into a GRAPH command - TODO 756 - so it gets every check
 * barch.call gets, and the transaction handling with them. `name_errors` puts `what` in
 * front of a command's own error, so barch.graph's still say which function failed.
 */
static void run_script_argv(lua_State* L, const std::string& space, const heap::vector<std::string>& argv,
                            const char* what, Variable& out, bool name_errors) {
    if (auto* rc = static_cast<run_ctx*>(lua_callbacks(L)->userdata); rc && rc->locked)
        luaL_error(L, "FUNCTION %s is not allowed inside a locked region", what);
    space_state* st = state_of(L);
    if (!st || !st->run_command())
        luaL_error(L, "FUNCTION %s is not available here", what);
    // a run from the log runs only what does the same on every member - TODO 673
    if (auto* rc = static_cast<run_ctx*>(lua_callbacks(L)->userdata); rc && rc->from_log) {
        const std::string in = !space.empty() ? space
                               : (st->call && st->call->iface ? st->call->iface->running_in : std::string());
        if (!space.empty())
            refuse_other_space(L, space);
        auto current = !in.empty() && barch::keyspace_exists(in) ? barch::get_keyspace(in) : nullptr;
        const auto why = barch::functions::refused_from_log(current, std::vector<std::string>(argv.begin(), argv.end()));
        if (!why.empty())
            luaL_error(L, "FUNCTION a function run from the log can't run this: %s - give it "
                          "--@barch {\"replicate\": \"effects\"}", why.c_str());
    }
    // a function that said it only reads runs no command that writes - TODO 672
    if (running_read_only(L)) {
        const std::string in = !space.empty() ? space
                               : (st->call && st->call->iface ? st->call->iface->running_in : std::string());
        auto current = !in.empty() && barch::keyspace_exists(in) ? barch::get_keyspace(in) : nullptr;
        if (barch::functions::command_writes(current, std::vector<std::string>(argv.begin(), argv.end())))
            luaL_error(L, "FUNCTION declared \"write\": false in its --@barch header, so it can't run %s",
                       argv[0].c_str());
    }
    std::string err;
    // taken before: a nested call on this state clears the state's pointer when it ends
    auto* rc = static_cast<run_ctx*>(lua_callbacks(L)->userdata);
    const bool ran = (*st->run_command())(space, argv, out, err);
    // a command can take as long as it likes - a CALLF, say - so the next interrupt
    // reads the clock instead of waiting its turn - TODO 809
    if (rc)
        rc->clock_wait = 0;
    if (!ran) {
        /*
         * A depth refusal is raised without position information, and every other
         * error keeps it. `luaL_error` prefixes the chunk and line, which is worth
         * having once - but a hundred nested levels each prefixing the one below
         * builds a message of nothing but prefixes, and the reason falls off the end
         * when the reply is truncated. Observed as a screen of "RECURSE:3: ERR "
         * with the actual cause nowhere in it. See TODO 98 E.
         */
        if (err.find(barch::foreign::too_deep_marker) != std::string::npos) {
            lua_pushlstring(L, err.data(), err.size());
            lua_error(L);
        }
        if (name_errors && err.rfind("FUNCTION", 0) != 0)
            luaL_error(L, "FUNCTION %s %s", what, err.c_str());
        luaL_error(L, "%s", err.empty() ? "FUNCTION call failed" : err.c_str());
    }
}

static int barch_call(lua_State* L) {
    return run_script_command(L, 1, std::string(), "barch.call");
}

/*
 * What a function returned, as a Variable. The shapes are fixed here once and are not
 * meant to move afterwards - a client that has to guess which of two encodings it is
 * getting cannot be written against.
 */
bool to_variable(lua_State* L, int idx, Variable& out, std::string& err, int depth) {
    if (depth > 8) {
        err = "FUNCTION return nests too deeply";
        return false;
    }
    switch (lua_type(L, idx)) {
        case LUA_TNONE:     // an index that holds nothing, not a value that is nil
        case LUA_TNIL:
            out = Variable(nullptr);
            return true;
        case LUA_TBOOLEAN:
            // redis answers 1 for true and nil for false, and a script written against
            // redis will expect that rather than a RESP boolean
            if (lua_toboolean(L, idx))
                out = Variable((int64_t) 1);
            else
                out = Variable(nullptr);
            return true;
        case LUA_TINTEGER:
            // a real 64 bit integer, which is why the library is open at all - it must
            // not go out through the double case and lose the bottom bits
            // lua_tointegerx is the 32 bit accessor and silently keeps the low word,
            // which turned 2^62 into 0. lua_tointeger64 is the one that means it
            out = Variable(lua_tointeger64(L, idx, nullptr));
            return true;
        case LUA_TNUMBER: {
            double d = lua_tonumber(L, idx);
            double whole = 0;
            if (std::modf(d, &whole) == 0.0 && d >= -9.2233720368547758e18
                && d <= 9.2233720368547758e18)
                out = Variable((int64_t) d);
            else
                out = Variable(d);
            return true;
        }
        /*
         * Marked as bulk strings - TODO 414. A var_string that starts with `$` is a
         * bulk string and every reader strips that one byte (Variable::to_string,
         * bulk_vt, the wire writers), so a Luau string stored raw that really began
         * with `$` lost it: `return "$x"` answered `x`, and an fs_source body
         * `$100 price` was stored as `100 price`. With the marker the strip takes
         * the marker off and the bytes stay. A script's strings go out as bulk
         * strings now, which is what redis does with a Lua string too.
         */
        case LUA_TSTRING: {
            size_t n = 0;
            const char* s = lua_tolstring(L, idx, &n);
            std::string v;
            v.reserve(n + 1);
            v.push_back('$');
            v.append(s, n);
            out = Variable(std::move(v));
            return true;
        }
        case LUA_TBUFFER: {
            size_t n = 0;
            void* b = lua_tobuffer(L, idx, &n);
            std::string v;
            v.reserve(n + 1);
            v.push_back('$');
            v.append(b ? static_cast<const char*>(b) : "", n);
            out = Variable(std::move(v));
            return true;
        }
        case LUA_TTABLE: {
            // {err = "..."} and {ok = "..."} first, as redis does, so a script can
            // answer an error without the host having to invent one
            lua_getfield(L, idx, "err");
            if (lua_type(L, -1) == LUA_TSTRING) {
                size_t n = 0;
                const char* s = lua_tolstring(L, -1, &n);
                err.assign(s, n);
                lua_pop(L, 1);
                return false;
            }
            lua_pop(L, 1);
            lua_getfield(L, idx, "ok");
            if (lua_type(L, -1) == LUA_TSTRING) {
                size_t n = 0;
                const char* s = lua_tolstring(L, -1, &n);
                // a simple string, unmarked - unless it starts with `$`, which a
                // reader would strip as the marker. That one goes out as a bulk
                // string instead, so no byte is lost - TODO 414
                std::string v;
                if (n && s[0] == '$')
                    v.push_back('$');
                v.append(s, n);
                out = Variable(std::move(v));
                lua_pop(L, 1);
                return true;
            }
            lua_pop(L, 1);
            heap::vector<wrapped_variable_t> items;
            for (int i = 1;; ++i) {
                lua_rawgeti(L, idx, i);
                if (lua_type(L, -1) == LUA_TNIL) {
                    lua_pop(L, 1);
                    break;
                }
                Variable item;
                if (!to_variable(L, lua_gettop(L), item, err, depth + 1)) {
                    lua_pop(L, 1);
                    return false;
                }
                items.emplace_back(item);
                lua_pop(L, 1);
            }
            out = Variable(items);
            return true;
        }
        default:
            err = "FUNCTION returned something that is not a value";
            return false;
    }
}


void install_barch_bindings(lua_State* L) {
    lua_newtable(L);
    push_lock_parking<barch_call>(L, "call");
    lua_setfield(L, -2, "call");
    lua_pushcfunction(L, barch_queue, "queue");
    lua_setfield(L, -2, "queue");
    // the space's own settings. Named `config` because `space` is now the key space
    // itself rather than a function describing it
    lua_pushcfunction(L, store_space, "config");
    lua_setfield(L, -2, "config");
    make_tomb(L);
    push_tomb(L);
    lua_setfield(L, -2, "tomb");
    lua_newtable(L);
    push_lock_parking<store_get>(L, "get");
    lua_setfield(L, -2, "get");
    push_lock_parking<store_fetch>(L, "fetch");
    lua_setfield(L, -2, "fetch");
    push_lock_parking<store_set>(L, "set");
    lua_setfield(L, -2, "set");
    push_lock_parking<store_remove>(L, "remove");
    lua_setfield(L, -2, "remove");
    push_lock_parking<store_get_json>(L, "getJson");
    lua_setfield(L, -2, "getJson");
    push_lock_parking<store_set_json>(L, "setJson");
    lua_setfield(L, -2, "setJson");
    lua_pushcfunction(L, store_parse_json, "parseJson");
    lua_setfield(L, -2, "parseJson");
    lua_pushcfunction(L, store_locked, "locked");
    lua_setfield(L, -2, "locked");
    lua_pushcfunction(L, store_shard_number, "shardNumber");
    lua_setfield(L, -2, "shardNumber");
    lua_pushcfunction(L, store_has_lock, "hasLock");
    lua_setfield(L, -2, "hasLock");
    push_lock_parking<store_exists>(L, "exists");
    lua_setfield(L, -2, "exists");
    lua_pushcfunction(L, store_count, "count");
    lua_setfield(L, -2, "count");
    lua_pushcfunction(L, store_loose_count, "looseCount");
    lua_setfield(L, -2, "looseCount");
    lua_pushcfunction(L, store_range, "range");
    lua_setfield(L, -2, "range");
    lua_pushcfunction(L, store_range_values, "rangeValues");
    lua_setfield(L, -2, "rangeValues");
    lua_pushcfunction(L, store_min, "min");
    lua_setfield(L, -2, "min");
    lua_pushcfunction(L, store_max, "max");
    lua_setfield(L, -2, "max");
    push_lock_parking<store_get_buffer_at>(L, "getBufferAt");
    lua_setfield(L, -2, "getBufferAt");
    push_lock_parking<store_get_buffers>(L, "getBuffers");
    lua_setfield(L, -2, "getBuffers");
    // the in-place writes lock in a pessimistic transaction now, so they park too (TODO 736)
    push_lock_parking<store_set_buffer_at>(L, "setBufferAt");
    lua_setfield(L, -2, "setBufferAt");
    push_lock_parking<store_get_int32_at>(L, "getInt32At");
    lua_setfield(L, -2, "getInt32At");
    push_lock_parking<store_set_int32_at>(L, "setInt32At");
    lua_setfield(L, -2, "setInt32At");
    push_lock_parking<store_get_int64_at>(L, "getInt64At");
    lua_setfield(L, -2, "getInt64At");
    push_lock_parking<store_set_int64_at>(L, "setInt64At");
    lua_setfield(L, -2, "setInt64At");
    push_lock_parking<store_incr_int32_by_at>(L, "incrInt32ByAt");
    lua_setfield(L, -2, "incrInt32ByAt");
    push_lock_parking<store_incr_int64_by_at>(L, "incrInt64ByAt");
    lua_setfield(L, -2, "incrInt64ByAt");
    push_lock_parking<store_get_f64_at>(L, "getF64At");
    lua_setfield(L, -2, "getF64At");
    push_lock_parking<store_get_f32_at>(L, "getF32At");
    lua_setfield(L, -2, "getF32At");
    push_lock_parking<store_set_f64_at>(L, "setF64At");
    lua_setfield(L, -2, "setF64At");
    push_lock_parking<store_set_f32_at>(L, "setF32At");
    lua_setfield(L, -2, "setF32At");
    lua_pushcfunction(L, store_size, "size");
    lua_setfield(L, -2, "size");
    lua_pushcfunction(L, store_leaves, "leaves");
    lua_setfield(L, -2, "leaves");
    lua_pushcfunction(L, store_nodes, "nodes");
    lua_setfield(L, -2, "nodes");
    lua_pushcfunction(L, store_free_list, "freeList");
    lua_setfield(L, -2, "freeList");
    lua_pushcfunction(L, store_stats, "stats");
    lua_setfield(L, -2, "stats");
    lua_pushcfunction(L, store_save, "save");
    lua_setfield(L, -2, "save");
    lua_pushcfunction(L, store_load, "load");
    lua_setfield(L, -2, "load");
    lua_pushcfunction(L, store_get_dictionary, "getDictionary");
    lua_setfield(L, -2, "getDictionary");
    lua_pushcfunction(L, store_set_dictionary, "setDictionary");
    lua_setfield(L, -2, "setDictionary");
    lua_setfield(L, -2, "store");
    push_vec_table(L);
    lua_setfield(L, -2, "vec");
    lua_pushcfunction(L, art_open, "art");
    lua_setfield(L, -2, "art");
    lua_pushcfunction(L, current_space_handle, "current");
    lua_setfield(L, -2, "current");
    lua_pushcfunction(L, running_space_name, "running");
    lua_setfield(L, -2, "running");
    lua_pushcfunction(L, barch_auth, "auth");
    lua_setfield(L, -2, "auth");
    lua_newtable(L);
    lua_pushcfunction(L, ws_send_frame, "send");
    lua_setfield(L, -2, "send");
    lua_pushcfunction(L, ws_close_socket, "close");
    lua_setfield(L, -2, "close");
    lua_setfield(L, -2, "ws");

    // barch.fs - the file store as paths rather than as keys, TODO 256. Bound to
    // nothing, so it works on the running space; barch.fs.space(name) builds the
    // same table bound to another one - TODO 374
    push_fs_table(L, nullptr, 0);
    lua_setfield(L, -2, "fs");
    // barch.graph - the graph store as paths, TODO 382. Same binding rule:
    // the running space, or barch.graph.space(name) for another one.
    push_graph_table(L, nullptr, 0);
    lua_setfield(L, -2, "graph");
    lua_pushcfunction(L, barch_user, "user");
    lua_setfield(L, -2, "user");
    lua_pushcfunction(L, barch_uuid, "uuid");
    lua_setfield(L, -2, "uuid");
    lua_pushcfunction(L, barch_uuid_as_buffer, "uuidAsBuffer");
    lua_setfield(L, -2, "uuidAsBuffer");
    lua_pushcfunction(L, barch_uuid_to_string, "uuidToString");
    lua_setfield(L, -2, "uuidToString");
    lua_pushcfunction(L, barch_uuid_write, "uuidWrite");
    lua_setfield(L, -2, "uuidWrite");
    lua_pushcfunction(L, barch_uuid_time, "uuidTime");
    lua_setfield(L, -2, "uuidTime");
    lua_pushcfunction(L, barch_uuid_version, "uuidVersion");
    lua_setfield(L, -2, "uuidVersion");
    lua_pushcfunction(L, barch_uuid_compare, "uuidCompare");
    lua_setfield(L, -2, "uuidCompare");
    lua_pushcfunction(L, barch_time, "time");
    lua_setfield(L, -2, "time");

    // a key space read and written as a value - see TODO 98 F2. The three handle
    // kinds go by tag: the tag carries the destructor and the metatable, and the
    // metatable is locked so a script can't reach the functions in it - TODO 572
    lua_setuserdatadtor(L, tag_space,
        [](lua_State*, void* p) { static_cast<space_handle*>(p)->~space_handle(); });
    lua_setuserdatadtor(L, tag_container,
        [](lua_State*, void* p) { static_cast<container_handle*>(p)->~container_handle(); });
    lua_setuserdatadtor(L, tag_row,
        [](lua_State*, void* p) { static_cast<row_cursor*>(p)->~row_cursor(); });
    lua_newtable(L);
    lua_pushcfunction(L, space_read, "__index");
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, space_write, "__newindex");
    lua_setfield(L, -2, "__newindex");
    lua_pushcfunction(L, space_iter, "__iter");
    lua_setfield(L, -2, "__iter");
    lua_pushcclosurek(L, lock_parking_entry<space_namecall_parking>, "__namecall", 0,
                      lock_parking_cont<space_namecall_parking>);
    lua_setfield(L, -2, "__namecall");
    lua_pushstring(L, "locked");
    lua_setfield(L, -2, "__metatable");
    lua_setreadonly(L, -1, true);
    lua_setuserdatametatable(L, tag_space);

    // a list, hash or ordered set, read and written the same way a space is
    lua_newtable(L);
    lua_pushcfunction(L, container_read, "__index");
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, container_write, "__newindex");
    lua_setfield(L, -2, "__newindex");
    lua_pushcfunction(L, container_iter, "__iter");
    lua_setfield(L, -2, "__iter");
    lua_pushstring(L, "locked");
    lua_setfield(L, -2, "__metatable");
    lua_setreadonly(L, -1, true);
    lua_setuserdatametatable(L, tag_container);

    lua_newtable(L);
    lua_pushcfunction(L, row_read, "__index");
    lua_setfield(L, -2, "__index");
    lua_pushstring(L, "locked");
    lua_setfield(L, -2, "__metatable");
    lua_setreadonly(L, -1, true);
    lua_setuserdatametatable(L, tag_row);

    // `barch.space` is itself userdata whose __index opens a space by name, so
    // `barch.space.sp1` and `barch.space["sp1"]` are the same thing
    lua_newuserdata(L, 1);
    lua_newtable(L);
    lua_pushcfunction(L, space_open, "__index");
    lua_setfield(L, -2, "__index");
    lua_pushstring(L, "locked");
    lua_setfield(L, -2, "__metatable");
    lua_setreadonly(L, -1, true);
    lua_setmetatable(L, -2);
    lua_setfield(L, -2, "space");
    push_tx_table(L, -1);
    lua_setfield(L, -2, "tx");

    lua_setglobal(L, "barch");
}

} // namespace detail

lock_wait*& script_lock_wait() {
    thread_local lock_wait* w = nullptr;
    return w;
}

/*
 * What fetch_luau asks before it opens a socket - see TODO 304.
 *
 * Null means there is no script context to ask: a foreign fill state has no
 * space_state, nobody authenticated it, and it is internal by construction. The
 * caller treats that as allowed, which is the same answer `store_for_owner` gives
 * for the fs source path that `imgsource.luau` runs on.
 */
const store_access* current_access(lua_State* L) {
    auto* st = detail::state_of(L);
    return st ? st->store() : nullptr;
}

void push_reply(lua_State* L, const Variable& v) {
    detail::push_variable(L, v);
}

void refuse_from_log(lua_State* L, const char* what) {
    auto* rc = static_cast<run_ctx*>(lua_callbacks(L)->userdata);
    if (rc && rc->from_log)
        luaL_error(L, "FUNCTION %s doesn't do the same on every member, so a function run from the log "
                      "can't use it - give the function --@barch {\"replicate\": \"effects\"}", what);
}

bool in_locked_region(lua_State* L) {
    auto* rc = static_cast<run_ctx*>(lua_callbacks(L)->userdata);
    return rc && rc->locked;
}

} // namespace foreign
} // namespace barch

#endif // BARCH_HAS_LUAU
