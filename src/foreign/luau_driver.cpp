#include "driver.h"
#include "pool.h"
#include "sql.h"
#include "key_space.h"
#include "configuration.h"
#include "lzr_log.h"
#include "art/nodes.h"

#include <cmath>
#include <fstream>

#include <functional>
#include <memory>
#include <string>
#include <string_view>

#ifdef BARCH_HAS_LUAU
#include "lua.h"
#include "lualib.h"
#include "luacode.h"
#endif

namespace barch {
namespace foreign {

#ifdef BARCH_HAS_LUAU

struct run_ctx {
    uint64_t left{0};
    uint64_t slice{0};
    int64_t deadline{0};
};

static void interrupt(lua_State* L, int gc) {
    if (gc >= 0)
        return;
    auto* ctx = static_cast<run_ctx*>(lua_callbacks(L)->userdata);
    if (!ctx)
        return;
    if (ctx->deadline && art::now() > ctx->deadline)
        luaL_error(L, "FOREIGN timeout");
    if (ctx->left == 0) {
        // slice is done. yield so the foreign worker can take other jobs.
        // cannot yield from sql.query (C call); the next Lua safepoint will.
        if (lua_isyieldable(L))
            lua_yield(L, 0);
        return;
    }
    --ctx->left;
}

static int blocked_require(lua_State* L) {
    luaL_error(L, "FOREIGN require is not allowed");
    return 0;
}

static int sql_query(lua_State* L) {
    lua_getfield(L, LUA_REGISTRYINDEX, "barch.foreign.space");
    const char* sp = lua_tostring(L, -1);
    lua_pop(L, 1);
    auto ks = sp ? get_keyspace(sp) : nullptr;
    if (!ks || !ks->sql)
        luaL_error(L, "FOREIGN no sql driver");
    size_t qn = 0, kn = 0;
    const char* q = luaL_checklstring(L, 1, &qn);
    const char* k = luaL_checklstring(L, 2, &kn);
    auto r = ks->sql->query({q, qn}, {k, kn}, ks->foreign_query_timeout_ms);
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

static void open_safe(lua_State* L) {
    luaopen_base(L);
    luaopen_math(L);
    luaopen_string(L);
    luaopen_table(L);
    luaopen_utf8(L);
    luaopen_bit32(L);
    luaopen_coroutine(L);
    lua_pushcfunction(L, blocked_require, "require");
    lua_setglobal(L, "require");
    lua_newtable(L);
    lua_pushcfunction(L, sql_query, "query");
    lua_setfield(L, -2, "query");
    lua_setglobal(L, "sql");
}

static std::string script_key(std::string_view k) {
    if (k.empty())
        return {};
    size_t start = 0;
    auto lead = static_cast<unsigned char>(k[0]);
    if (lead == art::tstring || lead == art::tinteger || lead == art::tdouble
        || lead == art::tshort || lead == art::tfloat
        || art::is_composite_lead(lead))
        start = 1;
    size_t end = k.size();
    while (end > start && k[end - 1] == 0)
        --end;
    return {k.data() + start, end - start};
}

static bool load_source(const std::string& spec, std::string& source, std::string& err) {
    if (spec.size() >= 2 && spec[0] == '-' && spec[1] == '-') {
        source = spec;
        return true;
    }
    std::ifstream in(spec, std::ios::binary);
    if (!in) {
        err = "cannot read luau script";
        return false;
    }
    source.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    return true;
}

/*
 * compile, load and run the chunk, then check it left `entry` behind as a function.
 *
 * The entry point is a parameter because the two callers do not share one. A foreign
 * fill answers through `resolve`; a stored function answers through `call`. They were
 * the same code with "resolve" written into it, which would have refused every stored
 * function ever written.
 */
static bool compile_entry(const std::string& source, std::string& bytecode,
                          const char* entry, std::string& err) {
    size_t n = 0;
    char* bc = luau_compile(source.data(), source.size(), nullptr, &n);
    if (!bc) {
        err = "luau compile failed";
        return false;
    }
    bytecode.assign(bc, n);
    free(bc);
    lua_State* L = luaL_newstate();
    if (!L) {
        err = "luau state failed";
        return false;
    }
    int rc = luau_load(L, "=barch", bytecode.data(), bytecode.size(), 0);
    if (rc != 0) {
        err = lua_tostring(L, -1) ? lua_tostring(L, -1) : "luau load failed";
        lua_close(L);
        return false;
    }
    if (lua_pcall(L, 0, 0, 0) != 0) {
        err = lua_tostring(L, -1) ? lua_tostring(L, -1) : "luau script error";
        lua_close(L);
        return false;
    }
    lua_getglobal(L, entry);
    if (lua_type(L, -1) != LUA_TFUNCTION) {
        err = std::string("luau script has no ") + entry + "()";
        lua_close(L);
        return false;
    }
    lua_close(L);
    return true;
}

static bool compile_source(const std::string& source, std::string& bytecode, std::string& err) {
    return compile_entry(source, bytecode, "resolve", err);
}

struct luau_job {
    lua_State* L{nullptr};
    lua_State* T{nullptr};
    run_ctx ctx{};
    bool requeue{true};
    std::function<void(result)> done;

    ~luau_job() {
        if (L)
            lua_close(L);
    }
};

static result take_result(lua_State* T, int status) {
    if (status != LUA_OK) {
        std::string err = lua_tostring(T, -1) ? lua_tostring(T, -1) : "FOREIGN luau";
        if (err.find("timeout") != std::string::npos)
            return {result::status::error, "FOREIGN timeout"};
        if (err.find("FOREIGN ") == 0)
            return {result::status::error, err};
        return {result::status::error, "FOREIGN " + err};
    }
    if (lua_isnil(T, -1))
        return {result::status::missing, {}};
    if (lua_isstring(T, -1)) {
        size_t n = 0;
        const char* s = lua_tolstring(T, -1, &n);
        return {result::status::value, std::string(s, n)};
    }
    return {result::status::error, "FOREIGN resolve must return a string or nil"};
}

static void pump(std::shared_ptr<luau_job> job, int narg) {
    for (;;) {
        int status = lua_resume(job->T, nullptr, narg);
        narg = 0;
        if (status == LUA_YIELD) {
            job->ctx.left = job->ctx.slice ? job->ctx.slice : 1;
            if (job->requeue) {
                enqueue([job] { pump(job, 0); });
                return;
            }
            continue;
        }
        result r = take_result(job->T, status);
        if (job->L) {
            lua_close(job->L);
            job->L = nullptr;
            job->T = nullptr;
        }
        auto done = std::move(job->done);
        done(std::move(r));
        return;
    }
}

static void start_resolve(std::string_view space, std::string_view key, uint64_t deadline_ms,
                          bool requeue, std::function<void(result)> done) {
    auto ks = get_keyspace(std::string(space));
    if (!ks) {
        done({result::status::error, "FOREIGN no space"});
        return;
    }
    if (ks->luau_bytecode.empty()) {
        done({result::status::error, "FOREIGN no script"});
        return;
    }
    auto job = std::make_shared<luau_job>();
    job->requeue = requeue;
    job->done = std::move(done);
    job->L = luaL_newstate();
    if (!job->L) {
        job->done({result::status::error, "FOREIGN luau state"});
        return;
    }
    lua_State* L = job->L;
    lua_pushlstring(L, space.data(), space.size());
    lua_setfield(L, LUA_REGISTRYINDEX, "barch.foreign.space");
    open_safe(L);
    job->ctx.slice = ks->script_insns();
    job->ctx.left = job->ctx.slice ? job->ctx.slice : 1;
    if (deadline_ms)
        job->ctx.deadline = art::now() + static_cast<int64_t>(deadline_ms);
    lua_callbacks(L)->userdata = &job->ctx;
    lua_callbacks(L)->interrupt = interrupt;
    lua_singlestep(L, 1);
    int rc = luau_load(L, "=foreign", ks->luau_bytecode.data(), ks->luau_bytecode.size(), 0);
    if (rc != 0) {
        std::string err = lua_tostring(L, -1) ? lua_tostring(L, -1) : "FOREIGN luau load";
        job->done({result::status::error, err});
        return;
    }
    if (lua_pcall(L, 0, 0, 0) != 0) {
        std::string err = lua_tostring(L, -1) ? lua_tostring(L, -1) : "FOREIGN luau script";
        if (err.find("timeout") != std::string::npos)
            job->done({result::status::error, "FOREIGN timeout"});
        else if (err.find("FOREIGN ") == 0)
            job->done({result::status::error, err});
        else
            job->done({result::status::error, "FOREIGN " + err});
        return;
    }
    job->T = lua_newthread(L);
    lua_getglobal(job->T, "resolve");
    if (lua_type(job->T, -1) != LUA_TFUNCTION) {
        job->done({result::status::error, "FOREIGN resolve missing"});
        return;
    }
    auto uk = script_key(key);
    lua_pushlstring(job->T, uk.data(), uk.size());
    lua_pushlstring(job->T, space.data(), space.size());
    pump(std::move(job), 2);
}

struct luau_driver_t : driver {
    result fetch(std::string_view space, std::string_view key, uint64_t deadline_ms) override {
        result out;
        start_resolve(space, key, deadline_ms, false, [&](result r) { out = std::move(r); });
        return out;
    }

    void fetch_async(std::string_view space, std::string_view key, uint64_t deadline_ms,
                     std::function<void(result)> done) override {
        start_resolve(space, key, deadline_ms, true, std::move(done));
    }
};

bool prepare_luau(key_space& ks) {
    std::string source, err;
    if (!load_source(ks.foreign_script, source, err)) {
        barch::err({"luau foreign source - ignoring it for space", ks.get_name(), err});
        return false;
    }
    if (!compile_source(source, ks.luau_bytecode, err)) {
        barch::err({"luau foreign source - ignoring it for space", ks.get_name(), err});
        ks.luau_bytecode.clear();
        return false;
    }
    return true;
}

/*
 * A stored function's interrupt. The foreign one yields so a slice can go back on the
 * pool; this one has nowhere to yield to yet, so the budget and the deadline are both
 * hard stops. Slicing is what turns this into a park - TODO 98 H.
 */
static void function_interrupt(lua_State* L, int gc) {
    if (gc >= 0)
        return;
    auto* ctx = static_cast<run_ctx*>(lua_callbacks(L)->userdata);
    if (!ctx)
        return;
    if (ctx->deadline && art::now() > ctx->deadline)
        luaL_error(L, "FUNCTION timeout");
    if (ctx->left == 0)
        luaL_error(L, "FUNCTION instruction budget exceeded");
    --ctx->left;
}

/*
 * The compiled functions a session holds.
 *
 * One Luau state per key space, because `require` resolves inside a space and this
 * puts the module cache where the resolution already is - and because a generation
 * bump or an UNLOAD then closes one state rather than picking entries out of a map.
 * Nothing shares a state between sessions, so there is no lock here and none needed:
 * a session runs one call at a time. See TODO 98 C.
 */
struct compiled {
    int fn{LUA_NOREF};      // the function itself, pinned in the registry
    int env{LUA_NOREF};     // its thread, which owns the globals table it closed over
    int envt{LUA_NOREF};    // that globals table, which is what require hands back
    int arity{0};           // 0 is "any", n exactly n, -n at least n. See TODO 98 I.3
};

struct space_state {
    lua_State* L{nullptr};
    heap::string_map<compiled> functions{};
    /**
     * where require gets a source from, for as long as a call is running. A session
     * runs one call at a time, so there is one of these and no lock around it.
     */
    const source_loader* load{nullptr};
    /** where barch.call goes, for as long as a call is running */
    const command_runner* run_command{nullptr};
    /** and where barch.store goes, on the same terms */
    const store_access* store{nullptr};
    /**
     * what is being compiled right now, innermost last. A require for something on
     * this stack is a cycle, and the stack is the path to put in the message.
     */
    std::vector<std::string> loading{};

    ~space_state() {
        if (L)
            lua_close(L);
    }
};

struct function_states {
    // keyed by canonical space name, never by key_space_ptr: a session holding one
    // would keep an UNLOADed space alive for as long as it stayed connected
    heap::string_map<std::unique_ptr<space_state>> spaces{};
};

function_states_ptr make_function_states() {
    return std::make_shared<function_states>();
}

// require and the state builder call each other: a state installs require, and require
// compiles into the state
static int function_require(lua_State* L);
static int barch_call(lua_State* L);
/** the state a lua_State belongs to, so a C function can find its way home */
static space_state*& state_of(lua_State* L);

/*
 * A command's reply, as a Luau value. The inverse of to_variable below, and it has to
 * stay the inverse: a script that writes a value with barch.call and reads it back
 * should see what it wrote.
 */
static void push_variable(lua_State* L, const Variable& v) {
    switch (v.index()) {
        case var_bool:
            lua_pushboolean(L, std::get<bool>(v));
            return;
        case var_int64:
            lua_pushnumber(L, (double) std::get<int64_t>(v));
            return;
        case var_uint64:
            lua_pushnumber(L, (double) std::get<uint64_t>(v));
            return;
        case var_double:
            lua_pushnumber(L, std::get<double>(v));
            return;
        case var_string: {
            const auto& s = std::get<std::string>(v);
            lua_pushlstring(L, s.data(), s.size());
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
static const store_access* store_of(lua_State* L, const char* what) {
    space_state* st = state_of(L);
    if (!st || !st->store)
        luaL_error(L, "FUNCTION barch.store.%s is not available here", what);
    return st->store;
}

static int store_get(lua_State* L) {
    size_t n = 0;
    const char* k = luaL_checklstring(L, 1, &n);
    const auto* s = store_of(L, "get");
    std::string value;
    if (!s->get({k, n}, value)) {
        lua_pushnil(L);
        return 1;
    }
    lua_pushlstring(L, value.data(), value.size());
    return 1;
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

/* barch.store.range(lo, hi, limit) - the keys in [lo, hi), at most limit of them.
 *
 * The limit is required and capped. An unbounded walk would copy the whole space into
 * one Luau table, which is the thing KEYS was made asynchronous to avoid. */
enum { max_range_batch = 10000 };

static int store_range(lua_State* L) {
    size_t ln = 0, hn = 0;
    const char* lo = luaL_checklstring(L, 1, &ln);
    const char* hi = luaL_checklstring(L, 2, &hn);
    int64_t limit = (int64_t) luaL_checknumber(L, 3);
    if (limit <= 0 || limit > max_range_batch)
        limit = max_range_batch;
    heap::vector<std::string> keys;
    store_of(L, "range")->range({lo, ln}, {hi, hn}, limit, keys);
    lua_createtable(L, (int) keys.size(), 0);
    int at = 1;
    for (const auto& k : keys) {
        lua_pushlstring(L, k.data(), k.size());
        lua_rawseti(L, -2, at++);
    }
    return 1;
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
 * barch.call("GET", "k") - run an ordinary command and hand back its reply.
 *
 * Named on the barch table rather than as a bare `call`, which is the name a script
 * gives its own entry point. A refusal or a failed command is raised as a Lua error,
 * so a script that wants to survive one wraps it in pcall - the same split redis has
 * between redis.call and redis.pcall.
 */
static int barch_call(lua_State* L) {
    int n = lua_gettop(L);
    if (n < 1)
        luaL_error(L, "FUNCTION barch.call needs a command name");
    space_state* st = state_of(L);
    if (!st || !st->run_command)
        luaL_error(L, "FUNCTION barch.call is not available here");
    heap::vector<std::string> argv;
    argv.reserve(n);
    for (int i = 1; i <= n; ++i) {
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
            luaL_error(L, "FUNCTION barch.call takes strings and numbers");
        argv.emplace_back(s, len);
    }
    Variable out;
    std::string err;
    if (!(*st->run_command)(argv, out, err))
        luaL_error(L, "%s", err.empty() ? "FUNCTION call failed" : err.c_str());
    push_variable(L, out);
    return 1;
}

/*
 * What a function returned, as a Variable. The shapes are fixed here once and are not
 * meant to move afterwards - a client that has to guess which of two encodings it is
 * getting cannot be written against.
 */
static bool to_variable(lua_State* L, int idx, Variable& out, std::string& err, int depth) {
    if (depth > 8) {
        err = "FUNCTION return nests too deeply";
        return false;
    }
    switch (lua_type(L, idx)) {
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
        case LUA_TSTRING: {
            size_t n = 0;
            const char* s = lua_tolstring(L, idx, &n);
            out = Variable(std::string(s, n));
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
                out = Variable(std::string(s, n));
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
                items.push_back(item);
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


/* how much one session may keep before its state for a space is thrown away and
 * built again. Crude next to the LRU that TODO 98 C asks for, but bounded, and the
 * bound is what stops a roaming client holding the whole space's functions open */
enum { max_cached_functions = 64, max_cached_spaces = 8 };

static space_state* state_for(function_states& cache, const std::string& space) {
    auto it = cache.spaces.find(space);
    if (it != cache.spaces.end())
        return it->second.get();
    if (cache.spaces.size() >= max_cached_spaces)
        cache.spaces.clear();
    auto st = std::make_unique<space_state>();
    st->L = luaL_newstate();
    if (!st->L)
        return nullptr;
    lua_State* L = st->L;
    // sql.query reads the space out of the registry, so a function gets the same SQL
    // its space is configured with, or the same error when there is none
    lua_pushlstring(L, space.data(), space.size());
    lua_setfield(L, LUA_REGISTRYINDEX, "barch.foreign.space");
    open_safe(L);
    auto* raw = st.get();
    // a back pointer, so require can find the state it is running in. Stored before
    // the sandbox freezes things, and read through the registry rather than a global
    // so no script can reach it
    auto** slot = static_cast<space_state**>(lua_newuserdata(L, sizeof(space_state*)));
    *slot = raw;
    lua_setfield(L, LUA_REGISTRYINDEX, "barch.function.state");
    // foreign fills have require blocked outright; a stored function gets a resolver
    // that can only see other functions in this space and the globals in configuration
    lua_pushcfunction(L, function_require, "require");
    lua_setglobal(L, "require");
    lua_newtable(L);
    lua_pushcfunction(L, barch_call, "call");
    lua_setfield(L, -2, "call");
    lua_pushcfunction(L, store_space, "space");
    lua_setfield(L, -2, "space");
    lua_newtable(L);
    lua_pushcfunction(L, store_get, "get");
    lua_setfield(L, -2, "get");
    lua_pushcfunction(L, store_exists, "exists");
    lua_setfield(L, -2, "exists");
    lua_pushcfunction(L, store_count, "count");
    lua_setfield(L, -2, "count");
    lua_pushcfunction(L, store_range, "range");
    lua_setfield(L, -2, "range");
    lua_pushcfunction(L, store_min, "min");
    lua_setfield(L, -2, "min");
    lua_pushcfunction(L, store_max, "max");
    lua_setfield(L, -2, "max");
    lua_setfield(L, -2, "store");
    lua_setglobal(L, "barch");
    // freeze the base globals. Each function then loads on a thread of its own with a
    // globals table that proxies reads here and keeps writes to itself, so one
    // function cannot leave anything behind for the next
    luaL_sandbox(L);
    lua_callbacks(L)->interrupt = function_interrupt;
    lua_singlestep(L, 1);
    cache.spaces.emplace(space, std::move(st));
    return raw;
}

static bool compile_into(space_state& st, const std::string& name,
                         const std::string& source, compiled& out, std::string& err);

static space_state*& state_of(lua_State* L) {
    lua_getfield(L, LUA_REGISTRYINDEX, "barch.function.state");
    auto** slot = static_cast<space_state**>(lua_touserdata(L, -1));
    lua_pop(L, 1);
    static space_state* none = nullptr;
    return slot ? *slot : none;
}

/*
 * require("NAME") - another function in the same space, or a global from
 * `configuration`, compiled if this session has not compiled it yet.
 *
 * What comes back is that function's globals table, so a required module can offer
 * helpers as well as its own `call`. Cycles are refused with the path that made them,
 * rather than recursing until the stack gives out. See TODO 98 D.
 */
static int function_require(lua_State* L) {
    size_t n = 0;
    const char* raw = luaL_checklstring(L, 1, &n);
    std::string name(raw, n);
    for (auto& ch : name) {
        ch = (char) toupper((unsigned char) ch);
    }
    space_state* st = state_of(L);
    if (!st || !st->load)
        luaL_error(L, "FUNCTION require is not available here");

    auto have = st->functions.find(name);
    if (have != st->functions.end()) {
        lua_getref(L, have->second.envt);
        return 1;
    }
    for (const auto& busy : st->loading) {
        if (busy == name) {
            std::string path;
            for (const auto& step : st->loading) {
                path += step;
                path += " -> ";
            }
            path += name;
            luaL_error(L, "FUNCTION cycle %s", path.c_str());
        }
    }
    std::string source;
    if (!(*st->load)(name, source))
        luaL_error(L, "FUNCTION require has no function %s", name.c_str());
    compiled c;
    std::string err;
    if (!compile_into(*st, name, source, c, err))
        luaL_error(L, "%s", err.c_str());
    st->functions.emplace(name, c);
    lua_getref(L, c.envt);
    return 1;
}

/** compile `source` into this space's state and pin what it left behind */
static bool compile_into(space_state& st, const std::string& name,
                         const std::string& source, compiled& out, std::string& err) {
    std::string bytecode;
    size_t n = 0;
    char* bc = luau_compile(source.data(), source.size(), nullptr, &n);
    if (!bc) {
        err = "luau compile failed";
        return false;
    }
    bytecode.assign(bc, n);
    free(bc);

    lua_State* L = st.L;
    lua_State* T = lua_newthread(L);
    int env = lua_ref(L, -1);   // pins the thread, and with it the globals it owns
    lua_pop(L, 1);
    luaL_sandboxthread(T);
    // the thread's own globals table, which is what require hands to whoever asked
    lua_pushvalue(T, LUA_GLOBALSINDEX);
    int envt = lua_ref(T, -1);
    lua_pop(T, 1);

    // on this stack for as long as its chunk is running, so a require that comes back
    // round to it is a cycle rather than a recursion
    st.loading.push_back(name);
    struct pop_on_exit {
        space_state& st;
        ~pop_on_exit() { st.loading.pop_back(); }
    } popper{st};

    auto give_up = [&](const char* fallback) {
        err = lua_tostring(T, -1) ? lua_tostring(T, -1) : fallback;
        lua_unref(L, env);
        lua_unref(L, envt);
        return false;
    };
    if (luau_load(T, ("=" + name).c_str(), bytecode.data(), bytecode.size(), 0) != 0)
        return give_up("luau load failed");
    // the chunk can require others, which compiles them into this same state
    if (lua_pcall(T, 0, 0, 0) != 0)
        return give_up("luau script error");
    lua_getglobal(T, "call");
    if (lua_type(T, -1) != LUA_TFUNCTION) {
        err = "luau script has no call()";
        lua_unref(L, env);
        lua_unref(L, envt);
        return false;
    }
    out.fn = lua_ref(T, -1);
    lua_pop(T, 1);
    out.env = env;
    out.envt = envt;
    // arity is declared by the script, so it travels with the source rather than
    // needing a second thing stored beside it. Redis's convention: n means exactly n,
    // -n means at least n, absent means the script will take whatever it is given
    lua_getglobal(T, "arity");
    out.arity = lua_isnumber(T, -1) ? (int) lua_tointeger(T, -1) : 0;
    lua_pop(T, 1);
    return true;
}

bool call_function(const std::string& space, const std::string& name,
                   const source_loader& load, const command_runner& run_command,
                   const store_access& store,
                   const heap::vector<std::string>& args, uint64_t insns,
                   uint64_t deadline_ms, const function_states_ptr& cache,
                   Variable& out, std::string& err) {
    // no session to hang a state on - the swig and module paths - gets one that lives
    // for this call, so there is a single code path rather than two
    auto local = cache ? cache : make_function_states();
    space_state* st = state_for(*local, space);
    if (!st) {
        err = "FUNCTION luau state";
        return false;
    }
    auto it = st->functions.find(name);
    if (it == st->functions.end()) {
        if (st->functions.size() >= max_cached_functions) {
            // dropping the map would strand every registry ref in it, so the state
            // goes and is built again - which is also what makes the cap a real bound
            local->spaces.erase(space);
            st = state_for(*local, space);
            if (!st) {
                err = "FUNCTION luau state";
                return false;
            }
        }
        st->load = &load;
        std::string source;
        if (!load(name, source)) {
            err = "no such function";
            return false;
        }
        compiled c;
        bool built = compile_into(*st, name, source, c, err);
        st->load = nullptr;
        if (!built)
            return false;
        it = st->functions.emplace(name, c).first;
    }
    const compiled& c = it->second;

    // arity is checked before the script runs, so a wrong count costs nothing
    int given = (int) args.size();
    if (c.arity > 0 && given != c.arity) {
        err = "wrong number of arguments for '" + name + "'";
        return false;
    }
    if (c.arity < 0 && given < -c.arity) {
        err = "wrong number of arguments for '" + name + "'";
        return false;
    }

    lua_State* L = st->L;
    run_ctx ctx;
    ctx.slice = insns;
    ctx.left = insns ? insns : 1;
    if (deadline_ms)
        ctx.deadline = art::now() + static_cast<int64_t>(deadline_ms);
    lua_callbacks(L)->userdata = &ctx;
    // barch.call reaches the store through here, and only while this call is running
    st->run_command = &run_command;
    st->store = &store;
    struct clear_on_exit {
        space_state* st;
        ~clear_on_exit() {
            st->run_command = nullptr;
            st->store = nullptr;
        }
    } clearer{st};

    int base = lua_gettop(L);
    lua_getref(L, c.fn);
    lua_createtable(L, (int) args.size(), 0);
    for (size_t i = 0; i < args.size(); ++i) {
        lua_pushlstring(L, args[i].data(), args[i].size());
        lua_rawseti(L, -2, (int) i + 1);
    }
    if (lua_pcall(L, 1, 1, 0) != 0) {
        err = lua_tostring(L, -1) ? lua_tostring(L, -1) : "FUNCTION luau call";
        lua_settop(L, base);
        lua_callbacks(L)->userdata = nullptr;
        return false;
    }
    bool ok = to_variable(L, lua_gettop(L), out, err, 0);
    // the state outlives the call now, so what was pushed has to come back off it
    lua_settop(L, base);
    lua_callbacks(L)->userdata = nullptr;
    return ok;
}

bool compile_function(const std::string& space, const std::string& name,
                      const std::string& source, const source_loader& load,
                      std::string& err) {
    // a state of its own, thrown away when this returns. It has to be a real one:
    // the script's top level may require others, and require needs both the loader
    // and a state to compile them into
    auto scratch = make_function_states();
    space_state* st = state_for(*scratch, space);
    if (!st) {
        err = "FUNCTION luau state";
        return false;
    }
    st->load = &load;
    // nothing is installed for barch.call or barch.store here: a script's top level
    // runs at SETF time, when there is no caller to run a command on, so one that
    // tries is told so rather than reaching a half-built one
    compiled c;
    bool ok = compile_into(*st, name, source, c, err);
    st->load = nullptr;
    return ok;
}

bool luau_available() {
    return true;
}

driver& luau_driver() {
    static luau_driver_t d;
    return d;
}

#else

bool prepare_luau(key_space& ks) {
    barch::err({"luau not built - ignoring it for space", ks.get_name()});
    return false;
}

bool compile_function(const std::string&, const std::string&, const std::string&,
                      const source_loader&, std::string& err) {
    err = "luau not built";
    return false;
}

struct function_states {};

function_states_ptr make_function_states() {
    return nullptr;
}

bool call_function(const std::string&, const std::string&,
                   const source_loader&, const command_runner&, const store_access&,
                   const heap::vector<std::string>&, uint64_t, uint64_t,
                   const function_states_ptr&, Variable&, std::string& err) {
    err = "luau not built";
    return false;
}

bool luau_available() {
    return false;
}

struct luau_stub : driver {
    result fetch(std::string_view, std::string_view, uint64_t) override {
        return {result::status::error, "FOREIGN luau not built"};
    }
};

driver& luau_driver() {
    static luau_stub d;
    return d;
}

#endif

}
}
