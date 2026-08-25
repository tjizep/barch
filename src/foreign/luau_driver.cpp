#include "driver.h"
#include "pool.h"
#include "sql.h"
#include "key_space.h"
#include "configuration.h"
#include "lzr_log.h"
#include "art/nodes.h"
#include "function_api.h"

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
    /*
     * Three more that are pure computation - no files, no clock, no network - and are
     * what a script needs to do real work rather than string handling.
     *
     * `integer` is the one that matters. Luau numbers are doubles, so anything with a
     * 64 bit identifier - an H3 geo cell, a snowflake id, a hash - could otherwise
     * only be carried as two halves through bit32, which is slow to run and worse to
     * write. This is a native 64 bit type with the arithmetic, the bit operations and
     * the unsigned comparisons.
     *
     * `buffer` is a mutable byte array with typed reads and writes, which is how a
     * lookup table wants to be stored rather than as a Luau table of numbers.
     */
    luaopen_integer(L);
    luaopen_buffer(L);
    luaopen_vector(L);
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

/*
 * Read a fill script from a file, if the spec is one.
 *
 * It used to take the value itself as the script when it began with `--`, which put
 * Luau source in the configuration space - a store whose job is settings. A script is
 * a function now: store it with SETF and point `foreign_script` at the name. See
 * TODO 139.
 *
 * A path is not the same mistake - it is a reference, not the code - so it still
 * works, and is tried after the name.
 */
static bool load_source(const std::string& spec, std::string& source, std::string& err) {
    if (spec.size() >= 2 && spec[0] == '-' && spec[1] == '-') {
        err = "a luau script belongs in a function: SETF it, then name it in "
              "foreign_script";
        return false;
    }
    std::ifstream in(spec, std::ios::binary);
    if (!in) {
        err = "no function or file of that name";
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
    // one entry point for every stored Luau function. A fill answers `call(key, space)`
    // where it used to answer `resolve(key, space)` - see TODO 139
    return compile_entry(source, bytecode, "call", err);
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
    return {result::status::error, "FOREIGN call must return a string or nil"};
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

/**
 * compile the space's fill script, once, on the first fill that needs it.
 *
 * The script is a stored function: looked up in this space and then in the default
 * one, the same order a call resolves in. A file path still works and is tried after
 * the name, since a path is a reference rather than code in the settings store.
 */
static bool fill_bytecode(const key_space_ptr& ks, std::string& err) {
    static std::mutex compiling;
    std::lock_guard lock(compiling);
    if (!ks->luau_bytecode.empty())
        return true;              // another fill got here first

    std::string source;
    if (!barch::functions::source_of(ks, ks->foreign_script, source)
        && !load_source(ks->foreign_script, source, err)) {
        if (err.empty())
            err = "no function or file named " + ks->foreign_script;
        return false;
    }
    if (!compile_source(source, ks->luau_bytecode, err)) {
        ks->luau_bytecode.clear();
        return false;
    }
    return true;
}

static void start_resolve(std::string_view space, std::string_view key, uint64_t deadline_ms,
                          bool requeue, std::function<void(result)> done) {
    auto ks = get_keyspace(std::string(space));
    if (!ks) {
        done({result::status::error, "FOREIGN no space"});
        return;
    }
    if (ks->luau_bytecode.empty()) {
        // first fill for this space: find the function the configuration names and
        // compile it. Deferred to here because prepare_luau runs before the space has
        // shards to read a key out of - see TODO 139
        std::string err;
        if (!fill_bytecode(ks, err)) {
            done({result::status::error, "FOREIGN " + err});
            return;
        }
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
    lua_getglobal(job->T, "call");
    if (lua_type(job->T, -1) != LUA_TFUNCTION) {
        job->done({result::status::error, "FOREIGN call missing"});
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

/*
 * Nothing is compiled here any more.
 *
 * This runs from the key_space constructor, before the space has any shards, so the
 * function the script now lives in cannot be read yet. The first fill compiles it and
 * the space keeps the bytecode after that - which also means a script stored *after*
 * its space was built starts working, where before the space had to be rebuilt.
 *
 * All that is left to check is that a name was given at all. See TODO 139.
 */
bool prepare_luau(key_space& ks) {
    if (ks.foreign_script.empty()) {
        barch::err({"luau foreign source needs a function name - ignoring it for space",
                    ks.get_name()});
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
    if (ctx->left == 0) {
        // the slice is spent. Yield so the pool thread can take other work and this
        // script comes back on the next one - the deadline is what ends a runaway,
        // not the slice. A call that is not on a coroutine has nowhere to yield to,
        // which is the synchronous path below, so there the slice is a cap
        if (lua_isyieldable(L)) {
            lua_yield(L, 0);
            return;
        }
        luaL_error(L, "FUNCTION instruction budget exceeded");
    }
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
    /** how barch.space reaches another one */
    const space_opener* open_space{nullptr};
    /** spaces already opened for this call, so a loop does not reopen one per step */
    heap::string_map<store_access> opened{};
    /**
     * what is being compiled right now, innermost last. A require for something on
     * this stack is a cycle, and the stack is the path to put in the message.
     */
    std::vector<std::string> loading{};
    /**
     * coroutines that have finished and can be used again.
     *
     * A call runs its script on a coroutine of its own, and making a fresh one every
     * time was 11% of the server between `stack_init` and the collector marking the
     * garbage it left. They are interchangeable - `lua_resetthread` puts one back to
     * empty - so a small pool of them removes both at once. See TODO 98 F5.
     */
    heap::vector<std::pair<lua_State*, int>> free_threads{};

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
static const store_access* store_of(lua_State* L, const char* what, bool writing = false) {
    space_state* st = state_of(L);
    if (!st || !st->store)
        luaL_error(L, "FUNCTION barch.store.%s is not available here", what);
    // the script runs as whoever called it. Reading the store directly must not be a
    // way round the rights a client would have been refused at the command - the same
    // check barch.call makes, one layer down where there is no command to read it off
    if (writing ? !st->store->may_write : !st->store->may_read)
        luaL_error(L, "FUNCTION not authorized to %s here", writing ? "write" : "read");
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

struct space_handle {
    space_state* st{nullptr};
    const store_access* store{nullptr};
};

/** a container reached through a space handle: sp:container("name") */
struct container_handle {
    const store_access* store{nullptr};
    std::string name{};
};

/** where a container walk has got to */
struct member_cursor {
    const store_access* store{nullptr};
    std::string name{};
    heap::vector<std::pair<std::string, std::string>> page{};
    size_t at{0};
    std::string after{};
    bool done{false};
};

/** where a walk has got to, and the page it is reading */
struct row_cursor {
    const store_access* store{nullptr};
    heap::vector<store_access::row> page{};
    size_t at{0};
    std::string after{};
    bool done{false};
};

static const store_access* handle_store(lua_State* L, int idx) {
    auto* h = static_cast<space_handle*>(lua_touserdata(L, idx));
    if (!h || !h->store)
        luaL_error(L, "FUNCTION that key space is not open");
    return h->store;
}

static int space_read(lua_State* L) {
    const store_access* s = handle_store(L, 1);
    if (!s->may_read)
        luaL_error(L, "FUNCTION not authorized to read there");
    size_t n = 0;
    const char* k = luaL_checklstring(L, 2, &n);
    std::string value;
    if (!s->get({k, n}, value)) {
        lua_pushnil(L);
        return 1;
    }
    lua_pushlstring(L, value.data(), value.size());
    return 1;
}

static int space_write(lua_State* L) {
    const store_access* s = handle_store(L, 1);
    if (!s->may_write)
        luaL_error(L, "FUNCTION not authorized to write there");
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
    auto* c = static_cast<row_cursor*>(lua_touserdata(L, 1));
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
        if (r.key.empty() || !c->store || !c->store->get) {
            lua_pushnil(L);
        } else {
            std::string v;
            if (!c->store->get(r.key, v)) lua_pushnil(L);
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
    auto* c = static_cast<row_cursor*>(lua_touserdata(L, lua_upvalueindex(1)));
    if (!c || !c->store) {
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
        c->store->page(c->after, 256, c->page, c->after);
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
    auto* c = static_cast<row_cursor*>(lua_newuserdatadtor(L, sizeof(row_cursor),
        [](void* p) { static_cast<row_cursor*>(p)->~row_cursor(); }));
    new (c) row_cursor();
    c->store = s;
    lua_getfield(L, LUA_REGISTRYINDEX, "barch.row.meta");
    lua_setmetatable(L, -2);
    lua_pushcclosure(L, row_next, "next", 1);
    return 1;
}

static container_handle* as_container(lua_State* L, int idx) {
    auto* h = static_cast<container_handle*>(lua_touserdata(L, idx));
    if (!h || !h->store)
        luaL_error(L, "FUNCTION that container is not open");
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
    if (!c || !c->store) {
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
        c->store->container_page(c->name, c->after, 128, c->page, c->after);
        if (c->page.empty()) {
            c->done = true;
            lua_pushnil(L);
            return 1;
        }
    }
    const auto& kv = c->page[c->at++];
    lua_pushlstring(L, kv.first.data(), kv.first.size());
    lua_pushlstring(L, kv.second.data(), kv.second.size());
    return 2;                              // member first: it is never nil until the end
}

static int container_iter(lua_State* L) {
    auto* h = as_container(L, 1);
    auto* c = static_cast<member_cursor*>(lua_newuserdatadtor(L, sizeof(member_cursor),
        [](void* p) { static_cast<member_cursor*>(p)->~member_cursor(); }));
    new (c) member_cursor();
    c->store = h->store;
    c->name = h->name;
    lua_pushcclosure(L, member_next, "next", 1);
    return 1;
}

/** sp:container("name") and sp:kind("name") */
static int space_namecall(lua_State* L) {
    const char* m = lua_namecallatom(L, nullptr);
    const store_access* s = handle_store(L, 1);
    size_t n = 0;
    const char* raw = luaL_checklstring(L, 2, &n);
    std::string name(raw, n);
    if (m && !strcmp(m, "kind")) {
        std::string k = s->container_kind ? s->container_kind(name) : std::string();
        if (k.empty()) lua_pushnil(L);
        else lua_pushlstring(L, k.data(), k.size());
        return 1;
    }
    if (m && !strcmp(m, "container")) {
        if (!s->container_kind || s->container_kind(name).empty())
            luaL_error(L, "FUNCTION no container called %s", name.c_str());
        auto* h = static_cast<container_handle*>(lua_newuserdatadtor(L,
            sizeof(container_handle),
            [](void* p) { static_cast<container_handle*>(p)->~container_handle(); }));
        new (h) container_handle();
        h->store = s;
        h->name = std::move(name);
        lua_getfield(L, LUA_REGISTRYINDEX, "barch.container.meta");
        lua_setmetatable(L, -2);
        return 1;
    }
    luaL_error(L, "FUNCTION no such method on a key space");
    return 0;
}

/** barch.space.NAME - the handle, built once per space per call */
static int space_open(lua_State* L) {
    size_t n = 0;
    const char* raw = luaL_checklstring(L, 2, &n);
    std::string name(raw, n);
    space_state* st = state_of(L);
    if (!st || !st->open_space)
        luaL_error(L, "FUNCTION barch.space is not available here");
    auto have = st->opened.find(name);
    if (have == st->opened.end()) {
        store_access opened;
        // an unknown name is not a key space and must not become one
        if (!(*st->open_space)(name, opened))
            luaL_error(L, "FUNCTION no key space called %s", name.c_str());
        have = st->opened.emplace(name, std::move(opened)).first;
    }
    auto* h = static_cast<space_handle*>(lua_newuserdata(L, sizeof(space_handle)));
    h->st = st;
    h->store = &have->second;
    lua_getfield(L, LUA_REGISTRYINDEX, "barch.space.meta");
    lua_setmetatable(L, -2);
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

/* what a call may run on the calling thread before it has to park. See start_function */
enum { inline_insns = 20000 };

/* how many spent coroutines a space keeps to hand out again */
enum { max_free_threads = 32 };

/** a coroutine to run a call on, reused where there is one going spare */
static void take_thread(space_state& st, lua_State*& T, int& ref) {
    if (!st.free_threads.empty()) {
        auto got = st.free_threads.back();
        st.free_threads.pop_back();
        T = got.first;
        ref = got.second;
        lua_resetthread(T);          // back to empty, whatever the last call left
        return;
    }
    T = lua_newthread(st.L);
    ref = lua_ref(st.L, -1);
    lua_pop(st.L, 1);
}

/** done with it: keep it for the next call, or let it go if there are enough */
static void give_thread(space_state& st, lua_State* T, int ref) {
    if (!T || ref == LUA_NOREF)
        return;
    if (st.free_threads.size() >= max_free_threads) {
        lua_unref(st.L, ref);
        return;
    }
    // reset on the way out as well as in: a thread that errored is holding the error
    // object, and nothing should keep that alive until it is next used
    lua_resetthread(T);
    st.free_threads.push_back({T, ref});
}

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
    // the space's own settings. Named `config` because `space` is now the key space
    // itself rather than a function describing it
    lua_pushcfunction(L, store_space, "config");
    lua_setfield(L, -2, "config");
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

    // a key space read and written as a value - see TODO 98 F2
    lua_newtable(L);
    lua_pushcfunction(L, space_read, "__index");
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, space_write, "__newindex");
    lua_setfield(L, -2, "__newindex");
    lua_pushcfunction(L, space_iter, "__iter");
    lua_setfield(L, -2, "__iter");
    lua_pushcfunction(L, space_namecall, "__namecall");
    lua_setfield(L, -2, "__namecall");
    lua_setreadonly(L, -1, true);
    lua_setfield(L, LUA_REGISTRYINDEX, "barch.space.meta");

    // a list, hash or ordered set, read and written the same way a space is
    lua_newtable(L);
    lua_pushcfunction(L, container_read, "__index");
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, container_write, "__newindex");
    lua_setfield(L, -2, "__newindex");
    lua_pushcfunction(L, container_iter, "__iter");
    lua_setfield(L, -2, "__iter");
    lua_setreadonly(L, -1, true);
    lua_setfield(L, LUA_REGISTRYINDEX, "barch.container.meta");

    lua_newtable(L);
    lua_pushcfunction(L, row_read, "__index");
    lua_setfield(L, -2, "__index");
    lua_setreadonly(L, -1, true);
    lua_setfield(L, LUA_REGISTRYINDEX, "barch.row.meta");

    // `barch.space` is itself userdata whose __index opens a space by name, so
    // `barch.space.sp1` and `barch.space["sp1"]` are the same thing
    lua_newuserdata(L, 1);
    lua_newtable(L);
    lua_pushcfunction(L, space_open, "__index");
    lua_setfield(L, -2, "__index");
    lua_setreadonly(L, -1, true);
    lua_setmetatable(L, -2);
    lua_setfield(L, -2, "space");

    lua_setglobal(L, "barch");
    // freeze the base globals. Each function then loads on a thread of its own with a
    // globals table that proxies reads here and keeps writes to itself, so one
    // function cannot leave anything behind for the next
    luaL_sandbox(L);
    /*
     * No `lua_singlestep`. It drives the *debugstep* hook, which nothing here sets,
     * and switching it on forces the interpreter off its computed goto dispatch - see
     * the comment at the top of luau_execute. It does not change how often `interrupt`
     * is called, which is what the budget and the deadline actually ride on: measured
     * at 20,000,001 firings for a twenty million iteration loop either way, and 1.30x
     * faster without it. It was here because the foreign driver had it.
     */
    lua_callbacks(L)->interrupt = function_interrupt;
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

/*
 * A call that is running in slices.
 *
 * It owns everything the script can reach, because the command that started it has
 * already answered "parked" and gone: the loader, the command runner and the store
 * access were built on that stack and would be dangling by the first yield. The cache
 * is held too, so the session's state for this space cannot be closed underneath a
 * suspended coroutine.
 */
struct call_job {
    function_states_ptr cache{};
    space_state* st{nullptr};
    /*
     * held, not copied. Copying the four interface objects into every job meant a
     * dozen std::function constructions and as many destructions per call, which the
     * profile showed as most of the reference counting and a good share of the
     * malloc traffic. The connection owns these and outlives the job, so one refcount
     * does the whole job. See TODO 98 F5.
     */
    call_interface_ptr iface{};
    lua_State* T{nullptr};
    int tref{LUA_NOREF};
    run_ctx ctx{};
    function_done done{};

    void release() {
        if (st && st->L && tref != LUA_NOREF) {
            give_thread(*st, T, tref);
            tref = LUA_NOREF;
        }
        T = nullptr;
    }
};

static void finish_job(const std::shared_ptr<call_job>& job, bool ok, Variable out,
                       std::string err) {
    job->st->load = nullptr;
    job->st->run_command = nullptr;
    job->st->store = nullptr;
    job->st->open_space = nullptr;
    // the handles a script kept hold of point into this, so it goes with the call
    job->st->opened.clear();
    lua_callbacks(job->st->L)->userdata = nullptr;
    job->release();
    auto done = job->done;
    job->done = nullptr;
    if (done)
        done(ok, std::move(out), std::move(err));
}

static void pump_call(std::shared_ptr<call_job> job, int narg) {
    for (;;) {
        // the interrupt reads its budget through the state's callback userdata, and
        // the state is shared with anything else this session might run, so it is
        // pointed at this job every time the job is resumed rather than once
        lua_callbacks(job->st->L)->userdata = &job->ctx;
        job->st->load = &job->iface->load;
        job->st->run_command = &job->iface->run_command;
        job->st->store = &job->iface->store;
        job->st->open_space = &job->iface->open_space;
        int status = lua_resume(job->T, nullptr, narg);
        narg = 0;
        if (status == LUA_YIELD) {
            // past the inline slice now, so back to the configured one
            job->ctx.left = job->ctx.slice ? job->ctx.slice : 1;
            enqueue([job] { pump_call(job, 0); });
            return;
        }
        if (status != LUA_OK) {
            std::string err = lua_tostring(job->T, -1) ? lua_tostring(job->T, -1)
                                                       : "FUNCTION luau call";
            finish_job(job, false, Variable(nullptr), err);
            return;
        }
        Variable out;
        std::string err;
        bool ok = to_variable(job->T, lua_gettop(job->T), out, err, 0);
        finish_job(job, ok, std::move(out), std::move(err));
        return;
    }
}

void start_function(const std::string& space, const std::string& name,
                    const call_interface_ptr& iface,
                    const heap::vector<std::string>& args, uint64_t insns,
                    uint64_t deadline_ms, const function_states_ptr& cache,
                    const function_done& done) {
    auto job = std::make_shared<call_job>();
    job->cache = cache ? cache : make_function_states();
    job->iface = iface;
    job->done = done;

    job->st = state_for(*job->cache, space);
    if (!job->st) {
        done(false, Variable(nullptr), "FUNCTION luau state");
        return;
    }
    space_state* st = job->st;
    st->load = &job->iface->load;

    auto it = st->functions.find(name);
    if (it == st->functions.end()) {
        if (st->functions.size() >= max_cached_functions) {
            job->cache->spaces.erase(space);
            st = job->st = state_for(*job->cache, space);
            if (!st) {
                done(false, Variable(nullptr), "FUNCTION luau state");
                return;
            }
            st->load = &job->iface->load;
        }
        std::string source;
        if (!job->iface->load(name, source)) {
            st->load = nullptr;
            done(false, Variable(nullptr), "no such function");
            return;
        }
        compiled c;
        std::string err;
        bool built = compile_into(*st, name, source, c, err);
        st->load = nullptr;
        if (!built) {
            done(false, Variable(nullptr), err);
            return;
        }
        it = st->functions.emplace(name, c).first;
    }
    st->load = nullptr;
    const compiled& c = it->second;

    int given = (int) args.size();
    if ((c.arity > 0 && given != c.arity) || (c.arity < 0 && given < -c.arity)) {
        done(false, Variable(nullptr), "wrong number of arguments for '" + name + "'");
        return;
    }

    // a coroutine of its own, so the interrupt has somewhere to yield to. Pinned in
    // the registry: nothing else refers to it while it is suspended
    take_thread(*st, job->T, job->tref);

    job->ctx.slice = insns;
    job->ctx.left = insns ? insns : 1;
    if (deadline_ms)
        job->ctx.deadline = art::now() + static_cast<int64_t>(deadline_ms);

    /*
     * Arguments arrive as varargs, not as one table:
     *
     *     arity = 1
     *     function call(key) return barch.store.get(key) end
     *
     * which reads the way a function of fixed arity should. A script that wants them
     * as a table writes `local argv = {...}` and has both, so the host does not have
     * to offer two shapes to give both.
     */
    lua_getref(job->T, c.fn);
    for (const auto& a : args) {
        lua_pushlstring(job->T, a.data(), a.size());
    }
    /*
     * The first slice runs here, on the thread that asked.
     *
     * Parking costs about 20us - a shard latch, a pool hop, a wake and a post back -
     * which is nothing against a script that runs for milliseconds and everything
     * against one that is a single line. A GET written in Luau measured 4.7x the
     * built-in, and all of it was this, so a script that finishes immediately should
     * not pay for machinery it never uses.
     *
     * The slice is deliberately short. It is not the full one: a full slice on a
     * service thread is exactly what parking was for. Long enough for a one liner,
     * bounded enough that a script which is not one parks almost at once.
     */
    job->ctx.left = inline_insns;
    pump_call(job, (int) args.size());
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

void start_function(const std::string&, const std::string&, const call_interface_ptr&,
                    const heap::vector<std::string>&, uint64_t, uint64_t,
                    const function_states_ptr&, const function_done& done) {
    done(false, Variable(nullptr), "luau not built");
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
