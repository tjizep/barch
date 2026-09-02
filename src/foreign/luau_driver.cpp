#include "driver.h"
#include "pool.h"
#include "sql.h"
#include "key_space.h"
#include "configuration.h"
#include "lzr_log.h"
#include "art/nodes.h"
#include "function_api.h"

#include <cmath>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

#ifdef BARCH_HAS_LUAU
#include "lua.h"
#include "lualib.h"
#include "luacode.h"
#include "nk_luau.h"
#include "fetch_luau.h"
#include "simdjson_luau.h"
#include "crow_luau.h"
#endif

namespace barch {
namespace foreign {

#ifdef BARCH_HAS_LUAU

struct run_ctx {
    uint64_t left{0};
    uint64_t slice{0};
    int64_t deadline{0};
    /**
     * inside a locked region a shard lock is held, so the three things that would
     * deadlock or stall behind it are refused: yielding (a parked call holding a shard
     * lock never comes back), barch.call (a command takes shard locks of its own), and
     * sql.query (network I/O with a shard stopped). See TODO 98 F6.
     */
    bool locked{false};
    /** what is left of the region's own cap, since it cannot end in a yield */
    uint64_t locked_left{0};
};

/*
 * Every Luau state barch builds is built with this, so the bytes they hold show up in
 * INFO MEMORY - see TODO 151.
 *
 * Counted in the allocator rather than asked for with `lua_gc(LUA_GCCOUNT)`, because
 * a state belongs to the session using it: reading its collector from the thread that
 * happens to be serving INFO would be a race. An allocator sees every state - the
 * per-session function states, the foreign fill states, and the scratch one a SETF
 * compile check uses - and it sees them live.
 */
static void* luau_alloc(void* ud, void* ptr, size_t osize, size_t nsize) {
    // a state built for a pool also reports into that pool's own counter, so HTTP
    // STATUS can say what its VMs hold without reading a live collector - TODO 181
    auto* local = static_cast<std::atomic<uint64_t>*>(ud);
    if (nsize > osize) {
        statistics::luau_bytes += nsize - osize;
        if (local)
            *local += nsize - osize;
    } else {
        statistics::luau_bytes -= osize - nsize;
        if (local)
            *local -= osize - nsize;
    }
    if (nsize == 0) {
        free(ptr);
        return nullptr;
    }
    return realloc(ptr, nsize);
}

/** a state built the way barch builds them, counted and countable */
static lua_State* new_counted_state(std::atomic<uint64_t>* local = nullptr) {
    lua_State* L = lua_newstate(luau_alloc, local);
    if (L)
        ++statistics::luau_states;
    return L;
}

static void close_counted_state(lua_State* L) {
    if (!L)
        return;
    lua_close(L);
    if (statistics::luau_states > 0)
        --statistics::luau_states;
}

/**
 * what a running call carries on its coroutine - TODO 150.
 *
 * Not in the function's environment, which is where it first looks like it belongs:
 * `luaL_sandboxthread` gives each function its own globals table and a script can
 * write to it, so a script could overwrite the space and point `sql.query` at another
 * space's database. Thread data is C side only, with no Lua-visible key to shadow.
 *
 * On the coroutine rather than the VM, because one state now serves every space a
 * session uses: a nested CALLF is a second job on the same VM, and anything kept per
 * VM would need saving and restoring around it.
 */
struct call_job;

struct running_call {
    /** the space the function was loaded from, which is where require looks */
    std::string space;
    /** the space the call is running against - barch.store / barch.current */
    std::string running;
    /**
     * the stored-function call this coroutine belongs to, when there is one.
     * Null for the foreign fill states and for a Crow handler, neither of which
     * can be parked - see `park_call` in driver.h.
     */
    call_job* owner{nullptr};
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

/** the space this call is running in, or empty on the foreign fill path - TODO 150 */
static std::string current_space(lua_State* L);

static int sql_query(lua_State* L) {
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
    luaopen_nk(L);
    luaopen_simdjson(L);
    luaopen_crowhttp(L);
    luaopen_fetch(L);
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
    lua_State* L = new_counted_state();
    if (!L) {
        err = "luau state failed";
        return false;
    }
    int rc = luau_load(L, "=barch", bytecode.data(), bytecode.size(), 0);
    if (rc != 0) {
        err = lua_tostring(L, -1) ? lua_tostring(L, -1) : "luau load failed";
        close_counted_state(L);
        return false;
    }
    if (lua_pcall(L, 0, 0, 0) != 0) {
        err = lua_tostring(L, -1) ? lua_tostring(L, -1) : "luau script error";
        close_counted_state(L);
        return false;
    }
    lua_getglobal(L, entry);
    if (lua_type(L, -1) != LUA_TFUNCTION) {
        err = std::string("luau script has no ") + entry + "()";
        close_counted_state(L);
        return false;
    }
    close_counted_state(L);
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
            close_counted_state(L);
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
            close_counted_state(job->L);
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
    job->L = new_counted_state();
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
    if (ctx->locked) {
        // a hard cap inside the region, because the usual budget ends in a yield and a
        // yield is exactly what must not happen while a shard lock is held
        if (ctx->locked_left == 0)
            luaL_error(L, "FUNCTION locked region ran too long");
        --ctx->locked_left;
        return;
    }
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
    /** exposed command name -> the function transport() named for it - TODO 188 */
    heap::string_map<int> methods{};
    /** and the arity each declared, same convention as the script-level one */
    heap::string_map<int> method_arity{};
};

/** hand a compiled function's registry refs back, or the state keeps it alive
 *  forever - the state now lives for the session, so nothing else will drop them */
static void drop_compiled(lua_State* L, compiled& c) {
    for (auto& m : c.methods)
        if (m.second != LUA_NOREF) lua_unref(L, m.second);
    c.methods.clear();
    if (c.fn != LUA_NOREF) lua_unref(L, c.fn);
    if (c.env != LUA_NOREF) lua_unref(L, c.env);
    if (c.envt != LUA_NOREF) lua_unref(L, c.envt);
    c.fn = c.env = c.envt = LUA_NOREF;
}

/** a function's key in the shared map: the space, then the name, so `A` + `B_C` and
 *  `A_B` + `C` cannot collide the way a printable separator would let them */
static std::string qualified(const std::string& space, const std::string& name) {
    std::string q;
    q.reserve(space.size() + name.size() + 1);
    q.append(space);
    q.push_back('\0');
    q.append(name);
    return q;
}

struct space_state {
    lua_State* L{nullptr};
    /** stack of space names gathered during require */
    heap::vector<std::string> space_stack;
    /** keyed by `qualified(space, name)` - one map for every space the session uses */
    heap::string_map<compiled> functions{};
    /**
     * scratch for building that key on the call path, so a warm call does not
     * allocate one per call. Measured: building it fresh cost about 8% of the fixed
     * cost of a call, which is the whole of what a function that does nothing does.
     */
    std::string keybuf{};
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
    /** where the spaces `barch.space.NAME` opened live - the interface owns them */
    heap::string_map<store_access>* opened{nullptr};
    /**
     * what is being compiled right now, innermost last. A require for something on
     * this stack is a cycle, and the stack is the path to put in the message.
     */
    heap::vector<std::string> loading{};
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
        // the functions go with it, so what they were counted as goes too
        if (statistics::luau_functions >= functions.size())
            statistics::luau_functions -= functions.size();
        close_counted_state(L);
    }
};

struct function_states {
    /** where this cache's Luau bytes are counted as well, if anyone asked - TODO 181 */
    std::shared_ptr<std::atomic<uint64_t>> bytes{};
    /*
     * One state for the session, not one per space it touches - TODO 150.
     *
     * A state is about 50kB (ten libraries, the barch and sql tables, the VM's own
     * globals) and a compiled function about 0.5kB, so duplicating the state per space
     * was paying the expensive part per space and capping the cheap one. Almost
     * nothing in it was ever space specific: the functions map is, and is keyed by
     * space now, and the space a call runs in travels on the coroutine.
     */
    std::unique_ptr<space_state> only{};
};

function_states_ptr make_function_states() {
    return std::make_shared<function_states>();
}

function_states_ptr make_function_states(std::shared_ptr<std::atomic<uint64_t>> into) {
    auto fs = std::make_shared<function_states>();
    fs->bytes = std::move(into);
    return fs;
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

static int store_get(lua_State* L) {
    size_t n = 0;
    const char* k = luaL_checklstring(L, 1, &n);
    const auto* s = store_of(L, "get");
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
    return 0;
}

static int store_remove(lua_State* L) {
    size_t n = 0;
    const char* k = luaL_checklstring(L, 1, &n);
    lua_pushboolean(L, store_of(L, "remove", true)->remove({k, n}));
    return 1;
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

    bool ok = s->locked(key, [&]() -> bool {
        if (rc) {
            rc->locked = true;
            // cannot yield while the shard is held, so the usual slice is a hard
            // stop here. function_slice_insns is the count; function_deadline_ms
            // is already on ctx.deadline and still applies
            rc->locked_left = rc->slice ? rc->slice : get_function_slice_insns();
        }
        // pcall rather than a straight call: an error has to come back through here so
        // the lock is given up on this side before it is re-raised to the script
        lua_pushvalue(L, fn_at);
        raised = lua_pcall(L, 0, 1, 0);
        if (rc)
            rc->locked = false;
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
    char* end = nullptr;
    long long parsed = strtoll(s, &end, 10);
    if (end != s + (ptrdiff_t) len || errno == ERANGE)
        luaL_error(L, "FUNCTION expected an integer");
    return (int64_t) parsed;
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

static int do_set_buffer_at(lua_State* L, const store_access* s, int key_idx) {
    if (!s->may_write)
        luaL_error(L, "FUNCTION not authorized to write there");
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
    if (!s->may_write)
        luaL_error(L, "FUNCTION not authorized to write there");
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

static int store_get_buffer_at(lua_State* L) {
    size_t n = 0;
    const char* k = luaL_checklstring(L, 1, &n);
    size_t offset = check_buf_offset(L, 2);
    return push_copied_buffer(L, store_of(L, "getBufferAt"), {k, n}, offset);
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
    // set when this is a private scratch space from barch.art()
    barch::key_space_ptr owned{};
    std::unique_ptr<store_access> owned_access{};
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
            // a walk never hands out a tomb as a row, so anything but present here
            // is an absent value rather than a cached miss
            if (c->store->get(r.key, v) != store_access::read_state::present)
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
    c->store = h->store;
    c->name = h->name;
    lua_pushcclosure(L, member_next, "next", 1);
    return 1;
}

static int push_store_get(lua_State* L, const store_access* s, const std::string& key) {
    if (!s->may_read)
        luaL_error(L, "FUNCTION not authorized to read there");
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
    if (!s->may_write)
        luaL_error(L, "FUNCTION not authorized to write there");
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

/** the store methods plus container/kind - same interface on a live space and on barch.art() */
static int space_namecall(lua_State* L) {
    const char* m = lua_namecallatom(L, nullptr);
    const store_access* s = handle_store(L, 1);
    if (!m)
        luaL_error(L, "FUNCTION no such method on a key space");

    if (!strcmp(m, "get")) {
        size_t n = 0;
        const char* k = luaL_checklstring(L, 2, &n);
        return push_store_get(L, s, {k, n});
    }
    if (!strcmp(m, "set")) {
        if (!s->may_write)
            luaL_error(L, "FUNCTION not authorized to write there");
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
    if (!strcmp(m, "remove")) {
        if (!s->may_write)
            luaL_error(L, "FUNCTION not authorized to write there");
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
    if (!strcmp(m, "range")) {
        size_t ln = 0, hn = 0;
        const char* lo = luaL_checklstring(L, 2, &ln);
        const char* hi = luaL_checklstring(L, 3, &hn);
        int64_t limit = (int64_t) luaL_checknumber(L, 4);
        if (limit <= 0 || limit > max_range_batch)
            limit = max_range_batch;
        heap::vector<std::string> keys;
        s->range({lo, ln}, {hi, hn}, limit, keys);
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
    if (!strcmp(m, "getBufferAt")) {
        if (!s->may_read)
            luaL_error(L, "FUNCTION not authorized to read there");
        size_t n = 0;
        const char* k = luaL_checklstring(L, 2, &n);
        size_t offset = check_buf_offset(L, 3);
        return push_copied_buffer(L, s, {k, n}, offset);
    }
    if (!strcmp(m, "setBufferAt"))
        return do_set_buffer_at(L, s, 2);
    if (!strcmp(m, "getInt32At"))
        return do_get_int_at(L, s, 2, 4);
    if (!strcmp(m, "setInt32At"))
        return do_set_int_at(L, s, 2, 4);
    if (!strcmp(m, "getInt64At"))
        return do_get_int_at(L, s, 2, 8);
    if (!strcmp(m, "setInt64At"))
        return do_set_int_at(L, s, 2, 8);
    luaL_error(L, "FUNCTION no such method on a key space");
    return 0;
}

/** push a space handle around an already-open store_access */
static int push_space_handle(lua_State* L, space_state* st, const store_access* store) {
    auto* h = static_cast<space_handle*>(lua_newuserdatadtor(L, sizeof(space_handle),
        [](void* p) { static_cast<space_handle*>(p)->~space_handle(); }));
    new (h) space_handle();
    h->st = st;
    h->store = store;
    lua_getfield(L, LUA_REGISTRYINDEX, "barch.space.meta");
    lua_setmetatable(L, -2);
    return 1;
}

/*
 * barch.current() - the space this call is running against, as a space handle.
 *
 * `barch.store` is the same data with the older method table. This is the handle
 * `barch.space.NAME` would give you if you already knew the name. See TODO 160.
 */
static int current_space_handle(lua_State* L) {
    space_state* st = state_of(L);
    if (!st || !st->store)
        luaL_error(L, "FUNCTION barch.current is not available here");
    return push_space_handle(L, st, st->store);
}

/** barch.running() - the canonical name of that space, empty for the default */
static int running_space_name(lua_State* L) {
    auto* rc = static_cast<running_call*>(lua_getthreaddata(L));
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
    if (!st || !st->open_space)
        luaL_error(L, "FUNCTION barch.space is not available here");
    if (!st->opened)
        luaL_error(L, "FUNCTION barch.space is not available here");
    auto have = st->opened->find(name);
    if (have == st->opened->end()) {
        store_access opened;
        // an unknown name is not a key space and must not become one
        if (!(*st->open_space)(name, opened))
            luaL_error(L, "FUNCTION no key space called %s", name.c_str());
        have = st->opened->emplace(name, std::move(opened)).first;
    }
    return push_space_handle(L, st, &have->second);
}

/*
 * barch.art() - a private one-shard space, same store_access as barch.store.
 *
 * Not named, not saved, not in the space map. The handle owns it; when Luau
 * collects the userdata the shard goes with it. min/max/range are the ordered
 * operations a working set (an HNSW candidate queue, a priority queue) needs
 * without walking the live store.
 */
static space_handle* new_space_handle(lua_State* L) {
    auto* h = static_cast<space_handle*>(lua_newuserdatadtor(L, sizeof(space_handle),
        [](void* p) { static_cast<space_handle*>(p)->~space_handle(); }));
    new (h) space_handle();
    lua_getfield(L, LUA_REGISTRYINDEX, "barch.space.meta");
    lua_setmetatable(L, -2);
    return h;
}

static int art_open(lua_State* L) {
    auto space = barch::key_space::make_scratch();
    auto access = barch::functions::store_for_owner(space);
    auto* h = new_space_handle(L);
    h->owned = std::move(space);
    h->owned_access = std::make_unique<store_access>(std::move(access));
    h->store = h->owned_access.get();
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
    if (auto* rc = static_cast<run_ctx*>(lua_callbacks(L)->userdata); rc && rc->locked)
        luaL_error(L, "FUNCTION barch.call is not allowed inside a locked region");
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
    if (!(*st->run_command)(argv, out, err)) {
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
        luaL_error(L, "%s", err.empty() ? "FUNCTION call failed" : err.c_str());
    }
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
        case LUA_TSTRING: {
            size_t n = 0;
            const char* s = lua_tolstring(L, idx, &n);
            out = Variable(std::string(s, n));
            return true;
        }
        case LUA_TBUFFER: {
            size_t n = 0;
            void* b = lua_tobuffer(L, idx, &n);
            out = Variable(std::string(b ? static_cast<const char*>(b) : "", n));
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


/* how much one session may keep before its state for a space is thrown away and
 * built again. Crude next to the LRU that TODO 98 C asks for, but bounded, and the
 * bound is what stops a roaming client holding the whole space's functions open */
/*
 * A backstop rather than a working limit - TODO 150.
 *
 * A compiled function is about 0.5kB, so this is a couple of megabytes for a session
 * that has called four thousand distinct functions, and it is bounded anyway by how
 * many are stored. The old 64 was a real limit and a cliff: the sixty fifth call threw
 * away the whole state, all its compiled functions and its libraries, and paid for a
 * fresh one. Nothing that expensive happens now.
 */
enum { max_cached_functions = 4096 };

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

static space_state* state_for(function_states& cache) {
    if (cache.only)
        return cache.only.get();
    auto st = std::make_unique<space_state>();
    st->L = new_counted_state(cache.bytes.get());
    if (!st->L)
        return nullptr;
    lua_State* L = st->L;
    // no space is written into the registry here any more: one state serves every
    // space this session touches, so which space a call runs in travels on the
    // coroutine instead - see `running_space` and TODO 150
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
    make_tomb(L);
    push_tomb(L);
    lua_setfield(L, -2, "tomb");
    lua_newtable(L);
    lua_pushcfunction(L, store_get, "get");
    lua_setfield(L, -2, "get");
    lua_pushcfunction(L, store_set, "set");
    lua_setfield(L, -2, "set");
    lua_pushcfunction(L, store_remove, "remove");
    lua_setfield(L, -2, "remove");
    lua_pushcfunction(L, store_locked, "locked");
    lua_setfield(L, -2, "locked");
    lua_pushcfunction(L, store_shard_number, "shardNumber");
    lua_setfield(L, -2, "shardNumber");
    lua_pushcfunction(L, store_has_lock, "hasLock");
    lua_setfield(L, -2, "hasLock");
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
    lua_pushcfunction(L, store_get_buffer_at, "getBufferAt");
    lua_setfield(L, -2, "getBufferAt");
    lua_pushcfunction(L, store_set_buffer_at, "setBufferAt");
    lua_setfield(L, -2, "setBufferAt");
    lua_pushcfunction(L, store_get_int32_at, "getInt32At");
    lua_setfield(L, -2, "getInt32At");
    lua_pushcfunction(L, store_set_int32_at, "setInt32At");
    lua_setfield(L, -2, "setInt32At");
    lua_pushcfunction(L, store_get_int64_at, "getInt64At");
    lua_setfield(L, -2, "getInt64At");
    lua_pushcfunction(L, store_set_int64_at, "setInt64At");
    lua_setfield(L, -2, "setInt64At");
    lua_setfield(L, -2, "store");
    lua_pushcfunction(L, art_open, "art");
    lua_setfield(L, -2, "art");
    lua_pushcfunction(L, current_space_handle, "current");
    lua_setfield(L, -2, "current");
    lua_pushcfunction(L, running_space_name, "running");
    lua_setfield(L, -2, "running");

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
    cache.only = std::move(st);
    return raw;
}


/*
 * Read a `transport()` of kind "resp" - TODO 188.
 *
 * Called on the chunk's own environment thread, so the globals it defined are
 * the ones in scope. `refs` is filled with a registry reference per exposed name
 * when the caller needs to run them later; the SETF check passes null and takes
 * only the names and categories.
 *
 * Absent transport(), or one of another kind, is not an error - most functions
 * have none, and a "resource" belongs to Crow.
 */
static bool read_resp_transport(lua_State* L, lua_State* T, resp_spec& spec,
                                heap::string_map<int>* refs, std::string& err) {
    lua_getglobal(T, "transport");
    if (lua_type(T, -1) != LUA_TFUNCTION) {
        lua_pop(T, 1);
        return true;
    }
    if (lua_pcall(T, 0, 1, 0) != 0) {
        err = lua_tostring(T, -1) ? lua_tostring(T, -1) : "transport() failed";
        lua_pop(T, 1);
        return false;
    }
    if (lua_type(T, -1) != LUA_TTABLE) {
        err = "transport() must return a table";
        lua_pop(T, 1);
        return false;
    }
    spec.has_transport = true;
    lua_getfield(T, -1, "kind");
    std::string kind = lua_isstring(T, -1) ? lua_tostring(T, -1) : "";
    lua_pop(T, 1);
    for (auto& ch : kind)
        ch = (char) tolower((unsigned char) ch);
    if (kind != "resp") {
        lua_pop(T, 1);
        return true;
    }
    spec.is_resp = true;

    lua_getfield(T, -1, "methods");
    if (lua_type(T, -1) != LUA_TTABLE) {
        err = "resp transport() needs a methods table";
        lua_pop(T, 2);
        return false;
    }
    // -1 methods, -2 transport table
    lua_pushnil(T);
    while (lua_next(T, -2) != 0) {
        // -1 value, -2 key
        if (lua_type(T, -2) != LUA_TSTRING) {
            err = "resp methods must be keyed by the exposed command name";
            lua_pop(T, 4);
            return false;
        }
        if (lua_type(T, -1) != LUA_TFUNCTION) {
            err = std::string("resp method '") + lua_tostring(T, -2) + "' is not a function";
            lua_pop(T, 4);
            return false;
        }
        std::string name = lua_tostring(T, -2);
        for (auto& ch : name)
            ch = (char) toupper((unsigned char) ch);
        if (name.empty()) {
            err = "resp method name is empty";
            lua_pop(T, 4);
            return false;
        }
        resp_method m;
        m.name = name;
        spec.methods.push_back(std::move(m));
        if (refs) {
            lua_pushvalue(T, -1);
            (*refs)[name] = lua_ref(T, -1);
            lua_pop(T, 1);
        }
        lua_pop(T, 1); // the value; the key stays for lua_next
    }
    lua_pop(T, 1); // methods

    if (spec.methods.empty()) {
        err = "resp transport() exposes no methods";
        lua_pop(T, 1);
        return false;
    }

    // categories, keyed by the same exposed names. Absent is allowed and means
    // the function declares nothing, which the caller decides what to do about
    lua_getfield(T, -1, "categories");
    if (lua_type(T, -1) == LUA_TTABLE) {
        for (auto& m : spec.methods) {
            lua_getfield(T, -1, m.name.c_str());
            if (lua_type(T, -1) == LUA_TTABLE) {
                int n = (int) lua_objlen(T, -1);
                for (int i = 1; i <= n; ++i) {
                    lua_rawgeti(T, -1, i);
                    if (lua_isstring(T, -1)) {
                        std::string c = lua_tostring(T, -1);
                        for (auto& ch : c)
                            ch = (char) tolower((unsigned char) ch);
                        m.categories.push_back(c);
                    }
                    lua_pop(T, 1);
                }
            } else if (!lua_isnil(T, -1)) {
                err = "categories for '" + m.name + "' must be a list of names";
                lua_pop(T, 3);
                return false;
            }
            lua_pop(T, 1);
        }
    } else if (!lua_isnil(T, -1)) {
        err = "resp transport() categories must be a table";
        lua_pop(T, 2);
        return false;
    }
    lua_pop(T, 1); // categories

    lua_getfield(T, -1, "arity");
    if (lua_type(T, -1) == LUA_TTABLE) {
        for (auto& m : spec.methods) {
            lua_getfield(T, -1, m.name.c_str());
            if (lua_isnumber(T, -1))
                m.arity = (int) lua_tointeger(T, -1);
            lua_pop(T, 1);
        }
    }
    lua_pop(T, 1); // arity

    lua_pop(T, 1); // the transport table
    (void) L;
    return true;
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
 * the default space. require("SPACE.NAME") loads NAME from SPACE, and a nested
 * require("helpers") stays in SPACE until that dotted require returns.
 *
 * What comes back is that function's globals table, so a required module can offer
 * helpers as well as its own `call`. Cycles are refused with the path that made them,
 * rather than recursing until the stack gives out. See TODO 98 D, 162.
 */
static int function_require(lua_State* L) {
    size_t n = 0;
    const char* raw = luaL_checklstring(L, 1, &n);
    std::string given(raw, n);
    space_state* st = state_of(L);
    if (!st || !st->load)
        luaL_error(L, "FUNCTION require is not available here");

    // split before folding: space names keep their case, same as HNSW.SET
    std::string space_name;
    std::string name;
    bool exact = false;
    bool pushed = false;
    auto dot = given.find('.');
    if (dot != std::string::npos && dot > 0 && dot + 1 < given.size()) {
        space_name = given.substr(0, dot);
        name = given.substr(dot + 1);
        exact = true;
        st->space_stack.push_back(space_name);
        pushed = true;
    } else {
        name = std::move(given);
        if (!st->space_stack.empty())
            space_name = st->space_stack.back();
        // else leave space_name empty: the loader uses the space being
        // compiled or run. Passing current_space() here broke function sync,
        // whose scratch is named `-sN` and is not in the space map.
    }
    for (auto& ch : name)
        ch = (char) toupper((unsigned char) ch);

    struct pop_if_pushed {
        space_state* st{};
        std::string space;
        bool armed{false};
        ~pop_if_pushed() {
            if (!armed || !st)
                return;
            if (!st->space_stack.empty() && st->space_stack.back() == space)
                st->space_stack.pop_back();
        }
    } popper{st, space_name, pushed};

    const std::string key = qualified(space_name.empty() ? current_space(L) : space_name, name);
    auto have = st->functions.find(key);
    if (have != st->functions.end()) {
        lua_getref(L, have->second.envt);
        return 1;
    }
    for (const auto& busy : st->loading) {
        if (busy == key) {
            std::string path;
            for (const auto& step : st->loading) {
                auto at = step.find('\0');
                path += at == std::string::npos ? step : step.substr(at + 1);
                path += " -> ";
            }
            path += name;
            luaL_error(L, "FUNCTION cycle %s", path.c_str());
        }
    }
    std::string source;
    if (!(*st->load)(space_name, name, exact, source))
        luaL_error(L, "FUNCTION require has no function %s", name.c_str());
    compiled c;
    std::string err;
    if (!compile_into(*st, key, source, c, err))
        luaL_error(L, "%s", err.c_str());
    st->functions.emplace(key, c);
    ++statistics::luau_functions;
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
    /*
     * The chunk's own top level can `require`, and require resolves within the space
     * the function belongs to - so this thread has to carry that space before the
     * chunk runs, not after. The key is qualified, so the space is the part before the
     * separator. Without this, a require during compilation looked in the wrong space
     * and a cycle went undetected, because the stack held one key shape and the lookup
     * built another. See TODO 150.
     */
    running_call scope;
    auto at = name.find('\0');
    if (at != std::string::npos)
        scope.space = name.substr(0, at);
    lua_setthreaddata(T, &scope);
    // the thread's own globals table, which is what require hands to whoever asked
    lua_pushvalue(T, LUA_GLOBALSINDEX);
    int envt = lua_ref(T, -1);
    lua_pop(T, 1);

    // on this stack for as long as its chunk is running, so a require that comes back
    // round to it is a cycle rather than a recursion
    st.loading.emplace_back(name);
    struct pop_on_exit {
        space_state& st;
        ~pop_on_exit() { st.loading.pop_back(); }
    } popper{st};

    // `scope` is a local, so it must not be readable from the thread once this
    // returns - the thread is kept as the function's environment
    struct clear_on_exit {
        lua_State* T;
        ~clear_on_exit() { lua_setthreaddata(T, nullptr); }
    } clearer{T};

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
    // and the commands a resp transport() exposes, so a call can reach one by the
    // name a client typed rather than by the key - TODO 188
    resp_spec spec;
    std::string terr;
    if (!read_resp_transport(L, T, spec, &out.methods, terr)) {
        err = terr;
        drop_compiled(L, out);
        out.env = LUA_NOREF;
        out.envt = LUA_NOREF;
        return false;
    }
    for (const auto& m : spec.methods)
        out.method_arity[m.name] = m.arity;
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
static std::string current_space(lua_State* L) {
    auto* rc = static_cast<running_call*>(lua_getthreaddata(L));
    return rc ? rc->space : std::string{};
}

/**
 * A call stopped mid-flight while something outside Lua finishes - TODO 186.
 *
 * `resume` holds the job alive for as long as it is parked; nothing else does,
 * because the pool thread that was running it has gone back to the queue. The
 * reference is dropped the moment the call is resumed, so the cycle between the
 * job and its parking slot lasts exactly as long as the wait.
 *
 * `push` is filled by whichever thread the work completed on, and is only ever
 * *called* on the pool thread that resumes the coroutine. That is what keeps a
 * completion off the lua_State.
 */
struct parked_call {
    std::mutex mu;
    push_results push{};
    std::function<void()> resume{};
    /** the work has finished and `push` is set */
    bool done{false};
    /** the pump has seen the yield, so a completion has to wake it */
    bool waiting{false};
};

struct call_job {
    function_states_ptr cache{};
    space_state* st{nullptr};
    /** points at this job's coroutine for as long as the call runs */
    running_call scope{};
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
    /** so a C function holding only the lua_State can take a strong reference */
    std::weak_ptr<call_job> self{};
    /** set while this call is waiting on something that is not Lua - TODO 186 */
    parked_call_ptr parked{};
    /** when that wait started, so the deadline can be moved past it */
    int64_t parked_since{0};

    void release() {
        if (st && st->L && tref != LUA_NOREF) {
            // cleared before the coroutine goes back in the pool, so the next call
            // cannot read this job's space out of a thread it inherited
            if (T)
                lua_setthreaddata(T, nullptr);
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
    // the spaces themselves stay on the interface; only the way in goes with the call,
    // which is what stops a handle reaching them once the call is over
    job->st->opened = nullptr;
    lua_callbacks(job->st->L)->userdata = nullptr;
    job->release();
    // an error or a return while parked would otherwise leave the job holding the
    // slot and the slot holding the job
    if (job->parked) {
        job->parked->resume = nullptr;
        job->parked.reset();
    }
    auto done = job->done;
    job->done = nullptr;
    if (done)
        done(ok, std::move(out), std::move(err));
}

static void pump_call(std::shared_ptr<call_job> job, int narg) {
    for (;;) {
        // a call coming back from a park pushes what it was waiting for first, and
        // those values become the return values of the C function that yielded
        if (job->parked) {
            auto p = job->parked;
            push_results push;
            {
                std::lock_guard<std::mutex> lk(p->mu);
                if (!p->done)
                    return; // not our turn: the completion will bring it back
                push = std::move(p->push);
            }
            // drop the mutual hold before running anything else
            job->parked.reset();
            p->resume = nullptr;
            /*
             * Waiting is not running. The deadline is there to stop a script
             * computing forever, and while parked this one was not computing at
             * all - it was a suspended coroutine and a socket. Charging it for
             * the wait would cap every request at the deadline, which for the
             * default 1000ms means no useful HTTP call at all. The request has
             * its own timeout for bounding the wait.
             */
            if (job->ctx.deadline && job->parked_since)
                job->ctx.deadline += art::now() - job->parked_since;
            job->parked_since = 0;
            narg = push ? push(job->T) : 0;
        }
        // the interrupt reads its budget through the state's callback userdata, and
        // the state is shared with anything else this session might run, so it is
        // pointed at this job every time the job is resumed rather than once
        lua_callbacks(job->st->L)->userdata = &job->ctx;
        job->st->load = &job->iface->load;
        job->st->run_command = &job->iface->run_command;
        job->st->store = &job->iface->store;
        job->st->open_space = &job->iface->open_space;
        job->st->opened = &job->iface->opened;
        int status = lua_resume(job->T, nullptr, narg);
        narg = 0;
        if (status == LUA_YIELD) {
            /*
             * Two yields reach here and they are not the same thing. The budget
             * yield means "this slice is used up", and the job goes straight back
             * on the queue. A parked yield means "waiting on something that is not
             * Lua", and it must NOT be requeued - the completion does that, or the
             * coroutine gets resumed twice.
             */
            if (job->parked) {
                auto p = job->parked;
                std::lock_guard<std::mutex> lk(p->mu);
                if (!p->done) {
                    p->waiting = true;
                    return;
                }
                // it finished before the yield was even seen; no lost wake-up
                continue;
            }
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
        /*
         * A script that returns nothing leaves an empty stack, and 0 is not a valid
         * index - `lua_type(L, 0)` walks off the frame rather than answering LUA_TNONE,
         * which took the server down for a `function call(k) end` with nothing else
         * wrong with it. Nothing to return is a null reply, the same answer redis gives
         * a script with no return statement.
         */
        bool ok = true;
        if (lua_gettop(job->T) == 0) {
            out = Variable(nullptr);
        } else {
            ok = to_variable(job->T, lua_gettop(job->T), out, err, 0);
        }
        finish_job(job, ok, std::move(out), std::move(err));
        return;
    }
}

parked_call_ptr park_call(lua_State* L) {
    // a Crow handler runs under lua_pcall while holding a VM slot from the space's
    // pool. It cannot yield, and it must not: the yield would return through
    // handle_route, the slot would go back to the pool, and the next request could
    // pick up that state while this coroutine is still suspended on it.
    if (!lua_isyieldable(L))
        return nullptr;
    auto* rc = static_cast<running_call*>(lua_getthreaddata(L));
    if (!rc || !rc->owner)
        return nullptr;
    auto job = rc->owner->self.lock();
    if (!job)
        return nullptr;
    auto p = std::make_shared<parked_call>();
    p->resume = [job] { pump_call(job, 0); };
    job->parked = p;
    job->parked_since = art::now();
    return p;
}

void complete_call(const parked_call_ptr& parked, push_results push) {
    if (!parked)
        return;
    std::function<void()> resume;
    {
        std::lock_guard<std::mutex> lk(parked->mu);
        if (parked->done)
            return; // a second completion is a no-op, not a second resume
        parked->done = true;
        parked->push = std::move(push);
        if (!parked->waiting)
            return; // the pump has not parked it yet and will see `done` itself
        parked->waiting = false;
        resume = parked->resume;
    }
    if (resume)
        enqueue(std::move(resume));
}

void start_function(const std::string& space, const std::string& name,
                    const call_interface_ptr& iface,
                    const heap::vector<std::string>& args, uint64_t insns,
                    uint64_t deadline_ms, const function_states_ptr& cache,
                    const function_done& done, const std::string& entry) {
    auto job = std::make_shared<call_job>();
    job->cache = cache ? cache : make_function_states();
    job->iface = iface;
    job->done = done;

    job->st = state_for(*job->cache);
    if (!job->st) {
        done(false, Variable(nullptr), "FUNCTION luau state");
        return;
    }
    space_state* st = job->st;
    job->scope.space = space;
    job->scope.running = job->iface->running_in;
    st->load = &job->iface->load;

    // into the state's buffer rather than a new string: assign keeps the capacity,
    // so the warm path allocates nothing
    std::string& key = st->keybuf;
    key.clear();
    key.append(space);
    key.push_back('\0');
    key.append(name);
    auto it = st->functions.find(key);
    if (it == st->functions.end()) {
        if (st->functions.size() >= max_cached_functions) {
            // the backstop. Give the refs back rather than just dropping the map, or
            // the state - which now outlives any one space - holds every one of them
            for (auto& e : st->functions)
                drop_compiled(st->L, e.second);
            if (statistics::luau_functions >= st->functions.size())
                statistics::luau_functions -= st->functions.size();
            st->functions.clear();
        }
        std::string source;
        if (!job->iface->load("", name, false, source)) {
            st->load = nullptr;
            done(false, Variable(nullptr), "no such function");
            return;
        }
        compiled c;
        std::string err;
        bool built = compile_into(*st, key, source, c, err);
        st->load = nullptr;
        if (!built) {
            done(false, Variable(nullptr), err);
            return;
        }
        it = st->functions.emplace(key, c).first;
        ++statistics::luau_functions;
    }
    st->load = nullptr;
    const compiled& c = it->second;

    /*
     * Which function in the chunk actually runs - TODO 188.
     *
     * With no entry it is `call`, as it always was. An entry names one of the
     * commands a resp transport() exposed, and the arity checked is the one that
     * method declared rather than the chunk's own.
     */
    int entry_ref = c.fn;
    int want_arity = c.arity;
    if (!entry.empty()) {
        auto m = c.methods.find(entry);
        if (m == c.methods.end()) {
            done(false, Variable(nullptr), "no such method '" + entry + "'");
            return;
        }
        entry_ref = m->second;
        auto a = c.method_arity.find(entry);
        want_arity = a == c.method_arity.end() ? 0 : a->second;
    }

    int given = (int) args.size();
    if ((want_arity > 0 && given != want_arity) || (want_arity < 0 && given < -want_arity)) {
        done(false, Variable(nullptr),
             "wrong number of arguments for '" + (entry.empty() ? name : entry) + "'");
        return;
    }

    // a coroutine of its own, so the interrupt has somewhere to yield to. Pinned in
    // the registry: nothing else refers to it while it is suspended
    take_thread(*st, job->T, job->tref);
    // the coroutine carries which space this call runs in, for as long as it runs.
    // The job outlives the coroutine's use of it, and `release` clears it - TODO 150
    lua_setthreaddata(job->T, &job->scope);
    // so a C function with only the lua_State can find this call and park it
    job->self = job;
    job->scope.owner = job.get();

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
    lua_getref(job->T, entry_ref);
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


bool http_vm_load(http_vm& vm, const std::string& name, const std::string& source,
                  http_route& out, std::string& err) {
    out = http_route{};
    out.name = name;
    if (!vm.cache)
        vm.cache = make_function_states();
    space_state* st = state_for(*vm.cache);
    if (!st) {
        err = "FUNCTION luau state";
        return false;
    }
    if (!vm.iface) {
        err = "HTTP luau interface";
        return false;
    }
    st->load = &vm.iface->load;
    compiled c;
    bool built = compile_into(*st, qualified(vm.space, name), source, c, err);
    st->load = nullptr;
    if (!built)
        return false;
    lua_getref(st->L, c.env);
    lua_State* T = lua_tothread(st->L, -1);
    lua_pop(st->L, 1);
    if (!T) {
        err = "HTTP function environment";
        drop_compiled(st->L, c);
        return false;
    }
    lua_getglobal(T, "transport");
    if (lua_type(T, -1) != LUA_TFUNCTION) {
        lua_pop(T, 1);
        drop_compiled(st->L, c);
        return true;
    }
    running_call scope;
    scope.space = vm.space;
    scope.running = vm.iface->running_in.empty() ? vm.space : vm.iface->running_in;
    lua_setthreaddata(T, &scope);
    st->load = &vm.iface->load;
    st->store = &vm.iface->store;
    st->open_space = vm.iface->open_space ? &vm.iface->open_space : nullptr;
    st->opened = &vm.iface->opened;
    int rc = lua_pcall(T, 0, 1, 0);
    lua_setthreaddata(T, nullptr);
    st->load = nullptr;
    st->store = nullptr;
    st->open_space = nullptr;
    st->opened = nullptr;
    if (rc != 0) {
        err = lua_tostring(T, -1) ? lua_tostring(T, -1) : "transport() failed";
        lua_pop(T, 1);
        drop_compiled(st->L, c);
        return false;
    }
    if (lua_isnil(T, -1)) {
        lua_pop(T, 1);
        drop_compiled(st->L, c);
        return true;
    }
    if (!crow_read_transport(T, -1, out, err)) {
        lua_pop(T, 1);
        drop_compiled(st->L, c);
        return false;
    }
    lua_pop(T, 1);
    out.name = name;
    // keep the compiled chunk so method closures stay alive
    std::string key = qualified(vm.space, name);
    st->functions.emplace(std::move(key), c);
    ++statistics::luau_functions;
    return true;
}

void http_vm_call(http_vm& vm, int fn_ref, const void* req, void* res, std::string& err) {
    err.clear();
    if (!vm.cache) {
        err = "HTTP luau state";
        return;
    }
    space_state* st = state_for(*vm.cache);
    if (!st || !st->L) {
        err = "HTTP luau state";
        return;
    }
    lua_State* L = st->L;
    run_ctx ctx;
    ctx.left = (std::numeric_limits<uint64_t>::max)() / 4;
    ctx.slice = ctx.left;
    if (vm.deadline_ms)
        ctx.deadline = art::now() + static_cast<int64_t>(vm.deadline_ms);
    lua_callbacks(L)->userdata = &ctx;
    st->load = vm.iface ? &vm.iface->load : nullptr;
    st->store = vm.iface ? &vm.iface->store : nullptr;
    st->run_command = vm.iface && vm.iface->run_command ? &vm.iface->run_command : nullptr;
    st->open_space = vm.iface && vm.iface->open_space ? &vm.iface->open_space : nullptr;
    st->opened = vm.iface ? &vm.iface->opened : nullptr;
    running_call scope;
    scope.space = vm.space;
    scope.running = vm.iface && !vm.iface->running_in.empty() ? vm.iface->running_in
                                                              : vm.space;
    lua_State* T = nullptr;
    int tref = LUA_NOREF;
    take_thread(*st, T, tref);
    lua_setthreaddata(T, &scope);
    lua_getref(T, fn_ref);
    crow_push_request(T, req);
    crow_push_response(T, res);
    int rc = lua_pcall(T, 2, 0, 0);
    lua_setthreaddata(T, nullptr);
    lua_callbacks(L)->userdata = nullptr;
    st->load = nullptr;
    st->store = nullptr;
    st->run_command = nullptr;
    st->open_space = nullptr;
    st->opened = nullptr;
    crow_http_end_call();
    if (rc != 0) {
        err = lua_tostring(T, -1) ? lua_tostring(T, -1) : "HTTP handler failed";
        lua_pop(T, 1);
    }
    give_thread(*st, T, tref);
}

bool compile_function(const std::string& space, const std::string& name,
                      const std::string& source, const source_loader& load,
                      std::string& err, resp_spec* spec) {
    // a state of its own, thrown away when this returns. It has to be a real one:
    // the script's top level may require others, and require needs both the loader
    // and a state to compile them into
    auto scratch = make_function_states();
    space_state* st = state_for(*scratch);
    if (!st) {
        err = "FUNCTION luau state";
        return false;
    }
    st->load = &load;
    // nothing is installed for barch.call or barch.store here: a script's top level
    // runs at SETF time, when there is no caller to run a command on, so one that
    // tries is told so rather than reaching a half-built one
    compiled c;
    // the same qualified key the call path uses, or require inside this chunk builds
    // one shape and the loading stack holds another - and a cycle goes undetected
    bool ok = compile_into(*st, qualified(space, name), source, c, err);
    if (ok && spec) {
        // read again for the categories, which the call path has no use for and so
        // does not keep. Only SETF pays this, and only once per stored function
        lua_getref(st->L, c.env);
        lua_State* T = lua_tothread(st->L, -1);
        lua_pop(st->L, 1);
        if (T) {
            st->load = &load;
            ok = read_resp_transport(st->L, T, *spec, nullptr, err);
        }
    }
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
                      const source_loader&, std::string& err, resp_spec*) {
    err = "luau not built";
    return false;
}

struct function_states {};

function_states_ptr make_function_states() {
    return nullptr;
}

function_states_ptr make_function_states(std::shared_ptr<std::atomic<uint64_t>>) {
    return nullptr;
}

parked_call_ptr park_call(lua_State*) {
    return nullptr;
}

void complete_call(const parked_call_ptr&, push_results) {
}

void start_function(const std::string&, const std::string&, const call_interface_ptr&,
                    const heap::vector<std::string>&, uint64_t, uint64_t,
                    const function_states_ptr&, const function_done& done,
                    const std::string&) {
    done(false, Variable(nullptr), "luau not built");
}

bool http_vm_load(http_vm&, const std::string&, const std::string&, http_route&,
                  std::string& err) {
    err = "luau not built";
    return false;
}

void http_vm_call(http_vm&, int, const void*, void*, std::string& err) {
    err = "luau not built";
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
