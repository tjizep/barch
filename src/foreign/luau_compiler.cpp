#include "luau_internal.h"

#ifdef BARCH_HAS_LUAU

#include "../fs_api.h"
#include "configuration.h"
#include "luacode.h"
#include "Luau/CodeGen.h"
#include "nk_luau.h"
#include "fetch_luau.h"
#include "mail_luau.h"
#include "resp_luau.h"
#include "simdjson_luau.h"
#include "crow_luau.h"
#include "luau_protect.h"

#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <string>
#include <vector>

namespace barch {
namespace foreign {

namespace detail {

static int library_require(lua_State* L);
static int require_package(lua_State* L, uint64_t from);
static int function_require(lua_State* L);

static int blocked_require(lua_State* L) {
    luaL_error(L, "FOREIGN require is not allowed");
    return 0;
}


void open_safe(lua_State* L) {
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
    luaopen_mail(L);
    luaopen_resp(L);
    lua_pushcfunction(L, blocked_require, "require");
    lua_setglobal(L, "require");
    lua_newtable(L);
    lua_pushcfunction(L, sql_query, "query");
    lua_setfield(L, -2, "query");
    lua_setglobal(L, "sql");
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
bool load_source(const std::string& spec, std::string& source, std::string& err) {
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
    lua_State* L = new_counted_state(nullptr, false); // check only, see TODO 400
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

/*
 * package.luau - TODO 582. Nothing of barch is opened into the state, so setup()
 * can only compute a table, and the time limit is wall clock because the state is
 * thrown away afterwards: there is no slice to yield to.
 */
static thread_local int64_t package_deadline = 0;

static void package_interrupt(lua_State* L, int gc) {
    if (gc >= 0)
        return;
    if (package_deadline && art::now() > package_deadline)
        luaL_error(L, "package.luau took longer than 2 seconds");
}

// encode under pcall: simdjson_encode_at raises on what it cannot encode
static int package_encode(lua_State* L) {
    auto* out = static_cast<std::string*>(lua_touserdata(L, 2));
    simdjson_encode_at(L, 1, *out);
    return 0;
}


bool compile_source(const std::string& source, std::string& bytecode, std::string& err) {
    // one entry point for every stored Luau function. A fill answers `call(key, space)`
    // where it used to answer `resolve(key, space)` - see TODO 139
    return compile_entry(source, bytecode, "call", err);
}


/**
 * Has this been published since it was compiled? - TODO 245.
 *
 * The global epoch is the fast path: an entry that matches it is current and nothing
 * is looked up. Once somebody publishes anything, every entry misses that check once,
 * asks whether *its own* name was the one published, and catches its epoch up either
 * way - so a publish costs one map lookup per cached entry, once, and not a lookup
 * per call for ever after.
 */
bool published_since(const std::string& key, compiled& c) {
    auto now = barch::functions::compile_epoch();
    if (c.epoch == now)
        return false;
    if (barch::functions::published_at(key) > c.epoch)
        return true;
    c.epoch = now;
    return false;
}

/** hand a compiled function's registry refs back, or the state keeps it alive
 *  forever - the state now lives for the session, so nothing else will drop them */
void drop_compiled(lua_State* L, compiled& c) {
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
std::string qualified(const std::string& space, const std::string& name) {
    std::string q;
    q.reserve(space.size() + name.size() + 1);
    q.append(space);
    q.push_back('\0');
    q.append(name);
    return q;
}


space_state* state_for(function_states& cache) {
    if (cache.only)
        return cache.only.get();
    auto st = std::make_unique<space_state>();
    st->L = new_counted_state(cache.bytes.get(), cache.native);
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
    install_barch_bindings(L);
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
    set_interrupt(L, function_interrupt);
    cache.only = std::move(st);
    return raw;
}


/*
 * Push the chunk's service declaration - TODO 557. `service()` is the name, and
 * `transport()` is still read because every declaration stored before the
 * rename says that. A chunk with both is read through service(). Leaves the
 * function on the stack and answers true, or leaves nothing and answers false:
 * most functions have neither, and that isn't an error.
 */
bool push_service_fn(lua_State* T) {
    for (const char* name : {"service", "transport"}) {
        lua_getglobal(T, name);
        if (lua_type(T, -1) == LUA_TFUNCTION)
            return true;
        lua_pop(T, 1);
    }
    return false;
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
static bool read_resp_table(lua_State* L, lua_State* T, resp_spec& spec,
                            heap::string_map<int>* refs, std::string& err) {
    if (!push_service_fn(T))
        return true;
    if (lua_pcall(T, 0, 1, 0) != 0) {
        err = lua_tostring(T, -1) ? lua_tostring(T, -1) : "service() failed";
        lua_pop(T, 1);
        return false;
    }
    if (lua_type(T, -1) != LUA_TTABLE) {
        err = "service() must return a table";
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
        err = "resp service() needs a methods table";
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
        err = "resp service() exposes no methods";
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
        err = "resp service() categories must be a table";
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

/*
 * read_resp_table under a pcall - TODO 871. Its getfield and getglobal calls
 * run the script's __index, and a raise there must not leave as a C++
 * exception. `refs` may hold some names after a failure; the caller drops
 * them with the rest of the compiled function, as it does for a refusal.
 */
bool read_resp_transport(lua_State* L, lua_State* T, resp_spec& spec,
                                heap::string_map<int>* refs, std::string& err) {
    return protected_read(T, 0, 0, err, [&](lua_State*) {
        return read_resp_table(L, T, spec, refs, err);
    });
}

/*
 * Read a `transport()` of kind "cron" - TODO 249.
 *
 * Same shape as read_resp_transport: called on the chunk's own environment thread,
 * transport() is a plain data table rather than a set of handlers, so there is
 * nothing here to keep a registry reference to. Absent transport(), or one of
 * another kind, is not an error.
 */
static bool read_cron_table(lua_State* L, lua_State* T, cron_spec& spec,
                            std::string& err) {
    if (!push_service_fn(T))
        return true;
    if (lua_pcall(T, 0, 1, 0) != 0) {
        err = lua_tostring(T, -1) ? lua_tostring(T, -1) : "service() failed";
        lua_pop(T, 1);
        return false;
    }
    if (lua_type(T, -1) != LUA_TTABLE) {
        err = "service() must return a table";
        lua_pop(T, 1);
        return false;
    }
    spec.has_transport = true;
    lua_getfield(T, -1, "kind");
    std::string kind = lua_isstring(T, -1) ? lua_tostring(T, -1) : "";
    lua_pop(T, 1);
    for (auto& ch : kind)
        ch = (char) tolower((unsigned char) ch);
    if (kind != "cron") {
        lua_pop(T, 1);
        return true;
    }
    spec.is_cron = true;

    auto str_field = [&](const char* name, std::string& out) {
        lua_getfield(T, -1, name);
        if (lua_isstring(T, -1))
            out = lua_tostring(T, -1);
        else if (!lua_isnil(T, -1) && err.empty())
            err = std::string("cron service() field '") + name + "' must be a string";
        lua_pop(T, 1);
    };

    str_field("space", spec.space);
    str_field("call", spec.call);
    str_field("every", spec.every);
    str_field("cron", spec.cron);
    str_field("user", spec.user);
    str_field("jitter", spec.jitter);
    std::string overlap;
    str_field("overlap", overlap);
    if (!overlap.empty())
        spec.overlap = overlap;
    std::string tz;
    str_field("tz", tz);
    if (!tz.empty())
        spec.tz = tz;

    lua_getfield(T, -1, "catchup");
    if (!lua_isnil(T, -1))
        spec.catchup = lua_toboolean(T, -1) != 0;
    lua_pop(T, 1);

    lua_getfield(T, -1, "enabled");
    if (!lua_isnil(T, -1))
        spec.enabled = lua_toboolean(T, -1) != 0;
    lua_pop(T, 1);

    lua_getfield(T, -1, "args");
    if (lua_type(T, -1) == LUA_TTABLE) {
        int n = (int) lua_objlen(T, -1);
        for (int i = 1; i <= n; ++i) {
            lua_rawgeti(T, -1, i);
            if (lua_isstring(T, -1))
                spec.args.push_back(lua_tostring(T, -1));
            else if (err.empty())
                err = "cron service() args must be a list of strings";
            lua_pop(T, 1);
        }
    } else if (!lua_isnil(T, -1) && err.empty()) {
        err = "cron service() args must be a list of strings";
    }
    lua_pop(T, 1); // args

    lua_pop(T, 1); // the transport table
    (void) L;

    if (!err.empty())
        return false;

    if (spec.space.empty() || spec.call.empty()) {
        err = "cron service() needs a space and a call";
        return false;
    }
    if (spec.every.empty() == spec.cron.empty()) {
        err = "cron service() needs exactly one of every or cron";
        return false;
    }
    if (spec.user.empty()) {
        err = "cron service() needs a user to run as";
        return false;
    }
    if (spec.overlap != "skip" && spec.overlap != "queue" && spec.overlap != "allow") {
        err = "cron service() overlap must be skip, queue or allow";
        return false;
    }
    return true;
}

/** read_cron_table under a pcall, for the reason read_resp_transport gives */
bool read_cron_transport(lua_State* L, lua_State* T, cron_spec& spec,
                                std::string& err) {
    return protected_read(T, 0, 0, err, [&](lua_State*) {
        return read_cron_table(L, T, spec, err);
    });
}

/*
 * Read a `transport()` of kind "queue" - TODO 366.
 *
 * Same shape as read_cron_transport, and for the same reason: transport() is a
 * plain data table, so there is nothing to keep a registry reference to, and a
 * script with no transport() or one of another kind is not an error.
 *
 * What is checked here is what can be checked from the table alone - a field of
 * the wrong type, a missing one, a number that cannot be an attempt count.
 * Whether the declaration is in the right place, and whether the durability and
 * the poll are things this build can parse, is check_queue_spec's job, because
 * those need the key space and the configuration parsers and this runs inside a
 * luau state that has neither.
 */
static bool read_queue_table(lua_State* L, lua_State* T, queue_spec& spec,
                             std::string& err) {
    if (!push_service_fn(T))
        return true;
    if (lua_pcall(T, 0, 1, 0) != 0) {
        err = lua_tostring(T, -1) ? lua_tostring(T, -1) : "service() failed";
        lua_pop(T, 1);
        return false;
    }
    if (lua_type(T, -1) != LUA_TTABLE) {
        err = "service() must return a table";
        lua_pop(T, 1);
        return false;
    }
    spec.has_transport = true;
    lua_getfield(T, -1, "kind");
    std::string kind = lua_isstring(T, -1) ? lua_tostring(T, -1) : "";
    lua_pop(T, 1);
    for (auto& ch : kind)
        ch = (char) tolower((unsigned char) ch);
    if (kind != "queue") {
        lua_pop(T, 1);
        return true;
    }
    spec.is_queue = true;

    auto str_field = [&](const char* fname, std::string& out) {
        lua_getfield(T, -1, fname);
        if (lua_isstring(T, -1))
            out = lua_tostring(T, -1);
        else if (!lua_isnil(T, -1) && err.empty())
            err = std::string("queue service() field '") + fname + "' must be a string";
        lua_pop(T, 1);
    };

    str_field("name", spec.name);
    str_field("space", spec.space);
    str_field("call", spec.call);
    str_field("user", spec.user);
    str_field("dir", spec.dir);
    std::string durability;
    str_field("durability", durability);
    if (!durability.empty())
        spec.durability = durability;
    std::string poll;
    str_field("poll", poll);
    if (!poll.empty())
        spec.poll = poll;

    lua_getfield(T, -1, "max_attempts");
    if (lua_isnumber(T, -1)) {
        const double n = lua_tonumber(T, -1);
        // a count has to be a whole number of deliveries, and at least one: zero
        // attempts would dead letter every message without ever calling anything
        if (n < 1 || n > 1000000 || n != (double) (long long) n) {
            if (err.empty())
                err = "queue service() max_attempts must be a whole number of at least 1";
        } else {
            spec.max_attempts = (uint32_t) n;
        }
    } else if (!lua_isnil(T, -1) && err.empty()) {
        err = "queue service() max_attempts must be a number";
    }
    lua_pop(T, 1);

    lua_getfield(T, -1, "enabled");
    if (!lua_isnil(T, -1))
        spec.enabled = lua_toboolean(T, -1) != 0;
    lua_pop(T, 1);

    lua_pop(T, 1); // the transport table
    (void) L;

    if (!err.empty())
        return false;

    if (spec.name.empty()) {
        err = "queue service() needs a name for senders to publish to";
        return false;
    }
    if (spec.space.empty() || spec.call.empty()) {
        err = "queue service() needs a space and a call";
        return false;
    }
    if (spec.user.empty()) {
        err = "queue service() needs a user to run as";
        return false;
    }
    return true;
}

/** read_queue_table under a pcall, for the reason read_resp_transport gives */
bool read_queue_transport(lua_State* L, lua_State* T, queue_spec& spec,
                                 std::string& err) {
    return protected_read(T, 0, 0, err, [&](lua_State*) {
        return read_queue_table(L, T, spec, err);
    });
}


space_state*& state_of(lua_State* L) {
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
/*
 * require("@name"), require("@name/path") or require({ name = "...", module = "..." })
 * - a library this code's package pinned, TODO 593. `from` is the version node of
 * the library asking, 0 for anything else.
 *
 * No version is ever given here. The version is the one package.luau pinned, so
 * two packages requiring the same name can each get their own and neither has to
 * know about the other.
 */
static int require_package(lua_State* L, uint64_t from) {
    std::string name, path;
    if (lua_istable(L, 1)) {
        lua_pushnil(L);
        while (lua_next(L, 1) != 0) {
            const char* k = lua_type(L, -2) == LUA_TSTRING ? lua_tostring(L, -2) : nullptr;
            std::string key = k ? k : "";
            if (key != "name" && key != "module") {
                lua_pop(L, 2);
                if (key == "url" || key == "branch" || key == "tag" || key == "commit" ||
                    key == "version")
                    luaL_error(L, "FUNCTION require takes no %s: the version is the one "
                                  "package.luau pins", key.c_str());
                luaL_error(L, "FUNCTION require takes name and module");
            }
            if (lua_type(L, -1) != LUA_TSTRING) {
                lua_pop(L, 2);
                luaL_error(L, "FUNCTION require's %s is a string", key.c_str());
            }
            (key == "name" ? name : path) = lua_tostring(L, -1);
            lua_pop(L, 1);
        }
        if (name.empty())
            luaL_error(L, "FUNCTION require wants a name");
    } else {
        size_t n = 0;
        const char* raw = luaL_checklstring(L, 1, &n);
        std::string given(raw, n);
        if (!given.empty() && given[0] == '@') {
            auto slash = given.find('/');
            name = given.substr(1, slash == std::string::npos ? std::string::npos : slash - 1);
            if (slash != std::string::npos)
                path = given.substr(slash + 1);
            if (name.empty())
                luaL_error(L, "FUNCTION require wants a name after the @");
        } else {
            path = given;               // a module of the library's own package
        }
    }
    space_state* st = state_of(L);
    if (!st)
        luaL_error(L, "FUNCTION require is not available here");
    const std::string space = current_space(L);
    barch::library::module mod;
    std::string err;
    uint64_t no_call = 0;
    uint64_t& pinset = st->call ? st->call->pinset : no_call;
    const bool found = barch::library::find_module(space, from, name, path, pinset, mod, err);
    // held even when the name isn't found: the call keeps that pin set either way
    if (st->call && pinset)
        st->call->hold.take(pinset);
    if (!found)
        luaL_error(L, "FUNCTION require %s", err.c_str());
    // a version never changes, so the file's node is the module. Per space, as
    // every module is: its top level runs once in each space that loads it
    const std::string key = qualified(space, "\x02" + std::to_string(mod.leaf));
    auto have = st->functions.find(key);
    if (have != st->functions.end() && published_since(key, have->second)) {
        drop_compiled(st->L, have->second);
        if (statistics::luau_functions > 0)
            --statistics::luau_functions;
        st->functions.erase(have);
        have = st->functions.end();
    }
    if (have != st->functions.end()) {
        lua_getref(L, have->second.envt);
        return 1;
    }
    for (const auto& busy : st->loading) {
        if (busy == key)
            luaL_error(L, "FUNCTION cycle through %s",
                       (name.empty() ? path : "@" + name + (path.empty() ? "" : "/" + path)).c_str());
    }
    compiled c;
    if (!compile_into(*st, key, mod.source, c, err, false, false, mod.version))
        luaL_error(L, "%s", err.c_str());
    st->functions.emplace(key, c);
    ++statistics::luau_functions;
    lua_getref(L, c.envt);
    return 1;
}

/** the require a library module has, bound to the version it is in */
static int library_require(lua_State* L) {
    const char* from = lua_tostring(L, lua_upvalueindex(1));
    return require_package(L, from ? std::strtoull(from, nullptr, 10) : 0);
}

static int function_require(lua_State* L) {
    if (lua_istable(L, 1) || (lua_type(L, 1) == LUA_TSTRING && lua_tostring(L, 1)[0] == '@'))
        return require_package(L, 0);
    size_t n = 0;
    const char* raw = luaL_checklstring(L, 1, &n);
    std::string given(raw, n);
    /*
     * require(what, true) throws this session's compiled copy away and reads the
     * source again - TODO 247. Session scoped on purpose: RELOAD on a write
     * publishes to everything, and this changes one VM, so a loader can ask for
     * current code without altering what any other connection is running.
     *
     * It compiles, so it belongs in something that runs once rather than in a
     * handler that runs per request.
     */
    const bool forced = lua_gettop(L) > 1 && lua_toboolean(L, 2);
    space_state* st = state_of(L);
    if (!st || !st->load())
        luaL_error(L, "FUNCTION require is not available here");

    /*
     * `space:path/file.luau`, or `:path/file.luau` for the space this is running in,
     * loads from the file store instead of a function key - see TODO 242. The colon
     * cannot be confused with the dot form: a dot may not start a name and this may,
     * and a path keeps its case where a function name is folded.
     *
     * The read goes through `st->store()`, which is the caller's own access, so a file
     * is exactly as reachable as any other key in that space and require is not a way
     * round an ACL.
     */
    auto colon = given.find(':');
    if (colon != std::string::npos) {
        std::string fs_space = given.substr(0, colon);
        std::string path = given.substr(colon + 1);
        if (path.empty())
            luaL_error(L, "FUNCTION require wants a path after the ':'");
        if (path.front() != '/')
            path.insert(path.begin(), '/');
        if (path.find("..") != std::string::npos)
            luaL_error(L, "FUNCTION require path may not contain '..'");
        const std::string in_space = fs_space.empty() ? current_space(L) : fs_space;
        // \x01 in front, so a path can never collide with a folded function name in
        // the same table - the two namespaces share one cache
        const std::string fs_key = qualified(in_space, "\x01" + path);
        /*
         * A forced require rebuilds - unless the file says it has not changed. The
         * version is one small read against a compile, so a loader can force on every
         * pass and only pay for it when something actually moved. A file whose writer
         * keeps no version reads as 0, and 0 means rebuild: assuming "unchanged"
         * there would serve stale code. See TODO 248.
         */
        auto cached = st->functions.find(fs_key);
        bool rebuild = false;
        if (cached != st->functions.end()) {
            rebuild = published_since(fs_key, cached->second);
            if (!rebuild && forced) {
                const store_access* look = st->store();
                if (!fs_space.empty() && st->opened()) {
                    auto o = st->opened()->find(fs_space);
                    if (o != st->opened()->end())
                        look = o->second.get();
                }
                uint64_t now = look ? barch::fs_file_version(*look, path) : 0;
                rebuild = now == 0 || now != cached->second.fs_version;
            }
        }
        if (cached != st->functions.end() && rebuild) {
            drop_compiled(st->L, cached->second);
            if (statistics::luau_functions > 0)
                --statistics::luau_functions;
            st->functions.erase(cached);
            cached = st->functions.end();
        }
        if (cached != st->functions.end()) {
            lua_getref(L, cached->second.envt);
            return 1;
        }
        for (const auto& busy : st->loading) {
            if (busy == fs_key)
                luaL_error(L, "FUNCTION cycle through %s", path.c_str());
        }
        /*
         * A named space is opened the way `barch.space.NAME` opens one, so the rights
         * asked for are the caller's *in that space* - per space ACLs, TODO 135 - and
         * an unknown name is refused rather than created. Without this the read went
         * to whatever space the call was running in and `modules:/x.luau` quietly
         * meant `:/x.luau`.
         */
        const store_access* from = st->store();
        if (!fs_space.empty()) {
            if (!st->open_space() || !st->opened())
                luaL_error(L, "FUNCTION require cannot reach another space here");
            auto have = st->opened()->find(fs_space);
            if (have == st->opened()->end()) {
                auto opened = std::make_unique<store_access>();
                if (!(*st->open_space())(fs_space, *opened))
                    luaL_error(L, "FUNCTION no key space called %s", fs_space.c_str());
                have = st->opened()->emplace(fs_space, std::move(opened)).first;
            }
            from = have->second.get();
        }
        if (!from)
            luaL_error(L, "FUNCTION require has no store here");
        std::string source, type;
        if (!barch::read_fs_file(*from, path, source, type))
            luaL_error(L, "FUNCTION require has no file %s", path.c_str());
        if (!fs_space.empty())
            st->space_stack.push_back(fs_space);
        struct pop_fs {
            space_state* st{};
            bool armed{false};
            ~pop_fs() { if (armed && st && !st->space_stack.empty()) st->space_stack.pop_back(); }
        } fs_popper{st, !fs_space.empty()};
        compiled c;
        std::string err;
        if (!compile_into(*st, fs_key, source, c, err, false))
            luaL_error(L, "%s", err.c_str());
        c.fs_version = barch::fs_file_version(*from, path);
        st->functions.emplace(fs_key, c);
        ++statistics::luau_functions;
        lua_getref(L, c.envt);
        return 1;
    }

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
    if (have != st->functions.end() && (forced || published_since(key, have->second))) {
        drop_compiled(st->L, have->second);
        if (statistics::luau_functions > 0)
            --statistics::luau_functions;
        st->functions.erase(have);
        have = st->functions.end();
    }
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
    if (!(*st->load())(space_name, name, exact, source))
        luaL_error(L, "FUNCTION require has no function %s", name.c_str());
    compiled c;
    std::string err;
    // a required function carries its own AOT flag, under its own key: the
    // work a function like VGRAPH does is all in the module, so installing
    // the module with AOT has to mean something when it is required rather
    // than called - TODO 396
    if (!compile_into(*st, key, source, c, err, true, barch::functions::wants_aot(key)))
        luaL_error(L, "%s", err.c_str());
    st->functions.emplace(key, c);
    ++statistics::luau_functions;
    lua_getref(L, c.envt);
    return 1;
}

/*
 * Does the source open with Luau's `--!native` hot comment? TODO 413.
 *
 * Luau only honours a hot comment in the header - the comments before the first
 * token - so this reads whitespace, line comments and --[[ block ]] comments from
 * the top and stops at the first thing that isn't one. A `--!native` further down,
 * after code, is ignored here the way Luau ignores it.
 */
static bool wants_native_marker(const std::string& s) {
    const size_t n = s.size();
    size_t i = 0;
    while (i < n) {
        const char c = s[i];
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
            ++i;
            continue;
        }
        if (c != '-' || i + 1 >= n || s[i + 1] != '-')
            return false;                       // the first token: the header is over
        size_t p = i + 2;
        if (p < n && s[p] == '[') {
            size_t q = p + 1;
            while (q < n && s[q] == '=')
                ++q;
            if (q < n && s[q] == '[') {         // --[[ … ]] or --[==[ … ]==]
                std::string close = "]" + std::string(q - p - 1, '=') + "]";
                size_t e = s.find(close, q + 1);
                if (e == std::string::npos)
                    return false;
                i = e + close.size();
                continue;
            }
        }
        size_t eol = s.find('\n', i);
        if (eol == std::string::npos)
            eol = n;
        if (p < n && s[p] == '!') {
            size_t e = eol;
            while (e > p + 1 && (s[e - 1] == ' ' || s[e - 1] == '\t' || s[e - 1] == '\r'))
                --e;
            if (s.compare(p + 1, e - p - 1, "native") == 0)
                return true;
        }
        i = eol;
    }
    return false;
}

/** compile `source` into this space's state and pin what it left behind */
bool compile_into(space_state& st, const std::string& name,
                         const std::string& source, compiled& out, std::string& err,
                         bool needs_call, bool aot, uint64_t library_version) {
    // `--!native` in the header asks for what SETF … AOT asks for. The flag is
    // kept in memory only and is gone after a restart or a plain SETF; the
    // comment is kept with the source, so GETF shows it and git sync carries
    // it - TODO 413
    if (!aot && wants_native_marker(source))
        aot = true;
    // its own deadline and slice, if its header gives them - TODO 434
    {
        function_meta meta;
        if (!read_function_meta(source, meta, err))
            return false;
        out.meta_deadline_ms = meta.deadline_ms;
        out.meta_slice_insns = meta.slice_insns;
        out.meta_read_only = meta.read_only;
        out.meta_overtime = meta.overtime;
    }
    std::string bytecode;
    size_t n = 0;
    // AOT gets the optimizer's best bytecode: level 2 inlines (level 1, the
    // default, deliberately does not), and full type info guides native code
    // generation. The interpreter path keeps the debuggable default.
    lua_CompileOptions aot_opts{};
    if (aot) {
        aot_opts.optimizationLevel = 2;
        aot_opts.debugLevel = 1;
        aot_opts.typeInfoLevel = 1;
    }
    char* bc = luau_compile(source.data(), source.size(), aot ? &aot_opts : nullptr, &n);
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
    // `scope` is a local, so it must not be readable from the thread once this
    // returns - the thread is kept as the function's environment. Right after the
    // set: the pushes below can throw, and a guard declared after them left the
    // thread pointing at a dead frame - TODO 873
    struct clear_on_exit {
        lua_State* T;
        ~clear_on_exit() { lua_setthreaddata(T, nullptr); }
    } clearer{T};
    /*
     * A library module gets a require of its own - TODO 593. It resolves from the
     * version the module is in, so `@dep` is the version this library pinned and a
     * bare path is a module of the same package. Set in the module's own globals,
     * before the chunk runs, so its top level and its functions both see it, at
     * whatever time and from whichever caller they run.
     */
    if (library_version) {
        lua_pushstring(T, std::to_string(library_version).c_str());
        lua_pushcclosure(T, library_require, "require", 1);
        lua_setglobal(T, "require");
    }
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

    auto give_up = [&](const char* fallback) {
        err = lua_tostring(T, -1) ? lua_tostring(T, -1) : fallback;
        lua_unref(L, env);
        lua_unref(L, envt);
        return false;
    };
    if (luau_load(T, ("=" + name).c_str(), bytecode.data(), bytecode.size(), 0) != 0)
        return give_up("luau load failed");
    /*
     * Native code for SETF ... AOT - TODO 392, 396.
     *
     * From the chunk root, and before the chunk runs. `compile` builds the
     * function it is given and every function inside it, so one pass from the
     * root covers the top level, call(), the methods a resp transport()
     * exposes and the local helpers they lean on. Compiling call() instead -
     * which is what this did first - got call() and its inner functions only,
     * and a transport method is a sibling of call(), not an inner function,
     * so a function whose work is all behind methods saw nothing at all.
     *
     * A failed native compile is not a failed SETF: the same bytecode runs
     * interpreted, only slower.
     */
    if (aot) {
        Luau::CodeGen::CompilationOptions nativeOptions;
        // cold functions too, not just what the bytecode heuristics call hot
        // - a stored function runs however often the client calls it, which
        // the summary cannot know.
        nativeOptions.flags = Luau::CodeGen::CodeGen_ColdFunctions;
        auto res = Luau::CodeGen::compile(T, -1, nativeOptions);
        out.aot = res.result == Luau::CodeGen::CodeGenCompilationResult::Success;
        if (out.aot)
            ++statistics::luau_native_compiled;
    }
    // the chunk can require others, which compiles them into this same state
    if (lua_pcall(T, 0, 0, 0) != 0)
        return give_up("luau script error");
    /*
     * A function key is a command and must define call(). A module required out of
     * the file store is not - it exports whatever it exports, the way a lua module
     * does, and forcing a dummy call() on every one of them would be silly. It has
     * no `fn`, so nothing can invoke it as a command by accident. See TODO 242.
     *
     * Both reads go through the chunk's own globals, and the chunk can put a
     * metatable on those whose __index raises. So they run under a pcall, like
     * the transport reads - TODO 871.
     */
    out.fn = LUA_NOREF;
    out.arity = 0;
    const bool read = protected_read(T, 0, 0, err, [&](lua_State*) {
        lua_getglobal(T, "call");
        if (lua_type(T, -1) == LUA_TFUNCTION)
            out.fn = lua_ref(T, -1);
        lua_pop(T, 1);
        // arity is declared by the script, so it travels with the source rather than
        // needing a second thing stored beside it. Redis's convention: n means exactly
        // n, -n means at least n, absent means the script will take whatever it is given
        lua_getglobal(T, "arity");
        out.arity = lua_isnumber(T, -1) ? (int) lua_tointeger(T, -1) : 0;
        lua_pop(T, 1);
        return true;
    });
    if (!read || (out.fn == LUA_NOREF && needs_call)) {
        if (read)
            err = "luau script has no call()";
        if (out.fn != LUA_NOREF)
            lua_unref(L, out.fn);
        out.fn = LUA_NOREF;
        lua_unref(L, env);
        lua_unref(L, envt);
        return false;
    }
    out.env = env;
    out.envt = envt;
    out.epoch = barch::functions::compile_epoch();
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

static bool load_route(http_vm& vm, const std::string& name, const std::string& source,
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
    call_ctx in_vm(vm.iface);
    compiled c;
    bool built;
    {
        call_scope loading(st, &in_vm);
        built = compile_into(*st, qualified(vm.space, name), source, c, err);
    }
    if (!built)
        return false;
    // what the handler's own header asks for, applied per request - TODO 552
    out.deadline_ms = c.meta_deadline_ms;
    lua_getref(st->L, c.env);
    lua_State* T = lua_tothread(st->L, -1);
    lua_pop(st->L, 1);
    if (!T) {
        err = "HTTP function environment";
        drop_compiled(st->L, c);
        return false;
    }
    running_call scope;
    scope.space = vm.space;
    scope.running = vm.iface->running_in.empty() ? vm.space : vm.iface->running_in;
    lua_setthreaddata(T, &scope);
    // transport() runs as the route loads, with no caller to run a command for
    in_vm.run_command = nullptr;
    /*
     * Finding service(), calling it and reading its table all run the script:
     * the lookup and the reads honour __index. So all three go under one pcall.
     * A raise that left here would unwind through HTTP START, which closes this
     * vm on the way, and what() would read the message off the closed state -
     * TODO 871.
     */
    bool has_route = false;
    bool read;
    {
        call_scope reading(st, &in_vm);
        read = protected_read(T, 0, 0, err, [&](lua_State*) {
            if (!push_service_fn(T))
                return true;
            if (lua_pcall(T, 0, 1, 0) != 0) {
                err = lua_tostring(T, -1) ? lua_tostring(T, -1) : "service() failed";
                return false;
            }
            if (lua_isnil(T, -1))
                return true;
            has_route = true;
            return crow_read_transport(T, -1, out, err);
        });
    }
    lua_setthreaddata(T, nullptr);
    if (!read || !has_route) {
        drop_compiled(st->L, c);
        return read;
    }
    out.name = name;
    // keep the compiled chunk so method closures stay alive
    std::string key = qualified(vm.space, name);
    // and replace rather than emplace: a reload compiles the same key again, and
    // emplace would keep the old entry and drop the new one's refs on the floor -
    // the closures the caller is about to use belong to this compile. See TODO 244
    auto had = st->functions.find(key);
    if (had != st->functions.end()) {
        drop_compiled(st->L, had->second);
        if (statistics::luau_functions > 0)
            --statistics::luau_functions;
        st->functions.erase(had);
    }
    st->functions.emplace(std::move(key), c);
    ++statistics::luau_functions;
    return true;
}



} // namespace detail

bool package_setup(const std::string& source, std::string& json, std::string& err) {
    using namespace detail;
    json.clear();
    size_t n = 0;
    char* bc = luau_compile(source.data(), source.size(), nullptr, &n);
    if (!bc) {
        err = "luau compile failed";
        return false;
    }
    std::string bytecode(bc, n);
    free(bc);
    lua_State* L = new_counted_state(nullptr, false);
    if (!L) {
        err = "luau state failed";
        return false;
    }
    struct closer {
        lua_State* L;
        ~closer() { package_deadline = 0; close_counted_state(L); }
    } guard{L};
    luaopen_base(L);
    luaopen_math(L);
    luaopen_string(L);
    luaopen_table(L);
    luaopen_utf8(L);
    luaopen_bit32(L);
    package_deadline = art::now() + 2000;
    set_interrupt(L, package_interrupt);
    auto failed = [&](const char* what) {
        err = lua_tostring(L, -1) ? lua_tostring(L, -1) : what;
        return false;
    };
    if (luau_load(L, "=package.luau", bytecode.data(), bytecode.size(), 0) != 0)
        return failed("luau load failed");
    if (lua_pcall(L, 0, 0, 0) != 0)
        return failed("package.luau raised an error");
    // a raw get: lua_getglobal honours __index, and a raise here would leave this
    // function as the guard closes L, so what() would read a closed state - TODO 871
    lua_rawgetfield(L, LUA_GLOBALSINDEX, "setup");
    if (lua_type(L, -1) != LUA_TFUNCTION) {
        err = "package.luau has no setup()";
        return false;
    }
    if (lua_pcall(L, 0, 1, 0) != 0)
        return failed("setup() raised an error");
    if (!lua_istable(L, -1)) {
        err = "setup() has to return a table";
        return false;
    }
    lua_pushcfunction(L, package_encode, "package_encode");
    lua_pushvalue(L, -2);
    lua_pushlightuserdata(L, &json);
    if (lua_pcall(L, 2, 0, 0) != 0)
        return failed("setup() returned something that is not plain data");
    return true;
}



/*
 * load_route, with nothing thrown past it. HTTP START owns the vm in a local,
 * so a lua_exception that left here would close the vm while unwinding and
 * what() would then read a closed state. The reads are protected; this catches
 * what is left, an allocation failure, and copies its text now - TODO 871.
 */
bool http_vm_load(http_vm& vm, const std::string& name, const std::string& source,
                  http_route& out, std::string& err) {
    using namespace detail;
    try {
        return load_route(vm, name, source, out, err);
    } catch (const std::exception& e) {
        err = e.what();
        return false;
    }
}

void http_vm_unref(http_vm& vm, int fn_ref) {
    using namespace detail;
    if (fn_ref == LUA_NOREF || !vm.cache)
        return;
    space_state* st = state_for(*vm.cache);
    if (st && st->L)
        lua_unref(st->L, fn_ref);
}


bool compile_function(const std::string& space, const std::string& name,
                      const std::string& source, const source_loader& load,
                      std::string& err, resp_spec* spec, cron_spec* cron,
                      queue_spec* queue, bool aot) {
    using namespace detail;
    // a WebAssembly module is checked by loading it, not compiled - TODO 820
    if (is_wasm_module(source)) {
        if (aot) {
            err = "FUNCTION AOT is for Luau functions; a WASM one is interpreted";
            return false;
        }
#ifdef BARCH_HAS_WASM
        // and its transport, from its barch section - TODO 823
        return check_wasm(name, source, err, spec, cron, queue);
#else
        err = "FUNCTION wasm not built in (ADD_WASM)";
        return false;
#endif
    }
    // a state of its own, thrown away when this returns. It has to be a real one:
    // the script's top level may require others, and require needs both the loader
    // and a state to compile them into
    auto scratch = make_function_states();
    // nothing in here is ever called, let alone natively: the chunk runs once at the
    // top level and the state goes. No codegen context, so this path stays off the
    // process-wide frame registry entirely - TODO 400.
    scratch->native = false;
    /*
     * `scratch` is closed as this returns, and a lua_exception's what() reads
     * its message off a thread in it. So nothing may throw past here: an error
     * that still gets out of the reads below is copied now, while the state is
     * open - TODO 871. Declared outside the try, so it outlives the catch.
     */
    try {
        space_state* st = state_for(*scratch);
        if (!st) {
            err = "FUNCTION luau state";
            return false;
        }
        // only the loader: nothing is installed for barch.call or barch.store here
        call_ctx compiling;
        compiling.load = &load;
        call_scope scope(st, &compiling);
        // nothing is installed for barch.call or barch.store here: a script's top level
        // runs at SETF time, when there is no caller to run a command on, so one that
        // tries is told so rather than reaching a half-built one
        compiled c;
        // the same qualified key the call path uses, or require inside this chunk builds
        // one shape and the loading stack holds another - and a cycle goes undetected
        /*
         * `call()` is not insisted on here, and is checked below instead. A cron entry
         * is a schedule and nothing else - only transport() - so there is no body to
         * declare, and the transport cannot be read until the chunk has run. An
         * ordinary function with neither a call() nor a transport() is still refused,
         * just a few lines later than it used to be. See TODO 249.
         */
        bool ok = compile_into(*st, qualified(space, name), source, c, err, false, aot);
        if (ok && (spec || cron || queue)) {
            // read again for the categories, which the call path has no use for and so
            // does not keep. Only SETF pays this, and only once per stored function
            lua_getref(st->L, c.env);
            lua_State* T = lua_tothread(st->L, -1);
            lua_pop(st->L, 1);
            if (T) {
                if (spec)
                    ok = read_resp_transport(st->L, T, *spec, nullptr, err);
                if (ok && cron)
                    ok = read_cron_transport(st->L, T, *cron, err);
                if (ok && queue)
                    ok = read_queue_transport(st->L, T, *queue, err);
            }
        }
        /*
         * The check compile_into was told to skip. A cron entry and a queue
         * declaration are excused, because neither has a body of its own - both
         * name a function somewhere else to run. A resp or resource transport still
         * has to carry a call(), because the call path compiles it again with
         * call() required, and a key accepted here that cannot be run there would
         * be a write that reports OK and a command that never works.
         */
        if (ok && c.fn == LUA_NOREF && !(cron && cron->is_cron)
            && !(queue && queue->is_queue)) {
            err = "luau script has no call()";
            ok = false;
        }
        return ok;
    } catch (const std::exception& e) {
        err = e.what();
        return false;
    }
}


} // namespace foreign
} // namespace barch

#endif // BARCH_HAS_LUAU
