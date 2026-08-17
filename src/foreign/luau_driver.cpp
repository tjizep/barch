#include "driver.h"
#include "pool.h"
#include "sql.h"
#include "key_space.h"
#include "configuration.h"
#include "lzr_log.h"
#include "art/nodes.h"

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

static bool compile_source(const std::string& source, std::string& bytecode, std::string& err) {
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
    int rc = luau_load(L, "=foreign", bytecode.data(), bytecode.size(), 0);
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
    lua_getglobal(L, "resolve");
    if (lua_type(L, -1) != LUA_TFUNCTION) {
        err = "luau script has no resolve()";
        lua_close(L);
        return false;
    }
    lua_close(L);
    return true;
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
