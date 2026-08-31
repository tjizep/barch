#include "fetch_luau.h"

#ifdef BARCH_HAS_COFETCH

#include "driver.h"
#include "lzr_log.h"

#include <asio.hpp>
#include <cofetch.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "lua.h"
#include "lualib.h"

namespace {

constexpr const char* request_mt = "barch.http.request";
/** nothing in the sandbox should be able to sit on a socket forever */
constexpr long default_timeout_ms = 30000;

enum verb { verb_get, verb_post, verb_put, verb_patch, verb_delete };

struct header_pair {
    std::string name;
    std::string value;
};

struct fetch_result {
    bool ok{false};
    long status{0};
    std::string body;
    std::string error;
    std::vector<header_pair> headers;
};

/** what the builder collects before a verb fires it */
struct request_build {
    std::string url;
    std::vector<std::string> headers;
    std::string body;
    long timeout_ms{default_timeout_ms};
    long redirects{-1};
    int method{verb_get};
};

/*
 * One reactor for the process: an io_context, the thread running it, and the
 * cofetch client on top.
 *
 * Not the RESP server's io_context and not Crow's. A stored function can be
 * called down any of four paths - the native RESP port, a valkey module command,
 * the embedded SWIG store, or a Crow handler - and only two of those have an
 * io_context anywhere near them. The foreign pool that runs stored functions is
 * a plain thread pool with no reactor at all. Owning one here is what makes
 * `http.request` behave the same whichever way the function was reached.
 *
 * Process-lifetime and never destroyed, the same as the foreign pool: the thread
 * is parked in `io.run()` and cofetch drops in-flight handlers on destruction.
 */
struct reactor {
    asio::io_context io;
    asio::executor_work_guard<asio::io_context::executor_type> guard;
    cofetch::Client client;
    std::thread thread;

    reactor() : guard(asio::make_work_guard(io)), client(io) {
        thread = std::thread([this] {
            try {
                io.run();
            } catch (const std::exception& e) {
                barch::err({"http reactor", e.what()});
            }
        });
        thread.detach();
    }
};

reactor& fetch_reactor() {
    static reactor* r = new reactor();
    return *r;
}

fetch_result to_result(const std::error_code& ec, const cofetch::Response& res) {
    fetch_result out;
    out.status = res.http_code_;
    out.body = res.data_;
    if (ec) {
        // a transport failure - refused, DNS, TLS, timeout. There is no status
        // and no body; the script gets ok=false and something to log
        out.ok = false;
        out.error = ec.message();
        return out;
    }
    out.ok = res.is_ok();
    if (!out.ok && out.error.empty())
        out.error = "HTTP " + std::to_string(out.status);
    for (const auto& h : res.headers())
        out.headers.push_back({h.first, h.second});
    return out;
}

/*
 * Start a request on the reactor thread.
 *
 * Posted rather than called here on purpose. cofetch's initiation runs
 * `Client::start` inline on whichever thread asks, and that reaches into the
 * easy-handle pool and `curl_multi_add_handle`, neither of which is safe beside
 * a thread already in `io.run()`. Posting puts every curl call on one thread.
 */
void start_request(request_build rb, std::function<void(fetch_result)> done) {
    auto& r = fetch_reactor();
    asio::post(r.io, [&r, rb = std::move(rb), done = std::move(done)]() mutable {
        auto builder = r.client.request(rb.url);
        if (!rb.headers.empty())
            builder.headers(std::move(rb.headers));
        if (!rb.body.empty())
            builder.body(std::move(rb.body));
        builder.timeout(std::chrono::milliseconds(rb.timeout_ms));
        if (rb.redirects >= 0)
            builder.follow_redirects(rb.redirects);
        auto cb = [done = std::move(done)](std::error_code ec, cofetch::Response res) {
            done(to_result(ec, res));
        };
        switch (rb.method) {
            case verb_post: builder.post(std::move(cb)); break;
            case verb_put: builder.put(std::move(cb)); break;
            case verb_patch: builder.patch(std::move(cb)); break;
            case verb_delete: builder.del(std::move(cb)); break;
            default: builder.get(std::move(cb)); break;
        }
    });
}

int push_result(lua_State* L, const fetch_result& r) {
    lua_newtable(L);
    lua_pushboolean(L, r.ok);
    lua_setfield(L, -2, "ok");
    lua_pushinteger(L, (int) r.status);
    lua_setfield(L, -2, "status");
    lua_pushlstring(L, r.body.data(), r.body.size());
    lua_setfield(L, -2, "body");
    if (!r.error.empty()) {
        lua_pushlstring(L, r.error.data(), r.error.size());
        lua_setfield(L, -2, "error");
    }
    lua_newtable(L);
    for (const auto& h : r.headers) {
        lua_pushlstring(L, h.value.data(), h.value.size());
        lua_setfield(L, -2, h.name.c_str());
    }
    lua_setfield(L, -2, "headers");
    return 1;
}

request_build* check_request(lua_State* L) {
    void* p = luaL_checkudata(L, 1, request_mt);
    return static_cast<request_build*>(p);
}

/** waits for a request that could not be parked */
struct sync_box {
    std::mutex mu;
    std::condition_variable cv;
    bool done{false};
    fetch_result result;
};

int fetch_verb(lua_State* L, int method) {
    auto* rb = check_request(L);
    rb->method = method;
    // copied out: the userdata belongs to the coroutine's stack and the request
    // outlives this frame the moment it is parked
    request_build sending = *rb;

    if (auto parked = barch::foreign::park_call(L)) {
        start_request(std::move(sending), [parked](fetch_result r) {
            barch::foreign::complete_call(parked, [r](lua_State* T) {
                return push_result(T, r);
            });
        });
        // from here the coroutine is suspended and the pool thread is free. The
        // values pushed by the completion become this call's return values
        return lua_yield(L, 0);
    }

    /*
     * Nothing to park: this is a Crow handler, running under lua_pcall and
     * holding a VM slot out of the space's pool. Yielding would hand that slot
     * back while the coroutine is still suspended on it, so the wait happens
     * here instead. It costs the Crow thread for the length of the request,
     * which is what the pool size is there to bound.
     */
    auto box = std::make_shared<sync_box>();
    start_request(std::move(sending), [box](fetch_result r) {
        {
            std::lock_guard<std::mutex> lk(box->mu);
            box->result = std::move(r);
            box->done = true;
        }
        box->cv.notify_one();
    });
    std::unique_lock<std::mutex> lk(box->mu);
    box->cv.wait(lk, [&] { return box->done; });
    fetch_result got = std::move(box->result);
    lk.unlock();
    return push_result(L, got);
}

int req_get(lua_State* L) { return fetch_verb(L, verb_get); }
int req_post(lua_State* L) { return fetch_verb(L, verb_post); }
int req_put(lua_State* L) { return fetch_verb(L, verb_put); }
int req_patch(lua_State* L) { return fetch_verb(L, verb_patch); }
int req_delete(lua_State* L) { return fetch_verb(L, verb_delete); }

int req_headers(lua_State* L) {
    auto* rb = check_request(L);
    luaL_checktype(L, 2, LUA_TTABLE);
    // either an array of "Name: value" lines, or a map of name to value
    lua_pushnil(L);
    while (lua_next(L, 2) != 0) {
        if (lua_type(L, -2) == LUA_TSTRING) {
            const char* name = lua_tostring(L, -2);
            const char* value = luaL_checkstring(L, -1);
            rb->headers.push_back(std::string(name) + ": " + value);
        } else {
            rb->headers.push_back(luaL_checkstring(L, -1));
        }
        lua_pop(L, 1);
    }
    lua_pushvalue(L, 1);
    return 1;
}

int req_body(lua_State* L) {
    auto* rb = check_request(L);
    size_t len = 0;
    const char* s = luaL_checklstring(L, 2, &len);
    rb->body.assign(s, len);
    lua_pushvalue(L, 1);
    return 1;
}

int req_timeout(lua_State* L) {
    auto* rb = check_request(L);
    long ms = (long) luaL_checkinteger(L, 2);
    if (ms <= 0)
        luaL_error(L, "http timeout must be positive");
    rb->timeout_ms = ms;
    lua_pushvalue(L, 1);
    return 1;
}

int req_redirects(lua_State* L) {
    auto* rb = check_request(L);
    rb->redirects = lua_isnoneornil(L, 2) ? 30 : (long) luaL_checkinteger(L, 2);
    lua_pushvalue(L, 1);
    return 1;
}

int req_gc(lua_State* L) {
    auto* rb = static_cast<request_build*>(lua_touserdata(L, 1));
    if (rb)
        rb->~request_build();
    return 0;
}

int http_request(lua_State* L) {
    size_t len = 0;
    const char* url = luaL_checklstring(L, 1, &len);
    void* mem = lua_newuserdata(L, sizeof(request_build));
    auto* rb = new (mem) request_build();
    rb->url.assign(url, len);
    luaL_getmetatable(L, request_mt);
    lua_setmetatable(L, -2);
    return 1;
}

const luaL_Reg request_methods[] = {
    {"headers", req_headers},   {"body", req_body},
    {"timeout", req_timeout},   {"redirects", req_redirects},
    {"get", req_get},           {"post", req_post},
    {"put", req_put},           {"patch", req_patch},
    {"delete", req_delete},     {nullptr, nullptr},
};

} // namespace

void luaopen_fetch(lua_State* L) {
    luaL_newmetatable(L, request_mt);
    lua_pushcfunction(L, req_gc, "__gc");
    lua_setfield(L, -2, "__gc");
    lua_newtable(L);
    for (const luaL_Reg* r = request_methods; r->name; ++r) {
        lua_pushcfunction(L, r->func, r->name);
        lua_setfield(L, -2, r->name);
    }
    lua_setfield(L, -2, "__index");
    // the metatable is reached through the userdata, never from a script
    lua_pushstring(L, "locked");
    lua_setfield(L, -2, "__metatable");
    lua_pop(L, 1);

    lua_newtable(L);
    lua_pushcfunction(L, http_request, "request");
    lua_setfield(L, -2, "request");
    lua_setglobal(L, "http");
}

#else

void luaopen_fetch(lua_State*) {}

#endif
