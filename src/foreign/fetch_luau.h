#pragma once

struct lua_State;

/**
 * Install the `http` table: an outbound HTTP client for stored functions.
 *
 *     local r = http.request(url):timeout(2000):get()
 *     if r.ok then return r.body else return r.error end
 *
 * The verb suspends the calling function rather than blocking the worker it is
 * on, so a slow server costs a socket and not a thread. Nothing is installed
 * when the build has no cofetch.
 */
void luaopen_fetch(lua_State* L);

#include <functional>
#include <string>
#include <utility>
#include <vector>

namespace barch::foreign {

/**
 * The same client without Luau, for WASM functions - TODO 822. `method` is 0 GET,
 * 1 POST, 2 PUT, 3 PATCH, 4 DELETE. `headers` are "Name: value" lines.
 */
struct http_ask {
    int method{0};
    std::string url;
    std::vector<std::string> headers;
    std::string body;
    long timeout_ms{30000};
};
struct http_answer {
    /** a transport failure leaves this false with no status; an HTTP error has a status */
    bool ok{false};
    long status{0};
    std::string body;
    std::string error;
    std::vector<std::pair<std::string, std::string>> headers;
};
/** false when the build has no HTTP client */
bool http_available();
/** `done` is called once, on the reactor thread, when the request has an answer */
void http_start(http_ask ask, std::function<void(http_answer)> done);

}
