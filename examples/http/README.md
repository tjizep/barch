# HTTP over stored Luau, with Crow

A tiny REST/HTML server whose routes are ordinary stored functions.
Each function that wants a URL defines `transport()`. Functions that
do not stay RESP commands, the way they already are.

POST `/echo` parses JSON with simdjson and sends a table back. GET
`/page` is a one-line HTML page, so the same Crow thread can do both.

## Load

From a RelWithDebInfo (or Release) build, with redis-py installed:

```
cd examples/http
python3 -m venv ./venv
./venv/bin/pip install ../../cmake-build-relwithdebinfo/
source ./venv/bin/activate
pip install redis
python3 deploy.py --start --demo
```

`--start` boots an embedded server on 14000 and Crow on 18088.
`--demo` hits `/page` and `/echo` once and checks the replies.

Without `--start`, the same script talks to whatever is already on
`--port`, and still runs `HTTP START`.

To leave it up:

```
python3 deploy.py --start
```

Then, from another shell:

```
curl http://127.0.0.1:18088/page
curl -H 'Content-Type: application/json' -d '{"a":7}' http://127.0.0.1:18088/echo
redis-cli -p 14000 HTTP STATUS
```

## How a function advertises HTTP

`call()` is still required, so the name works as a command. `transport()`
is what Crow looks at:

```
function echo(req, res)
    local j = simdjson.parse(req.body)
    res.body = simdjson.encode({ok = true, a = j.a})
end

function transport()
    return {
        kind = "resource",
        route = "/echo",
        methods = {POST = echo},
        accept = "application/json",
        send = "application/json",
        cors = "*",
    }
end
```

`kind` is `"resource"` for a route and `"http"` for server startup. If
it is left off, a table with a route is still a resource, and one
without is still the HTTP key.

`req` is the Crow request (`body`, `method`, `url`, `header`, `param`).
`res` is the response (`body`, `code`, `header`). They only last for
that one call.

`conf.luau` is `kind = "http"`: bind options and the list of resource
keys. `HTTP START CONF 18088 127.0.0.1` uses that table and still lets
the command override port and bind. Starting a resource as the HTTP
key is refused.

Session state is an ordinary key, the way people use Redis. GET `/sess`
sets a `sid` cookie if needed and bumps `sess:<sid>` in `barch.store`:

```
curl -c jar -b jar http://127.0.0.1:18088/sess
```

`req:cookie("sid")` reads it, `res:cookie("sid", id, {path="/", httponly=true})`
sets it.

```
HTTP START CONF 18088 127.0.0.1
HTTP STATUS
HTTP STOP
```

`HTTP STATUS` also reports the VM pool, since handlers run on a pool of
Luau states rather than one:

```
port=18088
bind=127.0.0.1
ssl=off
vms=8
executing=1
idle=7
luau_bytes=4699368
ECHO /echo POST
```

`executing` plus `idle` is always `vms`. `luau_bytes` is what this
space's VMs hold, counted by the allocator the states are built with —
the whole-server figure is `used_memory_luau` on INFO MEMORY.

SSL is optional on the same table: `ssl = {cert = "...", key = "...",
proto = "TLS"}`. Cert and key are file paths, or PEM if they start
with `-----BEGIN`.

## Calling out: `http.request`

The same functions can make outbound requests. `http.request(url)` builds a
chain and the verb fires it:

```lua
function call(url)
    local res = http.request(url)
        :headers({["Content-Type"] = "application/json"})
        :body('{"a":1}')
        :timeout(2000)
        :post()
    if not res.ok then return "failed: " .. res.error end
    return res.body
end
```

The result is a table: `ok`, `status`, `body`, `headers` (a name to value
map), and `error` when something went wrong. A refused connection or a
timeout is `ok = false` with a reason, not a raised error, so a script can
handle it. `:headers` takes either a map or a list of `"Name: value"`
lines; `:redirects(n)` follows up to n, off by default.

It runs on cofetch over asio, with libcurl underneath, on a reactor thread
of its own. That last part matters: stored functions run on a plain thread
pool with no event loop, and a function can be reached down four different
paths, so the client owns its reactor rather than borrowing one.

**A stored function does not hold its worker while it waits.** The verb
suspends the coroutine, the pool thread goes back to other work, and the
call resumes when the response lands. Time spent waiting is not charged
against `function_deadline_ms` — that budget is there to stop a script
computing forever, and a parked call is not computing. Bound the wait with
`:timeout()` instead.

**Inside a `transport()` handler it waits inline instead.** A handler runs
under `lua_pcall` holding a VM slot from the space's pool; yielding would
give that slot back while the coroutine was still suspended on it, and the
next request could pick up the same Luau state. So in a handler the request
blocks that Crow thread for its duration, which is what the pool size is
there to bound. Everything else about it is the same.

That has one consequence worth knowing before you hit it: **a handler must
not fetch from its own Crow server.** It would hold one slot while waiting
on a second, and once enough concurrent requests do that the pool runs out
and every one of them is waiting for a slot that another is holding. Calling
out to another service is fine; calling back into yourself deadlocks. If you
want one route to reuse another's work, call the Lua function directly
rather than going out over HTTP for it.

## Files

- `luau/echo.luau` — `kind = "resource"`, POST `/echo`
- `luau/page.luau` — `kind = "resource"`, GET `/page`
- `luau/session.luau` — `kind = "resource"`, GET `/sess`, cookie + store
- `luau/conf.luau` — `kind = "http"`, lists the other keys
- `deploy.py` — SETF in order, then `HTTP START CONF`
