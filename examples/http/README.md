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

SSL is optional on the same table: `ssl = {cert = "...", key = "...",
proto = "TLS"}`. Cert and key are file paths, or PEM if they start
with `-----BEGIN`.

## Files

- `luau/echo.luau` — `kind = "resource"`, POST `/echo`
- `luau/page.luau` — `kind = "resource"`, GET `/page`
- `luau/session.luau` — `kind = "resource"`, GET `/sess`, cookie + store
- `luau/conf.luau` — `kind = "http"`, lists the other keys
- `deploy.py` — SETF in order, then `HTTP START CONF`
