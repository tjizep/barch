# Crow HTTP for stored Luau: transport(), GET html, POST json via simdjson.
# Concurrent ingress hammers store.locked against overlapping RESP writes.
import scale
import http.client
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

import redis
import barch

PORT = scale.port(default=14088)
HTTP_PORT = scale.port(1, default=18088)
UP_PORT = scale.port(2, default=18089)

import http.server
import socketserver
import threading

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()


class Upstream(http.server.BaseHTTPRequestHandler):
    """What the handlers call out to with http.request.

    Deliberately a separate server. A handler that fetched from its own Crow
    server would hold one VM slot while waiting on a second, and with eight
    client threads against a pool of 2-8 the slots run out and the whole thing
    deadlocks. Calling out is fine; calling back into yourself is not.
    """

    def log_message(self, *args):
        pass

    def do_GET(self):
        body = b"upstream ok"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class UpstreamServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


upstream = UpstreamServer(("127.0.0.1", UP_PORT), Upstream)
threading.Thread(target=upstream.serve_forever, daemon=True).start()

print("start http luau test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")


def refused(*args):
    try:
        r.execute_command(*args)
        return None
    except redis.exceptions.ResponseError as e:
        return str(e)


def http_call(method, path, data=None, headers=None, timeout=3):
    hdrs = dict(headers or {})
    body = None
    if data is not None:
        body = data if isinstance(data, bytes) else data.encode()
        hdrs.setdefault("Content-Length", str(len(body)))
    hdrs.setdefault("Connection", "close")
    conn = http.client.HTTPConnection("127.0.0.1", HTTP_PORT, timeout=timeout)
    try:
        conn.request(method, path, body=body, headers=hdrs)
        resp = conn.getresponse()
        data_out = resp.read()
        return resp.status, data_out, dict(resp.getheaders())
    finally:
        conn.close()


ECHO = r'''
function call()
    return "echo"
end

function echo(req, res)
    local j = simdjson.parse(req.body)
    res.body = simdjson.encode({ok = true, a = j.a})
    res.code = 200
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
'''

BINECHO = r'''
function call()
    return "binecho"
end

-- reads the request as a buffer and answers from one, so the bytes never
-- become a lua string in either direction. See TODO 216.
function binecho(req, res)
    local b = req.bodyBuffer
    local n = buffer.len(b)
    local out = buffer.create(n + 1)
    -- first byte is the length the handler saw, so a truncated read shows up
    buffer.writeu8(out, 0, n % 256)
    buffer.copy(out, 1, b, 0, n)
    res.body = out
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/binecho",
        methods = {POST = binecho},
        send = "application/octet-stream",
    }
end
'''

PAGE = r'''
function call()
    return "page"
end

function getpage(req, res)
    res.body = "<html><body>hello</body></html>"
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/page",
        methods = {GET = getpage},
        send = "text/html",
        cors = "*",
    }
end
'''

CONF = r'''
function call()
    return "http"
end

function transport()
    return {
        kind = "http",
        port = %d,
        bind = "127.0.0.1",
        user = "web",
        keys = {"ECHO", "BINECHO", "PAGE", "SESS", "SLOW", "FETCH", "HITS",
                "JSON", "HEALTH", "STATS", "LOGIN", "WHO", "NOTES", "FILES"},
    }
end
''' % HTTP_PORT

# a handler that stays busy long enough for STATUS to catch it running - TODO 181.
# No clock in the sandbox, so the wait is a loop; the count is well inside the
# function deadline but takes a few hundred ms.
SLOW = r'''
function call()
    return "slow"
end

function spin(req, res)
    local x = 0.0
    for i = 1, 12000000 do
        x = x + i % 7
    end
    res.body = tostring(x)
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/slow",
        methods = {GET = spin},
        send = "text/plain",
    }
end
'''

# an outbound request from inside a handler. A handler cannot park - it holds a
# VM slot under lua_pcall - so this is the inline wait, under the same eight
# threads that are hammering everything else.
FETCH = """
function call()
    return "fetch"
end

function proxy(req, res)
    local got = http.request("http://127.0.0.1:%d/up"):timeout(5000):get()
    res.body = tostring(got.status) .. "|" .. got.body
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/fetch",
        methods = {GET = proxy},
        send = "text/plain",
    }
end
""" % UP_PORT

# Templated routes - TODO 222. The route is a pathspec in transport(), and the
# handler gets two more arguments: the {name} bindings out of the path, and the
# query string. Crow cannot match this itself (route_dynamic wants a handler
# arity that matches the rule, and these routes are only known at runtime), so
# barch hands Crow the literal prefix and does the segments itself.
NOTES = r'''
function call()
    return "notes"
end

function one(req, res, params, query)
    res.body = "id=" .. tostring(params.id) .. " n=" .. tostring(params.n) ..
               " q=" .. tostring(query.q1) .. " url=" .. req.url
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/notes/{id}/rev/{n}",
        methods = {GET = one},
        send = "text/plain",
    }
end
'''

# a trailing * binds the rest of the path under "*", which is the sub-routing
# case: one handler, and the dispatch below it written in luau.
FILES = r'''
function call()
    return "files"
end

function serve(req, res, params)
    local rest = params["*"]
    local head = string.match(rest, "^[^/]+") or ""
    res.body = "head=" .. head .. " rest=" .. rest
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/files/*",
        methods = {GET = serve},
        send = "text/plain",
    }
end
'''

PLAIN = r'''
function call()
    return "plain"
end
'''

JSON = r'''
function call()
    return "json"
end

function json(req, res)
    res.body = simdjson.encode(simdjson.parse(req.body))
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/json",
        methods = {POST = json},
        accept = "application/json",
        send = "application/json",
        cors = "*",
    }
end
'''

HEALTH = r'''
function call()
    return "health"
end

local function kvmap(arr)
    local m = {}
    if type(arr) ~= "table" then
        return m
    end
    local i = 1
    while arr[i] ~= nil do
        m[tostring(arr[i])] = arr[i + 1]
        i = i + 2
    end
    return m
end

function health(req, res)
    local ops = kvmap(barch.call("OPS"))
    res.body = simdjson.encode({
        ok = barch.call("PING") == "PONG",
        space = barch.running(),
        user = barch.user(),
        retrieve_ops = ops.retrieve_ops,
    })
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/health",
        methods = {GET = health},
        send = "application/json",
        cors = "*",
    }
end
'''

STATS = r'''
function call()
    return "stats"
end

local function kvmap(arr)
    local m = {}
    if type(arr) ~= "table" then
        return m
    end
    local i = 1
    while arr[i] ~= nil do
        m[tostring(arr[i])] = arr[i + 1]
        i = i + 2
    end
    return m
end

function stats(req, res)
    res.body = simdjson.encode({
        user = barch.user(),
        stats = kvmap(barch.call("STATS")),
        ops = kvmap(barch.call("OPS")),
    })
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/stats",
        methods = {GET = stats},
        send = "application/json",
        cors = "*",
        user = "web",
    }
end
'''

LOGIN = r'''
function call()
    return "login"
end

function login(req, res)
    local j = simdjson.parse(req.body)
    local user = j.user
    local pass = j.pass
    if type(user) ~= "string" or type(pass) ~= "string" then
        res.body = simdjson.encode({ok = false, error = "user and pass required"})
        res.code = 400
        return
    end
    if barch.auth(user, pass) then
        res.body = simdjson.encode({ok = true, user = barch.user()})
        res.code = 200
    else
        res.body = simdjson.encode({ok = false})
        res.code = 401
    end
end

function transport()
    return {
        kind = "resource",
        route = "/login",
        methods = {POST = login},
        accept = "application/json",
        send = "application/json",
        cors = "*",
    }
end
'''

WHO = r'''
function call()
    return "who"
end

function who(req, res)
    res.body = simdjson.encode({user = barch.user()})
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/who",
        methods = {GET = who},
        send = "application/json",
        cors = "*",
    }
end
'''

HITS = r'''
function call()
    return "hits"
end

function hits(req, res)
    local n
    barch.store.locked("hits", function()
        n = barch.store.getInt32At("hits") or 0
        n = n + 1
        barch.store.setInt32At("hits", n)
    end)
    res.body = simdjson.encode({n = n})
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/hits",
        methods = {GET = hits},
        send = "application/json",
    }
end
'''

SESS = r'''
function call()
    return "sess"
end

local function newsid()
    local t = {}
    for i = 1, 16 do
        t[i] = string.format("%02x", math.random(0, 255))
    end
    return table.concat(t)
end

function hits(req, res)
    local id = req:cookie("sid")
    if not id or id == "" then
        id = newsid()
        res:cookie("sid", id, {path = "/", httponly = true})
    end
    local key = "sess:" .. id
    local n
    barch.store.locked(key, function()
        n = tonumber(barch.store.get(key)) or 0
        n = n + 1
        barch.store.set(key, tostring(n))
    end)
    res.body = simdjson.encode({sid = id, n = n})
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/sess",
        methods = {GET = hits},
        send = "application/json",
    }
end
'''



try:
    assert r.execute_command("SETF", "echo", ECHO) == b"OK"
    assert r.execute_command("SETF", "binecho", BINECHO) == b"OK"
    assert r.execute_command("SETF", "page", PAGE) == b"OK"
    assert r.execute_command("SETF", "httpconf", CONF) == b"OK"
    assert r.execute_command("SETF", "plain", PLAIN) == b"OK"
    assert r.execute_command("SETF", "sess", SESS) == b"OK"
    assert r.execute_command("SETF", "hits", HITS) == b"OK"
    assert r.execute_command("SETF", "json", JSON) == b"OK"
    assert r.execute_command("SETF", "health", HEALTH) == b"OK"
    assert r.execute_command("SETF", "stats", STATS) == b"OK"
    assert r.execute_command("SETF", "login", LOGIN) == b"OK"
    assert r.execute_command("SETF", "who", WHO) == b"OK"
    assert r.execute_command("SETF", "slow", SLOW) == b"OK"
    assert r.execute_command("SETF", "fetch", FETCH) == b"OK"
    assert r.execute_command("SETF", "notes", NOTES) == b"OK"
    assert r.execute_command("SETF", "files", FILES) == b"OK"
    # no transport: still an ordinary function
    assert r.execute_command("plain") == b"plain"

    e = refused("HTTP", "START", "ECHO")
    assert e and "resource" in e.lower(), e

    started = r.execute_command("HTTP", "START", "HTTPCONF", str(HTTP_PORT), "127.0.0.1")
    text = b" ".join(started).decode()
    assert "port=" + str(HTTP_PORT) in text, started
    assert "ECHO /echo POST" in text, started
    assert "PAGE /page GET" in text, started
    assert "SESS /sess GET" in text, started
    assert "HITS /hits GET" in text, started
    assert "JSON /json POST" in text, started
    assert "HEALTH /health GET" in text, started
    assert "STATS /stats GET" in text, started
    assert "LOGIN /login POST" in text, started
    assert "WHO /who GET" in text, started
    assert "SLOW /slow GET" in text, started
    assert "FETCH /fetch GET" in text, started
    # STATUS shows the pathspec as it was written, not the /<path> Crow got
    assert "NOTES /notes/{id}/rev/{n} GET" in text, started
    assert "FILES /files/* GET" in text, started
    assert "PLAIN" not in text, started

    # Crow bind can take a beat after START returns
    last = None
    for _ in range(20):
        try:
            last = http_call("GET", "/page")
            if last[0] == 200:
                break
        except (TimeoutError, ConnectionError, OSError):
            time.sleep(0.05)
    assert last and last[0] == 200, last
    assert b"hello" in last[1], last
    assert "text/html" in last[2].get("Content-Type", last[2].get("content-type", "")), last[2]
    assert last[2].get("Access-Control-Allow-Origin", last[2].get("access-control-allow-origin")) == "*", last[2]

    status, body, hdrs = http_call(
        "POST", "/echo",
        data=json.dumps({"a": 7}),
        headers={"Content-Type": "application/json"},
    )
    assert status == 200, (status, body)
    got = json.loads(body)
    assert got["ok"] is True and got["a"] == 7, got
    assert "application/json" in hdrs.get("Content-Type", hdrs.get("content-type", "")), hdrs

    status, _, _ = http_call("GET", "/echo")
    assert status == 405, status

    # Keep-alive latency - TODO 234.
    #
    # Crow does not set TCP_NODELAY and its response leaves as two writes, so
    # without the patch in cmake/crow_tcp_nodelay.cmake every request after the
    # first on a connection waits ~40ms for a delayed ACK. That is invisible to
    # every other test here, because each one opens its own connection and the
    # first request on a connection is fast. Real clients keep the connection.
    print("keep-alive requests do not wait on a delayed ACK", flush=True)
    import socket as _socket
    _req = b"GET /page HTTP/1.1\r\nHost: x\r\nConnection: keep-alive\r\n\r\n"
    _sock = _socket.create_connection(("127.0.0.1", HTTP_PORT), timeout=10)
    try:
        _worst = 0.0
        for _ in range(6):
            _t0 = time.time()
            _sock.sendall(_req)
            _buf = b""
            while b"hello" not in _buf:
                _chunk = _sock.recv(4096)
                assert _chunk, "the server closed the connection"
                _buf += _chunk
            _worst = max(_worst, (time.time() - _t0) * 1000)
    finally:
        _sock.close()
    # the signature is 40ms exactly, so anything under 20 says Nagle is not in the
    # way. Generous on purpose: this is a latency floor, not a benchmark
    assert _worst < 20.0, (
        "a keep-alive request took %.1fms - TCP_NODELAY is not set on the accepted "
        "socket, see cmake/crow_tcp_nodelay.cmake" % _worst)

    # Templated routes - TODO 222.
    print("HTTP templated routes bind path segments and the query", flush=True)
    status, body, _ = http_call("GET", "/notes/7/rev/3?q1=v1")
    assert status == 200, (status, body)
    assert body == b"id=7 n=3 q=v1 url=/notes/7/rev/3", body

    # segments are percent decoded, one at a time. %2F is data inside a segment,
    # not a separator, which is only true because the split happens first.
    status, body, _ = http_call("GET", "/notes/a%20b/rev/9")
    assert status == 200 and body == b"id=a b n=9 q=nil url=/notes/a%20b/rev/9", body
    status, body, _ = http_call("GET", "/notes/a%2Fb/rev/9")
    assert status == 200 and body.startswith(b"id=a/b n=9"), body

    # `+` is a plus in a path and a space in a query - different rules, and the
    # handler sees both in the same call
    status, body, _ = http_call("GET", "/notes/a+b/rev/1?q1=one+two")
    assert status == 200 and body.startswith(b"id=a+b n=1 q=one two"), body

    # the literal segment has to match, and the count has to be exact
    for miss in ("/notes/7/rev", "/notes/7/rev/3/extra", "/notes/7/nope/3"):
        status, _, _ = http_call("GET", miss)
        assert status == 404, (miss, status)

    # a trailing * takes the rest, and the routing under it is the handler's job
    status, body, _ = http_call("GET", "/files/css/app.css")
    assert status == 200 and body == b"head=css rest=css/app.css", body
    status, body, _ = http_call("GET", "/files/one")
    assert status == 200 and body == b"head=one rest=one", body
    # <path> needs something to match, so the bare prefix is not a hit
    status, _, _ = http_call("GET", "/files/")
    assert status == 404, status

    # Crow's own /static/<path> is off (CROW_DISABLE_STATIC_DIR), so it neither
    # serves files out of the cwd nor takes the prefix from a route
    status, _, _ = http_call("GET", "/static/anything")
    assert status == 404, status

    # request read as a buffer, response written from one - the bytes are never
    # a lua string on either side. Includes an embedded NUL and every byte
    # value, which is the case a string round trip is most likely to spoil.
    # See TODO 216.
    payload = bytes(range(256)) + b"\x00tail"
    status, body, _ = http_call(
        "POST", "/binecho", data=payload,
        headers={"Content-Type": "application/octet-stream"}, timeout=10)
    assert status == 200, (status, body)
    assert len(body) == len(payload) + 1, ("length changed", len(body), len(payload))
    assert body[0] == len(payload) % 256, ("handler saw a different length", body[0])
    assert body[1:] == payload, "bytes did not survive the round trip"

    # and the setter still takes a plain string, which is what every other
    # handler in this file uses
    status, body, _ = http_call(
        "POST", "/echo", data=json.dumps({"a": 11}),
        headers={"Content-Type": "application/json"}, timeout=10)
    assert status == 200 and json.loads(body)["a"] == 11, (status, body)

    status, body, hdrs = http_call("GET", "/sess", timeout=10)
    assert status == 200, (status, body)
    first = json.loads(body)
    assert first["n"] == 1, first
    sid = None
    for k, v in hdrs.items():
        if k.lower() == "set-cookie" and v.startswith("sid="):
            sid = v.split(";")[0]
            break
    assert sid, hdrs
    status, body, _ = http_call("GET", "/sess", headers={"Cookie": sid}, timeout=10)
    assert status == 200, (status, body)
    second = json.loads(body)
    assert second["n"] == 2 and second["sid"] == first["sid"], second

    status, body, _ = http_call("GET", "/hits", timeout=10)
    assert status == 200, (status, body)
    assert json.loads(body)["n"] == 1, body

    nested = {"user": {"id": 1, "tags": ["a", "b"]}, "n": 2.5, "ok": True}
    status, body, _ = http_call(
        "POST", "/json", data=json.dumps(nested),
        headers={"Content-Type": "application/json"}, timeout=10)
    assert status == 200, (status, body)
    back = json.loads(body)
    assert back["user"]["id"] == 1 and back["user"]["tags"] == ["a", "b"], back
    assert back["ok"] is True and float(back["n"]) == 2.5, back
    status, _, _ = http_call("GET", "/json")
    assert status == 405, status

    status, body, _ = http_call("GET", "/health", timeout=10)
    assert status == 200, (status, body)
    health = json.loads(body)
    assert health["ok"] is True and "retrieve_ops" in health, health
    assert health.get("user") == "web", health

    status, body, _ = http_call("GET", "/stats", timeout=10)
    assert status == 200, (status, body)
    stbody = json.loads(body)
    assert stbody.get("user") == "web", stbody
    assert "retrieve_ops" in stbody["ops"], stbody
    assert "heap_bytes_allocated" in stbody["stats"], stbody
    assert int(stbody["stats"]["heap_bytes_allocated"]) > 0, stbody

    status, body, _ = http_call("GET", "/who", timeout=10)
    assert status == 200, (status, body)
    assert json.loads(body)["user"] == "web", body

    print("HTTP login upgrades the session; /stats stays pinned to web", flush=True)
    r.execute_command("ACL", "SETUSER", "alice", "on", ">secret",
                      "+read", "+write", "+stats", "+keys", "+connection")
    status, body, _ = http_call(
        "POST", "/login",
        data=json.dumps({"user": "alice", "pass": "wrong"}),
        headers={"Content-Type": "application/json"}, timeout=10)
    assert status == 401, (status, body)
    assert json.loads(body)["ok"] is False, body

    status, body, hdrs = http_call(
        "POST", "/login",
        data=json.dumps({"user": "alice", "pass": "secret"}),
        headers={"Content-Type": "application/json"}, timeout=10)
    assert status == 200, (status, body)
    assert json.loads(body)["ok"] is True and json.loads(body)["user"] == "alice", body
    login_sid = None
    for k, v in hdrs.items():
        if k.lower() == "set-cookie" and v.startswith("sid="):
            login_sid = v.split(";")[0]
            break
    assert login_sid, hdrs
    status, body, _ = http_call("GET", "/who", headers={"Cookie": login_sid}, timeout=10)
    assert status == 200, (status, body)
    assert json.loads(body)["user"] == "alice", body
    status, body, _ = http_call("GET", "/health", headers={"Cookie": login_sid}, timeout=10)
    assert status == 200, (status, body)
    assert json.loads(body)["user"] == "alice", body
    status, body, _ = http_call("GET", "/stats", headers={"Cookie": login_sid}, timeout=10)
    assert status == 200, (status, body)
    assert json.loads(body)["user"] == "web", body
    status, body, _ = http_call("GET", "/who", timeout=10)
    assert status == 200 and json.loads(body)["user"] == "web", body

    print("handler calls out with http.request", flush=True)
    status, body, _ = http_call("GET", "/fetch", timeout=15)
    assert status == 200 and body == b"200|upstream ok", (status, body)

    st = r.execute_command("HTTP", "STATUS")
    assert any(b"ECHO" in x for x in st), st

    # TODO 181: the pool counts and the Luau bytes those VMs hold
    def status_fields():
        out = {}
        for line in r.execute_command("HTTP", "STATUS"):
            text = line.decode()
            if "=" in text:
                k, _, v = text.partition("=")
                out[k] = v
        return out

    fields = status_fields()
    vms = int(fields["vms"])
    assert 2 <= vms <= 8, fields
    assert int(fields["idle"]) == vms and int(fields["executing"]) == 0, fields
    # every VM in the pool has a state with the same functions compiled in, so the
    # figure is well past a single state's ~50kB but nothing like a gigabyte
    idle_bytes = int(fields["luau_bytes"])
    assert idle_bytes > 50 * 1024, fields

    # and the executing count moves while a handler is actually in flight
    with ThreadPoolExecutor(max_workers=1) as spinner:
        fut = spinner.submit(http_call, "GET", "/slow", None, None, 20)
        seen = 0
        peak = 0
        for _ in range(400):
            f = status_fields()
            peak = max(peak, int(f["executing"]))
            if int(f["executing"]) >= 1:
                seen += 1
                assert int(f["idle"]) == vms - int(f["executing"]), f
                break
            if fut.done():
                break
            time.sleep(0.005)
        status, body, _ = fut.result()
    assert status == 200, (status, body)
    assert seen == 1, f"never caught the slow handler running, peak={peak}"
    after = status_fields()
    assert int(after["executing"]) == 0 and int(after["idle"]) == vms, after

    workers = 8
    per_worker = 20
    print(f"concurrent ingress {workers} threads x {per_worker}", flush=True)

    def http_worker(i):
        rc = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
        for n in range(per_worker):
            status, page, _ = http_call("GET", "/page", timeout=10)
            assert status == 200 and b"hello" in page, (status, page)
            status, body, _ = http_call(
                "POST", "/echo",
                data=json.dumps({"a": i * 100 + n}),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            assert status == 200, (status, body)
            got = json.loads(body)
            assert got["ok"] is True and got["a"] == i * 100 + n, got
            status, body, _ = http_call("GET", "/fetch", timeout=20)
            assert status == 200 and body == b"200|upstream ok", (status, body)
            status, body, _ = http_call("GET", "/hits", timeout=10)
            assert status == 200, (status, body)
            assert json.loads(body)["n"] >= 1, body
            rc.set(f"noise-{i}-{n}", n)
            assert rc.get(f"noise-{i}-{n}") == str(n).encode()
        return True

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(http_worker, i) for i in range(workers)]
        for f in as_completed(futs):
            f.result()

    hits = int.from_bytes(r.get("hits"), "little", signed=True)
    assert hits == 1 + workers * per_worker, hits

    assert r.execute_command("HTTP", "STOP") == b"OK"
    assert r.execute_command("HTTP", "STATUS") == b"stopped"
    print("complete http luau test")
finally:
    try:
        upstream.shutdown()
    except Exception:
        pass
    try:
        r.execute_command("HTTP", "STOP")
    except Exception:
        pass
    try:
        barch.stop()
    except Exception:
        pass
