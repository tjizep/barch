# http.request() inside stored Luau functions: cofetch over asio, and the
# coroutine parking that lets a request wait without holding a pool thread.
import scale
import http.server
import json
import socketserver
import threading
import time

import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

PORT = scale.port(default=14099)
WEB_PORT = scale.port(1, default=18099)
CROW_PORT = scale.port(2, default=18100)

print("start fetch luau test", flush=True)


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _send(self, code, body, ctype="text/plain"):
        raw = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("X-Barch-Test", "yes")
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path == "/hello":
            self._send(200, "hello from the test server")
        elif self.path == "/slow":
            time.sleep(1.0)
            self._send(200, "slow done")
        elif self.path == "/missing":
            self._send(404, "nope")
        else:
            self._send(400, "?")

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(n).decode()
        self._send(200, json.dumps({"echoed": body, "ct": self.headers.get("Content-Type")}),
                   "application/json")


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


web = Server(("127.0.0.1", WEB_PORT), Handler)
threading.Thread(target=web.serve_forever, daemon=True).start()

barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")

BASE = f"http://127.0.0.1:{WEB_PORT}"

GETBODY = '''
function call(url)
    local res = http.request(url):timeout(5000):get()
    if not res.ok then return "ERR " .. (res.error or "?") end
    return res.body
end
'''

GETSTATUS = '''
function call(url)
    local res = http.request(url):timeout(5000):get()
    return tostring(res.status) .. " ok=" .. tostring(res.ok)
end
'''

GETHEADER = '''
function call(url)
    local res = http.request(url):timeout(5000):get()
    return res.headers["X-Barch-Test"] or "missing"
end
'''

POSTJSON = '''
function call(url, payload)
    local res = http.request(url)
        :headers({["Content-Type"] = "application/json"})
        :body(payload)
        :timeout(5000)
        :post()
    return res.body
end
'''

REFUSED = '''
function call(url)
    local res = http.request(url):timeout(2000):get()
    if res.ok then return "unexpectedly ok" end
    return "handled: " .. (res.error or "no error")
end
'''

FAST = '''
function call()
    return "fast"
end
'''

try:
    assert r.execute_command("SETF", "getbody", GETBODY) == b"OK"
    assert r.execute_command("SETF", "getstatus", GETSTATUS) == b"OK"
    assert r.execute_command("SETF", "getheader", GETHEADER) == b"OK"
    assert r.execute_command("SETF", "postjson", POSTJSON) == b"OK"
    assert r.execute_command("SETF", "refused", REFUSED) == b"OK"
    assert r.execute_command("SETF", "fast", FAST) == b"OK"

    print("GET body", flush=True)
    got = r.execute_command("GETBODY", BASE + "/hello")
    assert got == b"hello from the test server", got

    print("GET status of a 404", flush=True)
    got = r.execute_command("GETSTATUS", BASE + "/missing")
    assert got == b"404 ok=false", got

    print("response headers", flush=True)
    got = r.execute_command("GETHEADER", BASE + "/hello")
    assert got == b"yes", got

    print("POST with a body and a header", flush=True)
    got = r.execute_command("POSTJSON", BASE + "/echo", '{"a":1}')
    payload = json.loads(got)
    assert payload["echoed"] == '{"a":1}', payload
    assert payload["ct"] == "application/json", payload

    print("connection refused is handled, not fatal", flush=True)
    got = r.execute_command("REFUSED", "http://127.0.0.1:9/nothing")
    assert got.startswith(b"handled: "), got
    # and the server is still fine afterwards
    assert r.execute_command("FAST") == b"fast"

    # The point of parking: while /slow is waiting, the pool must still run
    # other work. If the request held its worker this would serialise.
    print("a parked request does not hold its worker", flush=True)
    slow_done = []

    def slow_call():
        c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
        t0 = time.time()
        out = c.execute_command("GETBODY", BASE + "/slow")
        slow_done.append((out, time.time() - t0))

    t = threading.Thread(target=slow_call)
    t.start()
    time.sleep(0.25)  # the slow request is now in flight and parked
    fast_calls = 0
    t1 = time.time()
    while t.is_alive() and time.time() - t1 < 2.0:
        assert r.execute_command("FAST") == b"fast"
        fast_calls += 1
    t.join(timeout=10)
    assert slow_done, "slow call never came back"
    body, elapsed = slow_done[0]
    assert body == b"slow done", body
    assert elapsed >= 0.9, elapsed
    assert fast_calls > 5, fast_calls
    print(f"  {fast_calls} calls ran while the fetch was parked", flush=True)

    # Four at once: they overlap on the reactor rather than queueing.
    print("concurrent parked requests overlap", flush=True)
    results = []

    def one():
        c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
        results.append(c.execute_command("GETBODY", BASE + "/slow"))

    t0 = time.time()
    threads = [threading.Thread(target=one) for _ in range(4)]
    for x in threads:
        x.start()
    for x in threads:
        x.join(timeout=20)
    span = time.time() - t0
    assert results == [b"slow done"] * 4, results
    assert span < 3.0, f"four 1s requests took {span:.2f}s, so they serialised"
    print(f"  four 1s fetches in {span:.2f}s", flush=True)

    # The Crow path is the one that must NOT park: a handler runs under
    # lua_pcall holding a VM slot from the space's pool, so a yield would hand
    # that slot back while the coroutine was still suspended on it and the next
    # request could pick up the same lua_State. It waits inline instead. Eight
    # threads against a pool of 2-8 slots is what would show a slot being reused
    # underneath a suspended handler.
    print("http.request inside a Crow handler", flush=True)
    PROXY = """
function call()
    return "proxy"
end

function fetchit(req, res)
    local target = "http://127.0.0.1:%d/hello"
    local got = http.request(target):timeout(5000):get()
    res.body = tostring(got.status) .. "|" .. got.body
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/proxy",
        methods = {GET = fetchit},
        send = "text/plain",
    }
end
""" % WEB_PORT

    CONF = """
function call()
    return "conf"
end

function transport()
    return {
        kind = "http",
        port = %d,
        bind = "127.0.0.1",
        keys = {"PROXY"},
    }
end
""" % CROW_PORT

    assert r.execute_command("SETF", "proxy", PROXY) == b"OK"
    assert r.execute_command("SETF", "crowconf", CONF) == b"OK"
    started = r.execute_command("HTTP", "START", "CROWCONF", str(CROW_PORT), "127.0.0.1")
    assert any(b"PROXY /proxy GET" in x for x in started), started

    import http.client

    def proxy_once():
        conn = http.client.HTTPConnection("127.0.0.1", CROW_PORT, timeout=15)
        try:
            conn.request("GET", "/proxy", headers={"Connection": "close"})
            resp = conn.getresponse()
            return resp.status, resp.read()
        finally:
            conn.close()

    for _ in range(30):
        try:
            if proxy_once()[0] == 200:
                break
        except (TimeoutError, ConnectionError, OSError):
            pass
        time.sleep(0.1)

    status, body = proxy_once()
    assert status == 200, (status, body)
    assert body == b"200|hello from the test server", body

    got = []
    errs = []

    def hammer():
        try:
            for _ in range(6):
                s2, b2 = proxy_once()
                assert s2 == 200 and b2 == b"200|hello from the test server", (s2, b2)
                got.append(b2)
        except Exception as e:
            errs.append(repr(e))

    hs = [threading.Thread(target=hammer) for _ in range(8)]
    for x in hs:
        x.start()
    for x in hs:
        x.join(timeout=60)
    assert not errs, errs
    assert len(got) == 48, len(got)
    print("  %d handler fetches across 8 threads, all correct" % len(got), flush=True)
    assert r.execute_command("HTTP", "STOP") == b"OK"

    print("complete fetch luau test")
finally:
    try:
        r.execute_command("HTTP", "STOP")
    except Exception:
        pass
    try:
        web.shutdown()
    except Exception:
        pass
    try:
        barch.stop()
    except Exception:
        pass
