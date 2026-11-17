# A Crow handler that waits on I/O parks - TODO 911.
#
# http.request, resp.connect and mail.send used to wait inline in a route
# handler. The wait held the Crow thread and one of the space's VM slots, so as
# many slow upstream calls as the pool had slots stopped the server answering
# anything at all. A handler now parks like a stored function does. The thread
# and the slot go back to other requests, and the handler goes on once its call
# lands and the slot is free.
#
# The upstream here holds every request until the test lets it go, so the order
# is forced with an event, not with sleeps:
# 1. more slow requests than the pool has slots all reach the upstream, and a
#    fast route still answers while they wait;
# 2. let go, each answers with what its handler did after the park: a header,
#    a req:param read, its own query;
# 3. a handler that raises after the park answers 500;
# 4. STOP with handlers parked returns, the parked requests get 503, and the
#    server still runs once their calls come back.
import http.client
import http.server
import socketserver
import threading

import scale
import redis
import barch

scale.workdir()
PORT = scale.port(default=14660)
WEB = scale.port(1, default=14661)
CROW = scale.port(2, default=14662)
# a guard for a test that hangs, never a measure of anything
GUARD = 60

failures = []


def check(ok, what):
    print("  %-70s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures.append(what)


class Hold:
    def __init__(self):
        self.cv = threading.Condition()
        self.arrived = 0
        self.release = threading.Event()

    def reset(self):
        with self.cv:
            self.arrived = 0
        self.release = threading.Event()

    def wait_for(self, n):
        with self.cv:
            return self.cv.wait_for(lambda: self.arrived >= n, timeout=GUARD)


hold = Hold()


class Upstream(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        release = hold.release
        with hold.cv:
            hold.arrived += 1
            hold.cv.notify_all()
        release.wait(GUARD)
        raw = b"held"
        self.send_response(200)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


web = Server(("127.0.0.1", WEB), Upstream)
threading.Thread(target=web.serve_forever, daemon=True).start()

PARKED = """
function call() return "parked" end

function get(req, res, params, query)
    local got = http.request("http://127.0.0.1:%d/hold"):timeout(%d):get()
    res:header("X-After", "resumed")
    res.body = tostring(got.status) .. "|" .. (got.body or "") .. "|"
        .. (req:param("n") or "") .. "|" .. (query.n or "")
    res.code = 200
end

function transport()
    return {kind = "resource", route = "/park", methods = {GET = get}, send = "text/plain"}
end
""" % (WEB, GUARD * 1000)

PFAIL = """
function call() return "pfail" end

function get(req, res)
    http.request("http://127.0.0.1:%d/hold"):timeout(%d):get()
    error("raised after the park")
end

function transport()
    return {kind = "resource", route = "/pfail", methods = {GET = get}, send = "text/plain"}
end
""" % (WEB, GUARD * 1000)

QUICK = """
function call() return "quick" end

function get(req, res)
    res.body = "quick"
    res.code = 200
end

function transport()
    return {kind = "resource", route = "/quick", methods = {GET = get}, send = "text/plain"}
end
"""

CONF = """
function call() return "conf" end

function transport()
    return {kind = "http", port = %d, bind = "127.0.0.1", user = "web",
            keys = {"PARKED", "PFAIL", "QUICK"}}
end
""" % CROW


def get(path):
    conn = http.client.HTTPConnection("127.0.0.1", CROW, timeout=GUARD)
    try:
        conn.request("GET", path, headers={"Connection": "close"})
        resp = conn.getresponse()
        return resp.status, resp.getheader("X-After"), resp.read()
    finally:
        conn.close()


def fire(paths):
    """each path on its own thread; returns the threads and where answers land"""
    out = {}

    def one(p):
        try:
            out[p] = get(p)
        except Exception as e:
            out[p] = ("error", None, repr(e).encode())

    ts = [threading.Thread(target=one, args=(p,), daemon=True) for p in paths]
    for t in ts:
        t.start()
    return ts, out


def join(ts):
    for t in ts:
        t.join(GUARD)
    return not any(t.is_alive() for t in ts)


def status():
    out = {}
    for line in r.execute_command("HTTP", "STATUS"):
        k, eq, v = line.decode().partition("=")
        if eq:
            out[k] = v
    return out


print("start http park test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")

try:
    r.execute_command("ACL", "SETUSER", "web", "on", "+outbound")
    for name, src in (("parked", PARKED), ("pfail", PFAIL), ("quick", QUICK), ("conf", CONF)):
        assert r.execute_command("SETF", name, src) == b"OK"
    r.execute_command("HTTP", "START", "CONF", str(CROW), "127.0.0.1")
    check(get("/quick")[2] == b"quick", "the server answers")
    vms = int(status()["vms"])
    many = vms + 2
    print("parked handlers free their slots (%d slots, %d requests)" % (vms, many))

    ts, out = fire(["/park?n=%d" % i for i in range(many)])
    arrived = hold.wait_for(many)
    check(arrived, "all %d reach the upstream" % many)
    st, _, body = get("/quick") if arrived else ("skipped", None, b"")
    check(st == 200 and body == b"quick", "a fast route answers while they wait")
    # an inline wait reaches the upstream too, but only once the first ones give
    # up and free their slots, so it shows as answers before the upstream lets go
    check(not out, "and none of them has answered yet: %s" % sorted(out))
    check(status().get("parked") == str(many), "STATUS counts them parked")
    hold.release.set()
    check(join(ts), "each parked request is answered")
    want = {"/park?n=%d" % i: (200, "resumed", ("200|held|%d|%d" % (i, i)).encode()) for i in range(many)}
    check(out == want, "each answers with what its handler did after the park")
    if out != want:
        print("    got", out)
    # no check that parked is back to 0: the count drops just after the response
    # goes out, so a client can read it first. STOP below waits for 0, so a call
    # never counted off would hang it.

    print("a handler that raises after the park")
    hold.reset()
    ts, out = fire(["/pfail"])
    hold.wait_for(1)
    hold.release.set()
    join(ts)
    st, _, body = out.get("/pfail", (None, None, b""))
    check(st == 500 and b"raised after the park" in body, "answers 500 with the error")

    print("STOP while handlers are parked")
    hold.reset()
    ts, out = fire(["/park?n=%d" % i for i in range(many)])
    check(hold.wait_for(many), "they reach the upstream")
    check(r.execute_command("HTTP", "STOP") == b"OK", "STOP returns")
    check(join(ts), "every parked request is answered")
    check(all(v[0] == 503 for v in out.values()) and len(out) == many,
          "with 503: %s" % sorted(set(str(v[0]) for v in out.values())))
    # their calls come back now, with nothing left to answer
    hold.release.set()
    r.execute_command("HTTP", "START", "CONF", str(CROW), "127.0.0.1")
    hold.reset()
    hold.release.set()
    check(get("/quick")[2] == b"quick", "a new START answers")
    check(get("/park?n=1") == (200, "resumed", b"200|held|1|1"), "and parks again")
    check(r.ping(), "barchd still runs")
finally:
    hold.release.set()
    try:
        r.execute_command("HTTP", "STOP")
    except Exception:
        pass
    barch.stop()
    web.shutdown()

print()
if failures:
    print("FAILURES: %d" % len(failures))
    for f in failures:
        print("  " + f)
    raise SystemExit(1)
print("complete http park test")
