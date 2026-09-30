# A rewritten HTTP route lets its old handlers go - TODO 560.
#
# Each VM slot reloads a route's handlers when its function is published again
# (SETF ... RELOAD), and the refs the old handlers were held by were never
# released, so every reload kept the old closures, and the chunk's globals with
# them, for the life of the server. Here each version holds a 50kB string, so
# sixty rewrites would keep three megabytes that nothing can reach.
#
# And when a reload fails - the route's function removed - the slot goes on
# answering with the last handlers that worked, which it can only do if those
# are kept rather than released with the rest.
import http.client

import scale
import redis
import barch

scale.workdir()
PORT = scale.port(default=14600)
HTTP_PORT = scale.port(1, default=14601)
ROUNDS = 80
BIG = 50000

print("start http reload test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("USE", "hrl")

failures = []


def check(ok, what):
    print("  %-70s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures.append(what)


def get(path):
    h = http.client.HTTPConnection("127.0.0.1", HTTP_PORT, timeout=30)
    try:
        h.request("GET", path, headers={"Connection": "close"})
        resp = h.getresponse()
        return resp.status, resp.read()
    finally:
        h.close()


def route(n):
    return '''
function call() return "r" end
-- different in every version: Luau keeps one copy of equal strings
local big = string.sub(string.rep("%d-", %d), 1, %d)
function hit(req, res)
    res.body = "v%d:" .. #big
    res.code = 200
end
function service() return {kind = "resource", route = "/r", methods = {GET = hit}, send = "text/plain"} end
''' % (n, BIG, BIG, n)


def luau_bytes():
    for line in r.execute_command("HTTP", "STATUS"):
        line = line.decode() if isinstance(line, bytes) else str(line)
        if line.startswith("luau_bytes="):
            return int(line.split("=", 1)[1])
    return -1


assert r.execute_command("SETF", "R", route(0)) == b"OK"
assert r.execute_command("SETF", "CONF", '''
function call() return "conf" end
function service() return {kind = "http", bind = "127.0.0.1", keys = {"R"}} end
''') == b"OK"

try:
    r.execute_command("HTTP", "START", "CONF", str(HTTP_PORT), "127.0.0.1")
    print("a route rewritten %d times" % ROUNDS)
    wrong = []
    seen = []
    for i in range(1, ROUNDS + 1):
        assert r.execute_command("SETF", "R", route(i), "RELOAD") == b"OK"
        body = get("/r")
        if body != (200, ("v%d:%d" % (i, BIG)).encode()):
            wrong.append((i, body))
        seen.append(luau_bytes())
    check(not wrong, "every request answers with the version just written: %s" % wrong[:3])
    # the lowest of a window rather than one reading: what's been let go is only
    # counted out when the collector gets to it, so any one reading can be high.
    # The lows are what's actually held - they climb by about BIG per rewrite
    # when old versions are kept, and stay put when they aren't
    early = min(seen[10:20])
    late = min(seen[-10:])
    grew = late - early
    print("  luau bytes, lowest over rewrites 11-20 %d, over the last 10 %d" % (early, late))
    check(grew < (ROUNDS - 20) * BIG // 4,
          "the old versions go: %d bytes more after %d more rewrites" % (grew, ROUNDS - 20))

    print("a rewrite that fails to load")
    assert r.execute_command("REMF", "R", "RELOAD") == 1
    check(get("/r") == (200, ("v%d:%d" % (ROUNDS, BIG)).encode()),
          "with its function gone, the route answers with the last one that loaded")
    check(get("/r") == (200, ("v%d:%d" % (ROUNDS, BIG)).encode()), "and again")
    assert r.execute_command("SETF", "R", route(ROUNDS + 1), "RELOAD") == b"OK"
    check(get("/r") == (200, ("v%d:%d" % (ROUNDS + 1, BIG)).encode()),
          "and the next one that loads replaces it")
finally:
    try:
        r.execute_command("HTTP", "STOP")
    except Exception:
        pass
    barch.stop()

print()
if failures:
    print("FAILURES: %d" % len(failures))
    for f in failures:
        print("  " + f)
    raise SystemExit(1)
print("all http reload checks pass")
