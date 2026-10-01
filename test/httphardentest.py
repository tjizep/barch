# Two hardening fixes in the Crow/Luau glue - TODO 575.
#
# 1. A push that throws before the handler's lua_pcall. http_vm_call sets the
#    interrupt's userdata, the thread data and the request generation, takes a
#    pooled coroutine, and only then pushes the request, response, params and
#    query - all outside any pcall. A Luau error there is a C++ exception that
#    unwinds out of the function and skips the cleanup. The coroutine and its
#    registry ref are never handed back, so each failure pins another one, and
#    the pointers into the stack are left in the state. BARCH_TEST_HTTP_PUSH_THROW
#    makes a request with ?barch_test_throw raise at the last push, as an
#    out-of-memory would.
# 2. The request and response metatables weren't locked, so a handler could read
#    them with getmetatable. Every other handle in the bindings answers "locked".
import http.client
import os

# read once by the glue, so it has to be there before the server starts
os.environ["BARCH_TEST_HTTP_PUSH_THROW"] = "1"

import scale
import redis
import barch

scale.workdir()
PORT = scale.port(default=14640)
HTTP_PORT = scale.port(1, default=14641)
FAILING = scale.scaled(4000, floor=1500)

print("start http harden test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("USE", "hhd")

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
    except (OSError, http.client.HTTPException):
        return None, b""
    finally:
        h.close()


def luau_bytes():
    for line in r.execute_command("HTTP", "STATUS"):
        line = line.decode() if isinstance(line, bytes) else str(line)
        if line.startswith("luau_bytes="):
            return int(line.split("=", 1)[1])
    return -1


assert r.execute_command("SETF", "HD", '''
function call() return "r" end
function hit(req, res)
    res.body = "ok"
    res.code = 200
end
function service()
    return {kind = "resource", route = "/hd", methods = {GET = hit}, send = "text/plain"}
end
''') == b"OK"
assert r.execute_command("SETF", "HM", '''
function call() return "r" end
function metas(req, res)
    res.body = tostring(getmetatable(req)) .. "," .. tostring(getmetatable(res))
    res.code = 200
end
function service()
    return {kind = "resource", route = "/hm", methods = {GET = metas}, send = "text/plain"}
end
''') == b"OK"
assert r.execute_command("SETF", "CONF", '''
function call() return "conf" end
function service() return {kind = "http", bind = "127.0.0.1", keys = {"HD", "HM"}} end
''') == b"OK"

try:
    r.execute_command("HTTP", "START", "CONF", str(HTTP_PORT), "127.0.0.1")

    print("the request and response metatables")
    check(get("/hm") == (200, b"locked,locked"),
          "getmetatable(req), getmetatable(res) give the lock, not the table: %s" % (get("/hm"),))

    print("a push that throws before the handler runs")
    check(get("/hd") == (200, b"ok"), "an ordinary request works")
    for _ in range(50):
        get("/hd?barch_test_throw=1")
    check(get("/hd") == (200, b"ok"), "and still does after a failed one")
    seen = []
    for i in range(FAILING):
        get("/hd?barch_test_throw=1")
        if i % 100 == 0:
            seen.append(luau_bytes())
    check(get("/hd") == (200, b"ok"), "and after %d of them" % FAILING)
    # each pinned coroutine is about a kilobyte, so four thousand of them is
    # megabytes; handed back to the pool, none are held beyond its cap
    early = min(seen[:5])
    late = min(seen[-5:])
    grew = late - early
    print("  luau bytes, lowest of the first 5 readings %d, of the last 5 %d" % (early, late))
    check(grew < 256 * 1024,
          "failed pushes give their coroutine back: %d bytes more after %d" % (grew, FAILING))
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
print("all http harden checks pass")
