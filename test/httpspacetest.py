# barch.space from HTTP routes that run at the same time, and as different
# users - TODO 559.
#
# HTTP START built one call interface and gave it to every VM slot. The spaces a
# route opens with barch.space are kept in that interface, so:
#
# - two requests on different Crow threads opening a space each wrote the same
#   unlocked map at once. That corrupted the heap, and barchd died later with
#   "double free or corruption" wherever the next free happened to be.
# - the store kept for a space was built with the rights of whoever opened it
#   first, and every later request used it, whoever it ran as.
#
# The race only shows reliably under a sanitizer; the rights check fails anywhere.
import http.client
import threading

import scale
import redis
import barch

scale.workdir()
PORT = scale.port(default=14580)
HTTP_PORT = scale.port(1, default=14581)
SPACES = 40
ROUNDS = scale.scaled(5, floor=2)
THREADS = 8
PER_THREAD = scale.scaled(25, floor=10)

print("start http space test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)

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


for i in range(SPACES):
    r.execute_command("USE", "hs%d" % i)
    r.execute_command("SET", "k", "v%d" % i)
r.execute_command("USE", "hspriv")
r.execute_command("SET", "k", "secret")
r.execute_command("USE", "hsweb")

r.execute_command("ACL", "SETUSER", "hsreader", "on", ">pw", "+read", "+keys", "+function")
r.execute_command("ACL", "SETUSER", "hsnobody", "on", ">pw", "+function")

OPEN = '''
function call() return "open" end
function hit(req, res)
    local got = {}
    for i = 1, 10 do
        local n = math.random(0, %d)
        local v = barch.space["hs" .. n]:get("k")
        if v ~= "v" .. n then
            res.code = 500
            res.body = "hs" .. n .. " said " .. tostring(v)
            return
        end
    end
    res.code = 200
    res.body = "ok"
end
function service() return {kind = "resource", route = "/open", methods = {GET = hit}, send = "text/plain"} end
''' % (SPACES - 1)


def reader(name, route, user):
    return '''
function call() return "%s" end
function hit(req, res)
    local ok, v = pcall(function() return barch.space["hspriv"]:get("k") end)
    res.code = 200
    res.body = ok and tostring(v) or ("refused: " .. tostring(v))
end
function service() return {kind = "resource", route = "%s", user = "%s", methods = {GET = hit}, send = "text/plain"} end
''' % (name, route, user)


assert r.execute_command("SETF", "hsopen", OPEN) == b"OK"
assert r.execute_command("SETF", "hsasreader", reader("hsasreader", "/asreader", "hsreader")) == b"OK"
assert r.execute_command("SETF", "hsasnobody", reader("hsasnobody", "/asnobody", "hsnobody")) == b"OK"
assert r.execute_command("SETF", "hsconf", '''
function call() return "conf" end
function service() return {kind = "http", bind = "127.0.0.1", keys = {"HSOPEN", "HSASREADER", "HSASNOBODY"}} end
''') == b"OK"

try:
    print("requests opening spaces at the same time")
    bad = []
    for rnd in range(ROUNDS):
        r.execute_command("HTTP", "START", "HSCONF", str(HTTP_PORT), "127.0.0.1")

        def one():
            for _ in range(PER_THREAD):
                try:
                    st, body = get("/open")
                except Exception as e:
                    bad.append(repr(e))
                    return
                if st != 200:
                    bad.append((st, body))
        ts = [threading.Thread(target=one) for _ in range(THREADS)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        r.execute_command("HTTP", "STOP")
    check(not bad, "%d rounds of %d x %d requests, each opening 10 of %d spaces: %s"
          % (ROUNDS, THREADS, PER_THREAD, SPACES, bad[:3]))

    print("a space opened by one user isn't read with that user's rights by another")
    r.execute_command("HTTP", "START", "HSCONF", str(HTTP_PORT), "127.0.0.1")
    st, body = get("/asreader")
    check((st, body) == (200, b"secret"), "a user who may read it reads it: %r" % body)
    for _ in range(20):
        st, body = get("/asnobody")
        if body != b"refused: FUNCTION not authorized to read there":
            break
    check(body.startswith(b"refused:") and b"secret" not in body,
          "a user who may not is refused: %r" % body)
    st, body = get("/asreader")
    check(body == b"secret", "and the first still reads it after: %r" % body)
    r.execute_command("HTTP", "STOP")
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
print("all http space checks pass")
