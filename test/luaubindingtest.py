# Memory safety in the Luau bindings - TODO 572.
#
# 1. A route handler can keep `req` or `res` in a module local. The box only
#    holds a pointer to Crow's request, which is gone when the handler returns,
#    so a kept one has to be refused. The check used a per thread counter, but
#    VM slots are pooled across Crow threads, so a box made on one thread could
#    pass the check on another and read a freed request.
# 2. getmetatable on a space, container or row handle handed out the table of
#    C functions behind it. Those cast their first argument without checking
#    its type, so they mustn't be reachable with anything but their own handle.
# 3. simdjson parse and encode push a value per nesting level without asking
#    for stack. The audit read that as writing past the end of the Luau stack,
#    but Luau grows the stack on every push itself (ensure_stack in lapi.cpp),
#    and this ran clean under ASan on the old code. Kept so deep JSON stays
#    covered if that ever changes.
import http.client
import threading

import scale
import redis
import barch

scale.workdir()
PORT = scale.port(default=14630)
HTTP_PORT = scale.port(1, default=14631)
THREADS = 8
PER_THREAD = scale.scaled(60, floor=20)

print("start luau binding test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")

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


# each slot keeps the last request it served and tries to read it on the next
KEEPER = '''
local kept_req = nil
local kept_res = nil
function call() return "keeper" end
function hit(req, res)
    local said = "first"
    if kept_req then
        local rok = pcall(function() return kept_req.method end)
        local wok = pcall(function() kept_res.code = 299 end)
        said = (rok or wok) and "stale-ok" or "refused"
    end
    kept_req = req
    kept_res = res
    res.code = 200
    res.body = said
end
function service() return {kind = "resource", route = "/keep", methods = {GET = hit}, send = "text/plain"} end
'''

try:
    assert r.execute_command("SETF", "lbkeep", KEEPER) == b"OK"
    assert r.execute_command("SETF", "lbconf", '''
function call() return "conf" end
function service() return {kind = "http", bind = "127.0.0.1", keys = {"LBKEEP"}} end
''') == b"OK"

    print("a request kept past its handler is refused, on any thread")
    r.execute_command("HTTP", "START", "LBCONF", str(HTTP_PORT), "127.0.0.1")
    seen = {}
    bad = []
    lock = threading.Lock()

    def one():
        for _ in range(PER_THREAD):
            try:
                st, body = get("/keep")
            except Exception as e:
                bad.append(repr(e))
                return
            with lock:
                seen[(st, body)] = seen.get((st, body), 0) + 1

    ts = [threading.Thread(target=one) for _ in range(THREADS)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    r.execute_command("HTTP", "STOP")
    check(not bad, "every request answered: %s" % bad[:3])
    check((200, b"stale-ok") not in seen,
          "no kept request or response was usable: %r" % seen)
    check(seen.get((200, b"refused"), 0) > 0,
          "and the kept ones were seen and refused: %r" % seen)

    print("a handle's metatable isn't handed to the script")
    r.execute_command("RPUSH", "lblist", "a", "b")
    r.execute_command("SET", "lbkey", "v")
    assert r.execute_command("SETF", "lbmeta", '''
function call()
    local sp = barch.current()
    local row
    for x in sp do row = x break end
    local c = sp:container("lblist")
    return {
        type(getmetatable(sp)),
        type(getmetatable(c)),
        type(getmetatable(row)),
        sp:get("lbkey"),
        c[1] or "nil",
    }
end
''') == b"OK"
    got = r.execute_command("lbmeta")
    check(got[0] != b"table", "space handle metatable is locked: %r" % got[0])
    check(got[1] != b"table", "container handle metatable is locked: %r" % got[1])
    check(got[2] != b"table", "row metatable is locked: %r" % got[2])
    check(got[3] == b"v", "and the space handle still reads: %r" % got[3])

    print("deep JSON parses and encodes")
    for depth in (30, 200, 1000):
        assert r.execute_command("SETF", "lbdeep", '''
function call(n)
    n = tonumber(n)
    local t = simdjson.parse(string.rep("[", n) .. "7" .. string.rep("]", n))
    local d = 0
    while type(t) == "table" do t = t[1] d = d + 1 end
    local o = simdjson.parse(string.rep('{"a":', n) .. "7" .. string.rep("}", n))
    local od = 0
    while type(o) == "table" do o = o.a od = od + 1 end
    return {d, t, od, o}
end
''') == b"OK"
        got = r.execute_command("lbdeep", str(depth))
        check(got == [depth, 7, depth, 7], "parse %d deep: %r" % (depth, got))

    assert r.execute_command("SETF", "lbenc", '''
function call(n)
    n = tonumber(n)
    local t = 7
    for i = 1, n do t = {t} end
    local o = 7
    for i = 1, n do o = {a = o} end
    return {simdjson.encode(t), simdjson.encode(o)}
end
''') == b"OK"
    for depth in (30, 120):
        got = r.execute_command("lbenc", str(depth))
        want_a = ("[" * depth + "7" + "]" * depth).encode()
        want_o = ('{"a":' * depth + "7" + "}" * depth).encode()
        check(got == [want_a, want_o], "encode %d deep" % depth)
    check(r.ping(), "and the server is still up")
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
print("all luau binding checks pass")
