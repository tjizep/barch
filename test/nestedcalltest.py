# A stored function called from another with CALLF runs to the end in its caller -
# TODO 552.
#
# A function with a slice (`--@barch {"slice_insns": ...}`) runs its first short
# slice on the thread that asked and the rest on the function pool, and whoever
# called it parks until it's done. Called from another script through
# barch.call("CALLF", ...), that park was refused - "FUNCTION cannot call 'CALLF',
# it blocks" - on the grounds that a parked command hasn't done anything yet. A
# sliced function had: it was already running on the pool, still holding the
# caller's interface, which the refusal then dropped. It went on writing keys after
# its caller had been told it was refused, and a few at once corrupted a Luau state
# and brought the server down (SIGILL in the GC).
#
# An HTTP handler's own `--@barch` header was also ignored: every route ran with
# the space's function deadline, so a handler doing real work through CALLF timed
# out at the default second.
#
# Checked, against barchd, with the function from the report (random keys, a
# 200000 instruction slice):
#   - an HTTP route with a 30 s header calls it through CALLF and gets the count
#   - six such requests at once all answer, and the server is still up
#   - a stored function calling it through CALLF over RESP works the same
#   - a route with no header times out at the space's deadline, and nothing goes
#     on writing after the answer
import http.client
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14552)
HTTP = PORT + 1
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "nestedcall_data")
N = 100000

FILL = '''--[[@barch{"deadline_ms": 30000, "slice_insns": 200000}]]
local CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
local N = #CHARS
local function randKey()
    local out = table.create(10)
    for i = 1, 10 do local n = math.random(N); out[i] = string.sub(CHARS, n, n) end
    return table.concat(out)
end
function call(count)
    count = tonumber(count) or 1000000
    local sp = barch.current()
    for i = 1, count do sp[randKey()] = i end
    return sp:size()
end'''

GO = '''--[[@barch{"deadline_ms": 30000}]]
function call() return "go" end
function go(req, res)
    local ok, r = pcall(barch.call, "CALLF", "fill", "%d")
    res.body = tostring(ok) .. " " .. tostring(r); res.code = 200
end
function transport() return {kind="resource", route="/go", methods={GET=go}, send="text/plain"} end''' % N

# no header: the space's deadline, which a million keys doesn't fit in
SHORT = '''function call() return "short" end
function short(req, res)
    local ok, r = pcall(barch.call, "CALLF", "fill", "1000000")
    res.body = tostring(ok) .. " " .. tostring(r); res.code = 200
end
function transport() return {kind="resource", route="/short", methods={GET=short}, send="text/plain"} end'''

OUTER = '''--[[@barch{"deadline_ms": 30000}]]
function call(n) return barch.call("CALLF", "fill", n) end'''

CONF = '''function call() return "http" end
function transport()
    return {kind="http", port=%d, bind="127.0.0.1", user="default", keys={"GO", "SHORT"}}
end''' % HTTP

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def get(path):
    try:
        c = http.client.HTTPConnection("127.0.0.1", HTTP, timeout=120)
        c.request("GET", path)
        r = c.getresponse()
        return r.status, r.read().decode(errors="replace")
    except Exception as e:                          # noqa: BLE001
        return "failed", repr(e)


shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA,
                      "--no-save-on-exit"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    end = time.time() + 60
    while True:
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            break
        except OSError:
            if time.time() > end or p.poll() is not None:
                raise AssertionError("barchd did not start")
            time.sleep(0.1)
    r = redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=120)
    for name, src in (("fill", FILL), ("go", GO), ("short", SHORT), ("outer", OUTER), ("httpconf", CONF)):
        r.execute_command("SETF", name, src)
    try:
        r.execute_command("HTTP", "START", "HTTPCONF", str(HTTP), "127.0.0.1")
    except redis.exceptions.ResponseError as e:
        print("SKIP: no HTTP server in this build (%s)" % e)
        sys.exit(0)
    for _ in range(50):
        if get("/go")[0] != "failed":
            break
        time.sleep(0.1)

    print("CALLF of a sliced function from an HTTP handler", flush=True)
    status, body = get("/go")
    check(status == 200 and body.startswith("true "), "it answers with the count (%s %s)" % (status, body[:60]))

    got = []
    threads = [threading.Thread(target=lambda: got.append(get("/go"))) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    good = sum(1 for s, b in got if s == 200 and b.startswith("true "))
    check(good == 6, "six at once all answer (%d of 6: %s)" % (good, [b[:30] for _, b in got if not b.startswith("true ")][:2]))
    check(p.poll() is None, "and the server is still up")

    print("CALLF from a stored function over RESP", flush=True)
    before = r.dbsize()
    got = r.execute_command("CALLF", "outer", str(N))
    check(isinstance(got, int) and got >= before + N - 100, "it answers with the count (%s, %d before)" % (got, before))

    print("a handler that runs out of time leaves nothing running", flush=True)
    status, body = get("/short")
    check(status == 500 and "timeout" in body, "it times out at the space's deadline (%s %s)" % (status, body[:60]))
    after = r.dbsize()
    time.sleep(2)
    check(r.dbsize() == after, "and nothing goes on writing after it answered (%d then %d)" % (after, r.dbsize()))
    check(p.poll() is None, "and the server is still up")
finally:
    p.send_signal(signal.SIGKILL)
    p.wait(timeout=30)

print("\n%s" % ("nested call checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
