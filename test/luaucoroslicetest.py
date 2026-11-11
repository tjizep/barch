# A slice that ends inside a script's own coroutine - TODO 875.
#
# A call runs on a coroutine of its own, and when its slice runs out the
# interrupt yields it so the pool thread can take other work. It yielded
# whichever thread was running. Inside a coroutine the script made, that yield
# went back to the script's own coroutine.resume, with no values. The script saw
# a yield it never made: a loop of 30M in a coroutine answered
# "true nil nil suspended" instead of "true done 30000000 dead".
#
# Now the slice breaks out of the script's coroutines (lua_break, which the
# coroutine library passes up through each resume) and the pool resumes the
# innermost one first. The script sees nothing.
#
# A script coroutine also had no thread data, so inside one barch.running() was
# empty, require looked in the default space, and a request waited the blocking
# way instead of parking. Checked here as well.
#
# No clocks: each call is checked by what it answers and by INFO counters.
import http.server
import os
import signal
import socketserver
import subprocess
import sys
import threading

import scale
import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

scale.workdir()
PORT = scale.port(default=14974)
WEB_PORT = scale.port(1, default=14975)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "luaucoroslice_data")
os.makedirs(DATA, exist_ok=True)
for f in os.listdir(DATA):
    if f.endswith(".dat"):
        os.remove(os.path.join(DATA, f))


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        raw = b"fetched"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


web = Server(("127.0.0.1", WEB_PORT), Handler)
threading.Thread(target=web.serve_forever, daemon=True).start()

print("start luau coroutine slice test with %s" % BINARY, flush=True)
LOG = os.path.join(DATA, "barchd.log")
logfile = open(LOG, "wb")
proc = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA],
                        stdout=logfile, stderr=subprocess.STDOUT)
scale.wait_for_port(PORT, proc=proc, what="barchd")

failures = []


def check(ok, what):
    print("  %-70s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures.append(what)


def connect():
    # no retry: redis-py's own sends a timed out command again on a new
    # connection, which has no USE
    c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2,
                    socket_timeout=600, retry=Retry(NoBackoff(), 0))
    c.execute_command("USE", "corosl")
    return c


def said(c, *args):
    try:
        got = c.execute_command(*args)
        return got.decode() if isinstance(got, bytes) else got
    except redis.ResponseError as e:
        return "ERR " + str(e)
    except (redis.ConnectionError, redis.TimeoutError) as e:
        return "LOST " + type(e).__name__


admin = connect()


def counter(section, name):
    return int(admin.info(section).get(name, 0))


def alive():
    return proc.poll() is None and said(admin, "PING") in ("PONG", True)


# the user's repro
F2B = '''
function call(n)
    n = tonumber(n)
    local co = coroutine.create(function()
        local x = 0
        for i = 1, n do x = x + 1 end
        return "done", x
    end)
    local ok, a, b = coroutine.resume(co)
    return tostring(ok) .. " " .. tostring(a) .. " " .. tostring(b) .. " " .. coroutine.status(co)
end
'''
# a generator: each value takes many slices to make
GEN = '''
function call(n)
    n = tonumber(n)
    local gen = coroutine.wrap(function()
        for k = 1, 3 do
            local x = 0
            for i = 1, n do x = x + 1 end
            coroutine.yield(x + k)
        end
        return "end"
    end)
    local a = gen()
    local b = gen()
    local c = gen()
    return tostring(a) .. " " .. tostring(b) .. " " .. tostring(c) .. " " .. tostring(gen())
end
'''
# a coroutine that resumes another, once under a pcall. The slice ends in the
# inner one, in the outer one, and in the call's own thread
NESTED = '''
function call(n)
    n = tonumber(n)
    local function spin() local x = 0 for i = 1, n do x = x + 1 end return x end
    local inner = coroutine.create(function(a)
        local b = coroutine.yield(a + spin())
        return b + spin()
    end)
    local outer = coroutine.create(function()
        local ok1, v1 = coroutine.resume(inner, 1)
        local mid = coroutine.yield(v1 + spin())
        local ok2, v2 = pcall(function()
            local ok, v = coroutine.resume(inner, mid)
            return v
        end)
        return tostring(ok1) .. " " .. v1 .. " " .. tostring(ok2) .. " " .. v2
            .. " " .. coroutine.status(inner)
    end)
    local _, first = coroutine.resume(outer)
    spin()
    local _, last = coroutine.resume(outer, 5)
    return first .. "|" .. last .. "|" .. coroutine.status(outer)
end
'''
# a coroutine with no end. resume answers the deadline's raise; the loop after it
# is what the next raise lands in, so the call ends with it
SPIN_RESUME = '''
function call()
    local co = coroutine.create(function() local x = 0 while true do x = x + 1 end end)
    local ok, e = coroutine.resume(co)
    while true do end
end
'''
# wrap raises the coroutine's error in its caller
SPIN_WRAP = '''
function call()
    coroutine.wrap(function() local x = 0 while true do x = x + 1 end end)()
    return "came back"
end
'''
# what a coroutine sees of its call: barch.running(), and require from the
# function's own space
HELPER = 'function twice(n) return n * 2 end\nfunction call() return "helper" end'
SEES = '''
function call(n)
    n = tonumber(n)
    local co = coroutine.create(function()
        local x = 0
        for i = 1, n do x = x + 1 end
        return barch.running() .. " " .. require("cohelper").twice(21)
    end)
    local ok, v = coroutine.resume(co)
    return barch.running() .. "|" .. tostring(ok) .. " " .. tostring(v)
end
'''
# a request from inside a coroutine parks the call, as one from the call's own
# thread does
FETCH = '''
function call(url)
    local co = coroutine.create(function()
        local res = http.request(url):timeout(600000):get()
        return res.body
    end)
    local ok, v = coroutine.resume(co)
    return tostring(ok) .. " " .. tostring(v) .. " " .. coroutine.status(co)
end
'''

BIG = 30000000

try:
    check(alive(), "the server answers")
    check(all(said(admin, "SETF", name, src) == "OK" for name, src in (
        ("f2b", F2B), ("cogen", GEN), ("conest", NESTED), ("cospin", SPIN_RESUME),
        ("cowrapspin", SPIN_WRAP), ("cohelper", HELPER), ("cosees", SEES),
        ("cofetch", FETCH))), "SETF of the calls")
    # native code ends a slice by the deadline watch rather than a count
    aot = (said(admin, "SETF", "f2baot", F2B, "AOT") == "OK"
           and said(admin, "SETF", "conestaot", NESTED, "AOT") == "OK")
    print("  AOT %s" % ("on" if aot else "not built in"), flush=True)
    # the correctness cases must not end by the clock
    said(admin, "CONFIG", "SET", "function_deadline_max_ms", "600000")
    said(admin, "CONFIG", "SET", "function_deadline_ms", "600000")
    said(admin, "CONFIG", "SET", "foreign_timeout_ms", "600000")

    c = connect()
    try:
        got = said(c, "CALLF", "f2b", "1000")
        check(got == "true done 1000 dead", "a small loop in a coroutine: %r" % (got,))

        slices = counter("memory", "luau_slices")
        got = said(c, "CALLF", "f2b", str(BIG))
        check(got == "true done %d dead" % BIG, "a loop of %d in a coroutine: %r" % (BIG, got))
        check(counter("memory", "luau_slices") > slices, "and the call still yields to the pool")

        slices = counter("memory", "luau_slices")
        got = said(c, "CALLF", "cogen", str(BIG // 3))
        n = BIG // 3
        want = "%d %d %d end" % (n + 1, n + 2, n + 3)
        check(got == want, "a coroutine.wrap generator: %r" % (got,))
        check(counter("memory", "luau_slices") > slices, "and it yields to the pool")

        slices = counter("memory", "luau_slices")
        n = BIG // 5
        got = said(c, "CALLF", "conest", str(n))
        want = "%d|true %d true %d dead|dead" % (2 * n + 1, n + 1, n + 5)
        check(got == want, "nested coroutines, one under a pcall: %r" % (got,))
        check(counter("memory", "luau_slices") > slices, "and they yield to the pool")

        if aot:
            slices = counter("memory", "luau_slices")
            got = said(c, "CALLF", "f2baot", str(BIG * 4))
            check(got == "true done %d dead" % (BIG * 4), "the same, native: %r" % (got,))
            check(counter("memory", "luau_slices") > slices, "and it yields to the pool")
            n = BIG // 5
            got = said(c, "CALLF", "conestaot", str(n * 4))
            want = "%d|true %d true %d dead|dead" % (8 * n + 1, 4 * n + 1, 4 * n + 5)
            check(got == want, "nested coroutines, native: %r" % (got,))

        slices = counter("memory", "luau_slices")
        got = said(c, "CALLF", "cosees", str(BIG // 3))
        check(got == "corosl|true corosl 42",
              "barch.running() and require inside a coroutine: %r" % (got,))
        check(counter("memory", "luau_slices") > slices, "and it yields to the pool")

        parks = counter("memory", "luau_parks")
        got = said(c, "CALLF", "cofetch", "http://127.0.0.1:%d/x" % WEB_PORT)
        check(got == "true fetched dead", "a request inside a coroutine: %r" % (got,))
        check(counter("memory", "luau_parks") > parks, "and the call parks on it")

        # the session's state is fine after all that
        got = said(c, "CALLF", "f2b", "1000")
        check(got == "true done 1000 dead", "the session's next call: %r" % (got,))
    finally:
        c.close()

    # a coroutine that spins is still stopped by the deadline, not by the wait
    said(admin, "CONFIG", "SET", "function_deadline_ms", "200")
    for what, fn in (("resume", "cospin"), ("wrap", "cowrapspin")):
        c = connect()
        try:
            timeouts = counter("errors", "function_timeouts")
            stopped = counter("errors", "function_stopped")
            got = said(c, "CALLF", fn)
            check(isinstance(got, str) and "FUNCTION timeout" in got,
                  "a spinning coroutine under %s ends at the deadline: %r" % (what, got))
            check(counter("errors", "function_timeouts") == timeouts + 1, "and counts as a timeout")
            check(counter("errors", "function_stopped") == stopped, "and was not stopped by the wait")
            got = said(c, "CALLF", "f2b", "1000")
            check(got == "true done 1000 dead", "and the session's next call runs: %r" % (got,))
        finally:
            c.close()
    said(admin, "CONFIG", "SET", "function_deadline_ms", "600000")
    check(alive(), "the server answers at the end")
finally:
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            scale.stop_hung(proc, signal.SIGTERM, 60, log=LOG)
    logfile.close()
    web.shutdown()

print()
if failures:
    print("FAILURES: %d" % len(failures))
    for f in failures:
        print("  " + f)
    print("barchd log tail:\n" + open(LOG, "rb").read().decode(errors="replace")[-3000:])
    raise SystemExit(1)
# a sanitizer finding inside barchd is its exit status
assert proc.returncode == 0, "barchd exited with %s:\n%s" % (
    proc.returncode, open(LOG, "rb").read().decode(errors="replace")[-3000:])
print("all luau coroutine slice checks pass")
