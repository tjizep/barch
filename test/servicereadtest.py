# A service() table that throws when it is read - TODO 871.
#
# SETF runs the chunk, calls service() under a pcall, and then reads the table it
# returned. The reads used lua_getfield, which honours __index, and ran outside
# any pcall. So an __index that raises threw a lua_exception out of
# compile_function. Unwinding closed the scratch state SETF compiles in, and the
# catch in rpc_caller called what(), which reads the message off that closed
# state. The server died on signal 4. One SETF from any client with the right
# to store a function was enough.
#
# The same holds for the globals compile_into reads after the chunk runs, when
# the chunk put a throwing metatable on its own environment, and for the reads
# http_vm_load makes when HTTP START loads a route. A Luau fill was the same:
# start_resolve looked up call() with lua_getglobal outside any pcall, and the
# job it unwound through closed the fill's state.
#
# A call()'s return value is read the same way - TODO 872, 873. The raise
# skipped the end of the call. The job was freed while the state still pointed
# into it, so the session's next call read freed memory, and on the pool
# nothing answered.
#
# Checked here: each of those answers an error, and the server still answers
# PING, a later SETF and a CALLF.
import http.client
import os
import signal
import subprocess
import sys

import scale
import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

scale.workdir()
PORT = scale.port(default=14992)
HTTP_PORT = scale.port(1, default=14993)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "serviceread_data")
os.makedirs(DATA, exist_ok=True)
for f in os.listdir(DATA):
    if f.endswith(".dat"):
        os.remove(os.path.join(DATA, f))

print("start service read test with %s" % BINARY, flush=True)
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


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=30)
    c.execute_command("USE", "svcread")
    return c


def answer(*args):
    """What the server said, an error as its text, or None if it went away."""
    try:
        return client().execute_command(*args)
    except redis.ResponseError as e:
        return "ERR " + str(e)
    except (redis.ConnectionError, redis.TimeoutError):
        return None


def alive():
    return proc.poll() is None and answer("PING") in (b"PONG", True)


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


# a table whose raw fields are `raw` and whose every other field raises
def throwing(raw):
    return ('setmetatable({%s}, {__index = function() error("svc boom") end})' % raw)


CASES = [
    # the report: the first read, of kind, raises
    ("every field raises",
     'function service() return %s end\nfunction call() return "x" end' % throwing("")),
    # each reader past its kind check
    ("resp methods raises",
     'function service() return %s end\nfunction call() return "x" end'
     % throwing('kind = "resp"')),
    ("cron space raises",
     'function service() return %s end\nfunction call() return "x" end'
     % throwing('kind = "cron"')),
    ("queue name raises",
     'function service() return %s end\nfunction call() return "x" end'
     % throwing('kind = "queue"')),
    # compile_into reads arity off the chunk's own globals. `e` is a local
    # because a lookup of `error` would go through the same __index
    ("the chunk's globals raise",
     'local e = error\n'
     'setmetatable(getfenv(), {__index = function() e("svc boom") end})\n'
     'function call() return "x" end'),
]

try:
    check(alive(), "the server answers before anything is stored")
    for i, (what, source) in enumerate(CASES):
        got = answer("SETF", "bad%d" % i, source)
        if not alive():
            check(False, "SETF %s: the server died (exit %s)" % (what, proc.poll()))
            break
        check(isinstance(got, str) and "svc boom" in got,
              "SETF %s answers the script's error: %r" % (what, got))
        check(answer("GETF", "bad%d" % i) is None, "and stores nothing")

    if proc.poll() is None:
        check(answer("SETF", "good", 'function call(a) return "ok " .. a end') == b"OK",
              "a later SETF is stored")
        check(answer("CALLF", "good", "x") == b"ok x", "and CALLF runs it")

    # HTTP START reads a resource route's table in http_vm_load. kind and route
    # are raw, so SETF reads no further and stores it; the load reads on
    if proc.poll() is None:
        check(answer("SETF", "HB", '''
function call() return "r" end
function hit(req, res) res.body = "b" res.code = 200 end
function service()
    return setmetatable({kind = "resource", route = "/hb", methods = {GET = hit}},
        {__index = function(_, k) if k == "keys" then error("svc boom") end return nil end})
end
''') == b"OK", "SETF of a route whose keys field raises is stored")
        check(answer("SETF", "HOK", '''
function call() return "r" end
function hit(req, res) res.body = "ok" res.code = 200 end
function service()
    return {kind = "resource", route = "/hok", methods = {GET = hit}, send = "text/plain"}
end
''') == b"OK", "and an ordinary route beside it")
        check(answer("SETF", "CONF", '''
function call() return "conf" end
function service() return {kind = "http", bind = "127.0.0.1", keys = {"HB", "HOK"}} end
''') == b"OK", "and the config that names both")
        started = answer("HTTP", "START", "CONF", str(HTTP_PORT), "127.0.0.1")
        print("  HTTP START answered %r" % (started,))
        check(alive(), "HTTP START with the raising route leaves the server up (exit %s)"
              % proc.poll())
        if proc.poll() is None:
            # a route that fails to load fails the whole start, with its error
            check(isinstance(started, str) and "svc boom" in started,
                  "HTTP START answers the route's error")
            check(answer("SETF", "CONF2", '''
function call() return "conf" end
function service() return {kind = "http", bind = "127.0.0.1", keys = {"HOK"}} end
''') == b"OK", "a config without the raising route is stored")
            # START answers the routes it serves, or an error
            started = answer("HTTP", "START", "CONF2", str(HTTP_PORT), "127.0.0.1")
            check(isinstance(started, list), "and HTTP START with it runs: %r" % (started,))
            check(get("/hok") == (200, b"ok"), "and serves its route: %r" % (get("/hok"),))
            answer("HTTP", "STOP")
            check(answer("CALLF", "good", "y") == b"ok y", "CALLF still runs after HTTP STOP")

    # a Luau fill looks up call() in a state of its own, after the chunk has run.
    # The chunk defines call() where SETF and the compile check run it, and puts a
    # throwing metatable on _G where only the fill does. The check has no base
    # library, and SETF has `barch`; the fill has the one and not the other
    if proc.poll() is None:
        c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=30)
        try:
            c.execute_command("configuration:SET", "svcfill.foreign_script", "filler")
            c.execute_command("configuration:SET", "svcfill.foreign", "luau")
            c.execute_command("USE", "svcfill")
            check(c.execute_command("SETF", "filler", '''
if setmetatable and not barch then
    local e = error
    setmetatable(_G, {__index = function() e("fill boom") end})
else
    function call() return "x" end
end
''') == b"OK", "SETF of a fill script whose globals raise in the fill is stored")
            try:
                got = c.execute_command("GET", "k")
            except redis.ResponseError as e:
                got = "ERR " + str(e)
            except (redis.ConnectionError, redis.TimeoutError):
                got = None
            print("  GET answered %r" % (got,))
        finally:
            c.close()
        if not alive():
            check(False, "a fill that raises finding call(): the server died (exit %s)"
                  % proc.poll())
        else:
            # a raw lookup now, so __index never runs and call() is simply missing
            check(isinstance(got, str) and "call missing" in got,
                  "a fill whose globals raise answers that call() is missing")
            check(answer("CALLF", "good", "z") == b"ok z", "and CALLF still runs")

    # what call() returns is read for {err=} and {ok=} after the coroutine is done
    # - TODO 872, 873. The read honoured __index outside any pcall, and the raise
    # left pump_call past finish_job. The job was freed with the state's interrupt
    # userdata still pointing into it, so the session's next compile ran the
    # interrupt on freed memory: "instruction budget exceeded" on release, a
    # heap-use-after-free under ASan. On the pool nothing answered at all: the
    # worker logged the raise and dropped it. Each case has a connection of its
    # own, as the session state is its own
    if proc.poll() is None:
        def connect():
            # no retry: redis-py's own sends a timed out command again on a new
            # connection, which has no USE, so a hang came back as "no such function"
            c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2,
                            socket_timeout=30, retry=Retry(NoBackoff(), 0))
            c.execute_command("USE", "svcread")
            return c

        def said(c, *args):
            try:
                return c.execute_command(*args)
            except redis.ResponseError as e:
                return "ERR " + str(e)
            except (redis.ConnectionError, redis.TimeoutError) as e:
                return "LOST " + type(e).__name__

        def counter(section, name):
            return int(client().info(section).get(name, 0))

        RAISES = 'return setmetatable({}, {__index = function() error("boom") end})'
        # far past the inline slice and more than one default slice, so the call
        # yields and the raise happens on a pool thread
        LOOP = 'local n = 0 for i = 1, 3000000 do n = n + i end'
        check(answer("SETF", "f6", 'function call() %s end' % RAISES) == b"OK"
              and answer("SETF", "f6p", 'function call() %s %s end' % (LOOP, RAISES))
              == b"OK"
              and answer("SETF", "f6ok", 'function call() return "fine" end') == b"OK",
              "SETF of calls whose returned table raises when read")
        # the loop's run is measured against the deadline; keep that out of it
        deadline = answer("CONFIG", "GET", "function_deadline_ms")
        deadline = (deadline[b"function_deadline_ms"] if isinstance(deadline, dict)
                    else deadline[1])
        answer("CONFIG", "SET", "function_deadline_ms", "600000")
        for what, fn in (("an inline", "f6"), ("a pooled", "f6p")):
            c = connect()
            try:
                errors = counter("errors", "function_errors")
                slices = counter("memory", "luau_slices")
                got = said(c, "CALLF", fn)
                check(isinstance(got, str) and "boom" in got,
                      "%s call answers the read's error: %r" % (what, got))
                if fn == "f6p":
                    check(counter("memory", "luau_slices") > slices, "and it went to the pool")
                check(counter("errors", "function_errors") == errors + 1, "and counts it")
                # f6ok is compiled into this session's state now, which runs the
                # interrupt - on the freed job, before
                got = said(c, "CALLF", "f6ok")
                check(got == b"fine", "and the session's next call runs: %r" % (got,))
            finally:
                c.close()
        answer("CONFIG", "SET", "function_deadline_ms", deadline)
        check(alive(), "the server answers after the raising returns")
finally:
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            scale.stop_hung(proc, signal.SIGTERM, 60, log=LOG)
    logfile.close()

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
print("all service read checks pass")
