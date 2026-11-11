# A CALLF whose wait runs out - TODO 874.
#
# A call that yields goes on the foreign pool, and its connection waits for it.
# The wait is the space's foreign_timeout_ms. When it ran out the session
# answered nil and read the next command, but the job went on running. It owns
# the session's Luau state, so the next CALLF on that connection ran on the same
# lua_State as the job, from another thread. TSan reported about 50 races and
# then the server died on signal 4.
#
# A session runs one call at a time. So now the wait running out tells the job
# to stop, and the session waits until it has. The client gets "FUNCTION
# timeout", not nil, and nothing else runs on the state before the job is done.
#
# No clocks here. Each call can only end by being told to stop: a loop with no
# end, under a deadline far past the test, or a request the test's own server
# holds until the stop has been asked for. Checked: the call answers the
# timeout, the session's next calls run, and the server is still up.
#
# EXEC had the same hole without any timeout: a CALLF that parked inside it left
# its job running, and the next queued CALLF ran on the same state.
import concurrent.futures
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
PORT = scale.port(default=14994)
WEB_PORT = scale.port(1, default=14995)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "callftimeout_data")
os.makedirs(DATA, exist_ok=True)
for f in os.listdir(DATA):
    if f.endswith(".dat"):
        os.remove(os.path.join(DATA, f))

# the latch: a request to /hold waits here until the test lets it go
arrived = threading.Event()
release = threading.Event()


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        arrived.set()
        # bounded only so a broken run can't keep the thread for good
        release.wait(300)
        raw = b"held"
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

print("start callf timeout test with %s" % BINARY, flush=True)
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
                    socket_timeout=300, retry=Retry(NoBackoff(), 0))
    c.execute_command("USE", "cftime")
    return c


def said(c, *args):
    try:
        return c.execute_command(*args)
    except redis.ResponseError as e:
        return "ERR " + str(e)
    except (redis.ConnectionError, redis.TimeoutError) as e:
        return "LOST " + type(e).__name__


admin = connect()


def counter(name):
    return int(admin.info("errors").get(name, 0))


def alive():
    return proc.poll() is None and said(admin, "PING") in (b"PONG", True)


def timed_out(got):
    return isinstance(got, str) and "FUNCTION timeout" in got


SPIN = 'function call() local x = 0 while true do x = x + 1 end end'
# the stop raises inside the pcall, which catches it. The next interrupt is
# outside it, and raises again
CAUGHT = ('function call() local x = 0 while true do '
          'pcall(function() while true do x = x + 1 end end) end end')
HELD = ('function call(url) local res = http.request(url):timeout(600000):get() '
        'return res.body end')
# the user's f1b: enough work to yield, so it runs on the pool too
BUSY = ('function call() local t = {} for i = 1, 200000 do t[i] = tostring(i) end '
        'return #t end')
QUICK = 'function call() return "fine" end'

try:
    check(alive(), "the server answers")
    check(said(admin, "SETF", "spin", SPIN) == b"OK"
          and said(admin, "SETF", "caught", CAUGHT) == b"OK"
          and said(admin, "SETF", "held", HELD) == b"OK"
          and said(admin, "SETF", "busy", BUSY) == b"OK"
          and said(admin, "SETF", "quick", QUICK) == b"OK",
          "SETF of the calls")
    aot = said(admin, "SETF", "spinaot", SPIN, "AOT") == b"OK"
    print("  AOT %s" % ("on" if aot else "not built in"), flush=True)
    # nothing ends by its deadline here: only the wait running out ends a call
    said(admin, "CONFIG", "SET", "function_deadline_ms", "600000")
    said(admin, "CONFIG", "SET", "function_deadline_max_ms", "600000")
    said(admin, "CONFIG", "SET", "foreign_timeout_ms", "200")

    cases = [("a loop with no end", "spin"), ("a loop that catches the stop", "caught")]
    if aot:
        cases.append(("a native loop with no end", "spinaot"))
    for what, fn in cases:
        c = connect()
        try:
            stopped = counter("function_stopped")
            timeouts = counter("function_timeouts")
            got = said(c, "CALLF", fn)
            check(timed_out(got), "%s answers the timeout, not nil: %r" % (what, got))
            if not alive():
                check(False, "the server died (exit %s)" % proc.poll())
                break
            check(counter("function_stopped") == stopped + 1, "and the job was told to stop")
            check(counter("function_timeouts") == timeouts + 1, "and counts as a timeout")
            # on the state the job had: it has to be done with it
            got = said(c, "CALLF", "busy")
            check(got == 200000, "the session's next call runs: %r" % (got,))
            got = said(c, "CALLF", "quick")
            check(got == b"fine", "and the one after: %r" % (got,))
        finally:
            c.close()

    # EXEC runs its queue in one go. A call that parked there left its job on the
    # pool, and the next queued CALLF ran on the same state with it: two of them
    # killed the server. Now a CALLF in EXEC runs to its end where it is
    if proc.poll() is None:
        c = connect()
        try:
            p = c.pipeline(transaction=True)
            p.execute_command("CALLF", "busy")
            p.execute_command("CALLF", "busy")
            p.execute_command("CALLF", "quick")
            try:
                got = p.execute()
            except (redis.ConnectionError, redis.TimeoutError) as e:
                got = "LOST " + type(e).__name__
            check(got == [200000, 200000, b"fine"], "CALLFs in one EXEC each answer: %r" % (got,))
            check(alive(), "and the server is up (exit %s)" % proc.poll())
            if proc.poll() is None:
                got = said(c, "CALLF", "busy")
                check(got == 200000, "and the session's next call runs: %r" % (got,))
        finally:
            c.close()

    # parked on the latch: the wait runs out while the job waits on a request.
    # It can't stop until the request comes back, and the session has to wait
    # that long. Before, it answered nil right away
    if proc.poll() is None:
        c = connect()
        pool = concurrent.futures.ThreadPoolExecutor(1)
        try:
            stopped = counter("function_stopped")
            url = "http://127.0.0.1:%d/hold" % WEB_PORT
            reply = pool.submit(said, c, "CALLF", "held", url)
            check(arrived.wait(300), "the held call's request reaches the test server")
            # either the stop is asked for, or the call answers while the job is
            # still parked on the request, which is the bug
            while not reply.done() and counter("function_stopped") == stopped:
                concurrent.futures.wait([reply], timeout=0.05)
            check(not reply.done(), "the held call has no answer while its job waits: %r"
                  % (reply.result() if reply.done() else None,))
            release.set()
            got = reply.result(300)
            check(timed_out(got), "once let go it answers the timeout: %r" % (got,))
            got = said(c, "CALLF", "busy")
            check(got == 200000, "the session's next call runs: %r" % (got,))
        finally:
            release.set()
            pool.shutdown()
            c.close()
    check(alive(), "the server answers at the end")
finally:
    release.set()
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
print("all callf timeout checks pass")
