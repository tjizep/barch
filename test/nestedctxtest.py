# A function that calls another with CALLF keeps its own deadline afterwards - TODO 576.
#
# The audit of the Luau bindings suspected the opposite. The interrupt hook reads the
# running call's budget and deadline from lua_callbacks(L)->userdata, finish_job sets
# that to null, and nothing puts the caller's back, so a CALLF looked like it would
# leave its caller with no deadline. It doesn't, and this test passed on the code as
# it was: a nested call is run by a sub caller of its own (runner_for in
# function_api.cpp), every rpc_caller builds its own Luau states, and so the inner job
# runs on a different lua_State with callbacks of its own. The caller's are never
# touched.
#
# Kept as a guard on that. Anything that made a nested call share its caller's state
# - pooling the sub callers' states, say - would put the suspected bug there for real.
#
# Checked, against barchd:
#   - control: a function with a 300 ms deadline and a 2 s loop times out
#   - the same function after a CALLF of another one times out too
#   - and after two nested calls, not just the first
import os
import shutil
import signal
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14562)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "nestedctx_data")
failures = 0


def check(ok, what):
    global failures
    print("  %-70s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


BUSY = "local x = 0 for i = 1, tonumber(n) do x = x + i end return x"

# the loop's size is set per call, so one calibration serves every function
CAL = '--[[@barch{"deadline_ms": 30000}]]\nfunction call(n) %s end' % BUSY
INNER = 'function call() return "inner" end'
# 300 ms is far under the loop, so a deadline that is enforced always stops it
SOLO = '--[[@barch{"deadline_ms": 300}]]\nfunction call(n) %s end' % BUSY
ONCE = ('--[[@barch{"deadline_ms": 300}]]\n'
        'function call(n) barch.call("CALLF", "inner") %s end' % BUSY)
TWICE = ('--[[@barch{"deadline_ms": 300}]]\n'
         'function call(n) barch.call("CALLF", "inner") barch.call("CALLF", "inner") %s end' % BUSY)


def times_out(r, *cmd):
    """(timed out, seconds it took)"""
    t = time.perf_counter()
    try:
        r.execute_command(*cmd)
        return False, time.perf_counter() - t
    except redis.exceptions.ResponseError as e:
        return "timeout" in str(e).lower(), time.perf_counter() - t


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
    r = redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=120,
                    decode_responses=True)
    for name, src in (("cal", CAL), ("inner", INNER), ("solo", SOLO), ("once", ONCE),
                      ("twice", TWICE)):
        assert r.execute_command("SETF", name, src) == "OK"

    # how many iterations make about 2 s here
    n = 1_000_000
    while True:
        t = time.perf_counter()
        r.execute_command("CALLF", "cal", n)
        dt = time.perf_counter() - t
        if dt > 0.1:
            break
        n *= 4
    LOOP = int(n * 2.0 / dt)
    print("nested call context: %d iterations for about 2 s" % LOOP, flush=True)

    print("a deadline with no nested call", flush=True)
    hit, took = times_out(r, "CALLF", "solo", LOOP)
    check(hit and took < 1.5, "it stops the loop (timed out %s after %.2f s)" % (hit, took))

    print("a deadline after a nested CALLF", flush=True)
    hit, took = times_out(r, "CALLF", "once", LOOP)
    check(hit and took < 1.5, "it still stops the loop (timed out %s after %.2f s)" % (hit, took))
    hit, took = times_out(r, "CALLF", "twice", LOOP)
    check(hit and took < 1.5, "and after two of them (timed out %s after %.2f s)" % (hit, took))
    check(p.poll() is None, "and the server is still up")
finally:
    p.send_signal(signal.SIGKILL)
    p.wait(timeout=30)

print("\n%s" % ("nested context checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
