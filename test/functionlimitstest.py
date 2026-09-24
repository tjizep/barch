# A stored function's own deadline and slice, and a deadline that counts running time -
# TODO 434.
#
#   --@barch {"deadline_ms": 5000, "slice_insns": 200000}
#
# in a function's header lowers the space's values freely and raises them up to
# function_deadline_max_ms / function_slice_max_insns. The deadline counts running
# time: waiting for a worker between slices (and parked, and sql.query) doesn't eat it.
# function_wall_factor times the deadline is the wall clock ceiling that still ends a
# call that keeps waiting.
import threading
import time

import scale

import redis
import barch

scale.workdir()
PORT = scale.port(default=14520)

print("start function limits test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

conf = barch.KeyValue("configuration")
conf.set("fl.function_deadline_ms", "50")          # before the space opens
conf.set("flcap.function_deadline_max_ms", "120000")

failures = []


def check(ok, what):
    if not ok:
        print("  FAIL " + what, flush=True)
        failures.append(what)


def conn(space):
    c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, decode_responses=True)
    c.execute_command("USE", space)
    return c


def refused(c, want, *cmd):
    try:
        got = c.execute_command(*cmd)
    except redis.ResponseError as e:
        check(want in str(e), "%r should say %r, said %r" % (cmd[:2], want, str(e)))
        return
    check(False, "%r should be refused, got %r" % (cmd[:2], got))


def times_out(c, *cmd):
    try:
        c.execute_command(*cmd)
        return False
    except redis.ResponseError as e:
        return "timeout" in str(e).lower()


BUSY = "local x = 0 for i = 1, n do x = x + i end return x"
r = conn("fl")
check(r.execute_command("KSPACE", "OPTION", "GET", "FUNCTION_DEADLINE") in (50, "50"), "the space took 50ms")

# calibrate: how many iterations make about 150ms here
assert r.execute_command("SETF", "CAL", '--@barch {"deadline_ms": 20000}\nfunction call(n) %s end' % BUSY.replace("n do", "tonumber(n) do")) == "OK"
n = 1_000_000
while True:
    t = time.perf_counter()
    r.execute_command("fl.CAL", n)
    dt = time.perf_counter() - t
    if dt > 0.05:
        break
    n *= 4
N150 = int(n * 0.15 / dt)
print("function limits: %d iterations for about 150ms" % N150, flush=True)

def busy_fn(header):
    return (header + "\n" if header else "") + "function call(n) %s end" % BUSY.replace("n do", "tonumber(n) do")

# --- the header raises within the cap, lowers always ---------------------------------
assert r.execute_command("SETF", "PLAIN", busy_fn("")) == "OK"
check(times_out(r, "fl.PLAIN", N150), "no header: the space's 50ms deadline stops a 150ms call")
assert r.execute_command("SETF", "RAISED", busy_fn('--@barch {"deadline_ms": 2000}')) == "OK"
check(not times_out(r, "fl.RAISED", N150), "a header raising the deadline to 2000ms lets it finish")
assert r.execute_command("SETF", "BLOCK", busy_fn('--[[@barch\n  {"deadline_ms": 2000}\n]]')) == "OK"
check(not times_out(r, "fl.BLOCK", N150), "the block comment form works too")
assert r.execute_command("SETF", "LATE", "function call(n) %s end\n--@barch {\"deadline_ms\": 2000}" % BUSY.replace("n do", "tonumber(n) do")) == "OK"
check(times_out(r, "fl.LATE", N150), "a --@barch after code isn't the header, and doesn't count")

d = conn("fldef")                                          # default 1000ms
assert d.execute_command("SETF", "LOWERED", busy_fn('--@barch {"deadline_ms": 20}')) == "OK"
check(times_out(d, "fldef.LOWERED", N150), "a header lowering the deadline to 20ms stops a 150ms call")
assert d.execute_command("SETF", "PLAIN", busy_fn("")) == "OK"
check(not times_out(d, "fldef.PLAIN", N150), "and without it the default 1000ms is enough")

# --- the caps --------------------------------------------------------------------------
refused(r, "function_deadline_max_ms of 30000", "SETF", "HUGE", busy_fn('--@barch {"deadline_ms": 60000}'))
c = conn("flcap")
check(c.execute_command("SETF", "HUGE", busy_fn('--@barch {"deadline_ms": 60000}')) == "OK",
      "a space with a higher cap takes it")
refused(r, "function_slice_max_insns of 10000000", "SETF", "WIDE", busy_fn('--@barch {"slice_insns": 999999999}'))
assert r.execute_command("SETF", "NARROW", busy_fn('--@barch {"deadline_ms": 2000, "slice_insns": 1000}')) == "OK"
check(not times_out(r, "fl.NARROW", N150), "a small slice: many yields, and it still finishes")
refused(r, "isn't a JSON object", "SETF", "BAD", busy_fn('--@barch {deadline_ms: 5}'))
refused(r, "whole number above 0", "SETF", "NEG", busy_fn('--@barch {"deadline_ms": -5}'))
check(r.execute_command("SETF", "EXTRA", busy_fn('--@barch {"deadline_ms": 2000, "owner": "me"}')) == "OK",
      "fields it doesn't know are ignored")

# --- queue time isn't running time ----------------------------------------------------
# Many calls at once, each with a small slice so it keeps going back on the pool and
# waiting for a worker. Each runs about 50ms against a 300ms deadline. Waiting for the
# other calls used to count, and they timed out; now only running counts.
q = conn("flq")
assert q.execute_command("SETF", "Q", busy_fn('--@barch {"deadline_ms": 300, "slice_insns": 20000}')) == "OK"
N50 = N150 // 3


def crowd(k):
    outs = []

    def one():
        cc = conn("flq")
        outs.append(times_out(cc, "flq.Q", N50))
    ts = [threading.Thread(target=one) for _ in range(k)]
    t = time.perf_counter()
    for x in ts:
        x.start()
    for x in ts:
        x.join()
    return sum(outs), time.perf_counter() - t


# The wall ceiling is out of the way for this one (TODO 443). Slices are shared
# fairly, so the whole crowd finishes together at about the total time, and at
# the default factor of 10 the ceiling is 3s: a CI machine that needed about 3s
# for the lot timed out all 40 at once. That's the ceiling doing its job, which
# the next check covers, not queue time being charged.
r.execute_command("CONFIG", "SET", "function_wall_factor", "1000")
timed, took = crowd(40)
r.execute_command("CONFIG", "SET", "function_wall_factor", "10")
print("function limits: 40 calls of ~50ms at once took %.2fs, %d timed out" % (took, timed), flush=True)
check(timed == 0, "calls waiting for a worker aren't charged for it: %d of 40 timed out" % timed)
# and the wall ceiling still ends a call that waits too long: at a factor of 1 the
# ceiling is the deadline itself, so the same crowd times some out again
r.execute_command("CONFIG", "SET", "function_wall_factor", "1")
timed1, took1 = crowd(40)
r.execute_command("CONFIG", "SET", "function_wall_factor", "10")
print("function limits: with a wall factor of 1, %d of 40 timed out (%.2fs)" % (timed1, took1), flush=True)
if took > 0.6:
    check(timed1 > 0, "a wall factor of 1 makes the ceiling the deadline, and the crowd times out")

# --- the header is the record: it lasts --------------------------------------------------
d.execute_command("SAVE")
d.execute_command("UNLOAD", "fldef")
d = conn("fldef")
check(times_out(d, "fldef.LOWERED", N150), "after the space is opened again the header still applies")

if failures:
    print("function limits test FAILED: %d" % len(failures), flush=True)
    raise SystemExit(1)
print("function limits test ok", flush=True)
barch.stop()
