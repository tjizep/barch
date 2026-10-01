# A range sharded store isn't loaded hash routed - TODO 570.
#
# A shard file says how many shards its space had (TODO 314), and a load with a
# different count is refused. It didn't say how the space routed. So a range
# sharded space that came back with range_sharded off - or with ordered off,
# which drops range sharding with one log line - loaded as hash routed: about a
# quarter of its keys readable, DBSIZE still the full count, nothing said to the
# client. A write to a stranded name then landed on its hash shard beside the
# old copy, so turning range sharding back on didn't give one value either.
#
# Now the file carries the routing too, and that load is refused. Four cases:
#   1. range_sharded turned off: refused, and the refusal names the option
#   2. ordered turned off: refused, and the refusal names ordered
#   3. put back, the space loads with every key readable - the refusal touched
#      nothing
#   4. an empty range space turned hash isn't refused: no keys, no layout
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
PORT = scale.port(default=14981)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "rangerouting_data")
shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)

KEYS = scale.env_int("RANGEROUTING_KEYS", 20000, floor=2000)
SAMPLE = range(0, KEYS, max(1, KEYS // 1000))

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def start():
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            raise AssertionError("barchd exited with %s" % p.returncode)
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % PORT)


def stop(p):
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=30)


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=60)


def conf(r, **kv):
    for k, v in kv.items():
        r.execute_command("configuration:SET", k, v)
    r.execute_command("configuration:SAVE")


def key(i):
    return "k%07d" % i


def readable(r):
    return sum(1 for i in SAMPLE if r.get(key(i)) == b"x")


def open_space(r, space):
    """USE the space and read one key. The error if either is refused, else None"""
    try:
        r.execute_command("USE", space)
        r.get(key(0))
        return None
    except redis.exceptions.ResponseError as e:
        return str(e)


def fill(space):
    proc = start()
    try:
        r = client()
        conf(r, **{space + ".ordered": "1", space + ".shards": "4",
                   space + ".range_sharded": "1"})
        r.execute_command("USE", space)
        pipe = r.pipeline(transaction=False)
        for i in range(KEYS):
            pipe.set(key(i), "x")
        pipe.execute()
        time.sleep(3)           # let the rebalancer spread them
        r.execute_command("SAVE")
        return readable(r)
    finally:
        stop(proc)


def restart_with(space, option, value):
    """turn one option, restart, and say what opening the space answered"""
    proc = start()
    try:
        conf(client(), **{space + "." + option: value})
    finally:
        stop(proc)
    proc = start()
    try:
        return open_space(client(), space)
    finally:
        stop(proc)


print("TestRangeRouting")
for space, option, off, named in (("ra", "range_sharded", "0", "range_sharded"),
                                  ("rb", "ordered", "0", "ordered")):
    print(" %s off" % option)
    before = fill(space)
    check(before == len(SAMPLE), "range space readable before (%d of %d)" % (before, len(SAMPLE)))

    err = restart_with(space, option, off)
    check(err is not None, "turning %s off is refused" % option)
    check(err is not None and named in err and "range" in err,
          "and the refusal says to set %s (%s)" % (named, err))

    # put back: the refusal mustn't have changed a thing
    proc = start()
    try:
        r = client()
        conf(r, **{space + "." + option: "1"})
    finally:
        stop(proc)
    proc = start()
    try:
        r = client()
        err = open_space(r, space)
        got = readable(r) if err is None else 0
        check(err is None and got == len(SAMPLE),
              "put back, every key is readable (%d of %d) %s" % (got, len(SAMPLE), err or ""))
    finally:
        stop(proc)

print(" empty range space turned hash")
proc = start()
try:
    r = client()
    conf(r, **{"re.ordered": "1", "re.shards": "4", "re.range_sharded": "1"})
    r.execute_command("USE", "re")
    r.execute_command("SAVE")
finally:
    stop(proc)
err = restart_with("re", "range_sharded", "0")
check(err is None, "an empty one isn't refused %s" % (err or ""))

print("%s: %d failure(s)" % ("FAIL" if failures else "PASS", failures))
sys.exit(1 if failures else 0)
