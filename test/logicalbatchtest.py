# logical_allocated is batched per thread - TODO 868.
#
# Every allocation and free did a locked add on one global atomic. Under a fill
# that line bounced between cores on every key. Now each thread keeps what it
# added and freed in a signed batch and moves it to the global once the batch
# passes a cap, either way.
#
# Batching only the adds is wrong: a key set on one connection's thread and
# deleted on another's takes its bytes off the global before they were put on.
# The unsigned global wraps to near 2^64, every insert reads that as over
# max_memory, and writes are refused.
#
# Checked, with connections on different io threads:
#   - set on one connection, delete on another, many times: STATS stays sane
#     and a write under max_memory goes through
#   - after it all, the count is back near where it started, so nothing drifts
import os
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14868)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "logicalbatch_data")
ROUNDS = scale.scaled(200, floor=40)
KEYS = 100
# far above what the test holds, so any refusal is the counter, not the data
MAX_MEMORY = 256 << 20
# the global may lag the truth by a cap per thread that allocates; this is many caps
DRIFT = 4 << 20


def stat(r, name):
    s = r.execute_command("STATS")
    for i in range(0, len(s) - 1, 2):
        k = s[i].decode() if isinstance(s[i], bytes) else str(s[i])
        if k.lstrip("$") == name:
            return int(s[i + 1])
    raise AssertionError("no such stat: " + name)


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=60)
    c.ping()            # opens the connection now, so each client gets its own thread
    return c


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


print("start logical batch test", flush=True)
os.makedirs(DATA, exist_ok=True)
# a file, not a pipe: an unread pipe fills and stalls barchd
log = open(os.path.join(os.getcwd(), "logicalbatch.log"), "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA,
                      "--no-save-on-exit", "-c", "max_memory_bytes=%d" % MAX_MEMORY,
                      "-c", "eviction_policy=none"],
                     stdout=log, stderr=subprocess.STDOUT)
try:
    scale.wait_for_port(PORT, proc=p, what="barchd")
    r = client()
    r.flushall()
    start = stat(r, "logical_allocated")
    print("  logical_allocated at start: %d" % start, flush=True)

    worst = lowest = start
    refused = None
    for n in range(ROUNDS):
        setter, deleter = client(), client()
        keys = ["k:%d:%d" % (n, i) for i in range(KEYS)]
        setter.mset({k: "v" * 40 for k in keys})
        deleter.delete(*keys)
        now = stat(deleter, "logical_allocated")
        worst, lowest = max(worst, now), min(lowest, now)
        try:
            setter.set("probe", "x")
        except redis.exceptions.ResponseError as e:
            refused = str(e)
            break
        setter.close()
        deleter.close()

    check(refused is None, "writes under max_memory go through (%s)" % refused)
    # STATS shows the counter signed, so a wrapped one shows up below zero
    check(lowest >= 0, "logical_allocated never goes below zero (lowest %d)" % lowest)
    check(worst < start + DRIFT,
          "logical_allocated stays near the truth (worst %d)" % worst)
    end = stat(r, "logical_allocated")
    check(abs(end - start) < DRIFT, "and comes back near the start (%d)" % end)
finally:
    p.terminate()
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        scale.stop_hung(p, 15, 60)
    log.close()

if failures:
    print("logical batch test: %d failed" % failures)
    sys.exit(1)
print("logical batch test passed")
