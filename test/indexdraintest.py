# A FIND sees every write made before it, even one the maintenance thread is applying
# - TODO 643.
#
# Writes to an indexed space are queued, and both the space's maintenance thread and
# INDEX FIND apply the queue. FIND looked at the queue before taking the lock a drain
# holds, so when the maintenance thread had taken a batch off the queue and was still
# applying it, FIND found the queue empty and read the index without that batch.
# TestPermIndex caught it once in CI: "after writes and erases, 2 FINDs differ from
# brute force".
#
# BARCH_TEST_INDEX_APPLY_DELAY_MS makes the maintenance thread hold each batch 300ms
# before it applies it, so a FIND lands in that gap most of the times it's tried.
#
# What has to hold, 40 times each: a key written just before a FIND is found by it,
# and a key erased just before one isn't.
import os
import random
import time

os.environ["BARCH_TEST_INDEX_APPLY_DELAY_MS"] = "300"

import scale  # noqa: E402

import redis  # noqa: E402
import barch  # noqa: E402

scale.workdir()
PORT = scale.port(default=14520)

print("start index drain test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

barch.KeyValue("configuration").set("dr.key_split", "|")
failures = []


def check(ok, what):
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures.append(what)


r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, decode_responses=True)
r.execute_command("USE", "dr")
r.execute_command("FLUSHDB")
for i in range(50):
    r.set("a|b|seed%d|%d" % (i, i), "v")
check(r.execute_command("INDEX", "CREATE", "ix", "COMPOSITE", 3, "PINNED", 1) == "OK", "CREATE ix")
r.execute_command("INDEX", "BUILD", "ix", "ALL")

rnd = random.Random(643)
missed = 0
for i in range(40):
    key = "a|b|new%d|%d" % (i, 1000 + i)
    r.set(key, "v")
    time.sleep(rnd.uniform(0.0, 0.45))     # somewhere in the maintenance thread's 440ms
    if key not in r.execute_command("INDEX", "FIND", "ix", "2=new%d" % i):
        missed += 1
check(missed == 0, "a key written before a FIND is found by it (missed %d of 40)" % missed)

stale = 0
for i in range(40):
    key = "a|b|new%d|%d" % (i, 1000 + i)
    r.delete(key)
    time.sleep(rnd.uniform(0.0, 0.45))
    if key in r.execute_command("INDEX", "FIND", "ix", "2=new%d" % i):
        stale += 1
check(stale == 0, "a key erased before a FIND isn't (found %d of 40)" % stale)

r.execute_command("INDEX", "DROP", "ix")
barch.stop()
print("index drain test %s" % ("FAILED: %d" % len(failures) if failures else "passed"), flush=True)
if failures:
    raise SystemExit(1)
