#!/usr/bin/env python3
"""
A save survives defrag shrinking the arena while it writes - TODO 644.

A save freezes a shard: its arenas go into copy-on-write, a view borrows their page
memory, and the save writes the files from the view with the latch let go. Defrag,
on the maintenance thread, could take the latch in that time and pop trailing free
pages off the arena, remapping its page memory. On Linux that remap stays in place,
so only the popped page goes, which the save never reads. On Windows the remap always
moves and frees the whole old range, and the save then read freed memory: barchd died
in TestLoadResult in Windows CI, and its crash report (TODO 646) put the access
violation in append(), from shard::write_frozen.

Two knobs make it happen here: BARCH_TEST_REMAP_MOVES makes every arena remap move,
as Windows' does, and BARCH_TEST_SAVE_WRITE_DELAY_MS holds a save between taking its
view and writing, so the maintenance thread (polling every 50ms) gets its turn.

What has to hold, five rounds of fill, delete most of it, SAVE:
  - every SAVE answers OK, and barchd is still up;
  - its log has no crash report;
  - pages did get popped (vmm_pages_popped), so defrag really ran.
"""
import os
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14560)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY) and not os.path.exists(BINARY + ".exe"):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "savedefrag_data")
failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


os.makedirs(DATA, exist_ok=True)
log_path = os.path.join(DATA, "barchd.log")
env = dict(os.environ)
env["BARCH_TEST_REMAP_MOVES"] = "1"
env["BARCH_TEST_SAVE_WRITE_DELAY_MS"] = "1500"
with open(log_path, "wb") as log:
    proc = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA,
                             "-c", "maintenance_poll_delay=50"],
                            stdout=log, stderr=subprocess.STDOUT, env=env)
try:
    r = None
    end = time.time() + 30
    while time.time() < end:
        try:
            r = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60)
            r.ping()
            break
        except redis.RedisError:
            r = None
            time.sleep(0.2)
    check(r is not None, "barchd answers")

    def popped():
        def flat(x):
            for v in x:
                if isinstance(v, (list, tuple)):
                    yield from flat(v)
                else:
                    yield v
        vals = list(flat(r.execute_command("STATS")))
        for i, v in enumerate(vals[:-1]):
            if v in (b"vmm_pages_popped", "vmm_pages_popped"):
                return int(vals[i + 1])
        return -1

    before = popped()
    value = b"v" * 4000
    saved = 0
    for rnd in range(5):
        p = r.pipeline(transaction=False)
        for i in range(6000):
            p.set(b"k%d-%05d" % (rnd, i), value)
        p.execute()
        p = r.pipeline(transaction=False)
        for i in range(600, 6000):
            p.delete(b"k%d-%05d" % (rnd, i))
        p.execute()
        try:
            if r.execute_command("SAVE") in (b"OK", "OK", True):
                saved += 1
        except redis.RedisError as e:
            print("  SAVE: %s" % e, flush=True)
            break
    alive = proc.poll() is None
    check(saved == 5 and alive, "every SAVE answers OK and barchd is still up (%d of 5)" % saved)
    with open(log_path, "rb") as f:
        crashed = b"barchd crash" in f.read()
    check(not crashed, "and its log has no crash report")
    if alive:
        check(popped() > before, "pages were popped, so defrag ran (%d then %d)" % (before, popped()))
finally:
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(30)
        except subprocess.TimeoutExpired:
            proc.kill()

if failures:
    with open(log_path, "rb") as f:
        print(f.read().decode("utf-8", "replace")[-3000:])
print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
