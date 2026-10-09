#!/usr/bin/env python3
"""
A scan's worker count, while other scans and saves run - TODO 622.

logical_allocator::iterate_pages used to keep the count in a field of the
allocator, written by every scan with no lock and read by the scan's own workers
as they split the pages between them. A second scan that started with another
count changed the split under the first, so its workers skipped pages or visited
them twice. And a save, which writes that field into the shard file, read it while
a scan wrote it.

What has to hold, while one client flips iteration_worker_count between 1 and 8:
  - every KEYS answers every key, each once. This never failed before the fix, in
    535 scans: a scan is quick, and its workers may keep the count in a register,
    so it stays as a guard rather than a proof;
  - a SAVE running beside them doesn't race. Under TSan, barchd's exit code says,
    and before the fix it reported the race in every run;
  - barchd stops cleanly.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402

scale.workdir()
BASE = scale.port(default=26900)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

KEYS = scale.scaled(20000, floor=2000)
failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


node = clusternodes.Node(0, BASE, BINARY)
try:
    node.start()
    c = node.client()
    print("%d keys, scanned while the worker count changes" % KEYS)
    p = c.pipeline(transaction=False)
    for i in range(KEYS):
        p.execute_command("SET", "k%07d" % i, "v")
        if i % 1000 == 999:
            p.execute()
    p.execute()
    want = set("k%07d" % i for i in range(KEYS))

    stop = threading.Event()
    bad = []
    scans = [0]
    saves = [0]
    guard = threading.Lock()

    def flip():
        cl = node.client()
        n = 0
        while not stop.is_set():
            cl.execute_command("CONFIG", "SET", "iteration_worker_count", "1" if n % 2 else "8")
            n += 1

    def scan():
        cl = node.client()
        while not stop.is_set():
            got = cl.execute_command("KEYS", "k*")
            with guard:
                scans[0] += 1
                if len(got) != len(want) or set(got) != want:
                    bad.append((len(got), len(set(got))))

    def save():
        cl = node.client()
        while not stop.is_set():
            try:
                cl.execute_command("SAVE")
                with guard:
                    saves[0] += 1
            except redis.ResponseError:
                time.sleep(0.01)

    threads = [threading.Thread(target=f) for f in (flip, scan, scan, scan, save)]
    for t in threads:
        t.start()
    time.sleep(scale.scaled_seconds(4.0, floor=1.5))
    stop.set()
    for t in threads:
        t.join()
    print("    %d scans, %d saves" % (scans[0], saves[0]))
    check(scans[0] > 10, "the scans ran")
    check(not bad, "every scan answered every key once (%d short or doubled: %s)" % (len(bad), bad[:3]))
finally:
    node.stop()
    # node.stop records a bad exit; clusternodes fails the test at exit for it
check(not clusternodes.bad_exits, "barchd stopped cleanly %s" % clusternodes.bad_exits)

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
