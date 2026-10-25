#!/usr/bin/env python3
"""
Spaces share a maintenance pool rather than starting a thread each - TODO 748.

Every space used to start its own maintenance thread, waking every
maintenance_poll_delay. TestTxUse makes 800 spaces, and on a 4 vCPU runner under
coverage those 800 threads left the request threads so little CPU that its
transactions crawled until the test's 300 s ran out. Now every space's pass runs
on one small pool.

Checked against one barchd:
  - making 200 spaces doesn't add a thread for each (Linux only: it reads
    /proc/<pid>/status);
  - each space still gets its passes: a write in the first and in the last of
    them is saved by the interval save, which only maintenance does, while a
    space nobody wrote to saves nothing;
  - barchd still stops cleanly, and promptly, with all of them open.
"""
import glob
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
PORT = scale.port(default=14748)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACES = 200
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


DATA = os.path.join(os.getcwd(), "pool_data")
shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)
log = open(os.path.join(os.getcwd(), "pool.log"), "w")
# a short save interval, so a write is saved by the next pass of its space's maintenance
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=300"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

TAG = "mp%d" % (int(time.time()) % 100000)


def threads():
    """barchd's thread count, or None where /proc doesn't say"""
    try:
        with open("/proc/%d/status" % p.pid) as f:
            for line in f:
                if line.startswith("Threads:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return None


def saved(space):
    return glob.glob(os.path.join(DATA, "leaves_%s_*.dat" % space))


def wait_for(seconds, f):
    until = time.time() + seconds
    while time.time() < until:
        if f():
            return True
        time.sleep(0.1)
    return f()


try:
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)
    print("%d new spaces" % SPACES)
    # the default space's maintenance has started the pool by now
    before = threads()
    names = ["%s_%d" % (TAG, i) for i in range(SPACES)]
    for n in names:
        c.execute_command("USE", n)
    after = threads()
    if before is None or after is None:
        print("  (no /proc here: thread count not checked)")
    else:
        # a thread each would be 200 more; the pool's are already running
        check(after - before < 16, "they don't start a thread each (%d -> %d)" % (before, after))

    first, last, untouched = names[0], names[-1], names[1]
    for n in (first, last):
        c.execute_command("USE", n)
        c.execute_command("SET", "k", "v")
    check(wait_for(60, lambda: saved(first) and saved(last)),
          "a write in the first and the last is saved by their maintenance")
    check(not saved(untouched), "and a space nobody wrote to saves nothing")
finally:
    t = time.time()
    p.send_signal(signal.SIGTERM)
    try:
        code = p.wait(60)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()
        code = "killed after 60 s"
    took = time.time() - t

check(code == 0 and took < 30, "barchd stops cleanly with them all open (%s, %.1f s)" % (code, took))

if failures:
    log.flush()
    with open(os.path.join(os.getcwd(), "pool.log"), errors="replace") as f:
        print("the end of barchd's log:\n" + "".join(f.readlines()[-40:]), flush=True)
print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
