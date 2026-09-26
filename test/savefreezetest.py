# A range-sharded space keeps answering while it saves - TODO 465.
#
# `_save` writes straight from the live arena, so a shard's latch is held for the
# whole disk write. A range-sharded space saves through `save_space()`, which
# read-locks every shard at once (TODO 462). So for as long as the save takes, no
# write anywhere in the space can go through. The latch prefers writers, so once
# one write is queued, reads queue behind it too, and the whole space stops.
#
# The check: grow the space until a SAVE takes long enough to measure, then SAVE
# again with one client writing and another reading throughout. Neither should
# have to wait anything like as long as the save.
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14465)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "savefreeze_data")
shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)

SPACE = "sf"
SHARDS = 4
VALUE = "v" * 1000
# grow by this many keys until a SAVE takes MIN_SAVE seconds, up to MAX_KEYS
STEP = scale.env_int("SAVEFREEZE_STEP", 100000, floor=10000)
MAX_KEYS = scale.env_int("SAVEFREEZE_MAX_KEYS", 1500000, floor=STEP)
# the shard files land in the page cache, so even a big space saves quickly
MIN_SAVE = 0.4
# the longest one request may wait, as a share of the save. A copy-on-write save
# holds the latch for the copy only, which is a small part of it
SHARE = 0.25


def start():
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA,
                          "-c", "save_interval=86400000",
                          "-c", "max_modifications_before_save=1000000000"],
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            out = p.stdout.read().decode(errors="replace")
            raise AssertionError("barchd exited with %s:\n%s" % (p.returncode, out[-2000:]))
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % PORT)


def stop(p, sig=signal.SIGTERM):
    p.send_signal(sig)
    try:
        p.wait(timeout=120)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=30)


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=300)


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def key(i):
    return "k%08d" % i


def fill(r, first, count):
    pipe = r.pipeline(transaction=False)
    for i in range(first, first + count):
        pipe.execute_command(SPACE + ":SET", key(i), VALUE)
        if i % 5000 == 4999:
            pipe.execute()
    pipe.execute()


def timed_save(r):
    t = time.monotonic()
    r.execute_command(SPACE + ":SAVE")
    return time.monotonic() - t


def worker(op, stop_at, waits):
    """one request after another until told to stop, recording (start, wait)"""
    c = client()
    i = 0
    while not stop_at.is_set():
        k = key((i * 7919) % 1000)
        t = time.monotonic()
        if op == "set":
            c.execute_command(SPACE + ":SET", k, VALUE)
        else:
            c.execute_command(SPACE + ":GET", k)
        waits.append((t, time.monotonic() - t))
        i += 1


proc = start()
try:
    r = client()
    r.execute_command("configuration:SET", SPACE + ".ordered", "1")
    r.execute_command("configuration:SET", SPACE + ".shards", str(SHARDS))
    r.execute_command("configuration:SET", SPACE + ".range_sharded", "1")
    r.execute_command("configuration:SAVE")

    keys = 0
    took = 0.0
    while keys < MAX_KEYS and took < MIN_SAVE:
        fill(r, keys, STEP)
        keys += STEP
        took = timed_save(r)
        print("  %d keys save in %.2fs" % (keys, took), flush=True)
    check(took >= MIN_SAVE * 0.5, "a SAVE takes long enough to measure (%.2fs)" % took)

    stop_at = threading.Event()
    set_waits, get_waits = [], []
    threads = [threading.Thread(target=worker, args=("set", stop_at, set_waits)),
               threading.Thread(target=worker, args=("get", stop_at, get_waits))]
    for t in threads:
        t.start()
    time.sleep(0.5)                 # both going before the save starts
    save_from = time.monotonic()
    took = timed_save(r)
    save_to = save_from + took
    time.sleep(0.5)                 # and after it ends, so a blocked one comes back
    stop_at.set()
    for t in threads:
        t.join(timeout=300)

    def during(waits):
        # every request that was waiting at some point while the save ran
        return [w for s, w in waits if s < save_to and s + w > save_from]

    def started(waits):
        # a frozen space gets a handful in before the save takes its locks, then
        # none until it ends. One that isn't frozen answers thousands
        return [w for s, w in waits if save_from < s < save_to]

    limit = took * SHARE
    for name, waits in (("SET", set_waits), ("GET", get_waits)):
        d = during(waits)
        worst = max(d) if d else 0.0
        begun = len(started(waits))
        print("  %s: %d requests started during a %.2fs save, slowest %.3fs"
              % (name, begun, took, worst), flush=True)
        check(begun > 50, "%s kept being answered during the save" % name)
        check(worst < limit, "no %s waited %.2fs or more (%.2fs did)" % (name, limit, worst))
finally:
    stop(proc, signal.SIGKILL)

print("\n%s" % ("all save freeze checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
