# The interval save of one shard can't lose keys the rebalancer moved - TODO 462.
#
# SAVE freezes a range-sharded space so the shard files agree on where every key
# is. The interval save in shard maintenance doesn't: it saves one shard when that
# shard's own counters say so, under its own latch. If the rebalancer moved keys
# from shard 0 into shard 1 and then only shard 0 saves, the moved keys are in
# neither file - shard 0's new file doesn't have them, and shard 1's file is from
# before they arrived. A crash then loses them, and the files still form a clean
# partition, so nothing at load says so.
#
# Getting only one shard to save takes some care, because of what counts as a
# modification. In an ordered space a SET doesn't count at all, a DEL counts on
# its shard, and every rebalance move counts once on each of the two shards. So:
#
#   1. fill, let it balance, SAVE - both shards start counting from zero
#   2. write below every key, so shard 0 sheds keys into shard 1. Each shard's
#      count is now the number of moves, the same on both
#   3. DEL keys in shard 0 only, then set the threshold between the two counts,
#      so shard 0 is past it and shard 1 is under it
#   4. wait for shard 0's files to change, kill -9, and count what comes back
#
# No change log here: the loss is in the shard files alone.
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
PORT = scale.port(default=14463)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "rangesave_data")
shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)

SPACE = "rv"
SHARDS = 2
KEYS = scale.env_int("RANGESAVE_KEYS", 20000, floor=4000)
# DELs in shard 0. Few enough that the balance doesn't change and nothing moves
# back, which would count on shard 1 again
DELS = KEYS // 10
NEVER = "1000000000"


def start():
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA,
                          "-c", "save_interval=86400000",
                          "-c", "max_modifications_before_save=" + NEVER],
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
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=30)


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=120)


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def shard_sizes(conn):
    conn.execute_command("USE", SPACE)
    out = []
    for s in range(SHARDS):
        # the # form asks for a shard by number rather than hashing the argument
        raw = conn.execute_command("INFO SHARD #%d" % s)
        if isinstance(raw, bytes):
            raw = raw.decode()
        for line in raw.split("\n"):
            if line.startswith("size:"):
                out.append(int(line.split(":", 1)[1]))
    assert len(out) == SHARDS, raw
    return out


def settle(conn, expect, timeout=30.0):
    """wait until the rebalancer has stopped moving keys, and return the sizes.

    The sizes are read one shard at a time while the sweep runs, so a reading
    only counts once the total is right and two in a row agree."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        sizes = shard_sizes(conn)
        if sum(sizes) == expect and sizes == last:
            return sizes
        last = sizes
        time.sleep(0.5)
    raise AssertionError("never settled at %d keys: %s" % (expect, last))


def shard_files(shard):
    return [os.path.join(DATA, "%s_%s_%d.dat" % (kind, SPACE, shard))
            for kind in ("leaves", "nodes")]


def stamps(shard):
    return [os.stat(f).st_mtime_ns if os.path.exists(f) else 0 for f in shard_files(shard)]


def key(i):
    return "k%07d" % i


def low(i):
    # below every key(): routed to the bottom of shard 0
    return "a%07d" % i


def fill(conn, name, count, value):
    pipe = conn.pipeline(transaction=False)
    for i in range(count):
        pipe.execute_command(SPACE + ":SET", name(i), value)
        if i % 5000 == 4999:
            pipe.execute()
    pipe.execute()


# --- run 1 ---------------------------------------------------------------------
proc = start()
try:
    r = client()
    r.execute_command("configuration:SET", SPACE + ".ordered", "1")
    r.execute_command("configuration:SET", SPACE + ".shards", str(SHARDS))
    r.execute_command("configuration:SET", SPACE + ".range_sharded", "1")
    r.execute_command("configuration:SAVE")

    # 1. fill, balance, SAVE
    fill(r, key, KEYS, "old")
    before = settle(r, KEYS)
    r.execute_command(SPACE + ":SAVE")
    saved0, saved1 = stamps(0), stamps(1)
    check(all(saved0) and all(saved1), "SAVE wrote both shards")

    # 2. push shard 0's old keys up into shard 1
    fill(r, low, KEYS, "low")
    after = settle(r, 2 * KEYS)
    moved = before[0] + KEYS - after[0]
    print("  sizes %s after SAVE, %s now: %d saved keys moved into shard 1"
          % (before, after, moved), flush=True)
    check(moved > 0, "the rebalancer moved saved keys into shard 1")
    check(stamps(0) == saved0 and stamps(1) == saved1, "and nothing has saved since")

    # 3. DELs in shard 0, then a threshold shard 0 is over and shard 1 isn't.
    # `moved` is the net count, and each shard has counted at least that many;
    # the margin covers a key that went across and back. The DELs go first so
    # the save that follows has all of them - there's no change log to bring
    # back one that came after it
    for i in range(DELS):
        r.execute_command(SPACE + ":DEL", low(i))
    threshold = moved + DELS // 2
    r.execute_command("CONFIG", "SET", "max_modifications_before_save", str(threshold))

    # 4. wait for shard 0's interval save, then give it a moment to finish
    deadline = time.time() + 30
    while time.time() < deadline and any(a == b for a, b in zip(stamps(0), saved0)):
        time.sleep(0.2)
    check(all(a != b for a, b in zip(stamps(0), saved0)), "shard 0 saved on its own counters")
    time.sleep(2)
    # informational only: with the fix the whole space saves here, so shard 1's
    # files changing is the right answer then and the wrong setup now
    print("  shard 1 saved as well: %s" % (stamps(1) != saved1), flush=True)
    expect = 2 * KEYS - DELS
    check(r.execute_command(SPACE + ":DBSIZE") == expect, "the space holds %d keys" % expect)
finally:
    # no shutdown save: what comes back is what the interval save left on disk
    stop(proc, signal.SIGKILL)

# --- run 2 ---------------------------------------------------------------------
proc = start()
try:
    r = client()
    got = r.execute_command(SPACE + ":DBSIZE")
    check(got == expect, "all %d keys are back (%d are)" % (expect, got))
    pipe = r.pipeline(transaction=False)
    for i in range(KEYS):
        pipe.execute_command(SPACE + ":GET", key(i))
    lost = [key(i) for i, v in enumerate(pipe.execute()) if v != b"old"]
    if lost:
        print("  lost %s .. %s" % (lost[0], lost[-1]), flush=True)
    check(not lost, "no key the rebalancer moved is missing (%d are)" % len(lost))
finally:
    stop(proc)

print("\n%s" % ("all range interval save checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
