# A range-sharded space replays its change log into the shards that own the
# keys now, not the ones they were written to - TODO 461.
#
# Every change log record carries the shard its key was in when it was written,
# and with the same shard count replay puts it straight back there. For hash
# sharding that's a function of the key. For range sharding it isn't: the
# rebalancer moves keys between shards while the space runs, and on restart the
# shards come back from files written at the last checkpoint, with the
# boundaries they had then.
#
# So the sequence here is: fill, SAVE (the files and the checkpoint), push the
# boundary by writing a lot below every key so the rebalancer moves the old keys
# up a shard, overwrite and delete some of the keys that moved, kill -9. On
# restart the files put those keys back in shard 0, the log says shard 1, and a
# read routes to shard 0. An overwrite looks undone and a delete looks undone.
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
PORT = scale.port(default=14462)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "aofrange_data")
LOGS = os.path.join(os.getcwd(), "aofrange_logs")
for d in (DATA, LOGS):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)

SPACE = "ar"
SHARDS = 2
KEYS = scale.env_int("AOFRANGE_KEYS", 20000, floor=2000)
TOUCH = 100           # keys overwritten, and as many again deleted
# no interval saves: the shard files have to stay as the SAVE left them, or
# there's no difference between where the files and the log put a key
NO_SAVES = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000"]


def start():
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA] + NO_SAVES,
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


def key(i):
    return "k%07d" % i


def low(i):
    # below every key(): routed to the bottom of shard 0
    return "a%07d" % i


# --- run 1 ---------------------------------------------------------------------
proc = start()
try:
    r = client()
    r.execute_command("configuration:SET", SPACE + ".aof_dir", LOGS)
    r.execute_command("configuration:SET", SPACE + ".ordered", "1")
    r.execute_command("configuration:SET", SPACE + ".shards", str(SHARDS))
    r.execute_command("configuration:SET", SPACE + ".range_sharded", "1")
    r.execute_command("configuration:SAVE")

    pipe = r.pipeline(transaction=False)
    for i in range(KEYS):
        pipe.execute_command(SPACE + ":SET", key(i), "old")
        if i % 5000 == 4999:
            pipe.execute()
    pipe.execute()
    before = settle(r, KEYS)
    # keys go in in order and stay in order, so shard 0 holds key(0) up to here
    boundary_saved = before[0]
    print("  saved with sizes %s, shard 1 starting at %s" % (before, key(boundary_saved)),
          flush=True)
    check(0 < boundary_saved < KEYS, "the keys were spread over both shards before the save")
    r.execute_command(SPACE + ":SAVE")

    # as many again below every key, so shard 0 sheds its old keys up into shard 1
    pipe = r.pipeline(transaction=False)
    for i in range(KEYS):
        pipe.execute_command(SPACE + ":SET", low(i), "low")
        if i % 5000 == 4999:
            pipe.execute()
    pipe.execute()
    after = settle(r, 2 * KEYS)
    # shard 0 now holds every low() key and the first few key()s, if any
    boundary_now = max(0, after[0] - KEYS)
    print("  now sizes %s, shard 1 starting at %s" % (after, key(boundary_now)), flush=True)
    moved = boundary_saved - boundary_now
    check(moved >= 2 * TOUCH,
          "the rebalancer moved %d saved keys into shard 1 (need %d)" % (moved, 2 * TOUCH))

    # keys that were in shard 0 when the files were written and are in shard 1 now
    overwritten = [key(i) for i in range(boundary_now, boundary_now + TOUCH)]
    removed = [key(i) for i in range(boundary_now + TOUCH, boundary_now + 2 * TOUCH)]
    for k in overwritten:
        r.execute_command(SPACE + ":SET", k, "new")
    for k in removed:
        r.execute_command(SPACE + ":DEL", k)
    check(all(r.execute_command(SPACE + ":GET", k) == b"new" for k in overwritten),
          "the overwrites read back before the crash")
    check(all(r.execute_command(SPACE + ":GET", k) is None for k in removed),
          "and the deletes do too")
finally:
    # no shutdown save: the files are the SAVE's, and the log has the rest
    stop(proc, signal.SIGKILL)

# --- run 2 ---------------------------------------------------------------------
proc = start()
try:
    r = client()
    stale = [k for k in overwritten if r.execute_command(SPACE + ":GET", k) != b"new"]
    if stale:
        print("  first stale:", stale[:3], r.execute_command(SPACE + ":GET", stale[0]),
              flush=True)
    check(not stale, "every overwrite of a moved key is back (%d stale)" % len(stale))
    undead = [k for k in removed if r.execute_command(SPACE + ":GET", k) is not None]
    check(not undead, "every delete of a moved key held (%d back)" % len(undead))
    expect = 2 * KEYS - TOUCH
    got = r.execute_command(SPACE + ":DBSIZE")
    check(got == expect, "and the space holds %d keys (it has %d)" % (expect, got))
finally:
    stop(proc)

print("\n%s" % ("all aof range replay checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
