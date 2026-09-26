# A sorted-set STORE that fails partway clears its destination - TODO 472.
#
# store_ordered reserves change log room for the whole STORE, so the log can't refuse
# it partway (DONE 436, DONE 437). Anything else still can, and then it clears the
# destination, out of the same reserved room. This makes that happen for real.
#
# The shard refuses a key and value bigger than maximum_allocation_size together, and
# a member's index entry has the set's name in both its key and its value. So a member
# sized to just fit in a set called "s" doesn't fit in a destination whose name is a
# thousand bytes longer. Give it the highest score and a STORE writes the small members
# first, then fails on it, with members already in the destination.
#
# What's checked is that the STORE answers with an error and leaves the destination
# empty - not partly filled, and with no half written member - live, and after a
# kill -9 where the change log decides what comes back.
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
PORT = scale.port(default=14481)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "storefallback_data")
LOGS = os.path.join(os.getcwd(), "storefallback_logs")
for d in (DATA, LOGS):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)

SPACE = "zs"
NO_SAVES = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000"]
SMALL = {"a": 1.0, "b": 2.0, "c": 3.0}
# longer than "s", so the big member's keys don't fit under it. Longer than 256
# bytes on purpose, which DONE 438 had to avoid until TODO 473
DEST = "d" * 1000
PROBE = "p"             # the same length as "s", to size the big member against
BEFORE = {"old": 7.0}   # what the destination holds before the STORE


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


def cmd(r, *args):
    return r.execute_command(SPACE + ":" + args[0], *args[1:])


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def members(r, key):
    rows = cmd(r, "ZRANGE", key, 0, -1, "WITHSCORES")
    return {rows[i].decode(): float(rows[i + 1]) for i in range(0, len(rows), 2)}


def whole(r, key, names):
    """ZRANGE and ZSCORE agree about every member, so none is half there"""
    ranged = members(r, key)
    for m in names:
        s = cmd(r, "ZSCORE", key, m)
        if (s is None) != (m not in ranged):
            print("  %s: %s is in %s but not %s" % (
                key[:12], m[:12], "ZSCORE" if s is not None else "ZRANGE",
                "ZRANGE" if s is not None else "ZSCORE"), flush=True)
            return False
    return True


def fits(r, n):
    """does a member of n bytes fit in a set named like "s"?"""
    try:
        cmd(r, "ZADD", PROBE, 1, "x" * n)
    except redis.ResponseError:
        return False
    cmd(r, "DEL", PROBE)
    return True


# --- run 0: the opt in to a change log -------------------------------------------
proc = start()
try:
    r = client()
    r.execute_command("configuration:SET", SPACE + ".aof_dir", LOGS)
    r.execute_command("configuration:SAVE")
finally:
    stop(proc)

# --- run 1: a STORE that fails once some members are in --------------------------
proc = start()
try:
    r = client()
    lo, hi = 1, 1 << 20
    check(fits(r, lo) and not fits(r, hi), "a member can be too big to add")
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if fits(r, mid):
            lo = mid
        else:
            hi = mid
    big = "x" * lo
    print("  the biggest member that fits in a set named like \"s\": %d bytes" % lo, flush=True)

    for m, sc in SMALL.items():
        cmd(r, "ZADD", "s", sc, m)
    cmd(r, "ZADD", "s", 100, big)          # last, since the store writes by score
    check(cmd(r, "ZCARD", "s") == len(SMALL) + 1, "the source has the big member too")
    for m, sc in BEFORE.items():
        cmd(r, "ZADD", DEST, sc, m)

    names = list(SMALL) + list(BEFORE) + [big]
    for command in (("ZUNIONSTORE", DEST, 1, "s"), ("ZRANGESTORE", DEST, "s", 0, -1)):
        refused = False
        try:
            got = cmd(r, *command)
            print("  %s answered %r" % (command[0], got), flush=True)
        except redis.ResponseError as e:
            print("  %s refused: %s" % (command[0], e), flush=True)
            refused = True
        check(refused, "%s with a member too big for it answers with an error" % command[0])
        left = members(r, DEST)
        if left:
            print("  %s left %d members: %r" % (command[0], len(left),
                                                sorted(m[:12] for m in left)), flush=True)
        check(not left, "and leaves the destination empty")
        check(whole(r, DEST, names), "with no half written member")
        # put the old member back for the next one
        for m, sc in BEFORE.items():
            cmd(r, "ZADD", DEST, sc, m)
    # the last one's destination is cleared again, so replay has one thing to show
    try:
        cmd(r, "ZUNIONSTORE", DEST, 1, "s")
    except redis.ResponseError:
        pass
    check(not members(r, DEST), "the destination is empty going into the crash")
finally:
    # no shutdown save: what comes back is what the log holds
    stop(proc, signal.SIGKILL)

# --- run 2: the change log decides ---------------------------------------------------
proc = start()
try:
    r = client()
    left = members(r, DEST)
    if left:
        print("  after replay %d members: %r" % (len(left), sorted(m[:12] for m in left)),
              flush=True)
    check(not left, "the destination is empty after replay")
    check(whole(r, DEST, names), "with no half written member after replay")
    check(members(r, "s").keys() == set(SMALL) | {big}, "and the source is untouched")
finally:
    stop(proc)

print("\n%s" % ("all store fallback checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
