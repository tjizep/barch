# A sorted-set write the change log refused reaches the client as an error - TODO 469,
# and a refused STORE leaves its destination as it was - TODO 470.
#
# insert_ordered and remove_ordered used to catch what the shard threw, log it and
# carry on, so ZUNIONSTORE, ZRANGESTORE and ZREMRANGEBYSCORE answered with a count
# of writes that hadn't happened. And a member is two keys - the score key and the
# member index - so a failure between them could leave one without the other.
#
# The change log is made to fail the way aoffailtest.py does it: barchd runs under
# RLIMIT_FSIZE, so once the log can't grow every append fails with EFBIG, the same
# way it would on a full disk. What's checked is that each of those commands answers
# with an error, and that every set is whole afterwards - live, and after a kill -9
# where only the log decides what comes back.
#
# The STOREs run while the log still has a little room, less than they need. They
# used to write members until it ran out, so the destination came back partly
# filled next to the error. Now they ask the log for all the room first and write
# nothing when it can't grow, so the destination keeps what it had.
import os
import resource
import shutil
import signal
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14471)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "zsetfail_data")
LOGS = os.path.join(os.getcwd(), "zsetfail_logs")
for d in (DATA, LOGS):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)

SPACE = "zf"
FILE_LIMIT = 1 << 20
# no interval saves, so the only file that hits the limit is the log
NO_SAVES = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000"]
MEMBERS = ["member_%03d" % i for i in range(40)]
# what the destinations hold before the STOREs, which a refused STORE has to leave
OLD = {"old_%d" % i: float(100 + i) for i in range(3)}
MEMBERS += list(OLD)
SETS = ["src", "other", "union", "ranged"]


def start(file_limit=None):
    def limit():
        if file_limit is not None:
            signal.signal(signal.SIGXFSZ, signal.SIG_IGN)   # inherited through exec
            resource.setrlimit(resource.RLIMIT_FSIZE, (file_limit, file_limit))

    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA] + NO_SAVES,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                         preexec_fn=limit)
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


def refused(r, *args):
    """True if the command answered with an error rather than a result"""
    try:
        got = cmd(r, *args)
        print("  %s answered %r" % (args[0], got), flush=True)
        return False
    except redis.ResponseError as e:
        print("  %s refused: %s" % (args[0], e), flush=True)
        return True


def members(r, key):
    rows = cmd(r, "ZRANGE", key, 0, -1, "WITHSCORES")
    return {rows[i].decode(): float(rows[i + 1]) for i in range(0, len(rows), 2)}


def whole(r, key):
    """ZRANGE and ZSCORE agree about every member, so none is half there"""
    rows = cmd(r, "ZRANGE", key, 0, -1, "WITHSCORES")
    ranged = {rows[i].decode(): float(rows[i + 1]) for i in range(0, len(rows), 2)}
    bad = []
    for m in MEMBERS:
        s = cmd(r, "ZSCORE", key, m)
        if (s is None) != (m not in ranged) or (s is not None and float(s) != ranged[m]):
            bad.append((m, ranged.get(m), s))
    if bad:
        print("  %s half written: %r" % (key, bad[:3]), flush=True)
    return not bad


def small_refused_after(r):
    """small SETs until even a short record doesn't fit. Returns how many were refused"""
    i = 0
    small_refused = 0
    while small_refused < 3 and i < 100000:
        try:
            cmd(r, "SET", "t%05d" % i, "v")
        except redis.ResponseError:
            small_refused += 1
        i += 1
    return small_refused


# --- run 0: the opt in, saved with no limit ---------------------------------------
proc = start()
try:
    r = client()
    r.execute_command("configuration:SET", SPACE + ".aof_dir", LOGS)
    r.execute_command("configuration:SAVE")
finally:
    stop(proc)

# --- run 1: fill the log until it can't grow, then ask the sorted sets to write -----
proc = start(file_limit=FILE_LIMIT)
try:
    r = client()
    for i, m in enumerate(MEMBERS[:40]):
        cmd(r, "ZADD", "src", i, m)
        if i % 2:
            cmd(r, "ZADD", "other", i, m)
    for dest in ("union", "ranged"):
        for m, sc in OLD.items():
            cmd(r, "ZADD", dest, sc, m)

    big = "b" * 4096
    i = 0
    full = False
    while not full and i < 10 * FILE_LIMIT // len(big):
        try:
            cmd(r, "SET", "big%06d" % i, big)
        except redis.ResponseError:
            full = True
        i += 1
    check(full, "the change log can't grow")

    # the log has less room left than a 4 KiB value, and a STORE of 40 members
    # needs about twice that
    check(refused(r, "ZUNIONSTORE", "union", 2, "src", "other"),
          "ZUNIONSTORE answers with an error")
    check(refused(r, "ZRANGESTORE", "ranged", "src", 0, -1),
          "ZRANGESTORE answers with an error")
    for dest in ("union", "ranged"):
        got = members(r, dest)
        if got != OLD:
            print("  %s holds %d members: %r" % (dest, len(got), sorted(got)[:4]), flush=True)
        check(got == OLD, "a refused STORE leaves %s as it was" % dest)

    check(small_refused_after(r) == 3, "the change log is full")
    check(refused(r, "ZREMRANGEBYSCORE", "src", "-inf", "+inf"),
          "ZREMRANGEBYSCORE answers with an error")
    check(refused(r, "ZINCRBY", "src", 1, "member_new"),
          "ZINCRBY of a new member answers with an error")

    for k in SETS:
        check(whole(r, k), "%s has no half written member" % k)
finally:
    # no shutdown save: what comes back is what the log holds
    stop(proc, signal.SIGKILL)

# --- run 2: no limit, and the log decides what's there ------------------------------
proc = start()
try:
    r = client()
    for k in SETS:
        check(whole(r, k), "%s has no half written member after replay" % k)
    check(cmd(r, "ZCARD", "src") > 0, "src is still there after replay")
    for dest in ("union", "ranged"):
        check(members(r, dest) == OLD, "%s is as it was after replay" % dest)
finally:
    stop(proc)

print("\n%s" % ("all zset failure checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
