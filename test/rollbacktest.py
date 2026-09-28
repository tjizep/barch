# ROLLBACK puts the space back everywhere it's kept, not only in memory - TODO 539
# and 543.
#
# A transaction is the whole space's: BEGIN takes every shard's pages as they are,
# writes carry on into copies, and ROLLBACK drops the copies. The writes were in
# the change log all the same, and a SAVE inside the transaction put them in the
# files, so a restart brought them back. On a range-sharded space the rebalancer
# can move keys during the transaction; the rollback put the pages back and left
# the route table sending those keys to where they had been moved.
#
# Checked, against barchd:
#   - with a change log: a SET, a new key and a DEL inside the transaction, then
#     ROLLBACK and kill -9 - after the replay the space is as it was at BEGIN
#   - with no log, a SAVE inside the transaction, ROLLBACK and kill -9 - the
#     files hold the space as it was at BEGIN
#   - range-sharded: enough keys written inside the transaction that the sweep
#     moves some of the older ones, then ROLLBACK - every older key reads back,
#     and writing one doesn't make a second copy of it
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14539)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "rollback_data")
LOGS = os.path.join(os.getcwd(), "rollback_logs")
QUIET = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000"]
WITH_LOG = ["-c", "aof_dir=" + LOGS, "-c", "aof_durability=each"]

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def start(args):
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA,
                          "--no-save-on-exit"] + args,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            raise AssertionError("barchd exited with %s" % p.returncode)
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % PORT)


def kill(p):
    if p is not None and p.poll() is None:
        p.send_signal(signal.SIGKILL)
        p.wait(timeout=30)


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=120)


def fresh(args, conf):
    for d in (DATA, LOGS):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)
    p = start(args)
    try:
        r = client()
        for k, v in conf:
            r.execute_command("configuration:SET", k, v)
        r.execute_command("configuration:SAVE")
    finally:
        kill(p)


def state(r):
    return (r.execute_command("tx:GET", "k"), r.execute_command("tx:GET", "added"),
            r.execute_command("tx:GET", "gone"))


BEFORE = (b"before", None, b"x")


def inside(r):
    r.execute_command("tx:SET", "k", "before")
    r.execute_command("tx:SET", "gone", "x")
    r.execute_command("tx:BEGIN")
    r.execute_command("tx:SET", "k", "inside")
    r.execute_command("tx:SET", "added", "y")
    r.execute_command("tx:DEL", "gone")


print("with a change log, a ROLLBACK isn't replayed back", flush=True)
fresh(QUIET + WITH_LOG, [("tx.shards", "2"), ("tx.aof", "on")])
p = start(QUIET + WITH_LOG)
try:
    r = client()
    inside(r)
    check(r.execute_command("tx:ROLLBACK") == b"OK", "ROLLBACK answers OK")
    check(state(r) == BEFORE, "the space reads as it was at BEGIN (%s)" % (state(r),))
    kill(p)
    p = start(QUIET + WITH_LOG)
    r = client()
    check(state(r) == BEFORE, "and still does after kill -9 and the replay (%s)" % (state(r),))
finally:
    kill(p)

print("with no log, a SAVE inside the transaction doesn't outlive the ROLLBACK", flush=True)
fresh(QUIET, [("tx.shards", "2")])
p = start(QUIET)
try:
    r = client()
    inside(r)
    check(r.execute_command("tx:SAVE") == b"OK", "SAVE inside the transaction")
    check(r.execute_command("tx:ROLLBACK") == b"OK", "ROLLBACK answers OK")
    kill(p)
    p = start(QUIET)
    r = client()
    check(state(r) == BEFORE, "after kill -9 the files hold the space as it was at BEGIN (%s)"
          % (state(r),))
finally:
    kill(p)

print("range-sharded: the route table goes back with the shards", flush=True)
OLD, NEW = 4000, 20000
fresh(QUIET + ["-c", "maintenance_poll_delay=20"], [("tx.shards", "4"), ("tx.range_sharded", "1")])
p = start(QUIET + ["-c", "maintenance_poll_delay=20"])
try:
    r = client()
    pipe = r.pipeline(transaction=False)
    for i in range(OLD):
        pipe.execute_command("tx:SET", "k%06d" % i, "v")
    pipe.execute()
    sample = ["k%06d" % i for i in range(0, OLD, 97)]

    def where():
        # the raw reply: redis-py only parses INFO when the command is named INFO
        found = []
        for k in sample:
            text = r.execute_command("tx:INFO", "SHARD", k).decode()
            found.append(int(re.search(r"number:(\d+)", text).group(1)))
        return found

    time.sleep(1)
    at_begin = where()
    r.execute_command("tx:BEGIN")
    pipe = r.pipeline(transaction=False)
    for i in range(NEW):
        pipe.execute_command("tx:SET", "a%06d" % i, "v")   # all below the k keys
    pipe.execute()
    deadline = time.time() + 30
    while time.time() < deadline and where() == at_begin:
        time.sleep(0.2)
    check(where() != at_begin, "the sweep moved some older keys during the transaction")
    check(r.execute_command("tx:ROLLBACK") == b"OK", "ROLLBACK answers OK")
    missing = sum(1 for i in range(OLD) if r.execute_command("tx:GET", "k%06d" % i) is None)
    check(missing == 0, "every older key reads back (%d of %d missing)" % (missing, OLD))
    check(r.execute_command("tx:DBSIZE") == OLD, "DBSIZE is %d (%s)" % (OLD, r.execute_command("tx:DBSIZE")))
    for i in range(0, OLD, 400):
        r.execute_command("tx:SET", "k%06d" % i, "again")
    check(r.execute_command("tx:DBSIZE") == OLD,
          "writing ten of them makes no second copies (DBSIZE %s)" % r.execute_command("tx:DBSIZE"))
finally:
    kill(p)

print("\n%s" % ("rollback checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
