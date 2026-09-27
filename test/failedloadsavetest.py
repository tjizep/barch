# A shard whose load failed is never saved over its files - TODO 501.
#
# Startup called `shard->load(true)` and dropped the result, and LOAD clears a
# shard before it loads. So a load that failed on files that were fine - out of
# file descriptors, out of memory, a read error - left an empty shard in a space
# that carried on. The first write made it due, the interval save wrote the
# nearly empty tree over the good pair, and the checkpoint that followed trimmed
# the change log too. The data was gone from both.
#
# Here shard 0's nodes file is swapped for a directory while the server starts,
# which fails the load the way a transient error would and leaves the file itself
# whole. Then writes, interval saves and a SAVE all get their chance to write
# over it. After a kill, with the file put back, every key saved before and
# every key written since has to be there: the old ones from the files, the new
# ones from the change log.
#
# Run for a hash-sharded space (interval saves go a shard at a time, then
# checkpoint_saved) and a range-sharded one (every shard frozen together).
import hashlib
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
PORT = scale.port(default=14501)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "failedloadsave_data")
LOGS = os.path.join(os.getcwd(), "failedloadsave_logs")
KEYS = 2000
# interval saves every second, so maintenance tries to save the broken shard
ARGS = ["-c", "save_interval=1000", "-c", "max_modifications_before_save=1000000000",
        "-c", "aof_durability=timer"]


def start():
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA] + ARGS,
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


def stop(p, sig=signal.SIGTERM):
    p.send_signal(sig)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=30)


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=60)


failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def answer(r, *args):
    """the reply, or the error text"""
    try:
        return r.execute_command(*args)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def ok(reply):
    return reply in (b"OK", "OK")


def fill(r, space, prefix, value):
    pipe = r.pipeline(transaction=False)
    for i in range(KEYS):
        pipe.execute_command(space + ":SET", "%s%05d" % (prefix, i), value)
    pipe.execute()


def count(r, space, prefix, value):
    pipe = r.pipeline(transaction=False)
    for i in range(KEYS):
        pipe.execute_command(space + ":GET", "%s%05d" % (prefix, i))
    return sum(1 for v in pipe.execute() if v == value.encode())


def shard_file(kind, space, n):
    return os.path.join(DATA, "%s_%s_%d.dat" % (kind, space, n))


def digest(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def run_space(space, range_sharded):
    print("%s space" % ("range-sharded" if range_sharded else "hash-sharded"), flush=True)
    for d in (DATA, LOGS):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)

    proc = start()
    try:
        r = client()
        r.execute_command("configuration:SET", space + ".aof_dir", LOGS)
        r.execute_command("configuration:SET", space + ".shards", "2")
        if range_sharded:
            r.execute_command("configuration:SET", space + ".ordered", "1")
            r.execute_command("configuration:SET", space + ".range_sharded", "1")
        r.execute_command("configuration:SAVE")
    finally:
        stop(proc)

    proc = start()
    try:
        r = client()
        fill(r, space, "old", "saved")
        check(ok(answer(r, space + ":SAVE")), "SAVE of the first keys works")
    finally:
        stop(proc)

    leaves, nodes = shard_file("leaves", space, 0), shard_file("nodes", space, 0)
    before = {leaves: digest(leaves), nodes: digest(nodes)}
    # the file is whole; it just can't be opened while this is in its place
    os.rename(nodes, nodes + ".aside")
    os.makedirs(nodes)

    proc = start()
    try:
        r = client()
        fill(r, space, "new", "logged")
        # a few interval saves' worth of maintenance ticks
        time.sleep(3.5)
        reply = answer(r, space + ":SAVE")
        print("  SAVE with shard 0 unloaded answered: %s" % (reply,), flush=True)
        check(not ok(reply), "SAVE says so when a shard won't be saved")
        check(digest(leaves) == before[leaves], "shard 0's leaves file is as it was")
        check(not os.path.exists(leaves + ".wal"), "and no wal was left beside it")
        reply = answer(r, space + ":RELOAD")
        check(not ok(reply), "RELOAD refuses too, since it saves first")
        check(digest(leaves) == before[leaves], "and still leaves the file alone")
    finally:
        stop(proc, signal.SIGKILL)

    os.rmdir(nodes)
    os.rename(nodes + ".aside", nodes)
    check(digest(nodes) == before[nodes], "shard 0's nodes file is as it was")

    proc = start()
    try:
        r = client()
        got = count(r, space, "old", "saved")
        check(got == KEYS, "every key saved before is back (%d of %d)" % (got, KEYS))
        got = count(r, space, "new", "logged")
        check(got == KEYS, "every key written since came back from the log (%d of %d)"
              % (got, KEYS))
        check(ok(answer(r, space + ":SAVE")), "and a shard that loaded saves again")
    finally:
        stop(proc)


run_space("fh", False)
run_space("fr", True)

print("\n%s" % ("all failed load save checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
