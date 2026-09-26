# LOAD and RELOAD say when a shard didn't load - TODO 476.
#
# `shard::load`, `load_holding_lock`, `reload` and `reload_holding_lock` called
# `_load` and ignored what it returned, so LOAD answered OK over a shard file it
# couldn't read. RELOAD also ignored its own save: when that failed it cleared
# the shard anyway and loaded the older files, losing every write since.
#
# `_load` also returns false when there are no files at all, which is fine - a
# space that has never been saved - so that has to stay OK.
#
# Run for a hash-sharded space (LOAD goes through `load`) and a range-sharded
# one (LOAD takes the space and goes through `load_holding_lock`).
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
PORT = scale.port(default=14476)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "loadresult_data")
KEYS = 2000
ARGS = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000"]


def start():
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA] + ARGS,
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
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=60)


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
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


def run_space(space, range_sharded):
    print("%s space" % ("range-sharded" if range_sharded else "hash-sharded"), flush=True)
    shutil.rmtree(DATA, ignore_errors=True)
    os.makedirs(DATA)

    proc = start()
    try:
        r = client()
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
        # never saved: nothing on disk isn't a failure
        check(ok(answer(r, space + ":LOAD")), "LOAD of a space never saved is OK")
        check(ok(answer(r, space + ":RELOAD")), "and so is RELOAD")

        fill(r, space, "k", "saved")
        check(ok(answer(r, space + ":SAVE")), "SAVE works")
        check(ok(answer(r, space + ":LOAD")), "LOAD of what was saved is OK")
        check(count(r, space, "k", "saved") == KEYS, "and every key is back")

        # RELOAD whose save can't write: the shard keeps what it has
        fill(r, space, "n", "unsaved")
        blockers = []
        for n in range(2):
            b = shard_file("nodes", space, n) + ".wal"
            os.makedirs(b)
            with open(os.path.join(b, "keep"), "w") as f:
                f.write("stops the wal being removed or opened")
            blockers.append(b)
        reply = answer(r, space + ":RELOAD")
        print("  RELOAD with the save blocked answered: %s" % (reply,), flush=True)
        check(not ok(reply), "RELOAD says so when its save fails")
        got = count(r, space, "n", "unsaved")
        check(got == KEYS, "and the writes it couldn't save are still there (%d of %d)"
              % (got, KEYS))
        for b in blockers:
            shutil.rmtree(b, ignore_errors=True)
        check(ok(answer(r, space + ":SAVE")), "SAVE works again once it can write")

        # a leaves file that isn't whole: its completion stamp is gone
        with open(shard_file("leaves", space, 0), "r+b") as f:
            f.write(b"\0" * 8)
        reply = answer(r, space + ":LOAD")
        print("  LOAD over a broken leaves file answered: %s" % (reply,), flush=True)
        check(not ok(reply), "LOAD says so when a shard file won't load")
        # nothing half loaded: every read answers, with a value or with nothing
        try:
            count(r, space, "k", "saved")
            count(r, space, "n", "unsaved")
            answered = True
        except redis.RedisError as e:
            print("  %s" % e, flush=True)
            answered = False
        check(answered, "and the space still answers reads")
    finally:
        stop(proc, signal.SIGKILL)


run_space("lh", False)
run_space("lg", True)

print("\n%s" % ("all load result checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
