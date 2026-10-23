# A SAVE while a transaction is open doesn't trim writes it didn't save - TODO 740.
#
# A save inside a transaction writes the space as it was at BEGIN (TODO 678), so
# writes made after BEGIN aren't in its files. The SAVE then checkpointed the
# change log at the mark it read when it started, and trimmed everything before
# it. Writes between BEGIN and that SAVE were in neither place, and a kill -9
# lost them even after a COMMIT. TestSaveCopyOnWrite caught it about seven runs
# in eight, at 4 CPUs, but only by luck of timing; this does it in order.
#
# Done for a hash-sharded space, where shards save one at a time, and a
# range-sharded one, which saves every shard together.
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
PORT = scale.port(default=14468)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "txsavemark_data")
LOGS = os.path.join(os.getcwd(), "txsavemark_logs")
KEYS = 50
# no interval saves, so the only saves are the ones this test asks for
ARGS = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000",
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
        p.wait(timeout=120)
    except subprocess.TimeoutExpired:
        scale.stop_hung(p, sig, 120)


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=60)


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def run_space(space, range_sharded):
    print("%s space" % ("range-sharded" if range_sharded else "hash-sharded"), flush=True)
    for d in (DATA, LOGS):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)

    proc = start()
    try:
        r = client()
        r.execute_command("configuration:SET", space + ".aof_dir", LOGS)
        r.execute_command("configuration:SET", space + ".shards", "4")
        if range_sharded:
            r.execute_command("configuration:SET", space + ".ordered", "1")
            r.execute_command("configuration:SET", space + ".range_sharded", "1")
        r.execute_command("configuration:SAVE")
    finally:
        stop(proc)

    proc = start()
    try:
        r = client()
        for i in range(KEYS):
            r.execute_command(space + ":SET", "old%03d" % i, "o%d" % i)
        check(r.execute_command(space + ":SAVE") in (b"OK", "OK"), "the first SAVE works")

        tx = client()
        tx.execute_command("USE", space)
        tx.execute_command("BEGIN")
        # from another connection, after BEGIN and before the SAVE
        for i in range(KEYS):
            r.execute_command(space + ":SET", "new%03d" % i, "n%d" % i)
            r.execute_command(space + ":SET", "old%03d" % i, "u%d" % i)
        check(r.execute_command(space + ":SAVE") in (b"OK", "OK"), "a SAVE during the transaction works")
        tx.execute_command("COMMIT")
    finally:
        # no shutdown save: what comes back is the files and the log
        stop(proc, signal.SIGKILL)

    proc = start()
    try:
        r = client()
        lost = [k for k, v in [("new%03d" % i, "n%d" % i) for i in range(KEYS)]
                + [("old%03d" % i, "u%d" % i) for i in range(KEYS)]
                if r.execute_command(space + ":GET", k) != v.encode()]
        if lost:
            print("  first lost: %s" % lost[0], flush=True)
        check(not lost, "after kill -9, every write is back (%d aren't)" % len(lost))
        check(r.execute_command(space + ":DBSIZE") == 2 * KEYS, "and the size is right")
    finally:
        stop(proc)


run_space("mh", False)
run_space("mr", True)

print("\n%s" % ("all save mark checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
