# A flushed space stays flushed across SAVE, RELOAD, LOAD and a restart - TODO 477.
#
# `_save` skips a shard whose arenas hold no bytes at all, and TODO 477 asked
# whether that leaves old shard files behind that bring flushed keys back. It
# doesn't: FLUSHDB clears the arenas, but the shard allocates again straight
# after (INFO shows 1MB where a new shard shows 0), so a SAVE after FLUSHDB
# writes an empty pair over the old one. Only a shard that has loaded and
# written nothing sits at zero bytes, and when it has files beside it, its load
# failed - keeping those files is right.
#
# So this stays as the check that a flush sticks. RELOAD is the sharp one: it
# saves and then loads, so a save that kept the old files would bring every key
# back at once.
#
# Run for a hash-sharded and a range-sharded space, with a change log. FLUSHDB
# itself isn't in the change log, so a crash between FLUSHDB and the next SAVE
# still brings keys back - that's TODO 478, and this test SAVEs first.
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
PORT = scale.port(default=14477)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "emptysave_data")
LOGS = os.path.join(os.getcwd(), "emptysave_logs")
KEYS = 2000
SHARDS = 2
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


def ok(reply):
    return reply in (b"OK", "OK")


def fill(r, space):
    pipe = r.pipeline(transaction=False)
    for i in range(KEYS):
        pipe.execute_command(space + ":SET", "k%05d" % i, "v")
    pipe.execute()


def run_space(space, range_sharded):
    print("%s space" % ("range-sharded" if range_sharded else "hash-sharded"), flush=True)
    for d in (DATA, LOGS):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)

    proc = start()
    try:
        r = client()
        r.execute_command("configuration:SET", space + ".shards", str(SHARDS))
        r.execute_command("configuration:SET", space + ".aof_dir", LOGS)
        if range_sharded:
            r.execute_command("configuration:SET", space + ".ordered", "1")
            r.execute_command("configuration:SET", space + ".range_sharded", "1")
        r.execute_command("configuration:SAVE")
    finally:
        stop(proc)

    proc = start()
    try:
        r = client()
        fill(r, space)
        check(ok(r.execute_command(space + ":SAVE")), "SAVE of a full space works")

        # RELOAD saves and then loads, so a save that skips the empty shards
        # loads the old files straight back
        r.execute_command("USE", space)
        r.execute_command("FLUSHDB")
        check(r.execute_command(space + ":DBSIZE") == 0, "FLUSHDB empties it")
        check(ok(r.execute_command(space + ":RELOAD")), "RELOAD of the empty space is OK")
        got = r.execute_command(space + ":DBSIZE")
        check(got == 0, "and it stays empty (%d keys came back)" % got)

        r.execute_command("FLUSHDB")
        check(ok(r.execute_command(space + ":SAVE")), "SAVE of the empty space is OK")
        check(ok(r.execute_command(space + ":LOAD")), "LOAD after it is OK")
        got = r.execute_command(space + ":DBSIZE")
        check(got == 0, "and brings nothing back (%d keys)" % got)
    finally:
        stop(proc, signal.SIGKILL)

    proc = start()
    try:
        r = client()
        got = r.execute_command(space + ":DBSIZE")
        check(got == 0, "after kill -9 the space is still empty (%d keys)" % got)
        # and it can be filled and saved again from there
        fill(r, space)
        check(ok(r.execute_command(space + ":SAVE")), "SAVE after that works")
    finally:
        stop(proc, signal.SIGKILL)

    proc = start()
    try:
        r = client()
        got = r.execute_command(space + ":DBSIZE")
        check(got == KEYS, "and what it saved comes back (%d of %d)" % (got, KEYS))
    finally:
        stop(proc)


run_space("eh", False)
run_space("eg", True)

print("\n%s" % ("all empty save checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
