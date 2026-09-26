# Read-modify-write commands survive a crash - TODO 486.
#
# INCR, EXPIRE, PERSIST, GETEX, HINCRBY and the rest change a key through
# shard::update, and that never wrote to the change log. So on a logged space a
# kill -9 took each of them back: INCR read 3 before the kill and 1 after it. Now
# update records the leaf it installed, as a set with its own expiry and flags.
#
# Every command here runs after the last checkpoint, so what comes back after the
# kill comes from the change log alone.
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
PORT = scale.port(default=14486)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "aofupdate_data")
LOGS = os.path.join(os.getcwd(), "aofupdate_logs")
for d in (DATA, LOGS):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)

SPACE = "up"
# no interval saves, so nothing is in a shard file and the log has to carry it all.
# `timer` is enough for kill -9, which leaves the page cache alone
ARGS = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000",
        "-c", "aof_durability=timer"]


def start():
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA] + ARGS,
                         stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
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
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def c(r, *args):
    return r.execute_command(SPACE + ":" + args[0], *args[1:])


def state(r):
    """everything the checks look at, read the same way before and after"""
    return {
        "n": c(r, "GET", "n"),
        "f": c(r, "GET", "f"),
        "e_ttl": c(r, "TTL", "e") > 0,
        "p_ttl": c(r, "TTL", "p"),
        "g_ttl": c(r, "TTL", "g") > 0,
        "at": c(r, "PEXPIRETIME", "at"),
        "h_f": c(r, "HGET", "h", "f"),
        "h_g": c(r, "HGET", "h", "g"),
    }


proc = start()
try:
    r = client()
    r.execute_command("configuration:SET", SPACE + ".aof_dir", LOGS)
    r.execute_command("configuration:SAVE")

    c(r, "SET", "n", "1")
    c(r, "INCR", "n")
    c(r, "INCRBY", "n", "5")
    c(r, "DECR", "n")
    c(r, "DECRBY", "n", "2")                    # 1 + 1 + 5 - 1 - 2 = 4
    c(r, "SET", "f", "1.5")
    c(r, "INCRBYFLOAT", "f", "2")               # 3.5
    c(r, "SET", "e", "v")
    c(r, "EXPIRE", "e", "100000")               # gains a TTL
    c(r, "SET", "p", "v", "EX", "100000")
    c(r, "PERSIST", "p")                        # loses it
    c(r, "SET", "g", "v")
    c(r, "GETEX", "g", "EX", "100000")          # gains one
    c(r, "SET", "at", "v")
    # an exact expiry, so the check can say it came back to the millisecond
    c(r, "PEXPIREAT", "at", str(int(time.time() * 1000) + 100000000))
    c(r, "HSET", "h", "f", "1")
    c(r, "HINCRBY", "h", "f", "4")              # 5
    c(r, "HSET", "h", "g", "1")
    c(r, "HINCRBYFLOAT", "h", "g", "0.5")       # 1.5
    time.sleep(0.05)
    before = state(r)
    print("  before the kill:", before, flush=True)
finally:
    stop(proc, signal.SIGKILL)

proc = start()
try:
    r = client()
    after = state(r)
    print("  after the kill: ", after, flush=True)
finally:
    stop(proc)

check(before["n"] == b"4", "the INCR family ran (n is 4 before the kill)")
check(after["n"] == before["n"], "INCR, INCRBY, DECR and DECRBY survive the kill")
check(after["f"] == before["f"], "INCRBYFLOAT survives the kill")
check(after["e_ttl"], "EXPIRE survives the kill")
check(after["p_ttl"] == -1, "PERSIST survives the kill")
check(after["g_ttl"], "GETEX's expiry survives the kill")
check(before["at"] > 0 and after["at"] == before["at"], "PEXPIREAT comes back to the millisecond")
check(after["h_f"] == before["h_f"] and before["h_f"] == b"5", "HINCRBY survives the kill")
check(after["h_g"] == before["h_g"], "HINCRBYFLOAT survives the kill")

print("\n%s" % ("all aof update checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
