# A save of a hash sharded space holds every write that changed two shards as
# one, on both sides or on neither - TODO 519.
#
# A hash sharded space used to save a shard at a time while writes carried on.
# RENAME takes both shards' locks, so it's atomic in memory, but the save could
# freeze the destination before a RENAME and the source after it. The files then
# held the key under neither name, and with no change log nothing could put it
# back. Now any write that changes two shards is counted, and until a whole save
# covers it no shard saves alone: SAVE and the interval save freeze the whole
# space instead.
#
# Checked, with no change log:
#   - RENAMEs bouncing keys between shards while SAVE runs, then kill -9 the
#     moment SAVE answers, so the files are exactly what it wrote: every key is
#     under exactly one of its two names, several rounds over
#
# Not the interval save: there's no telling from outside when one has finished,
# and a kill in the middle of any save, whole or not, leaves shard files from
# two moments. The interval save goes through the same check in shard::save.
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14519)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "crosssave_data")
N = 4000
ROUNDS = 4


# no interval save, so the only save is the SAVE each round asks for and nothing
# is half written when the kill comes
QUIET = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000"]


def start(extra=()):
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA, "--no-save-on-exit"] + QUIET + list(extra),
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
    p.send_signal(signal.SIGKILL)
    p.wait(timeout=30)


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=120)


failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def fill(r):
    pipe = r.pipeline(transaction=False)
    for i in range(N):
        pipe.set("a%05d" % i, "v%05d" % i)
    pipe.execute()


def bounce(stop, done):
    """RENAME each key to its other name and back, over and over"""
    r = client()
    where = ["a"] * N
    try:
        while not stop.is_set():
            pipe = r.pipeline(transaction=False)
            for i in range(N):
                other = "b" if where[i] == "a" else "a"
                pipe.rename("%s%05d" % (where[i], i), "%s%05d" % (other, i))
                where[i] = other
            pipe.execute()
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
        pass                        # the server was killed under it, which is the point
    finally:
        done.set()


def count(r):
    """(keys under neither name, keys under both, keys with a wrong value)"""
    pipe = r.pipeline(transaction=False)
    for i in range(N):
        pipe.get("a%05d" % i)
        pipe.get("b%05d" % i)
    got = pipe.execute()
    neither = both = wrong = 0
    for i in range(N):
        a, b = got[2 * i], got[2 * i + 1]
        if a is None and b is None:
            neither += 1
        elif a is not None and b is not None:
            both += 1
        elif (a or b) != ("v%05d" % i).encode():
            wrong += 1
    return neither, both, wrong


def round_trip(label, save_now, extra=()):
    shutil.rmtree(DATA, ignore_errors=True)
    os.makedirs(DATA)
    p = start(extra)
    try:
        r = client()
        fill(r)
        r.execute_command("SAVE")
        stop, done = threading.Event(), threading.Event()
        t = threading.Thread(target=bounce, args=(stop, done), daemon=True)
        t.start()
        time.sleep(0.3)             # well into the renames before the save starts
        save_now(r)
    finally:
        kill(p)
        stop.set()
    done.wait(30)
    p = start(extra)
    try:
        neither, both, wrong = count(client())
        check(neither == 0 and both == 0 and wrong == 0,
              "%s: lost %d, doubled %d, wrong %d" % (label, neither, both, wrong))
    finally:
        kill(p)


print("SAVE while RENAMEs move keys between shards, then kill -9", flush=True)
for n in range(ROUNDS):
    round_trip("SAVE, round %d" % (n + 1), lambda r: r.execute_command("SAVE"))


if failures:
    print("cross shard save test: %d FAILED" % failures)
    sys.exit(1)
print("cross shard save test passed")
