# Interval saves trim a hash-sharded space's change log - TODO 484.
#
# The change log is only trimmed after a checkpoint. Interval saves go one shard
# at a time, so none of them can checkpoint on its own (TODO 355), and only
# SAVE used to. Unless someone typed SAVE, the log kept every write the space
# ever took, grew without end, and a restart replayed all of it. Now each shard
# remembers the log mark its last save covered, and the space's maintenance
# thread checkpoints up to the lowest mark that still holds anything back.
#
# This lets interval saves write every shard of a logged hash-sharded space, with
# no SAVE, then kills the server and reads what the restart says it replayed.
# Everything was already in the shard files, so the replay should be small - at
# most the writes made after the last interval save, and here there are none.
#
# It also checks that SETs alone make a shard due for its interval save. They
# didn't: an ordered SET wasn't counted as a modification. The test deletes some
# keys as well, so the checkpoint half runs even if that ever breaks again.
import glob
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
PORT = scale.port(default=14484)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "aofinterval_data")
LOGS = os.path.join(os.getcwd(), "aofinterval_logs")
OUT = os.path.join(os.getcwd(), "aofinterval_restart.log")
for d in (DATA, LOGS):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)

SPACE = "iv"
SHARDS = 4
N = int(os.environ.get("AOFINTERVAL_KEYS", "20000"))
# interval saves every second; nothing else triggers one
ARGS = ["-c", "save_interval=1000", "-c", "max_modifications_before_save=1000000000",
        "-c", "aof_durability=timer"]


def start(out=subprocess.PIPE):
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA] + ARGS,
                         stdout=out, stderr=subprocess.STDOUT)
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
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=120)


def shard_files():
    return glob.glob(os.path.join(DATA, "leaves_%s_*.dat" % SPACE))


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


proc = start(subprocess.DEVNULL)
try:
    r = client()
    r.execute_command("configuration:SET", SPACE + ".aof_dir", LOGS)
    r.execute_command("configuration:SET", SPACE + ".shards", str(SHARDS))
    r.execute_command("configuration:SAVE")

    pipe = r.pipeline(transaction=False)
    for i in range(N):
        pipe.execute_command(SPACE + ":SET", "iv%07d" % i, "v%d" % i)
        if i % 5000 == 4999:
            pipe.execute()
    pipe.execute()
    written = time.time()
    check(r.execute_command(SPACE + ":DBSIZE") == N, "the space is filled")

    def wait_for_saves(after, seconds):
        deadline = time.time() + seconds
        saved = []
        while time.time() < deadline:
            saved = [f for f in shard_files() if os.path.getmtime(f) > after]
            if len(saved) >= SHARDS:
                break
            time.sleep(0.5)
        return saved

    # SETs alone should make every shard due. An ordered SET goes through
    # insert_unlogged -> art::insert, which didn't count in `inserts`, so
    # save_due saw no change and nothing was saved - found writing this test
    saved = wait_for_saves(written, 8)
    check(len(saved) >= SHARDS, "SETs alone make the interval save every shard (%d of %d)"
          % (len(saved), SHARDS))

    # deletes are counted, so these get the interval saves going either way and
    # the rest of the test is about the checkpoint
    pipe = r.pipeline(transaction=False)
    for i in range(0, N, 10):
        pipe.execute_command(SPACE + ":DEL", "iv%07d" % i)
    pipe.execute()
    N -= len(range(0, N, 10))
    deleted = time.time()
    saved = wait_for_saves(deleted, 60)
    check(len(saved) >= SHARDS, "interval saves wrote every shard after the deletes (%d of %d)"
          % (len(saved), SHARDS))
    # a few more intervals, for anything that would checkpoint on a later tick
    time.sleep(3)
finally:
    stop(proc, signal.SIGKILL)

with open(OUT, "wb") as out:
    proc = start(out)
    try:
        r = client()
        check(r.execute_command(SPACE + ":DBSIZE") == N, "every key is back after the kill")
    finally:
        stop(proc)

text = open(OUT, "rb").read().decode(errors="replace")
replayed = 0
m = re.search(r"replayed (\d+) writes, (\d+) deletes.*?into %s" % SPACE, text)
if m:
    replayed = int(m.group(1)) + int(m.group(2))
    print("  restart said: " + m.group(0), flush=True)
elif re.search(r"change log for %s\S* holds nothing after its last checkpoint" % SPACE, text):
    print("  restart said the log holds nothing after its last checkpoint", flush=True)
else:
    print("  no replay line for %s in the restart output" % SPACE, flush=True)
# nothing was written after the last interval save, so nothing needs replaying
check(replayed == 0, "the restart replayed nothing the interval saves had (%d records)"
      % replayed)

# --- writes carrying on while shards save and the log is checkpointed and trimmed
#
# The checkpoint above only has to move forward. This is the half that matters
# more: it must never cover a write that isn't in a shard file yet, or the trim
# after it drops the only copy. So writers keep setting, overwriting and deleting
# across several interval saves, the server is killed mid-stream, and every write
# that was answered has to be there after the restart.
import random
import threading

for d in (DATA, LOGS):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
SPACE2 = "iw"
WRITERS = 3
KEYS_EACH = 2000
RUN = float(os.environ.get("AOFINTERVAL_RUN", "8"))

proc = start(subprocess.DEVNULL)
expected = {}           # key -> value, or None when the last answered op was a DEL
pending = {}            # thread -> (key, value) in flight when the server died
ops = [0]
lock = threading.Lock()
halt = threading.Event()


def writer(n):
    w = client()
    rng = random.Random(n)
    v = 0
    while not halt.is_set():
        k = "w%d_%05d" % (n, rng.randrange(KEYS_EACH))
        v += 1
        val = None if rng.random() < 0.15 else "v%d_%d" % (n, v)
        pending[n] = (k, val)
        try:
            if val is None:
                w.execute_command(SPACE2 + ":DEL", k)
            else:
                w.execute_command(SPACE2 + ":SET", k, val)
        except (redis.RedisError, OSError):
            return              # the kill
        with lock:
            expected[k] = val
            ops[0] += 1
        pending.pop(n, None)


try:
    r = client()
    r.execute_command("configuration:SET", SPACE2 + ".aof_dir", LOGS)
    r.execute_command("configuration:SET", SPACE2 + ".shards", str(SHARDS))
    r.execute_command("configuration:SAVE")
    threads = [threading.Thread(target=writer, args=(n,)) for n in range(WRITERS)]
    for t in threads:
        t.start()
    time.sleep(RUN)
    saved = [f for f in glob.glob(os.path.join(DATA, "leaves_%s_*.dat" % SPACE2))]
    check(len(saved) >= SHARDS, "interval saves ran while the writers did (%d files)" % len(saved))
finally:
    # mid-stream, with the writers still going
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=30)
    halt.set()
    for t in threads:
        t.join()
print("  %d writes answered before the kill" % ops[0], flush=True)

OUT2 = os.path.join(os.getcwd(), "aofinterval_restart2.log")
with open(OUT2, "wb") as out:
    proc = start(out)
    try:
        r = client()
        keys = sorted(expected)
        pipe = r.pipeline(transaction=False)
        for k in keys:
            pipe.execute_command(SPACE2 + ":GET", k)
        got = dict(zip(keys, pipe.execute()))
    finally:
        stop(proc)
in_flight = {k: v for k, v in pending.values()}
wrong = []
for k, v in expected.items():
    have = got[k].decode() if got[k] is not None else None
    if have == v:
        continue
    # the write that was in flight when the server died may or may not be in
    if k in in_flight and have == in_flight[k]:
        continue
    wrong.append((k, v, have))
if wrong:
    print("  first wrong (key, expected, got):", wrong[:5], flush=True)
check(not wrong, "every answered write is back after the kill (%d wrong of %d keys)"
      % (len(wrong), len(expected)))

text = open(OUT2, "rb").read().decode(errors="replace")
m = re.search(r"replayed (\d+) writes, (\d+) deletes.*?into %s" % SPACE2, text)
replayed = int(m.group(1)) + int(m.group(2)) if m else 0
print("  the restart replayed %d of %d writes" % (replayed, ops[0]), flush=True)
# the log was trimmed while the writers ran, so only the tail since the last
# checkpoint is replayed rather than everything
check(replayed < ops[0] // 2, "the log was trimmed while the space ran")

print("\n%s" % ("all aof interval checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
