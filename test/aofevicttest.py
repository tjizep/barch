# A key the eviction sweep took stays gone after a crash - TODO 483.
#
# The change log only hears about SET and DEL. The LRU, LFU and random sweeps, the
# expiry sweep and defrag remove keys through shard::evict, which never appends an
# erase. So a space that keeps itself under max_memory by evicting comes back from a
# kill -9 holding every key written since the last checkpoint, evicted or not, and
# the replay that puts them back runs into the same memory limit the sweep was
# holding it under. Replay inserts through shard::insert, which throws "not enough
# memory" past the limit, nothing catches it, and the space doesn't open.
#
# It restarts three times from the same log: under the same max_memory, with none,
# and under a quarter of it. Each time the space has to open, and a key that was
# gone before the kill has to still be gone after it. The last one needs the
# replay itself to go over the limit, which is allowed so nothing is refused.
#
# Its own process and its own barchd, like the other aof tests: it drops max_memory
# far enough that the sweep takes most of the space.
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
PORT = scale.port(default=14483)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "aofevict_data")
LOGS = os.path.join(os.getcwd(), "aofevict_logs")
for d in (DATA, LOGS):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)

SPACE = "ev"
N = int(os.environ.get("AOFEVICT_KEYS", "40000"))
VALUE = "x" * 200
# no interval saves, so everything the space ever held is in the log and nothing
# has been checkpointed - the case where the log alone has to be right.
# `timer` is enough for kill -9, which leaves the page cache alone
ARGS = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000",
        "-c", "aof_durability=timer"]


def start(extra=(), out=subprocess.PIPE):
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA] + ARGS + list(extra),
                         stdout=out, stderr=subprocess.STDOUT)
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


def stat(r, name):
    s = r.execute_command("STATS")
    for i in range(0, len(s) - 1, 2):
        k = s[i].decode() if isinstance(s[i], bytes) else str(s[i])
        if k.lstrip("$") == name:
            return int(s[i + 1])
    raise AssertionError("no such stat: " + name)


def key(i):
    return "ev%07d" % i


def which_present(r):
    pipe = r.pipeline(transaction=False)
    for i in range(N):
        pipe.execute_command(SPACE + ":EXISTS", key(i))
    return {i for i, v in enumerate(pipe.execute()) if v}


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


proc = start()
limit = 0
before = set()
try:
    r = client()
    r.execute_command("configuration:SET", SPACE + ".aof_dir", LOGS)
    r.execute_command("configuration:SET", SPACE + ".shards", "2")
    r.execute_command("configuration:SAVE")

    pipe = r.pipeline(transaction=False)
    for i in range(N):
        pipe.execute_command(SPACE + ":SET", key(i), VALUE)
        if i % 5000 == 4999:
            pipe.execute()
    pipe.execute()
    check(r.execute_command(SPACE + ":DBSIZE") == N, "the space is filled")

    # the sweep for a named space is switched on the space, not with CONFIG
    u = client()
    u.execute_command("USE", SPACE)
    u.execute_command("KSPACE", "OPTION", "SET", "LRU", "ON")

    # eviction only starts once logical_allocated passes max_memory *
    # pre_evict_thresh, so bring the ceiling down under what's already held
    held = stat(r, "logical_allocated")
    limit = max(held // 2, 1 << 20)
    r.execute_command("CONFIG", "SET", "max_memory_bytes", str(limit))

    evicted0 = stat(r, "keys_evicted")
    deadline = time.time() + 60
    while time.time() < deadline and stat(r, "keys_evicted") == evicted0:
        time.sleep(0.2)
    # and let it settle, so the set taken below is what the space really holds
    last = -1
    while time.time() < deadline:
        size = r.execute_command(SPACE + ":DBSIZE")
        if size == last:
            break
        last = size
        time.sleep(1)
    took = stat(r, "keys_evicted") - evicted0
    check(took > 0, "the sweep evicted something (%d keys)" % took)
    before = which_present(r)
    print("  %d of %d keys held before the kill, limit %d bytes" % (len(before), N, limit),
          flush=True)
    check(len(before) < N, "some keys are gone before the kill")
finally:
    # no shutdown save: what comes back has to come from the change log
    stop(proc, signal.SIGKILL)


def restarted(extra, what, out=subprocess.PIPE):
    """start, check the space opens and nothing evicted came back, stop"""
    p = start(extra, out)
    try:
        r = client()
        try:
            size = r.execute_command(SPACE + ":DBSIZE")
        except redis.ResponseError as e:
            print("  the space did not open:", e, flush=True)
            check(False, "the space opens " + what)
            return
        check(True, "the space opens " + what)
        after = which_present(r)
        back = sorted(after - before)
        if back:
            print("  %d evicted keys came back, the first: %s"
                  % (len(back), [key(i) for i in back[:5]]), flush=True)
        check(not back, "no evicted key is back " + what)
        check(size <= len(before), "no bigger than before the kill (%d, was %d)"
              % (size, len(before)))
    finally:
        # SIGKILL again, so the first restart leaves the log as it found it for
        # the second - nothing is saved or checkpointed on the way out
        stop(p, signal.SIGKILL)


# the ceiling the space was running under: replay hits it
restarted(["-c", "max_memory_bytes=%d" % limit], "under the same max_memory")
# and without one, which gets past the replay and shows what it put back
restarted([], "with no max_memory")
# and under a lower one than the space ran with, so the replay has to go over it:
# it's let through and the first maintenance pass trims. Last, since the evictions
# that pass makes are logged and change what the next start would see
QUARTER = os.path.join(os.getcwd(), "aofevict_quarter.log")
with open(QUARTER, "wb") as out:
    restarted(["-c", "max_memory_bytes=%d" % (limit // 4)], "under a quarter of the max_memory",
              out)
# and it did go over, or the case above tested nothing new
text = open(QUARTER, "rb").read().decode(errors="replace")
check("over max_memory" in text, "the replay under a quarter went over the limit")

print("\n%s" % ("all aof eviction checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
