# A RETRIEVE that doesn't finish leaves the change log in step with the files -
# TODO 524.
#
# RETRIEVE put each shard's received files in place and only then wrote the
# checkpoint that makes the log's older records dead. So a shard that failed to
# install, or a crash between the first shard going in and the checkpoint, left
# every record from before the RETRIEVE live, and a restart replayed them over the
# files that did go in. Now it saves every shard, writes the checkpoint, and only
# then installs: each shard is the copy or its own save, whatever happens next.
#
# Part 1: a shard that can't be saved (a directory where its .wal has to go, with
# a file in it so nothing clears it on the way). Nothing is installed, and a
# restart doesn't change what the space holds.
#
# Part 2: a kill -9 as soon as the first shard's received files are renamed in.
# After the restart every shard is exactly the copy or exactly its own keys.
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
LOCAL = scale.port(default=14524)
REMOTE = LOCAL + 1

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

LOCAL_DATA = os.path.join(os.getcwd(), "retrievecrash_local")
REMOTE_DATA = os.path.join(os.getcwd(), "retrievecrash_remote")
LOGS = os.path.join(os.getcwd(), "retrievecrash_logs")
ARGS = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000",
        "-c", "aof_durability=timer"]
SPACE = "rx"
SHARDS = 4
FAILING = 2                     # the shard whose install is made to fail
N = 200


def start(port, data):
    p = subprocess.Popen([BINARY, "--port", str(port), "--bind", "127.0.0.1",
                          "--dir", data, "--no-save-on-exit"] + ARGS,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            raise AssertionError("barchd exited with %s" % p.returncode)
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % port)


def kill(p):
    if p is not None and p.poll() is None:
        p.send_signal(signal.SIGKILL)
        p.wait(timeout=30)


def client(port):
    return redis.Redis(host="127.0.0.1", port=port, protocol=2, socket_timeout=120)


failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def cmd(r, *args):
    return r.execute_command(SPACE + ":" + args[0], *args[1:])


def configure(r, log):
    r.execute_command("configuration:SET", SPACE + ".shards", str(SHARDS))
    if log:
        r.execute_command("configuration:SET", SPACE + ".aof_dir", LOGS)
    r.execute_command("configuration:SAVE")


def fill(r, prefix):
    pipe = r.pipeline(transaction=False)
    for i in range(N):
        pipe.execute_command(SPACE + ":SET", "%s%d" % (prefix, i), "%s%d" % (prefix, i))
    pipe.execute()


def contents(r):
    keys = sorted(k.decode() for k in cmd(r, "KEYS", "*"))
    pipe = r.pipeline(transaction=False)
    for k in keys:
        pipe.execute_command(SPACE + ":GET", k)
    return dict(zip(keys, pipe.execute()))


def describe(before, after):
    back = sorted(k for k in after if k not in before)
    gone = sorted(k for k in before if k not in after)
    changed = sorted(k for k in before if k in after and before[k] != after[k])
    by_prefix = {}
    for k in back:
        p = k.rstrip("0123456789")
        by_prefix[p] = by_prefix.get(p, 0) + 1
    return "%d came back %s, %d went, %d changed" % (len(back), by_prefix, len(gone), len(changed))


def shard_of(r, keys):
    """where each key lives, by the routing the store itself uses. Not INFO SHARD:
    that hashes the argument as given rather than as a key is stored, and puts
    keys on shards they aren't on"""
    r.execute_command(SPACE + ":SETF", "shardof", """
        function call(k)
            return barch.store.shardNumber(k)
        end
    """)
    pipe = r.pipeline(transaction=False)
    for k in keys:
        pipe.execute_command(SPACE + ":shardof", k)
    return dict(zip(keys, pipe.execute()))


def present(r, keys):
    pipe = r.pipeline(transaction=False)
    for k in keys:
        pipe.execute_command(SPACE + ":GET", k)
    return {k for k, v in zip(keys, pipe.execute()) if v == k.encode()}


# ------------------------------------------------------------------ part 1
# A shard whose files can't be written. The save RETRIEVE now makes before it
# replaces anything fails, so nothing is installed: RETRIEVE says so, the space is
# as it was, and a restart doesn't change it.
print("a RETRIEVE into a space with a shard that can't be saved (TODO 524)", flush=True)
for d in (LOCAL_DATA, REMOTE_DATA, LOGS):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
local = start(LOCAL, LOCAL_DATA)
remote = start(REMOTE, REMOTE_DATA)
try:
    lc, rc = client(LOCAL), client(REMOTE)
    configure(lc, log=True)
    configure(rc, log=False)

    # saved, so there's a checkpoint - then more, in the log only, past it
    fill(lc, "own")
    cmd(lc, "SAVE")
    fill(lc, "late")
    time.sleep(2)                       # past the timer's sync
    fill(rc, "src")

    planted = os.path.join(LOCAL_DATA, "leaves_%s_%d.dat.wal" % (SPACE, FAILING))
    os.makedirs(planted)
    open(os.path.join(planted, "keep"), "w").close()
    try:
        cmd(lc, "RETRIEVE", "127.0.0.1", str(REMOTE))
        said = ""
    except redis.exceptions.ResponseError as e:
        said = str(e)
    check("nothing here changed" in said, "RETRIEVE refuses, and says nothing changed")

    before = contents(lc)
    kinds = {p: sum(1 for k in before if k.rstrip("0123456789") == p)
             for p in ("own", "late", "src")}
    check(kinds == {"own": N, "late": N, "src": 0},
          "the space is as it was (%s)" % kinds)

    shutil.rmtree(planted)
    kill(local)
    local = start(LOCAL, LOCAL_DATA)
    after = contents(client(LOCAL))
    check(after == before, "a restart doesn't change what the space holds (%s)"
          % describe(before, after))
finally:
    kill(local)
    kill(remote)

# ------------------------------------------------------------------ part 2
# A crash partway through the installs. The kill lands as soon as the first shard's
# received files have been renamed in, so some shards have the source's copy and
# some don't. Whatever the moment, each shard has to come back as exactly one of
# the two: the source's keys for that shard, or its own keys with the writes the
# log alone had. A shard with the source's keys and its own old writes replayed
# over them is TODO 524.
print("a RETRIEVE killed partway through putting the shards in (TODO 524)", flush=True)
BIG = 40000                         # so installing takes long enough to land in
DELAYS = [0.002, 0.01, 0.03, 0.08, 0.2, 0.4]
ATTEMPTS = len(DELAYS)
shutil.rmtree(REMOTE_DATA, ignore_errors=True)
os.makedirs(REMOTE_DATA)
remote = start(REMOTE, REMOTE_DATA)
local = None
landed = 0
try:
    rc = client(REMOTE)
    configure(rc, log=False)
    pipe = rc.pipeline(transaction=False)
    for i in range(BIG):
        pipe.execute_command(SPACE + ":SET", "src%d" % i, "src%d" % i)
    pipe.execute()
    src_keys = ["src%d" % i for i in range(BIG)]

    for attempt in range(ATTEMPTS):
        for d in (LOCAL_DATA, LOGS):
            shutil.rmtree(d, ignore_errors=True)
            os.makedirs(d)
        local = start(LOCAL, LOCAL_DATA)
        lc = client(LOCAL)
        configure(lc, log=True)
        fill(lc, "own")
        cmd(lc, "SAVE")
        fill(lc, "late")
        time.sleep(2)                   # past the timer's sync

        # RETRIEVE on a thread of its own; this one watches for the first rename
        def go():
            try:
                client(LOCAL).execute_command(SPACE + ":RETRIEVE", "127.0.0.1", str(REMOTE))
            except redis.RedisError:
                pass                    # killed under it
        t = threading.Thread(target=go, daemon=True)
        t.start()
        pending = lambda: [f for f in os.listdir(LOCAL_DATA) if f.endswith(".retrieve")]
        deadline = time.time() + 120
        seen_all = False
        while time.time() < deadline:
            n = len(pending())
            if n == 2 * SHARDS:
                seen_all = True
            elif seen_all and n < 2 * SHARDS:
                break                   # the first shard's files are being renamed in
            if not t.is_alive():
                break
            time.sleep(0.0005)
        # A shard renamed in but not committed is rolled back at the next start, so
        # right at the first rename is too early to find one in. Each attempt waits
        # a little longer, to land somewhere between the first commit and the end
        time.sleep(DELAYS[attempt])
        finished = not t.is_alive()
        kill(local)
        t.join(timeout=30)

        local = start(LOCAL, LOCAL_DATA)
        lc = client(LOCAL)
        own_keys = ["own%d" % i for i in range(N)] + ["late%d" % i for i in range(N)]
        where = shard_of(lc, own_keys + src_keys)
        have = present(lc, own_keys + src_keys)
        installed = {where[k] for k in have if k.startswith("src")}
        wrong = []
        for s in range(SHARDS):
            mine = {k for k in own_keys if where[k] == s}
            theirs = {k for k in src_keys if where[k] == s}
            held = {k for k in have if where[k] == s}
            if s in installed and held != theirs:
                wrong.append("shard %d: the copy with %d of its own keys"
                             % (s, len(held & mine)))
            if s not in installed and held != mine:
                wrong.append("shard %d: its own keys, %d of %d"
                             % (s, len(held & mine), len(mine)))
        print("    attempt %d: killed %.0f ms after the first rename, %s, with %d of %d shards in"
              % (attempt + 1, DELAYS[attempt] * 1000,
                 "after RETRIEVE had answered" if finished else "RETRIEVE still running",
                 len(installed), SHARDS), flush=True)
        check(not wrong, "every shard is the copy or its own, whole (%s)"
              % ("; ".join(wrong[:3]) or "yes"))
        kill(local)
        local = None
        if 0 < len(installed):
            landed += 1
            if len(installed) < SHARDS:
                break                   # a mixed one is what this is for
    check(landed > 0, "at least one kill landed after a shard went in (%d of %d)"
          % (landed, ATTEMPTS))
finally:
    kill(local)
    kill(remote)

print("\n%s" % ("all RETRIEVE crash checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
