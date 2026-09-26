# A shard's leaves and nodes files are saved as a pair, and synced - TODO 464.
#
# `_save` writes the leaves file and then the nodes file. Each goes to
# `<file>.wal` and is renamed into place on its own, so when the nodes save
# fails the new leaves file is already there beside the old nodes file. SAVE
# reports the error, but the pair on disk is now from two different saves, and a
# load resolves the root in one against the arena in the other.
#
# The nodes save is made to fail by putting a non-empty directory where its wal
# goes: the save can't remove it and can't open it as a file, and nothing else is
# touched, so the leaves save before it runs as normal.
#
# The second half is the power cut, which can't be done here. What can be checked
# is the order of the system calls that make a save survive one: each shard file
# synced before it's renamed, and the directory synced after the renames and
# before the change log checkpoint that says they're on disk. That half runs
# under strace, and is skipped when strace isn't there or can't trace.
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
PORT = scale.port(default=14464)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "savepair_data")
LOGS = os.path.join(os.getcwd(), "savepair_logs")
TRACE = os.path.join(os.getcwd(), "savepair.strace")

SPACE = "sp"
KEYS = scale.env_int("SAVEPAIR_KEYS", 20000, floor=2000)
NO_SAVES = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000"]


def fresh():
    for d in (DATA, LOGS):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)


def wait_listening(p):
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


def start(prefix=(), extra=()):
    p = subprocess.Popen(list(prefix) + [BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                                         "--dir", DATA] + NO_SAVES + list(extra),
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return wait_listening(p)


def stop(p, sig=signal.SIGTERM):
    p.send_signal(sig)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=30)


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=120)


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def shard_file(kind):
    return os.path.join(DATA, "%s_%s_0.dat" % (kind, SPACE))


def stamp(kind):
    f = shard_file(kind)
    return os.stat(f).st_mtime_ns if os.path.exists(f) else 0


def save(r):
    """SAVE, and whether it said it worked"""
    try:
        reply = r.execute_command(SPACE + ":SAVE")
    except redis.ResponseError as e:
        print("  SAVE answered: %s" % e, flush=True)
        return False
    print("  SAVE answered: %r" % (reply,), flush=True)
    return reply in (b"OK", "OK", 0, True)


def fill(r, fmt, count, value):
    pipe = r.pipeline(transaction=False)
    for i in range(count):
        pipe.execute_command(SPACE + ":SET", fmt % i, value)
        if i % 5000 == 4999:
            pipe.execute()
    pipe.execute()


def configure(extra=()):
    proc = start()
    try:
        r = client()
        r.execute_command("configuration:SET", SPACE + ".shards", "1")
        for k, v in extra:
            r.execute_command("configuration:SET", SPACE + "." + k, v)
        r.execute_command("configuration:SAVE")
    finally:
        stop(proc)


# === part 1: a nodes save that fails leaves the last good save loadable ===========
print("part 1: the nodes file can't be written", flush=True)
fresh()
configure()

blocker = shard_file("nodes") + ".wal"
proc = start()
try:
    r = client()
    fill(r, "old%07d", KEYS, "old")
    check(save(r), "the first SAVE works")
    leaves0, nodes0 = stamp("leaves"), stamp("nodes")
    check(leaves0 and nodes0, "and wrote both files")

    # a different tree: half the keys gone and as many new ones, so the nodes
    # arena the new leaves file points into isn't the one on disk
    pipe = r.pipeline(transaction=False)
    for i in range(0, KEYS, 2):
        pipe.execute_command(SPACE + ":DEL", "old%07d" % i)
    pipe.execute()
    fill(r, "new%07d", KEYS, "new" * 20)

    os.makedirs(blocker)
    with open(os.path.join(blocker, "keep"), "w") as f:
        f.write("stops the wal being removed or opened")
    time.sleep(0.01)            # so a rewrite shows up in the mtime
    check(not save(r), "the second SAVE says it failed")
    print("  leaves rewritten: %s, nodes rewritten: %s"
          % (stamp("leaves") != leaves0, stamp("nodes") != nodes0), flush=True)
    # the setup, not the finding: what matters is what loads below
    check(stamp("nodes") == nodes0, "the nodes file wasn't replaced")
finally:
    # no shutdown save: what loads is what the failed SAVE left behind
    stop(proc, signal.SIGKILL)
shutil.rmtree(blocker, ignore_errors=True)

proc = None
try:
    proc = start()
    r = client()
    got = r.execute_command(SPACE + ":DBSIZE")
    check(got == KEYS, "the first save's %d keys load (%d do)" % (KEYS, got))
    pipe = r.pipeline(transaction=False)
    for i in range(KEYS):
        pipe.execute_command(SPACE + ":GET", "old%07d" % i)
    wrong = [i for i, v in enumerate(pipe.execute()) if v != b"old"]
    if wrong:
        print("  first wrong: old%07d" % wrong[0], flush=True)
    check(not wrong, "with the values they were saved with (%d aren't)" % len(wrong))
    check(r.execute_command(SPACE + ":GET", "new%07d" % 0) is None,
          "and none of the unsaved keys")
except (AssertionError, redis.RedisError) as e:
    print("  %s" % str(e)[-1500:], flush=True)
    check(False, "the server starts and answers after the failed save")
finally:
    if proc is not None and proc.poll() is None:
        stop(proc)


# === part 2: the syscalls that make a save survive a power cut ====================
print("part 2: sync order, under strace", flush=True)
strace = shutil.which("strace")
if strace is None:
    print("  SKIP: no strace", flush=True)
else:
    fresh()
    configure([("aof_dir", LOGS)])
    try:
        os.remove(TRACE)
    except OSError:
        pass
    traced = start([strace, "-f", "-y", "-qq", "-o", TRACE,
                    "-e", "trace=fsync,fdatasync,rename,renameat,renameat2"],
                   # the log synced only by the checkpoint, so a timer sync can't
                   # land between a rename and its directory sync and look like one
                   extra=["-c", "aof_durability=none"])
    try:
        r = client()
        fill(r, "k%07d", KEYS, "v")
        check(save(r), "SAVE works under strace")
    finally:
        # SIGTERM to strace would leave barchd running; the child is what stops
        kids = []
        try:
            with open("/proc/%d/task/%d/children" % (traced.pid, traced.pid)) as f:
                kids = [int(x) for x in f.read().split()]
        except OSError:
            pass
        for k in kids:
            os.kill(k, signal.SIGKILL)
        try:
            traced.wait(timeout=60)
        except subprocess.TimeoutExpired:
            traced.kill()
            traced.wait(timeout=30)

    lines = open(TRACE).read().splitlines() if os.path.exists(TRACE) else []
    if not any("rename" in l for l in lines):
        print("  SKIP: strace saw no renames - tracing isn't allowed here?", flush=True)
    else:
        data_dir = os.path.realpath(DATA)
        logs_dir = os.path.realpath(LOGS)
        shard_name = re.compile(r"/(leaves|nodes)_%s_\d+\.dat$" % SPACE)
        sync_call = re.compile(r"\b(fsync|fdatasync)\(\d+<([^>]*)>\)\s*=\s*0")
        rename_call = re.compile(r"\brename(?:at2?)?\(.*\"([^\"]+)\",.*\"([^\"]+)\"")

        synced = set()           # files synced since their last rename
        renamed = []             # shard files renamed and not yet dir-synced
        unsynced_renames = []    # renamed into place without a sync first
        early_checkpoints = 0    # a log sync with renamed shards not yet dir-synced
        shard_renames = 0
        for line in lines:
            m = sync_call.search(line)
            if m:
                path = os.path.realpath(m.group(2))
                synced.add(path)
                if path == data_dir:
                    renamed = []
                elif path.startswith(logs_dir + os.sep) and renamed:
                    early_checkpoints += 1
                continue
            m = rename_call.search(line)
            if m:
                # relative to barchd's working directory, which is --dir
                src, dst = (os.path.realpath(os.path.join(DATA, p)) for p in m.groups())
                if not shard_name.search(dst):
                    continue
                shard_renames += 1
                if src not in synced:
                    unsynced_renames.append(os.path.basename(src))
                synced.discard(src)
                renamed.append(dst)
        print("  %d shard file renames, %d without a sync of the file first"
              % (shard_renames, len(unsynced_renames)), flush=True)
        check(shard_renames > 0, "strace saw the shard files renamed into place")
        check(not unsynced_renames,
              "every shard file is synced before it's renamed (%d aren't)"
              % len(unsynced_renames))
        check(not renamed, "the data directory is synced after the renames")
        check(early_checkpoints == 0,
              "the change log isn't synced before that (%d times)" % early_checkpoints)


# === part 3: a save that stopped part way is finished or dropped on load ===========
# The crash states a save can leave, made from the files of two real saves: A,
# then B. A load has to come up with one whole save, never half of each.
print("part 3: loading after a save stopped part way", flush=True)


def two_saves():
    """the leaves and nodes files of save A and save B, as bytes"""
    fresh()
    configure()
    proc = start()
    try:
        r = client()
        fill(r, "old%07d", KEYS, "old")
        check(save(r), "save A works")
        a = {k: open(shard_file(k), "rb").read() for k in ("leaves", "nodes")}
        pipe = r.pipeline(transaction=False)
        for i in range(0, KEYS, 2):
            pipe.execute_command(SPACE + ":DEL", "old%07d" % i)
        pipe.execute()
        fill(r, "new%07d", KEYS, "new")
        check(save(r), "save B works")
        b = {k: open(shard_file(k), "rb").read() for k in ("leaves", "nodes")}
    finally:
        stop(proc, signal.SIGKILL)
    return a, b


def lay_out(files):
    """write {path: bytes}, and remove any wal not named"""
    for kind in ("leaves", "nodes"):
        try:
            os.remove(shard_file(kind) + ".wal")
        except OSError:
            pass
    for path, data in files.items():
        with open(path, "wb") as f:
            f.write(data)


def loads_as(which, what):
    proc = None
    try:
        proc = start()
        r = client()
        size = r.execute_command(SPACE + ":DBSIZE")
        if which == "A":
            want = KEYS
            ok = r.execute_command(SPACE + ":GET", "old%07d" % 0) == b"old" \
                and r.execute_command(SPACE + ":GET", "new%07d" % 0) is None
        else:
            want = KEYS // 2 + KEYS
            ok = r.execute_command(SPACE + ":GET", "old%07d" % 0) is None \
                and r.execute_command(SPACE + ":GET", "old%07d" % 1) == b"old" \
                and r.execute_command(SPACE + ":GET", "new%07d" % 0) == b"new"
        check(size == want and ok, "%s: loads as save %s (%d keys)" % (what, which, size))
        wals = [k for k in ("leaves", "nodes") if os.path.exists(shard_file(k) + ".wal")]
        check(not wals, "%s: and leaves no wal behind (%s)" % (what, ", ".join(wals) or "none"))
    except (AssertionError, redis.RedisError) as e:
        print("  %s" % str(e)[-1500:], flush=True)
        check(False, "%s: the server starts and answers" % what)
    finally:
        if proc is not None and proc.poll() is None:
            stop(proc, signal.SIGKILL)


a, b = two_saves()
L, N = shard_file("leaves"), shard_file("nodes")

# between the two renames: leaves is B, nodes is A, and B's nodes wal is whole
lay_out({L: b["leaves"], N: a["nodes"], N + ".wal": b["nodes"]})
loads_as("B", "stopped between the renames")

# both wals whole, neither renamed: the save never committed
lay_out({L: a["leaves"], N: a["nodes"], L + ".wal": b["leaves"], N + ".wal": b["nodes"]})
loads_as("A", "stopped before the renames")

# the leaves wal cut short: the save was still writing
lay_out({L: a["leaves"], N: a["nodes"], L + ".wal": b["leaves"][:len(b["leaves"]) // 2]})
loads_as("A", "stopped while writing")

print("\n%s" % ("all save pair checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
