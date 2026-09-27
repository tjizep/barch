# A shard's two arena snapshots load as a pair or not at all - TODO 510.
#
# With arena_dir set, a clean stop writes a `.meta` beside each mapped arena, and
# the next start maps the pages back instead of loading the shard files. Each
# arena used to do that on its own. When only one of the two snapshots could be
# used, one arena came from the snapshot and the other from an older shard file,
# and the root pointed into pages from another time: barchd died on start with
# "unknown or invalid node type", or started up with wrong values.
#
# The shard files only fall behind the snapshots when the save at exit fails, so
# that's how this gets there: the data dir is made read-only before the stop.
#
# Checked here:
#   1. a leaves `.meta` missing: every key reads as the shard files have it
#   2. a nodes `.meta` missing: the same
#   3. a leaves `.meta` from an earlier stop beside a newer nodes one: the same
#   4. both present: every key reads as it was at the stop
#   5. arena_map=leaves writes no snapshot, and the files load
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
PORT = scale.port(default=14370)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)
if os.geteuid() == 0:
    print("SKIP: root can write to a read-only data dir")
    sys.exit(0)

N = 6000


def start(data, arenas, *config):
    extra = []
    for c in config:
        extra += ["--config", c]
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", data,
                          "--config", "arena_dir=" + arenas] + extra,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            out = p.stdout.read().decode(errors="replace")
            raise AssertionError("barchd exited with %s:\n%s" % (p.returncode, out[-2000:]))
        try:
            socket.create_connection(("127.0.0.1", PORT), 0.5).close()
            return p, redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=60)
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % PORT)


def stop(p):
    p.send_signal(signal.SIGTERM)
    out = p.communicate(timeout=120)[0].decode(errors="replace")
    assert p.returncode == 0, "barchd exited with %s:\n%s" % (p.returncode, out[-2000:])
    return out


def fill(r, prefix, value):
    for s in range(0, N, 2000):
        r.mset({"%s%06d" % (prefix, i): "%s%d" % (value, i) for i in range(s, s + 2000)})


def metas(arenas, kind=""):
    return sorted(f for f in os.listdir(arenas) if f.endswith(".meta") and f.startswith(kind))


def check(r, value, extra):
    """every a key holds `value`, and the b keys are there only when `extra`"""
    wrong = [i for i in range(N) if r.get("a%06d" % i) != ("%s%d" % (value, i)).encode()]
    assert not wrong, "%d a keys don't read %r, the first a%06d = %r" % (
        len(wrong), value, wrong[0], r.get("a%06d" % wrong[0]))
    b = sum(1 for i in range(0, N, 50) if r.get("b%06d" % i) is not None)
    assert b == (len(range(0, N, 50)) if extra else 0), "%d b keys" % b


def unsaved_stop(name):
    """a stop whose save fails: the shard files hold `old`, the snapshots `new` and the b keys"""
    root = os.path.join(os.getcwd(), "snappair_" + name)
    shutil.rmtree(root, ignore_errors=True)
    data, arenas = os.path.join(root, "data"), os.path.join(root, "arenas")
    os.makedirs(data)
    os.makedirs(arenas)
    p, r = start(data, arenas)
    fill(r, "a", "old")
    r.execute_command("SAVE")
    fill(r, "a", "new")
    fill(r, "b", "x")
    os.chmod(data, 0o555)
    try:
        stop(p)
    finally:
        os.chmod(data, 0o755)
    assert metas(arenas, "leaves") and metas(arenas, "nodes"), "the stop wrote no snapshots"
    return data, arenas


for kind in ("leaves", "nodes"):
    print("a %s .meta missing loads both arenas from the files" % kind, flush=True)
    data, arenas = unsaved_stop(kind)
    for f in metas(arenas, kind):
        os.remove(os.path.join(arenas, f))
    p, r = start(data, arenas)
    try:
        check(r, "old", extra=False)
    finally:
        log = stop(p)
    assert "aren't a pair" in log, "nothing said the snapshots were refused"

print("a leaves .meta from an earlier stop isn't paired with a newer nodes one", flush=True)
root = os.path.join(os.getcwd(), "snappair_stale")
shutil.rmtree(root, ignore_errors=True)
data, arenas = os.path.join(root, "data"), os.path.join(root, "arenas")
old_metas = os.path.join(root, "old_metas")
os.makedirs(data)
os.makedirs(arenas)
os.makedirs(old_metas)
p, r = start(data, arenas)
fill(r, "a", "old")
stop(p)
for f in metas(arenas, "leaves"):
    shutil.copy(os.path.join(arenas, f), old_metas)
p, r = start(data, arenas)
fill(r, "a", "new")
fill(r, "b", "x")
stop(p)                             # saved this time, so the files hold `new`
for f in os.listdir(old_metas):
    shutil.copy(os.path.join(old_metas, f), arenas)
p, r = start(data, arenas)
try:
    check(r, "new", extra=True)
finally:
    log = stop(p)
assert "aren't a pair" in log, "nothing said the snapshots were refused"

print("both snapshots present map back", flush=True)
data, arenas = unsaved_stop("both")
p, r = start(data, arenas)
try:
    check(r, "new", extra=True)
    assert not metas(arenas), "a snapshot survived being read"
finally:
    log = stop(p)
assert "aren't a pair" not in log, log[-1500:]
assert not [f for f in os.listdir(arenas) if f.endswith(".meta.wal")], "a snapshot .wal was left behind"

print("with arena_map picking one arena, no snapshot is written and the files load", flush=True)
root = os.path.join(os.getcwd(), "snappair_half")
shutil.rmtree(root, ignore_errors=True)
data, arenas = os.path.join(root, "data"), os.path.join(root, "arenas")
os.makedirs(data)
os.makedirs(arenas)
p, r = start(data, arenas, "arena_map=leaves")
fill(r, "a", "new")
fill(r, "b", "x")
stop(p)
assert not metas(arenas), "a half mapped shard wrote %s" % metas(arenas)[:3]
p, r = start(data, arenas, "arena_map=leaves")
try:
    check(r, "new", extra=True)
finally:
    stop(p)

print("snapshot pair test passed")
