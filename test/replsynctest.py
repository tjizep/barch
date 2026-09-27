# Replication never goes on over a gap without saying so - TODO 502.
#
# It used to, four ways. A replica that couldn't apply a record skipped it for
# good, and the primary counted the refusal as delivered. A batch that started
# past the next sequence was logged and applied. A primary that restarted came
# back with a new random origin at sequence 1, and what it hadn't sent was gone.
# And a replica that restarted forgot every position, so the gap check was off.
#
# Now a replica refuses a batch it can't take in order (NEEDSYNC), stops at a
# record it can't apply (REPLFAILED), and keeps its position with each primary in
# a file, only as far as its shard files hold. A primary treats any refusal as
# "this replica needs a full copy", and after a clean stop carries on where it
# left off. RETRIEVE, then PUBLISH, is how a replica that needs a copy gets back.
#
# Part 1 sends REPLAPPLY batches straight to a barchd, to check the replica's
# rules one at a time. Part 2 runs a primary and a replica, both barchd, and
# stops and kills each.
import os
import shutil
import signal
import socket
import subprocess
import sys
import time

import scale
import redis
import barch

scale.workdir()
PRIMARY = scale.port(default=14502)
REPLICA = scale.port(1, default=14503)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

ROOT = os.getcwd()
PRIMARY_DATA = os.path.join(ROOT, "replsync_primary")
REPLICA_DATA = os.path.join(ROOT, "replsync_replica")
PRIMARY_LOG = os.path.join(ROOT, "replsync_primary.log")
N = 200
WAIT = 20


def start(port, data, log=None):
    out = open(log, "ab") if log else subprocess.DEVNULL
    p = subprocess.Popen([BINARY, "--port", str(port), "--bind", "127.0.0.1", "--dir", data],
                         stdout=out, stderr=subprocess.STDOUT)
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


def stop(p, sig=signal.SIGTERM):
    if p is None or p.poll() is not None:
        return
    p.send_signal(sig)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=30)


def client(port):
    return redis.Redis(host="127.0.0.1", port=port, protocol=2, socket_timeout=30)


failures = 0


def check(ok, what):
    global failures
    print("  %-70s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def fresh_dir(d):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)


# ------------------------------------------------------------------ part 1
def replapply(origin, first, *records):
    """[applied] when the batch is taken, [] when it's refused"""
    c = barch.Caller("127.0.0.1", REPLICA)
    args = [barch.Value(origin), barch.Value(str(first))] + [barch.Value(r) for r in records]
    return [v.i() for v in c.call("REPLAPPLY", args)]


def taken(reply):
    return len(reply) == 1


print("the replica's rules", flush=True)
fresh_dir(REPLICA_DATA)
REPLICA_LOG = os.path.join(ROOT, "replsync_replica.log")
if os.path.exists(REPLICA_LOG):
    os.remove(REPLICA_LOG)


def said(log, text):
    with open(log, "rb") as f:
        return text.encode() in f.read()


replica = start(REPLICA, REPLICA_DATA, REPLICA_LOG)
try:
    A, B = "aaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb"
    check(not taken(replapply(A + "/i1", 5)),
          "a batch from a primary it doesn't follow, not fresh, is refused")
    check(taken(replapply(A + "/i1/fresh", 1)),
          "a fresh one starts following it")
    check(taken(replapply(A + "/i1", 3)),
          "a gap from a primary that writes no space here goes on: no copy could lack it")
    check(not taken(replapply(A + "/i2", 1)),
          "a batch from another incarnation that isn't a fresh start is refused")
    # the primary names a space it dropped writes for (TODO 504): held, not refused
    check(not taken(replapply(A + "/i2/fresh:x" + "lostspace".encode().hex() + "@7", 1)),
          "a fresh start that names a space with dropped writes isn't taken")
    check(said(REPLICA_LOG, "held:"), "it's held, not refused (TODO 505)")
    check(taken(replapply(B + "/i1/fresh", 1)),
          "while another primary can start being followed")

    stop(replica, signal.SIGKILL)
    replica = start(REPLICA, REPLICA_DATA, REPLICA_LOG)
    check(taken(replapply(B + "/i1", 1)),
          "after a kill, the position it wrote down when it started is kept")
    check(not taken(replapply(A + "/i2/fresh", 1)),
          "and so is the space the other primary still needs retrieved")
    check(not taken(replapply(B + "/i1", 2, "not a record")),
          "a record that can't be applied stops the batch")
    check(taken(replapply(B + "/i1", 2)),
          "and a primary that writes no space here goes on after it")

    r = client(REPLICA)
    r.execute_command("LOAD")
    check(taken(replapply(B + "/i1", 2)),
          "a LOAD of a space no primary writes here leaves them alone")
finally:
    stop(replica)


# ------------------------------------------------------------------ part 2
def write(prefix, n=N):
    p = client(PRIMARY)
    pipe = p.pipeline(transaction=False)
    for i in range(n):
        pipe.set("%s%d" % (prefix, i), "%s%d" % (prefix, i))
    pipe.execute()


def arrived(prefix, n=N, seconds=WAIT):
    deadline = time.time() + seconds
    have = 0
    while True:
        try:
            pipe = client(REPLICA).pipeline(transaction=False)
            for i in range(n):
                pipe.get("%s%d" % (prefix, i))
            values = pipe.execute()
            have = sum(1 for i, v in enumerate(values)
                       if v == ("%s%d" % (prefix, i)).encode())
        except redis.RedisError:
            have = 0
        if have == n or time.time() >= deadline:
            if 0 < have < n and os.environ.get("REPLSYNC_DEBUG"):
                print("   ", [("%s%d" % (prefix, i)) for i, v in enumerate(values)
                            if v == ("%s%d" % (prefix, i)).encode()][:5], flush=True)
            return have
        time.sleep(0.5)


def primary_said(text):
    with open(PRIMARY_LOG, "rb") as f:
        return text.encode() in f.read()


def publish():
    return client(PRIMARY).execute_command("PUBLISH", "127.0.0.1", str(REPLICA))


def retrieve():
    return client(REPLICA).execute_command("RETRIEVE", "127.0.0.1", str(PRIMARY))


print("a primary and a replica", flush=True)
fresh_dir(PRIMARY_DATA)
fresh_dir(REPLICA_DATA)
if os.path.exists(PRIMARY_LOG):
    os.remove(PRIMARY_LOG)
primary = start(PRIMARY, PRIMARY_DATA, PRIMARY_LOG)
replica = start(REPLICA, REPLICA_DATA)
try:
    publish()
    write("a")
    check(arrived("a") == N, "writes reach the replica")

    # the replica is killed with nothing saved: what it had is gone
    stop(replica, signal.SIGKILL)
    replica = start(REPLICA, REPLICA_DATA)
    write("b")
    got = arrived("b", seconds=6)
    check(got == 0, "a killed replica takes nothing more (%d of %d)" % (got, N))
    time.sleep(1)
    check(primary_said("is held"), "and the primary keeps its writes, held (TODO 505)")

    # RETRIEVE, and the held stream goes on - no PUBLISH needed
    check(retrieve() in (b"OK", "OK"), "RETRIEVE on the replica works")
    check(arrived("b") == N, "then the held writes arrive")
    write("c")
    check(arrived("c") == N, "and new ones")
    check(arrived("a", seconds=1) == N, "and it has what it lost")

    # a clean stop of the replica keeps its place
    stop(replica)
    replica = start(REPLICA, REPLICA_DATA)
    write("d")
    check(arrived("d") == N, "a replica stopped cleanly carries on")

    # a clean stop of the primary: it carries on too, without a new PUBLISH
    stop(primary)
    primary = start(PRIMARY, PRIMARY_DATA, PRIMARY_LOG)
    write("e")
    check(arrived("e") == N, "a primary stopped cleanly carries on, to the same replicas")

    # a primary killed with writes queued: they're gone, and the replica knows
    stop(replica)
    write("f")
    stop(primary, signal.SIGKILL)
    replica = start(REPLICA, REPLICA_DATA)
    primary = start(PRIMARY, PRIMARY_DATA, PRIMARY_LOG)
    publish()
    write("g")
    got = arrived("g", seconds=6)
    check(got == 0, "after a primary is killed, the replica takes nothing more (%d of %d)"
          % (got, N))
    check(retrieve() in (b"OK", "OK"), "until RETRIEVE")
    write("h")
    # "f" died with the primary, which never saved it, so it's in neither
    check(arrived("h") == N and arrived("g", seconds=1) == N,
          "after which it has what the primary has, the held writes too")
finally:
    stop(replica)
    stop(primary)

# ------------------------------------------------------------------ part 3
# Two primaries, X writing space sx and Y writing sy, and one replica - TODO 504.
# A RETRIEVE of X's space used to forget every primary, so Y was refused, and a
# PUBLISH from Y afterwards started over past whatever it had dropped. Now a
# RETRIEVE touches only the space's own primary, a space takes writes from one
# primary, and a refused primary needs every space it writes here retrieved from
# it - the ones it names as dropped too - before it starts over.
OTHER = scale.port(2, default=14504)
OTHER_DATA = os.path.join(ROOT, "replsync_other")
OTHER_LOG = os.path.join(ROOT, "replsync_other.log")


def put(port, space, prefix, n=N):
    pipe = client(port).pipeline(transaction=False)
    for i in range(n):
        pipe.execute_command(space + ":SET", "%s%d" % (prefix, i), "%s%d" % (prefix, i))
    pipe.execute()


def holds(space, prefix, n=N, seconds=WAIT):
    deadline = time.time() + seconds
    have = 0
    while True:
        try:
            pipe = client(REPLICA).pipeline(transaction=False)
            for i in range(n):
                pipe.execute_command(space + ":GET", "%s%d" % (prefix, i))
            have = sum(1 for i, v in enumerate(pipe.execute())
                       if v == ("%s%d" % (prefix, i)).encode())
        except redis.RedisError:
            have = 0
        if have == n or time.time() >= deadline:
            return have
        time.sleep(0.5)


def publish_from(port):
    return client(port).execute_command("PUBLISH", "127.0.0.1", str(REPLICA))


def retrieve_from(space, port):
    return client(REPLICA).execute_command(space + ":RETRIEVE", "127.0.0.1", str(port))


print("two primaries and a replica (TODO 504)", flush=True)
for d in (PRIMARY_DATA, OTHER_DATA, REPLICA_DATA):
    fresh_dir(d)
for f in (PRIMARY_LOG, OTHER_LOG):
    if os.path.exists(f):
        os.remove(f)
x = start(PRIMARY, PRIMARY_DATA, PRIMARY_LOG)
y = start(OTHER, OTHER_DATA, OTHER_LOG)
if os.path.exists(REPLICA_LOG):
    os.remove(REPLICA_LOG)
replica = start(REPLICA, REPLICA_DATA, REPLICA_LOG)
try:
    publish_from(PRIMARY)
    publish_from(OTHER)
    put(PRIMARY, "sx", "a")
    put(OTHER, "sy", "b")
    check(holds("sx", "a") == N and holds("sy", "b") == N, "both primaries' writes arrive")

    check(retrieve_from("sx", PRIMARY) in (b"OK", "OK"), "RETRIEVE of X's space works")
    put(OTHER, "sy", "c")
    check(holds("sy", "c") == N, "Y's writes still arrive after a RETRIEVE of X's space")
    publish_from(OTHER)
    put(OTHER, "sy", "cc")
    check(holds("sy", "cc") == N, "and after Y PUBLISHes again")
    publish_from(PRIMARY)
    put(PRIMARY, "sx", "d")
    check(holds("sx", "d") == N, "X carries on once PUBLISHed again after its RETRIEVE")

    # Y writes into X's space: refused, and Y drops what it had queued
    client(OTHER).execute_command("sx:SET", "intruder", "y")
    time.sleep(3)
    check(said(OTHER_LOG, "takes writes from primary"),
          "a write to another primary's space is refused, and Y says why")
    put(OTHER, "sy", "e")       # dropped: Y is behind
    put(OTHER, "sz", "f")       # dropped, to a space the replica has never seen from Y
    # long enough for Y to hand these to the replica it's behind on, which drops
    # them and notes their spaces. PUBLISHed sooner, they'd still be in Y's buffer
    # and go out in the new stream, which is fine too, but not what's checked here
    time.sleep(3)
    publish_from(OTHER)
    put(OTHER, "sy", "g")       # held
    got = holds("sy", "g", seconds=6)
    check(got == 0, "Y PUBLISHing again isn't enough (%d of %d arrived)" % (got, N))
    check(said(OTHER_LOG, "is held") and said(OTHER_LOG, "RETRIEVE"),
          "Y keeps its writes, held, and says what to retrieve")
    check(retrieve_from("sy", OTHER) in (b"OK", "OK"), "RETRIEVE of sy from Y")
    put(OTHER, "sy", "gg")      # held too, after sy's copy
    got = holds("sy", "gg", seconds=6)
    check(got == 0, "still held while sz, which it only named, isn't retrieved (%d arrived)"
          % got)
    check(retrieve_from("sz", OTHER) in (b"OK", "OK"), "RETRIEVE of sz from Y")
    check(holds("sy", "g") == N and holds("sy", "gg") == N,
          "then the held writes arrive, with no PUBLISH")
    put(OTHER, "sy", "h")
    put(OTHER, "sz", "i")
    check(holds("sy", "h") == N and holds("sz", "i") == N, "and new ones")
    check(holds("sy", "e", seconds=1) == N and holds("sz", "f", seconds=1) == N,
          "and the replica has everything Y dropped, from the copies")
    check(client(REPLICA).execute_command("sx:GET", "intruder") is None
          and holds("sx", "d", seconds=1) == N,
          "while X's space has X's writes and not Y's")
finally:
    stop(replica)
    stop(y)
    stop(x)

# ------------------------------------------------------------------ part 4
# Writes between RETRIEVE and PUBLISH - TODO 505. They weren't numbered, since the
# primary had nothing published, so a replica took the stream that started after
# them and never had them. Now a primary that has served a copy counts them, the
# replica holds a stream that started past its copy, and RETRIEVE again lines it
# up. And the other order - PUBLISH, then RETRIEVE while writes carry on - ends
# with every write and no second RETRIEVE: batches touching the space are held
# while the copy runs.
import threading


def keys_of(port, prefix):
    r = client(port)
    return sorted(k for k in r.execute_command("KEYS", prefix + "*"))


print("writes between RETRIEVE and PUBLISH (TODO 505)", flush=True)
for d in (PRIMARY_DATA, REPLICA_DATA):
    fresh_dir(d)
if os.path.exists(PRIMARY_LOG):
    os.remove(PRIMARY_LOG)
primary = start(PRIMARY, PRIMARY_DATA, PRIMARY_LOG)
replica = start(REPLICA, REPLICA_DATA)
try:
    write("pa")
    check(retrieve() in (b"OK", "OK"), "RETRIEVE with nothing published")
    check(arrived("pa", seconds=1) == N, "the copy has what was there")
    write("pb")                 # after the copy, before PUBLISH
    publish()
    write("pc")
    got = arrived("pc", seconds=6)
    check(got == 0, "a stream that started after writes the copy lacks is held (%d arrived)" % got)
    check(arrived("pb", seconds=1) == 0, "and the writes in between aren't there yet")
    check(primary_said("is held"), "the primary keeps its writes and says why")
    check(retrieve() in (b"OK", "OK"), "RETRIEVE again")
    check(arrived("pc") == N and arrived("pb", seconds=1) == N,
          "then the replica has the writes in between and the held ones")
    write("pd")
    check(arrived("pd") == N, "and new ones")
finally:
    stop(replica)
    stop(primary)

print("PUBLISH, then RETRIEVE while writes carry on", flush=True)
for d in (PRIMARY_DATA, REPLICA_DATA):
    fresh_dir(d)
primary = start(PRIMARY, PRIMARY_DATA, PRIMARY_LOG)
replica = start(REPLICA, REPLICA_DATA)
try:
    write("qa")                 # there before the replica is
    publish()
    write("qb")
    stop_writing = threading.Event()
    written = [0]

    def keep_writing():
        r = client(PRIMARY)
        while not stop_writing.is_set():
            r.set("qw%d" % written[0], "qw%d" % written[0])
            written[0] += 1

    writer = threading.Thread(target=keep_writing)
    writer.start()
    time.sleep(0.5)
    ok = retrieve() in (b"OK", "OK")
    time.sleep(0.5)
    stop_writing.set()
    writer.join()
    check(ok, "RETRIEVE while the primary is being written to")
    write("qc")
    check(arrived("qc") == N, "writes after the copy arrive, with no second RETRIEVE")
    deadline = time.time() + WAIT
    while time.time() < deadline and keys_of(REPLICA, "q") != keys_of(PRIMARY, "q"):
        time.sleep(0.5)
    here, there = keys_of(PRIMARY, "q"), keys_of(REPLICA, "q")
    check(here == there, "and the replica has exactly the primary's keys (%d written during"
          " the copy, %d here, %d there)" % (written[0], len(here), len(there)))
finally:
    stop(replica)
    stop(primary)

# ------------------------------------------------------------------ part 5
# A primary that crashes loses what it still owed its replicas: its queue, and the
# spaces of what it dropped - TODO 508. A replica that followed it asked only for
# the spaces it had writes in, so a space whose first writes died in the queue was
# never asked for. Now a primary that starts without a clean stop names every
# space it holds anything in on its first fresh stream, and the replica holds
# until each one is copied.
print("a primary killed with writes queued for a space the replica never saw (TODO 508)",
      flush=True)
for d in (PRIMARY_DATA, REPLICA_DATA):
    fresh_dir(d)
for f in (PRIMARY_LOG, REPLICA_LOG):
    if os.path.exists(f):
        os.remove(f)
primary = start(PRIMARY, PRIMARY_DATA, PRIMARY_LOG)
replica = start(REPLICA, REPLICA_DATA, REPLICA_LOG)
try:
    publish()
    write("ra")
    check(arrived("ra") == N, "writes reach the replica")
    stop(replica)                       # down, cleanly
    put(PRIMARY, "nz", "rb")            # a space the replica has never had from it
    client(PRIMARY).execute_command("SAVEALL")     # on the primary's disk, not in any stream
    stop(primary, signal.SIGKILL)       # and its queue is gone
    replica = start(REPLICA, REPLICA_DATA, REPLICA_LOG)
    primary = start(PRIMARY, PRIMARY_DATA, PRIMARY_LOG)
    publish()
    write("rc")
    got = arrived("rc", seconds=6)
    check(got == 0, "the restarted primary's stream is held (%d arrived)" % got)
    check(said(REPLICA_LOG, "nz"), "and the replica names the new space among what to retrieve")
    check(retrieve() in (b"OK", "OK"), "RETRIEVE of the space it wrote before")
    # rc came in that copy; rd is only in the held stream
    write("rd")
    got = arrived("rd", seconds=6)
    check(got == 0, "still held without the new space (%d arrived)" % got)
    check(retrieve_from("nz", PRIMARY) in (b"OK", "OK"), "RETRIEVE of the new space")
    check(arrived("rd") == N, "then the held writes arrive")
    check(holds("nz", "rb", seconds=1) == N, "and the replica has the new space's writes")
finally:
    stop(replica)
    stop(primary)

print("\n%s" % ("all replication sync checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
