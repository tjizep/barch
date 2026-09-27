# A replica doesn't take the primary's shard numbers on trust - TODO 503.
#
# A replication record carries the shard its write went to and the space's shard
# count, and a replica used that shard whenever the counts matched. A primary
# that routes by range puts keys by its boundaries, which mean nothing to a
# replica that hashes, so even plain keys landed where the replica's reads never
# looked. With different counts the replica routed each record by its own key,
# which is right for a plain key and wrong for a list, hash or ordered set entry:
# those live where their container's *name* routes, and the name can't be had
# back from the entry's key.
#
# Now a record says how its space routed, the recorded shard is only used when
# it means the same thing here, and a container entry that can't be placed stops
# the batch, which the primary reports, rather than being stored out of reach.
#
# Three pairs of barchd, a named space on each with the layouts below. Plain
# keys have to be readable on the replica in every case, and hash fields where
# the layouts match. Where they don't, a hash write is refused and said.
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
PRIMARY = scale.port(default=14506)
REPLICA = scale.port(1, default=14507)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

ROOT = os.getcwd()
PRIMARY_DATA = os.path.join(ROOT, "repllayout_primary")
REPLICA_DATA = os.path.join(ROOT, "repllayout_replica")
PRIMARY_LOG = os.path.join(ROOT, "repllayout_primary.log")
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


def configure(port, data, space, shards, ranged):
    """a data directory whose space has this layout from the start"""
    shutil.rmtree(data, ignore_errors=True)
    os.makedirs(data)
    p = start(port, data)
    try:
        r = client(port)
        r.execute_command("configuration:SET", space + ".shards", str(shards))
        if ranged:
            r.execute_command("configuration:SET", space + ".ordered", "1")
            r.execute_command("configuration:SET", space + ".range_sharded", "1")
        r.execute_command("configuration:SAVE")
    finally:
        stop(p)


def settled(space, prefix, n, seconds=WAIT):
    """how many prefix0.. the replica can read back, waiting up to `seconds`"""
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


def fields(space, seconds):
    deadline = time.time() + seconds
    have = 0
    while True:
        try:
            have = client(REPLICA).execute_command(space + ":HLEN", "h") or 0
        except redis.RedisError:
            have = 0
        if have == 10 or time.time() >= deadline:
            return have
        time.sleep(0.5)


def primary_said(text):
    with open(PRIMARY_LOG, "rb") as f:
        return text.encode() in f.read()


def run(what, space, primary_layout, replica_layout, matches):
    print(what, flush=True)
    configure(PRIMARY, PRIMARY_DATA, space, *primary_layout)
    configure(REPLICA, REPLICA_DATA, space, *replica_layout)
    if os.path.exists(PRIMARY_LOG):
        os.remove(PRIMARY_LOG)
    primary = start(PRIMARY, PRIMARY_DATA, PRIMARY_LOG)
    replica = start(REPLICA, REPLICA_DATA)
    try:
        p = client(PRIMARY)
        p.execute_command("PUBLISH", "127.0.0.1", str(REPLICA))
        pipe = p.pipeline(transaction=False)
        for i in range(N):
            pipe.execute_command(space + ":SET", "k%d" % i, "k%d" % i)
        pipe.execute()
        got = settled(space, "k", N)
        check(got == N, "plain keys can be read on the replica (%d of %d)" % (got, N))

        p.execute_command(space + ":HSET", "h", *sum((["f%d" % i, "v%d" % i] for i in range(10)), []))
        got = fields(space, WAIT if matches else 4)
        if matches:
            check(got == 10, "hash fields can be read on the replica (%d of 10)" % got)
        else:
            check(got == 0, "hash fields aren't stored out of reach (%d of 10 readable)" % got)
            time.sleep(1)
            check(primary_said("list, hash or ordered set entry"),
                  "and the primary says the replica refused them, and why")
    finally:
        stop(replica)
        stop(primary)


# (shards, range sharded)
run("the same layout", "lm", (2, False), (2, False), True)
run("range-sharded primary, hash-sharded replica, same count", "lr", (2, True), (2, False), False)
run("two shards on the primary, three on the replica", "lc", (2, False), (3, False), False)

print("\n%s" % ("all replication layout checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
