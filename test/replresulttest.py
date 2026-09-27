# Replication sends what the shard did, not what the client asked for - TODO 498.
#
# It used to be the Python binding that replicated: each KeyValue/List/... method
# queued its own command before running it, whatever happened next. So a write that
# failed here was still sent, two objects racing on one key could queue in one order
# and apply in the other, BLPOP/BRPOP/POPMIN/POPMAX queued nothing, EXPIRE's
# deadline was worked out again on the replica after the queue delay, the space a
# KeyValue was bound to never went with the command, and writes made over RESP
# weren't replicated at all.
#
# Now a shard records each write that took, under its lock, as a change log record,
# and the replica applies the records with REPLAPPLY. Checked here, with this
# process as the primary and a barchd as the replica:
#
#   1. a write in a named space lands in that space on the replica, not the default
#   2. BLPOP and POPMIN on the primary take the element off the replica too
#   3. two threads racing SETs on one key end the same on both
#   4. writes over RESP to the primary reach the replica, INCR and HSET included
#   5. an EXPIRE made while the replica is down keeps its deadline: the replica's
#      TTL is what's left, not the full length again
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
import barch

scale.workdir()
PRIMARY = scale.port(default=14498)
REPLICA = scale.port(1, default=14499)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "replresult_data")
shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)

WAIT = 30                       # seconds a write gets to reach the replica
RACE_ROUNDS = 10
RACE_WRITES = 300


def start():
    p = subprocess.Popen([BINARY, "--port", str(REPLICA), "--bind", "127.0.0.1",
                          "--dir", DATA],
                         stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            raise AssertionError("barchd exited with %s" % p.returncode)
        try:
            socket.create_connection(("127.0.0.1", REPLICA), timeout=0.5).close()
            return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % REPLICA)


def stop(p, sig=signal.SIGTERM):
    p.send_signal(sig)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=30)


def replica():
    return redis.Redis(host="127.0.0.1", port=REPLICA, protocol=2, socket_timeout=30,
                       single_connection_client=True)


fence_n = 0


def settled(kv):
    """wait until everything written so far has reached the replica.

    One sender sends in order, so once a key written last has arrived, so has
    everything before it."""
    global fence_n
    fence_n += 1
    want = str(fence_n).encode()
    kv.set("fence", str(fence_n))
    deadline = time.time() + WAIT
    while time.time() < deadline:
        try:
            if replica().get("fence") == want:
                return True
        except redis.RedisError:
            pass
        time.sleep(0.1)
    return False


failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


proc = start()
barch.start("127.0.0.1", str(PRIMARY))
try:
    barch.setConfiguration("maintenance_poll_delay", "20")
    barch.publish("127.0.0.1", str(REPLICA))
    kv = barch.KeyValue()
    check(settled(kv), "writes reach the replica at all")

    # 1. a named space
    spaced = barch.KeyValue("replresult")
    spaced.set("inspace", "yes")
    settled(kv)
    r = replica()
    in_default = r.get("inspace")
    r.execute_command("USE", "replresult")
    in_space = r.get("inspace")
    r.execute_command("USE", "0")
    check(in_space == b"yes" and in_default is None,
          "a named space's write lands in that space (space %r, default %r)"
          % (in_space, in_default))

    # 2. pops that used to queue nothing
    lst = barch.List()
    lst.push("plist", ["a", "b", "c"])
    lst.blpop("plist", 1)
    zs = barch.OrderedSet()
    zs.add("pzset", ["1", "one", "2", "two", "3", "three"])
    zs.popmin("pzset")
    settled(kv)
    r = replica()
    llen, zcard = r.llen("plist"), r.zcard("pzset")
    check(llen == 2, "BLPOP takes the element off the replica too (LLEN %s)" % llen)
    check(zcard == 2, "POPMIN takes the member off the replica too (ZCARD %s)" % zcard)

    # 3. two objects racing on one key: the primary applies them in some order, and
    # the replica has to end where the primary did
    wrong = []
    for round_ in range(RACE_ROUNDS):
        key = "race%d" % round_
        def writer(tag):
            mine = barch.KeyValue()
            for i in range(RACE_WRITES):
                mine.set(key, "%s%d" % (tag, i))
        crowd = [threading.Thread(target=writer, args=(t,)) for t in ("x", "y")]
        for t in crowd: t.start()
        for t in crowd: t.join()
        settled(kv)
        here, there = kv.get(key), replica().get(key)
        if there is None or here != there.decode():
            wrong.append((key, here, there))
    if wrong:
        print("  ended apart:", wrong[:3], flush=True)
    check(not wrong, "racing SETs end the same on both (%d of %d rounds apart)"
          % (len(wrong), RACE_ROUNDS))

    # 4. writes over RESP to the primary
    p = redis.Redis(host="127.0.0.1", port=PRIMARY, protocol=2, socket_timeout=30)
    p.set("viaresp", "1")
    p.incr("viaresp")
    p.incr("viaresp")
    p.hset("viarespH", mapping={"f1": "v1", "f2": "v2"})
    p.set("viarespGone", "x")
    p.delete("viarespGone")
    settled(kv)
    r = replica()
    got = (r.get("viaresp"), r.hget("viarespH", "f2"), r.exists("viarespGone"))
    check(got == (b"3", b"v2", 0),
          "RESP writes reach the replica, INCR and HSET included (%r)" % (got,))

    # 5. an expiry keeps its deadline across a delay. The replica is down while the
    # EXPIRE is made, so the record waits in the queue for the retry
    stop(proc, signal.SIGKILL)
    kv.set("ttlkey", "v")
    kv.expire("ttlkey", 30, "")
    time.sleep(4)
    proc = start()
    arrived = settled(kv)
    primary_ms = p.pttl("ttlkey")
    replica_ms = replica().pttl("ttlkey")
    check(arrived and 0 < replica_ms <= 27000 and abs(replica_ms - primary_ms) < 3000,
          "an EXPIRE keeps its deadline (primary %d ms left, replica %d ms)"
          % (primary_ms, replica_ms))
finally:
    stop(proc)
    barch.stop()

print("\n%s" % ("all replication result checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
