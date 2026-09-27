# A replica fed only by shard records ends up equal to its primary - TODO 506.
#
# The RESP session used to send every builtin write+data command to replicas as a
# plain command, on top of the shard records (TODO 498) that already carry what
# each write did. TODO 503 stopped it. What had to be shown is that nothing
# relied on the plain command: that every family of write reaches the replica as
# records, including the ones whose effect isn't obviously a key - GRAPH, FS,
# SETF and REMF, LOADKEYS - and a named space.
#
# GRAPH is the case TODO 411 found wrong the old way. Replaying the command made
# the replica hand out its own ids, and after a restart those differed from the
# primary's, so LINK and UNLINK by id did something else there. With records the
# replica gets the primary's nodes, edges and id counters as they are.
#
# And the count TODO 506 asked for: after one RESP SET on the primary, the
# replica has run REPLAPPLY and no SET.
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

import scale
import redis

scale.workdir()
PRIMARY = scale.port(default=14510)
REPLICA = scale.port(1, default=14511)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

ROOT = os.getcwd()
PRIMARY_DATA = os.path.join(ROOT, "replequiv_primary")
REPLICA_DATA = os.path.join(ROOT, "replequiv_replica")
WAIT = 30


def start(port, data):
    p = subprocess.Popen([BINARY, "--port", str(port), "--bind", "127.0.0.1", "--dir", data],
                         stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
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


def cmdstats(r):
    """{command: calls} from INFO commandstats, which redis-py parses"""
    info = r.info("commandstats")
    return {k[len("cmdstat_"):].lower(): v.get("calls", 0)
            for k, v in info.items() if k.startswith("cmdstat_")}


def dump(r, prefix, key):
    """what a key holds, by whichever read answers for it - there's no TYPE"""
    for read in (("GET", key), ("HGETALL", key), ("LRANGE", key, 0, -1),
                 ("ZRANGE", key, 0, -1, "WITHSCORES")):
        try:
            got = r.execute_command(prefix + read[0], *read[1:])
        except redis.ResponseError:
            continue
        if got not in (None, [], {}):
            return read[0], sorted(got.items()) if isinstance(got, dict) else got
    return None, None


def snapshot(port, space=""):
    r = client(port)
    prefix = space + ":" if space else ""
    keys = sorted(r.execute_command(prefix + "KEYS", "*"))
    data = {k: dump(r, prefix, k) for k in keys}
    fns = {}
    if not space:
        for name in sorted(r.execute_command("KEYSF")):
            fns[name] = r.execute_command("GETF", name)
    return data, fns


def same(space=""):
    """wait until the replica reads back what the primary holds"""
    deadline = time.time() + WAIT
    while True:
        try:
            here, there = snapshot(PRIMARY, space), snapshot(REPLICA, space)
        except redis.RedisError as e:
            here, there = (str(e),), ()
        if here == there or time.time() >= deadline:
            if here != there and len(here) == 2 and len(there) == 2:
                missing = set(here[0]) - set(there[0])
                extra = set(there[0]) - set(here[0])
                differ = [k for k in here[0] if k in there[0] and here[0][k] != there[0][k]]
                print("    missing %r extra %r differ %r fns %r/%r" % (
                    sorted(missing)[:5], sorted(extra)[:5], differ[:5],
                    sorted(here[1]), sorted(there[1])), flush=True)
            return here == there
        time.sleep(0.5)


def graph_stat(port, path):
    line = client(port).execute_command("GRAPH", "STAT", path)
    return None if line is None else dict(p.partition("=")[::2] for p in line.decode().split(" "))


for d in (PRIMARY_DATA, REPLICA_DATA):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)

# the named space's layout, the same on both
for port, data in ((PRIMARY, PRIMARY_DATA), (REPLICA, REPLICA_DATA)):
    proc = start(port, data)
    try:
        client(port).execute_command("configuration:SET", "eq.shards", "3")
        client(port).execute_command("configuration:SAVE")
    finally:
        stop(proc)

primary = start(PRIMARY, PRIMARY_DATA)
replica = start(REPLICA, REPLICA_DATA)
src = tempfile.mkdtemp(prefix="replequiv")
try:
    p = client(PRIMARY)
    p.execute_command("PUBLISH", "127.0.0.1", str(REPLICA))

    print("one RESP SET", flush=True)
    before = cmdstats(client(REPLICA))
    p.set("counted", "1")
    deadline = time.time() + WAIT
    while time.time() < deadline and client(REPLICA).get("counted") != b"1":
        time.sleep(0.2)
    after = cmdstats(client(REPLICA))
    grew = {k: after.get(k, 0) - before.get(k, 0) for k in after if after.get(k, 0) != before.get(k, 0)}
    print("    the replica's calls went up by %r" % (grew,), flush=True)
    check(grew.get("replapply", 0) >= 1, "the replica applied it as REPLAPPLY")
    check(grew.get("set", 0) == 0, "and ran no SET of its own")

    print("every family of write", flush=True)
    p.set("s", "v")
    p.append("s", "w")
    p.setrange("s", 1, "Z")
    p.mset({"m1": "1", "m2": "2"})
    p.incr("n")
    p.incrby("n", 41)
    p.incrbyfloat("f", 1.5)
    p.set("ttl", "x", ex=300)
    p.expire("m1", 600)
    p.persist("ttl")
    p.getset("gs", "new")
    p.set("gone", "x")
    p.delete("gone")
    p.set("getdel", "x")
    p.getdel("getdel")
    p.set("from", "moved")
    p.rename("from", "to")
    p.copy("to", "copied")
    p.hset("h", mapping={"a": "1", "b": "2", "c": "3"})
    p.hincrby("h", "a", 9)
    p.hdel("h", "b")
    p.hsetnx("h", "d", "4")
    p.lpush("l", "c", "b", "a")
    p.rpush("l", "d", "e")
    p.lpop("l")
    p.rpop("l")
    p.lpush("l2", "x")
    p.lmove("l", "l2", "LEFT", "RIGHT")
    p.zadd("z", {"a": 1, "b": 2, "c": 3, "d": 4})
    p.zincrby("z", 10, "a")
    p.zrem("z", "b")
    p.zpopmin("z")
    p.zadd("z2", {"c": 5, "q": 1})
    p.zunionstore("zu", ["z", "z2"])
    held = len(snapshot(PRIMARY)[0])
    check(held >= 15, "the primary holds what was written (%d keys)" % held)
    check(same(), "the default space reads back the same on the replica")

    p.execute_command("eq:SET", "k", "v")
    p.execute_command("eq:HSET", "eh", "f", "v")
    p.execute_command("eq:RPUSH", "el", "1", "2")
    p.execute_command("eq:ZADD", "ez", "1", "m")
    check(same("eq"), "and so does a named space with its own layout")

    print("stored functions, the file store and LOADKEYS", flush=True)
    p.execute_command("SETF", "greet", 'function call(who) return "hi " .. tostring(who) end')
    p.execute_command("SETF", "dropme", 'function call() return 1 end')
    p.execute_command("REMF", "dropme")
    p.execute_command("FS", "PUT", "/docs/a.txt", b"file bytes", "TYPE", "text/plain")
    p.execute_command("FS", "MKDIR", "/docs/empty")
    os.makedirs(os.path.join(src, "conf"))
    open(os.path.join(src, "banner.txt"), "wb").write(b"a banner")
    open(os.path.join(src, "conf", "settings.json"), "wb").write(b'{"a":1}')
    open(os.path.join(src, "hello.luau"), "w").write('function call() return "hello" end')
    p.execute_command("LOADKEYS", src, "site")
    fns = sorted(snapshot(PRIMARY)[1])
    check(fns == [b"GREET", b"HELLO"], "the primary has the two functions left (%r)" % (fns,))
    check(same(), "keys, functions and files read back the same")
    check(client(REPLICA).execute_command("greet", "you") == b"hi you",
          "and a function set on the primary runs on the replica")
    check(client(REPLICA).execute_command("FS", "GET", "/docs/a.txt") == b"file bytes",
          "and a file put on the primary reads there")

    print("GRAPH ids across a primary restart (TODO 411)", flush=True)
    for d in ("/a", "/b", "/c"):
        p.execute_command("GRAPH", "MKDIR", d)
    p.execute_command("GRAPH", "PUT", "/a/one.txt", "one")
    check(same(), "graph nodes read back the same")
    stop(primary)
    primary = start(PRIMARY, PRIMARY_DATA)
    p = client(PRIMARY)
    p.execute_command("GRAPH", "MKDIR", "/d")
    p.execute_command("GRAPH", "PUT", "/d/two.txt", "two")
    d_id = graph_stat(PRIMARY, "/d")["id"]
    p.execute_command("GRAPH", "LINK", d_id, "/b/d_again")
    one = graph_stat(PRIMARY, "/a/one.txt")["id"]
    p.execute_command("GRAPH", "LINK", one, "/c/one_again")
    check(same(), "after the restart, LINK by id reads back the same")
    check(graph_stat(REPLICA, "/b/d_again") is not None
          and graph_stat(REPLICA, "/b/d_again")["id"] == d_id,
          "the linked node is the primary's node %s on the replica too" % d_id)
    check(graph_stat(REPLICA, "/c/one_again") is not None
          and graph_stat(REPLICA, "/c/one_again")["id"] == one,
          "and so is the second link")
finally:
    stop(replica)
    stop(primary)
    shutil.rmtree(src, ignore_errors=True)

print("\n%s" % ("all replication equivalence checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
