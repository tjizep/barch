#!/usr/bin/env python3
"""
Every write command is either replicated or refused in a Raft space - TODO 663.

A space replicated with Raft stays the same on every member only if every write to it
goes through the shard's committing paths. Some didn't: setBufferAt's in-place write
(TODO 662), a ROLLBACK that undid writes on one node, a KSPACE MERGE. So every write
command is classed in src/raft_writes.cpp as replicated, server or local, a Raft space
refuses the local ones, and CLUSTER WRITES lists the classes.

What has to hold, on a three-node cluster:
  - no write command is unlisted, and every replicated one has a sample here: a new
    command fails this test until it's classed, and covered if it's replicated;
  - each replicated command, run on the leader, leaves every member's copy the same
    (checked after each one, so a failure names it);
  - each local command is refused in the replicated space, and FLUSHALL / CLEARALL are
    refused on any node of a cluster;
  - each Luau store write, run on the leader from a function, leaves every member the
    same, the in-place ones run twice so the second finds the key; barch.call of a
    local command and store.load are refused;
  - a range-sharded replicated space keeps every member the same through the
    maintenance thread's sweeps, which don't move keys in a replicated space.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=30900)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
failures = 0


def check(ok, what):
    global failures
    print("  %-70s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


# one sample per replicated command: the commands to run, in order, on the leader. The
# last one is the command itself; the ones before set up what it needs. Every one has
# to succeed, so the write really happens
NOW = int(time.time())
SAMPLES = {
    "SET": [["SET", "s", "v"]],
    "SETEX": [["SETEX", "sx", "100", "v"]],
    "PSETEX": [["PSETEX", "psx", "100000", "v"]],
    "SETNX": [["SETNX", "snx", "v"]],
    "SETRANGE": [["SET", "sr", "hello"], ["SETRANGE", "sr", "1", "EE"]],
    "MSET": [["MSET", "m1", "a", "m2", "b"]],
    "MSETNX": [["MSETNX", "mn1", "a", "mn2", "b"]],
    "GETSET": [["GETSET", "gs", "v2"]],
    "GETDEL": [["SET", "gd", "v"], ["GETDEL", "gd"]],
    "GETEX": [["SET", "gx", "v"], ["GETEX", "gx", "EX", "100"]],
    "APPEND": [["APPEND", "ap", "abc"]],
    "PREPEND": [["SET", "pp", "def"], ["PREPEND", "pp", "abc"]],
    "INCR": [["INCR", "i"]],
    "INCRBY": [["INCRBY", "i", "5"]],
    "INCRBYFLOAT": [["INCRBYFLOAT", "f", "1.5"]],
    "DECR": [["DECR", "i"]],
    "DECRBY": [["DECRBY", "i", "2"]],
    "UINCRBY": [["SET", "u", "10"], ["UINCRBY", "u", "3"]],
    "UDECRBY": [["UDECRBY", "u", "1"]],
    "DEL": [["SET", "d", "v"], ["DEL", "d"]],
    "REM": [["SET", "r", "v"], ["REM", "r"]],
    "ADD": [["ADD", "ad", "v"]],
    "RENAME": [["SET", "rn", "v"], ["RENAME", "rn", "rn2"]],
    "RENAMENX": [["SET", "rx", "v"], ["RENAMENX", "rx", "rx2"]],
    "COPY": [["SET", "cp", "v"], ["COPY", "cp", "cp2"]],
    "MOVE": [["SET", "mv", "v"], ["MOVE", "mv", "1"]],
    "EXPIRE": [["SET", "e1", "v"], ["EXPIRE", "e1", "100"]],
    "EXPIREAT": [["SET", "e2", "v"], ["EXPIREAT", "e2", str(NOW + 100)]],
    "PEXPIRE": [["SET", "e3", "v"], ["PEXPIRE", "e3", "100000"]],
    "PEXPIREAT": [["SET", "e4", "v"], ["PEXPIREAT", "e4", str((NOW + 100) * 1000)]],
    "PERSIST": [["SET", "e5", "v", "EX", "100"], ["PERSIST", "e5"]],
    "HSET": [["HSET", "h", "f1", "a", "f2", "b"]],
    "HMSET": [["HMSET", "h", "f3", "c"]],
    "HSETNX": [["HSETNX", "h", "f4", "d"]],
    "HDEL": [["HDEL", "h", "f4"]],
    "HGETDEL": [["HGETDEL", "h", "FIELDS", "1", "f3"]],
    "HINCRBY": [["HINCRBY", "hn", "n", "3"]],
    "HINCRBYFLOAT": [["HINCRBYFLOAT", "hn", "x", "1.5"]],
    "HEXPIRE": [["HEXPIRE", "h", "100", "FIELDS", "1", "f1"]],
    "HEXPIREAT": [["HEXPIREAT", "h", str(NOW + 100), "FIELDS", "1", "f2"]],
    "LPUSH": [["LPUSH", "l", "a", "b", "c", "d", "e", "f", "g", "h"]],
    "LPUSHX": [["LPUSHX", "l", "x"]],
    "RPUSH": [["RPUSH", "l", "y", "z"]],
    "RPUSHX": [["RPUSHX", "l", "w"]],
    "LPOP": [["LPOP", "l"]],
    "RPOP": [["RPOP", "l"]],
    "LINSERT": [["LINSERT", "l", "BEFORE", "c", "cc"]],
    "LMOVE": [["LMOVE", "l", "l2", "LEFT", "RIGHT"]],
    "LMPOP": [["LMPOP", "1", "l", "LEFT"]],
    "RPOPLPUSH": [["RPOPLPUSH", "l", "l2"]],
    "BLPOP": [["BLPOP", "l", "1"]],
    "BRPOP": [["BRPOP", "l", "1"]],
    "BLMOVE": [["BLMOVE", "l", "l2", "LEFT", "LEFT", "1"]],
    "BLMPOP": [["BLMPOP", "1", "1", "l", "RIGHT"]],
    "BRPOPLPUSH": [["BRPOPLPUSH", "l", "l2", "1"]],
    "ZADD": [["ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e", "6", "f", "7", "g", "8", "h"],
             ["ZADD", "z2", "1", "a", "9", "i"]],
    "ZINCRBY": [["ZINCRBY", "z", "10", "a"]],
    "ZREM": [["ZREM", "z", "h"]],
    "ZREMRANGEBYLEX": [["ZADD", "zl", "0", "a", "0", "b", "0", "c"], ["ZREMRANGEBYLEX", "zl", "[a", "[b"]],
    "ZREMRANGEBYRANK": [["ZREMRANGEBYRANK", "z", "0", "0"]],
    "ZREMRANGEBYSCORE": [["ZREMRANGEBYSCORE", "z", "7", "7"]],
    "ZPOPMIN": [["ZPOPMIN", "z"]],
    "ZPOPMAX": [["ZPOPMAX", "z"]],
    "ZMPOP": [["ZMPOP", "1", "z", "MIN"]],
    "BZPOPMIN": [["BZPOPMIN", "z", "1"]],
    "BZPOPMAX": [["BZPOPMAX", "z", "1"]],
    "BZMPOP": [["BZMPOP", "1", "1", "z", "MAX"]],
    "ZDIFFSTORE": [["ZDIFFSTORE", "zd", "2", "z", "z2"]],
    "ZINTERSTORE": [["ZINTERSTORE", "zi", "2", "z", "z2"]],
    "ZUNIONSTORE": [["ZUNIONSTORE", "zu", "2", "z", "z2"]],
    "ZRANGESTORE": [["ZRANGESTORE", "zr", "z2", "0", "-1"]],
    "LOCK": [["LOCK", "lk", "me", "100000"]],
    "UNLOCK": [["UNLOCK", "lk", "me"]],
    "SETF": [["SETF", "fn1", "function call() return 1 end"]],
    "REMF": [["SETF", "fn2", "function call() return 2 end"], ["REMF", "fn2"]],
    "FLUSHDB": [["SET", "before_flush", "v"], ["FLUSHDB"]],
}
# arguments for a local command: enough for the dispatcher to see what it is. It's
# refused before its handler reads them
LOCAL_ARGS = {
    "KSPACE": ["MERGE", "stage", "TO", SPACE],
}


def same_everywhere(nodes):
    l = leader_of(nodes, SPACE)
    if l is None:
        return False
    top = l.field(SPACE, "applied")
    if not wait_for(30, lambda: all(n.field(SPACE, "applied") >= top for n in nodes)):
        return False
    return wait_for(10, lambda: len(set(n.digest(SPACE)[0] for n in nodes)) == 1)


def retryable(e):
    # redis-py raises TRYAGAIN as TryAgainError, with the word taken off
    return isinstance(e, redis.exceptions.TryAgainError) or str(e).startswith(("NOTLEADER", "TRYAGAIN"))


clients = {}


def on(space, *cmd):
    """cmd on the space's leader, following it through an election or a lease lapse"""
    for _ in range(300):
        try:
            if space not in clients:
                l = leader_of(nodes, space)
                if l is None:
                    time.sleep(0.1)
                    continue
                clients[space] = l.client(space)
            return clients[space].execute_command(*cmd)
        except redis.ResponseError as e:
            if not retryable(e):
                raise
            clients.pop(space, None)
            time.sleep(0.1)
    raise RuntimeError("no leader of %s took %s" % (space, cmd))


nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]
try:
    print("a replicated space on three nodes")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE) for n in nodes)),
        "three voters and a leader")
    leader = leader_of(nodes, SPACE)
    c = leader.client(SPACE)

    print("CLUSTER WRITES")
    table = {}
    for line in leader.client().execute_command("CLUSTER", "WRITES"):
        words = line.split()
        table[words[0]] = words[1:]
    unlisted = sorted(n for n, w in table.items() if "unlisted" in w)
    check(not unlisted, "every write command is classed (unlisted: %s)" % unlisted)
    replicated = sorted(n for n, w in table.items() if w[0] == "replicated")
    local = sorted(n for n, w in table.items() if w[0] == "local")
    uncovered = [n for n in replicated if n not in SAMPLES]
    check(not uncovered, "every replicated command has a sample here (missing: %s)" % uncovered)
    print("    %d replicated, %d local, %d server" % (len(replicated), len(local),
                                                    len(table) - len(replicated) - len(local)))

    print("each replicated command on the leader, then every member compared")
    for name in replicated:
        if name not in SAMPLES:
            continue
        err = None
        for cmd in SAMPLES[name]:
            try:
                on(SPACE, *cmd)
            except redis.RedisError as e:
                err = "%s: %s" % (" ".join(cmd), e)
                break
        if err:
            check(False, "%s ran (%s)" % (name, err))
            continue
        check(same_everywhere(nodes), "%s leaves every member the same" % name)

    print("each local command refused in the replicated space")
    for name in local:
        if name in ("FLUSHALL", "CLEARALL"):
            continue
        args = LOCAL_ARGS.get(name, ["x"])
        try:
            got = c.execute_command(name, *args)
        except redis.ResponseError as e:
            got = "ERR " + str(e)
        check(isinstance(got, str) and "changes this node's copy only" in got, "%s is refused (%r)" % (name, got))
    for name in ("FLUSHALL", "CLEARALL"):
        try:
            got = nodes[1].client().execute_command(name)
        except redis.ResponseError as e:
            got = "ERR " + str(e)
        check(isinstance(got, str) and "a node in a cluster refuses it" in got, "%s is refused in a cluster (%r)" % (name, got))
    check(same_everywhere(nodes), "and every member is still the same")

    print("Luau store writes on the leader, then every member compared")
    # the in-place writers run twice: the first creates the key through the write
    # path, the second finds it long enough and took the in-place one (TODO 662)
    LUAU = {
        "set": ("function call(k) barch.store.set(k, 'v') end", 1),
        "remove": ("function call(k) barch.store.set(k, 'v') barch.store.remove(k) end", 1),
        "setJson": ("function call(k) barch.store.setJson(k, {a = 1}) end", 1),
        "setBufferAt": ("function call(k) local b = buffer.create(4) buffer.writei32(b, 0, 7) "
                        "barch.store.setBufferAt(k, b) end", 2),
        "setInt32At": ("function call(k) barch.store.setInt32At(k, 5, 0) end", 2),
        "setInt64At": ("function call(k) barch.store.setInt64At(k, 6, 0) end", 2),
        "setF32At": ("function call(k) barch.store.setF32At(k, 0.5, 0) end", 2),
        "setF64At": ("function call(k) barch.store.setF64At(k, 2.5, 0) end", 2),
        "incrInt32ByAt": ("function call(k) return barch.store.incrInt32ByAt(k, 1, 1, 0) end", 20),
        "incrInt64ByAt": ("function call(k) return barch.store.incrInt64ByAt(k, 1, 1, 0) end", 20),
        "handle set": ("function call(k) barch.current():set(k, 'h') end", 1),
        "barch.call SET": ("function call(k) return barch.call('SET', k, 'c') end", 1),
    }
    for i, (what, (code, times)) in enumerate(LUAU.items()):
        fn = "lw%d" % i
        err = None
        try:
            on(SPACE, "SETF", fn, code)
            for _ in range(times):
                on(SPACE, fn, "lk%d" % i)
        except redis.RedisError as e:
            err = str(e)
        if err:
            check(False, "%s ran (%s)" % (what, err))
            continue
        check(same_everywhere(nodes), "Luau %s leaves every member the same" % what)
    REFUSED = {
        "barch.call of a local command": "function call() return barch.call('KSPACE', 'MERGE', 'stage', 'TO', 'orders') end",
        "store.load": "function call() return barch.store.load(function(block, shard) return nil end) end",
    }
    for i, (what, code) in enumerate(REFUSED.items()):
        fn = "lr%d" % i
        try:
            on(SPACE, "SETF", fn, code)
            got = on(SPACE, fn)
        except redis.ResponseError as e:
            got = "ERR " + str(e)
        check(isinstance(got, str) and "copy only" in got, "Luau %s is refused (%r)" % (what, got))
    check(same_everywhere(nodes), "and every member is still the same")

    print("a range-sharded replicated space: writes, the maintenance sweeps, more writes")
    conf = nodes[0].client("configuration")
    for k, v in (("rs.ordered", "1"), ("rs.range_sharded", "1"), ("rs.shards", "4"), ("rs.raft", "on")):
        conf.execute_command("SET", k, v)
    check(wait_for(90, lambda: leader_of(nodes, "rs") is not None and all(
        "members 3" in n.group_line("rs") and "learners 0" in n.group_line("rs") for n in nodes)),
        "rs has three voters and a leader")
    def rs(*cmd):
        return on("rs", *cmd)

    for i in range(3000):
        rs("SET", "a%05d" % i, "v")          # all low in the order: skewed
    time.sleep(scale.scaled_seconds(5.0))
    for i in range(0, 3000, 3):
        rs("DEL", "a%05d" % i)
    for i in range(1000):
        rs("SET", "z%05d" % i, "v")
    rl = leader_of(nodes, "rs")
    top = rl.field("rs", "applied")
    wait_for(30, lambda: all(n.field("rs", "applied") >= top for n in nodes))
    digests = [n.digest("rs")[0] for n in nodes]
    check(len(set(digests)) == 1 and digests[0].startswith("keys 3000 "),
          "every member holds the same 3000 keys (%s)" % digests)
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
