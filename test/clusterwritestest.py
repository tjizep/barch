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
  - each logged command (TODO 669), which goes in the log for every member to run,
    leaves every member answering the same for itself: the option it set, the
    dictionary it trained, the indexes it made;
  - no write command is local (TODO 688): each is replicated, logged or server;
  - the digest changes when a hash, list or ordered set's contents do (TODO 668), so
    the comparisons above see them;
  - each Luau store write, run on the leader from a function, leaves every member the
    same, the in-place ones run twice so the second finds the key; barch.call of a
    local command and store.load are refused;
  - a range-sharded replicated space keeps every member the same through the
    maintenance thread's sweeps, which don't move keys in a replicated space.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import shutil
import sys
import tempfile
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


# a directory on this machine, which is every node's disk, for LOADFS and LOADKEYS.
# AUSER requires ZMOD, which sorts after it, so the import only installs if a
# function sees a module the same batch installs - TODO 665
IMPORT_DIR = tempfile.mkdtemp(prefix="clusterwrites")
os.makedirs(os.path.join(IMPORT_DIR, "sub"))
open(os.path.join(IMPORT_DIR, "a.txt"), "w").write("a file")
open(os.path.join(IMPORT_DIR, "sub", "b.txt"), "w").write("another")
open(os.path.join(IMPORT_DIR, "zmod.luau"), "w").write("function call() return 1 end\nfunction helper() return 41 end\n")
open(os.path.join(IMPORT_DIR, "auser.luau"), "w").write(
    "local m = require('ZMOD')\nfunction call() return m.helper() + 1 end\n")

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
    "FLUSHDB": [["SET", "before_flush", "v"], ["FLUSHDB"]],
    # a merge into the replicated space from one only the leader holds - TODO 670
    "KSPACE/MERGE": [["stage:SET", "m1", "a"], ["stage:SET", "m2", "b"],
                     ["KSPACE", "MERGE", "stage", "INTO", SPACE]],
    # every space at once, which would take this test's spaces with it: covered by
    # TestClusterFlushAll - TODO 680
    "FLUSHALL": None,
    "CLEARALL": None,
    # an export's commands, each replicated as a client's: TestClusterBulk - TODO 681
    "IMPORT": None,
    # needs a declared queue and its handler: TestClusterQueue - TODO 682
    "QUEUE": None,
    # a primary's stream, through the leader: TestClusterReplApply - TODO 684
    "REPLAPPLY": None,
    "REPLRAFT": None,
    # a primary's cached miss, named with its space: TestClusterReplApply - TODO 686
    "REPLMISS": None,
    # needs a git repository: TestClusterFunctionSync - TODO 683
    "FUNCTIONS/SYNC": None,
    # one staged batch per write, committed as one entry - TODO 665
    "FS": [["FS", "PUT", "/f/a.txt", "x" * 3000, "CHUNK", "1024"], ["FS", "MKDIR", "/f/d"],
           ["FS", "CP", "/f/a.txt", "/f/d/b.txt"], ["FS", "MV", "/f/d/b.txt", "/f/c.txt"],
           ["FS", "PUT", "/f/a.txt", "y" * 1500, "CHUNK", "1024"], ["FS", "RM", "/f/c.txt"],
           ["FS", "MKDIR", "/f/e"], ["FS", "RMDIR", "/f/e"]],
    "GRAPH": [["GRAPH", "MKDIR", "/g"], ["GRAPH", "PUT", "/g/a.txt", "graph-bytes"], ["GRAPH", "MKDIR", "/g/d"],
              lambda: ["GRAPH", "LINK", graph_id("/g/a.txt"), "/g/d/alias.txt"],
              ["GRAPH", "UNLINK", "/g/d/alias.txt"], ["GRAPH", "CP", "/g/a.txt", "/g/b.txt"],
              ["GRAPH", "MV", "/g/b.txt", "/g/d/b.txt"], ["GRAPH", "RM", "/g/a.txt"]],
    "LOADFS": [["LOADFS", IMPORT_DIR, "/imported"]],
    "LOADKEYS": [["LOADKEYS", IMPORT_DIR, "imp"], ["AUSER"]],
    "DIR": [["MSET", "t:a:1", "1", "t:a:2", "2", "t:b:1", "3"], ["DIR", "CP", "t:a", "t:c"],
            ["DIR", "MV", "t:c", "t:d"], ["DIR", "RM", "t:b"]],
}


def graph_id(path):
    """a graph node's id, which LINK takes, from GRAPH STAT on the leader"""
    line = on(SPACE, "GRAPH", "STAT", path)
    if isinstance(line, bytes):
        line = line.decode()
    for part in line.split(" "):
        k, _, v = part.partition("=")
        if k == "id":
            return v
    raise RuntimeError("no id for %s in %r" % (path, line))
# training text for TRAIN: varied enough that zstd makes a dictionary from it
TRAINING = ["order %d for customer %d: %d widgets at %d.%02d each, ship to depot %d"
            % (i, i * 7 % 101, i % 13, i % 50, i % 100, i % 9) for i in range(400)]


def raw(n):
    """a client on node n that leaves replies as bytes, for the dictionary"""
    c = redis.Redis(host="127.0.0.1", port=n.port, single_connection_client=True)
    c.execute_command("USE", SPACE)
    return c


def functions_on(n):
    """the stored functions member n holds, read on n itself"""
    c = n.client(SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return sorted(c.execute_command("KEYSF"))


def read_on(n, space, key):
    """`key` in `space`, as member n holds it"""
    c = n.client(space)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c.execute_command("GET", key)


def base_everywhere():
    """dbase, a space every node holds on its own, with the same key on each"""
    for n in nodes:
        n.client("dbase").execute_command("SET", "dk", "from-dbase")
    return ["GET", "dk"]


def drop_space_ready():
    """dropme, a second replicated space, with a key in it, led somewhere"""
    nodes[0].client("configuration").execute_command("SET", "dropme.raft", "on")
    if not wait_for(90, lambda: leader_of(nodes, "dropme") is not None and all(
            "members 3" in n.group_line("dropme") and "learners 0" in n.group_line("dropme") for n in nodes)):
        raise RuntimeError("dropme has no group")
    on("dropme", "SET", "dropped", "v")
    return ["GET", "dk"]


def drop_on_its_leader():
    on("dropme", "KSPACE", "DROP", "dropme")
    return ["GET", "dk"]


def index_lines(c):
    # pending counts writes this member hasn't applied to its indexes yet, which
    # differs from member to member and is no part of what the index is
    return sorted(" ".join(w for w in line.split() if not w.startswith("pending="))
                  for line in c.execute_command("INDEX", "LIST"))


# one sample per logged command or subcommand - TODO 669: the commands to run on the
# leader, in order, then what each member answers for itself afterwards, which has to
# be the same everywhere. The leader's DICTIONARY GET is set back as it is, which
# every member takes since each trained the same one
LOGGED = [
    ("KSOPTIONS", [["KSOPTIONS", "SET", "UNORDERED"], ["KSOPTIONS", "SET", "ORDERED"]],
     lambda n: n.client(SPACE).execute_command("KSPACE", "OPTION", "GET", "ORDERED")),
    ("KSPACE/OPTION/SET", [["KSPACE", "OPTION", "SET", "LRU", "ON"]],
     lambda n: n.client(SPACE).execute_command("KSPACE", "OPTION", "GET", "LRU")),
    ("TRAIN", [["TRAIN", t] for t in TRAINING] + [["TRAIN"]],
     lambda n: raw(n).execute_command("DICTIONARY", "GET")),
    ("DICTIONARY/SET", [lambda: ["DICTIONARY", "SET", raw(leader_of(nodes, SPACE)).execute_command("DICTIONARY", "GET")]],
     lambda n: raw(n).execute_command("DICTIONARY", "GET")),
    ("COMPRESS", [["SET", "cz", " ".join(TRAINING[:20])], ["COMPRESS", "cz"]], None),
    ("DECOMPRESS", [["DECOMPRESS", "cz"]], None),
    # the space each changes, every member - TODO 670
    ("KSPACE/DEPENDS", [base_everywhere, ["KSPACE", "DEPENDS", SPACE, "ON", "dbase"]],
     lambda n: read_on(n, SPACE, "dk")),
    ("KSPACE/RELEASE", [["KSPACE", "RELEASE", "dbase", "FROM", SPACE]],
     lambda n: ("released", read_on(n, SPACE, "dk"))),
    ("KSPACE/DROP", [drop_space_ready, drop_on_its_leader],
     lambda n: ("dropped", read_on(n, "dropme", "dropped"))),
    # a transaction marks the whole space on every member - TODO 677. Each sample ends
    # its own, so nothing after it runs inside one
    ("BEGIN", [["BEGIN"], ["SET", "tx_b", "v"], ["COMMIT"]], lambda n: read_on(n, SPACE, "tx_b")),
    ("COMMIT", [["BEGIN"], ["SET", "tx_c", "v"], ["COMMIT"]], lambda n: read_on(n, SPACE, "tx_c")),
    ("ROLLBACK", [["SET", "tx_r", "before"], ["BEGIN"], ["SET", "tx_r", "during"], ["SET", "tx_r2", "v"],
                  ["ROLLBACK"]],
     lambda n: (read_on(n, SPACE, "tx_r"), read_on(n, SPACE, "tx_r2"))),
    # the space replaced from the leader's files or another server, which the others
    # copy: TestClusterBulk - TODO 681
    ("LOAD", None, None),
    ("RETRIEVE", None, None),
    # the fake source and cached misses need a space with a foreign source:
    # TestClusterForeign - TODO 685
    ("FOREIGN/FAKE/SET", None, None),
    ("FOREIGN/FAKE/DEL", None, None),
    ("FOREIGN/FAKE/FAIL", None, None),
    ("FOREIGN/FAKE/DELAY", None, None),
    ("FOREIGN/FAKE/RESET", None, None),
    ("FOREIGN_MISS", None, None),
    # every member installs a stored function - TODO 667
    ("SETF", [["SETF", "fn1", "function call() return 1 end"]],
     functions_on),
    ("REMF", [["SETF", "fn2", "function call() return 2 end"], ["REMF", "fn2"]],
     functions_on),
    ("INDEX/CREATE", [["INDEX", "CREATE", "ix1", "COMPOSITE", "2"]], lambda n: index_lines(n.client(SPACE))),
    ("INDEX/BUILD", [["INDEX", "BUILD", "ix1", "ALL"]], lambda n: index_lines(n.client(SPACE))),
    ("INDEX/DROP", [["INDEX", "CREATE", "ix2", "COMPOSITE", "2"], ["INDEX", "DROP", "ix2"]],
     lambda n: index_lines(n.client(SPACE))),
]

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


def on(space, *cmd, idempotent=False):
    """cmd on the space's leader, following it through an election or a lease lapse.
    An UNKNOWN answer - leadership moved before the write was known to commit - is
    sent again when sending it twice is harmless, and otherwise counts as done: the
    members are compared with each other afterwards, and "may or may not have
    happened" still leaves them the same - TODO 664"""
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
            if str(e).startswith("UNKNOWN"):
                clients.pop(space, None)
                if not idempotent:
                    return None
            elif not retryable(e):
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
    for other in ("stage", "dbase"):
        nodes[0].client("configuration").execute_command("SET", other + ".shards",
                                                          os.environ.get("BARCH_TEST_RAFT_SHARDS", "32"))
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
    logged = sorted(n for n, w in table.items() if w[0] == "logged")
    local = sorted(n for n, w in table.items() if w[0] == "local")
    uncovered = [n for n in replicated if n not in SAMPLES]
    check(not uncovered, "every replicated command has a sample here (missing: %s)" % uncovered)
    uncovered = [n for n in logged if n not in [name for name, _, _ in LOGGED]]
    check(not uncovered, "every logged command has a sample here (missing: %s)" % uncovered)
    print("    %d replicated, %d logged, %d local, %d server" % (
        len(replicated), len(logged), len(local), len(table) - len(replicated) - len(logged) - len(local)))

    print("the digest counts what a hash, list or ordered set holds - TODO 668")
    for name, first, second in (("hash", ["HSET", "dg_h", "f", "a"], ["HSET", "dg_h", "f", "b"]),
                                ("list", ["RPUSH", "dg_l", "a"], ["RPUSH", "dg_l", "b"]),
                                ("ordered set", ["ZADD", "dg_z", "1", "a"], ["ZADD", "dg_z", "2", "a"])):
        on(SPACE, *first)
        before = on(SPACE, "CLUSTER", "DIGEST", SPACE)[0]
        on(SPACE, *second)
        after = on(SPACE, "CLUSTER", "DIGEST", SPACE)[0]
        check(before != after, "a changed %s changes the digest" % name)

    print("each replicated command on the leader, then every member compared")
    for name in replicated:
        if SAMPLES.get(name) is None:
            continue
        err = None
        for cmd in SAMPLES[name]:
            try:
                if callable(cmd):
                    cmd = cmd()
                on(SPACE, *cmd)
            except redis.RedisError as e:
                err = "%s: %s" % (" ".join(cmd), e)
                break
        if err:
            check(False, "%s ran (%s)" % (name, err))
            continue
        check(same_everywhere(nodes), "%s leaves every member the same" % name)

    print("each logged command on the leader, then what each member answers for itself")
    for name, cmds, answer in LOGGED:
        if cmds is None:
            continue                # covered by a test of its own
        err = None
        for cmd in cmds:
            try:
                if callable(cmd):
                    cmd = cmd()
                got = on(SPACE, *cmd)
                if isinstance(got, str) and got.startswith("ERR"):
                    raise redis.ResponseError(got)
            except redis.RedisError as e:
                err = "%s: %s" % (" ".join(str(w)[:40] for w in cmd), e)
                break
        if err:
            check(False, "%s ran (%s)" % (name, err))
            continue
        same = same_everywhere(nodes)
        if answer is not None:
            got = [answer(n) for n in nodes]
            same = same and all(g == got[0] for g in got) and got[0] not in (None, [], 0)
            if not same:
                print("    answers: %s" % [str(g)[:80] for g in got])
        check(same, "%s ran on every member" % name)

    # every write reaches every member, one way or another - TODO 688 took the last
    # local entry (bare KSPACE, a syntax error) out of the table
    check(not local, "no write command is local (%s)" % local)

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
        # a run can't freeze the space it's in; REPLAPPLY was the example until it
        # stopped being local - TODO 684
        "barch.call of SAVE": "function call() return barch.call('SAVE') end",
        "store.load": "function call() return barch.store.load(function(block, shard) return nil end) end",
    }
    for i, (what, code) in enumerate(REFUSED.items()):
        fn = "lr%d" % i
        try:
            on(SPACE, "SETF", fn, code)
            got = on(SPACE, fn)
        except redis.ResponseError as e:
            got = "ERR " + str(e)
        # a function that may write runs from the log (TODO 673), which refuses both
        # before the replicated space would
        check(isinstance(got, str) and ("copy only" in got or "run from the log" in got),
              "Luau %s is refused (%r)" % (what, got))
    check(same_everywhere(nodes), "and every member is still the same")

    print("a range-sharded replicated space: writes, the maintenance sweeps, more writes")
    conf = nodes[0].client("configuration")
    for k, v in (("rs.ordered", "1"), ("rs.range_sharded", "1"), ("rs.shards", "4"), ("rs.raft", "on")):
        conf.execute_command("SET", k, v)
    check(wait_for(90, lambda: leader_of(nodes, "rs") is not None and all(
        "members 3" in n.group_line("rs") and "learners 0" in n.group_line("rs") for n in nodes)),
        "rs has three voters and a leader")
    def rs(*cmd):
        return on("rs", *cmd, idempotent=True)        # SETs and DELs of fixed values

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
    shutil.rmtree(IMPORT_DIR, ignore_errors=True)

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
