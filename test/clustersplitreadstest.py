#!/usr/bin/env python3
"""
Single-key reads in a split space are checked against the key's group - TODO 796.

In a space split over several Raft groups, a read that touches one key (a hash,
list or ordered-set read, or GET and the like) isn't checked up front: a command
can't say which group its key lands in. It's checked where it takes the key's
shard latch instead. That check was only in the shared latch, so the reads that
take the write latch were never checked at all. Sent to a node that doesn't lead
the key's group, they answered from its own copy, which can be behind, where they
should have said NOTLEADER.

Three groups led by three different nodes, and a hash, a list and an ordered set
in every group. For every family read, on every node:
  - a key whose group the node leads is answered there;
  - a key whose group it doesn't lead gets NOTLEADER, naming that group's leader;
  - and with CLUSTER READS FOLLOWER, it's answered there too.
A type check also looks at the other kind of key of the same name (a hash read at
the plain key, a GET at the containers), and that's often in another group. So for
missing keys, and keys of the wrong type, exactly one node answers and the other two
name it, where before no node could, or every node did.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for  # noqa: E402

scale.workdir()
BASE = scale.port(default=24400)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
LABELS = ["%s/%d" % (SPACE, k) for k in range(3)]
failures = 0


def check(ok, what):
    global failures
    print("  %-70s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]
by_port = {n.port: n for n in nodes}

# each read, by the kind of key it reads
READS = {
    "hash": [["HGET", "{k}", "f"], ["HMGET", "{k}", "f", "g"], ["HLEN", "{k}"], ["HKEYS", "{k}"],
             ["HVALS", "{k}"], ["HEXISTS", "{k}", "f"], ["HSTRLEN", "{k}", "f"], ["HGETALL", "{k}"],
             ["HSCAN", "{k}", "0"], ["HRANDFIELD", "{k}"], ["HTTL", "{k}", "FIELDS", "1", "f"],
             ["HEXPIRETIME", "{k}", "FIELDS", "1", "f"]],
    "list": [["LLEN", "{k}"], ["LRANGE", "{k}", "0", "-1"], ["LFRONT", "{k}"], ["LBACK", "{k}"]],
    "zset": [["ZCARD", "{k}"], ["ZSCORE", "{k}", "a"], ["ZMSCORE", "{k}", "a", "b"],
             ["ZRANGE", "{k}", "0", "-1"], ["ZREVRANGE", "{k}", "0", "-1"], ["ZCOUNT", "{k}", "0", "10"],
             ["ZRANGEBYSCORE", "{k}", "0", "10"], ["ZREVRANGEBYSCORE", "{k}", "10", "0"],
             ["ZRANGEBYLEX", "{k}", "-", "+"], ["ZREVRANGEBYLEX", "{k}", "+", "-"],
             ["ZLEXCOUNT", "{k}", "-", "+"], ["ZRANK", "{k}", "a"], ["ZREVRANK", "{k}", "a"],
             ["ZRANDMEMBER", "{k}"]],
    "string": [["GET", "{k}"], ["STRLEN", "{k}"], ["TTL", "{k}"]],
}
WRITES = {
    "hash": [["HSET", "{k}", "f", "1", "g", "2"]],
    "list": [["RPUSH", "{k}", "x", "y"]],
    "zset": [["ZADD", "{k}", "1", "a", "2", "b"]],
    "string": [["SET", "{k}", "v"]],
}


def line_of(n, label):
    for l in n.info():
        if (" label %s" % label) in l:
            return l
    return ""


def leader_of_group(label):
    for n in nodes:
        try:
            if "(this node)" in line_of(n, label):
                return n
        except redis.RedisError:
            pass
    return None


def following(args, start=None):
    """a command through NOTLEADER redirects; the reply and the node that answered"""
    target = start or nodes[0]
    for _ in range(10):
        try:
            return target.client(SPACE).execute_command(*args), target
        except redis.ResponseError as e:
            m = re.match(r"NOTLEADER 127\.0\.0\.1:(\d+)", str(e))
            if not m:
                time.sleep(0.2)
                continue
            target = by_port[int(m.group(1))]
    raise AssertionError("no answer for %s" % (args,))


def fill(cmd, key):
    return [a.format(k=key) for a in cmd]


try:
    print("a space split over three groups, led by three nodes")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    conf = nodes[0].client("configuration")
    conf.execute_command("SET", SPACE + ".raft_groups", "3")
    conf.execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(60, lambda: all("members 3" in line_of(n, l) and "learners 0" in line_of(n, l)
                                   for n in nodes for l in LABELS)),
          "three groups, three voters each")
    check(wait_for(60, lambda: len({leader_of_group(l) for l in LABELS} - {None}) == 3),
          "led by three different nodes")

    # keys of each kind until every node has one it leads and one it doesn't
    print("keys of each kind in every group")
    keys = {kind: {} for kind in READS}       # kind -> {key: the node that leads its group}
    for kind in READS:
        for i in range(60):
            key = "%s%d" % (kind, i)
            for w in WRITES[kind]:
                # a write is answered by the leader of its key's group
                _, owner = following(fill(w, key))
            keys[kind][key] = owner
            if len(set(keys[kind].values())) == 3 and i >= 8:
                break
        check(len(set(keys[kind].values())) == 3, "%s keys in every group (%d)" % (kind, len(keys[kind])))
    # every node's copy has them, so a read answered here isn't just missing a key
    for n in nodes:
        def has_all(n=n):
            try:
                c = n.client(SPACE)
                c.execute_command("CLUSTER", "READS", "FOLLOWER")
                return all(c.execute_command("EXISTS", k) == 1 for kk in keys.values() for k in kk)
            except redis.RedisError:
                return False
        check(wait_for(30, has_all), "node %d's copy holds every key" % (n.i + 1))

    print("each read on every node, for a key of its own groups and of the others")
    for kind, cmds in READS.items():
        for cmd in cmds:
            answered_here, refused, wrong, follower = 0, 0, [], 0
            for n in nodes:
                c = n.client(SPACE)
                f = n.client(SPACE)
                f.execute_command("CLUSTER", "READS", "FOLLOWER")
                for key, owner in keys[kind].items():
                    args = fill(cmd, key)
                    try:
                        c.execute_command(*args)
                        if owner is n:
                            answered_here += 1
                        else:
                            wrong.append("node %d answered %s" % (n.i + 1, key))
                    except redis.ResponseError as e:
                        m = re.match(r"NOTLEADER 127\.0\.0\.1:(\d+)", str(e))
                        if owner is not n and m and by_port[int(m.group(1))] is owner:
                            refused += 1
                        else:
                            wrong.append("node %d: %s: %s" % (n.i + 1, key, e))
                    if owner is not n:
                        try:
                            f.execute_command(*args)
                            follower += 1
                        except redis.ResponseError as e:
                            wrong.append("node %d, follower reads: %s: %s" % (n.i + 1, key, e))
            check(not wrong and answered_here > 0 and refused > 0 and follower == refused,
                  "%-16s here %d, NOTLEADER %d, follower reads %d%s"
                  % (cmd[0], answered_here, refused, follower, (" - " + wrong[0]) if wrong else ""))

    # A type check looks at the other kind of key too: a container command at the
    # plain key, a string command at the containers. The two are often in different
    # groups, and a node only has to lead the one the command reads.
    def one_answers(args):
        """what each node said: exactly one answers, and the others name it"""
        answered, refused = [], []
        for n in nodes:
            try:
                answered.append((n, n.client(SPACE).execute_command(*args)))
            except redis.ResponseError as e:
                m = re.match(r"NOTLEADER 127\.0\.0\.1:(\d+)", str(e))
                if m:
                    refused.append(by_port[int(m.group(1))])
                else:
                    answered.append((n, str(e).split()[0]))
        if len(answered) != 1 or any(r is not answered[0][0] for r in refused):
            return None, "answered %s, sent to %s" % (
                [(a.i + 1, v) for a, v in answered], [r.i + 1 for r in refused])
        return answered[0][1], ""

    print("keys that aren't there, and keys of the wrong type")
    cases = [
        ("GET of a missing key", lambda i: ["GET", "none%d" % i], lambda v: v is None),
        ("STRLEN of a missing key", lambda i: ["STRLEN", "none%d" % i], lambda v: v == 0),
        ("GETRANGE of a missing key", lambda i: ["GETRANGE", "none%d" % i, "0", "-1"], lambda v: v == b"" or v == ""),
        ("HGET of a missing key", lambda i: ["HGET", "none%d" % i, "f"], lambda v: v is None),
        ("LLEN of a missing key", lambda i: ["LLEN", "none%d" % i], lambda v: v == 0),
        ("ZCARD of a missing key", lambda i: ["ZCARD", "none%d" % i], lambda v: v == 0),
        ("HGET of a string", lambda i: ["HGET", "string%d" % i, "f"], lambda v: v == "WRONGTYPE"),
        ("LLEN of a string", lambda i: ["LLEN", "string%d" % i], lambda v: v == "WRONGTYPE"),
        ("GET of a hash", lambda i: ["GET", "hash%d" % i], lambda v: v == "WRONGTYPE"),
        ("STRLEN of a list", lambda i: ["STRLEN", "list%d" % i], lambda v: v == "WRONGTYPE"),
    ]
    for what, make, ok in cases:
        odd = []
        for i in range(9):
            value, why = one_answers(make(i))
            if why or not ok(value):
                odd.append("%s: %s" % (make(i)[1], why or repr(value)))
        check(not odd, "%-26s one node answers%s" % (what, (" - " + odd[0]) if odd else ""))
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
