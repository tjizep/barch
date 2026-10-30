#!/usr/bin/env python3
"""
A primary's stream into a replicated space - TODO 684.

REPLAPPLY applied a primary's records under repl::applying, which is how a Raft member
applies its log, so in a replicated space it wrote the receiving node's copy only and
the members diverged without a word. Now the records of a replicated space go into its
log as batch entries, as they came, and every member applies them. The receiving node
stays the replica and keeps the positions; when it doesn't lead the space it sends the
records to the leader (REPLRAFT). A refusal that passes, such as an election, is HOLD,
so the primary keeps the batch and sends it again.

What has to hold, with a standalone primary publishing to a follower of a three-node
cluster:
  - strings, a hash, a list and deletes in the replicated space reach every member,
    with matching digests and the primary's values, through the leader;
  - a space that isn't replicated takes the stream on the receiving node only, as
    before;
  - a stored function that writes, called on the primary, arrives once, as records
    (TODO 686: its call isn't sent as well), and a cached miss on the primary is on
    every member, through the leader (REPLMISS);
  - the space's leader killed while the primary goes on writing: nothing is lost,
    and the old leader comes back to the same digest.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import shutil
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=18700)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
OWN = "mine"
SHARDS = os.environ.get("BARCH_TEST_RAFT_SHARDS", "32")
PRIMARY = BASE + 19
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def local(n, space=SPACE):
    c = n.client(space)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c


def settled(members):
    l = leader_of(members, SPACE)
    if l is None:
        return False
    top = l.field(SPACE, "applied")
    return wait_for(60, lambda: all(n.field(SPACE, "applied") >= top for n in members))


def same(members):
    return len(set(n.digest(SPACE)[0] for n in members)) == 1


def replraft_calls(n):
    """how many REPLRAFT calls a node took, from INFO commandstats"""
    info = n.client().info("commandstats")
    return info.get("cmdstat_replraft", {}).get("calls", 0)


def count(n, prefix, seconds=60):
    """
    KEYS on one member, asked again while it says not now - TODO 781. A follower read is
    refused while the member can't see a live leader, and a leader's while it's still
    applying. A member just back from a kill can be in either for a moment after
    settled() and same() pass, when its groups time out and call elections. Only those
    refusals are retried; the count itself still has to be right.
    """
    end = time.time() + seconds
    while True:
        try:
            return len(local(n).execute_command("KEYS", prefix + "*"))
        except redis.ResponseError as e:
            # redis-py turns TRYAGAIN into its own error, with the word taken off
            not_now = isinstance(e, redis.exceptions.TryAgainError) or str(e).startswith("NOTLEADER")
            if not not_now or time.time() >= end:
                raise
            time.sleep(0.1)


nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]
primary = None
try:
    print("a replicated space on three nodes, and a standalone primary")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE)
        and "learners 0" in n.group_line("cluster") for n in nodes)), "three voters and a leader")

    pdir = os.path.join(os.getcwd(), "primary")
    shutil.rmtree(pdir, ignore_errors=True)
    os.makedirs(pdir)
    primary = subprocess.Popen([BINARY, "--port", str(PRIMARY), "--bind", "127.0.0.1", "--dir", pdir],
                               stdout=open(pdir + ".log", "a"), stderr=subprocess.STDOUT)
    p = redis.Redis(port=PRIMARY, decode_responses=True)
    check(wait_for(30, lambda: p.ping()), "the primary is up")
    p.execute_command("configuration:SET", SPACE + ".shards", SHARDS)
    # the primary reads through a fake source, so a miss there is published - TODO 686
    p.execute_command("configuration:SET", SPACE + ".foreign", "fake")
    p.execute_command("configuration:SET", SPACE + ".missing_ttl", "600")
    p.execute_command("configuration:SET", OWN + ".shards", SHARDS)
    # each node's own space, with the primary's shard count, before the stream
    (leader_of(nodes, "cluster") or nodes[0]).client("configuration").execute_command(
        "SET", OWN + ".shards", SHARDS)
    receiver = next(n for n in nodes if n is not leader_of(nodes, SPACE))
    ps = redis.Redis(port=PRIMARY, decode_responses=True, single_connection_client=True)
    ps.execute_command("USE", SPACE)
    pm = redis.Redis(port=PRIMARY, decode_responses=True, single_connection_client=True)
    pm.execute_command("USE", OWN)
    p.execute_command("PUBLISH", "127.0.0.1", str(receiver.port))

    print("writes on the primary, published to a follower")
    for i in range(200):
        ps.set("k%d" % i, "v%d" % i)
    ps.hset("h", mapping={"a": "1", "b": "2"})
    ps.rpush("l", "x", "y", "z")
    for i in range(0, 200, 10):
        ps.delete("k%d" % i)
    for i in range(20):
        pm.set("m%d" % i, "own")
    ps.set("last", "1")
    check(wait_for(60, lambda: all(local(n).execute_command("GET", "last") == "1" for n in nodes)),
          "every member has the last write")
    check(settled(nodes) and all(count(n, "k") == 180 for n in nodes),
          "every member holds the 180 keys left (%s)" % [count(n, "k") for n in nodes])
    check(all(local(n).execute_command("HGET", "h", "b") == "2"
              and local(n).execute_command("LRANGE", "l", 0, -1) == ["x", "y", "z"] for n in nodes),
          "and the hash and the list")
    check(all(local(n).execute_command("GET", "k5") == "v5" and local(n).execute_command("GET", "k10") is None
              for n in nodes), "with the primary's values and deletes")
    check(same(nodes), "every member's digest matches")
    check(replraft_calls(leader_of(nodes, SPACE)) > 0 and replraft_calls(receiver) == 0,
          "the follower sent them to the leader (REPLRAFT %s)" % [replraft_calls(n) for n in nodes])
    check(wait_for(30, lambda: receiver.client(OWN).execute_command("GET", "m19") == "own"),
          "the space that isn't replicated takes it on the receiver")
    check(all(n.client(OWN).execute_command("GET", "m19") is None for n in nodes if n is not receiver),
          "and only there")

    print("a stored function that writes, and a cached miss, on the primary")
    bump = """
function bump(k) local n = tonumber(barch.store.get(k) or "0") + 1 barch.store.set(k, tostring(n)) return n end
function call() return 1 end
function transport() return {kind = "resp", methods = {BUMPIT = bump}, categories = {BUMPIT = {"write", "data"}},
                             arity = {BUMPIT = 1}} end
"""
    ps.execute_command("SETF", "bumper", bump)
    check(ps.execute_command("BUMPIT", "cnt") == 1, "BUMPIT on the primary")
    ps.set("last", "3")
    check(wait_for(60, lambda: all(local(n).execute_command("GET", "last") == "3" for n in nodes)),
          "the stream reaches every member")
    check(all(local(n).execute_command("GET", "cnt") == "1" for n in nodes),
          "its write arrives once, as a record (%s)" % [local(n).execute_command("GET", "cnt") for n in nodes])
    before = [n.digest(SPACE)[0] for n in nodes]
    check(ps.get("missing") is None, "a GET of a key the primary's source doesn't have")
    ps.set("last", "4")
    check(wait_for(60, lambda: settled(nodes) and all(local(n).execute_command("GET", "last") == "4" for n in nodes)
                   and same(nodes)), "the stream goes on, every member the same")
    after = [n.digest(SPACE)[0] for n in nodes]
    check(after[0] != before[0] and len(set(after)) == 1,
          "the miss is on every member (%s -> %s)" % (before[0], after[0]))

    print("the space's leader killed while the primary writes")
    leader = leader_of(nodes, SPACE)
    for i in range(200, 300):
        ps.set("k%d" % i, "v%d" % i)
    leader.kill()
    rest = [n for n in nodes if n is not leader]
    for i in range(300, 400):
        ps.set("k%d" % i, "v%d" % i)
    ps.set("last", "2")
    check(wait_for(120, lambda: all(local(n).execute_command("GET", "last") == "2" for n in rest)),
          "the stream goes on with the new leader")
    check(settled(rest) and all(count(n, "k") == 380 for n in rest),
          "nothing is lost (%s)" % [count(n, "k") for n in rest])
    leader.start()
    check(wait_for(120, lambda: settled(nodes) and same(nodes)), "the old leader comes back to the same digest")
    check(count(leader, "k") == 380, "and holds every key")
finally:
    for n in nodes:
        n.stop()
    if primary:
        primary.terminate()
        primary.wait(30)

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
