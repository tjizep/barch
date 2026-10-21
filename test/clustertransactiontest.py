#!/usr/bin/env python3
"""
A transaction in a replicated space - TODO 677.

A transaction is a mark on the whole space: BEGIN marks it, every client's writes go on
landing, ROLLBACK puts the space back to the mark and COMMIT drops it. ROLLBACK used to
do that on the leader only, so it was refused. Now BEGIN, COMMIT and ROLLBACK go in the
space's log and every member runs them at the same place. The mark is in memory only,
so BEGIN also writes a key saying a transaction is open, which a ROLLBACK takes away
with the rest and COMMIT removes. A member with the key and no mark (it restarted or
copied the space during the transaction) is rebuilt when ROLLBACK reaches it.

What has to hold, on a three-node cluster that snapshots every 200 entries:
  - after BEGIN, writes from several clients (past the snapshot interval), a follower
    restarted and the leader killed, a ROLLBACK on the new leader leaves every member
    with the space as it was at BEGIN, and their digests match. A member that
    restarted during the transaction lost the mark, which is in memory only: its
    ROLLBACK says so and it's rebuilt from the others;
  - after BEGIN, writes and COMMIT, every member keeps the writes.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=32900)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def on(members, *cmd):
    """cmd on the space's leader, following it through an election"""
    for _ in range(600):
        l = leader_of(members, SPACE)
        if l is not None:
            try:
                return l.client(SPACE).execute_command(*cmd)
            except redis.exceptions.TryAgainError:
                pass
            except redis.ResponseError as e:
                if not str(e).startswith(("NOTLEADER", "TRYAGAIN")):
                    raise
            except redis.ConnectionError:
                pass
        time.sleep(0.1)
    raise RuntimeError("no leader took %s" % (cmd,))


def local_get(n, key):
    c = n.client(SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c.execute_command("GET", key)


def settled(members):
    l = leader_of(members, SPACE)
    if l is None:
        return False
    top = l.field(SPACE, "applied")
    return wait_for(60, lambda: all(n.field(SPACE, "applied") >= top for n in members))


def writers(members, prefix, count):
    """three clients writing `count` keys each, through whoever leads"""
    def run(w):
        for i in range(count):
            on(members, "SET", "%s%d_%d" % (prefix, w, i), "x")
    ts = [threading.Thread(target=run, args=(w,)) for w in range(3)]
    [t.start() for t in ts]
    [t.join() for t in ts]


nodes = [clusternodes.Node(i, BASE, BINARY, ["raft_snapshot_entries=200"]) for i in range(3)]
try:
    print("a replicated space on three nodes, snapshots every 200 entries")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE)
        and "learners 0" in n.group_line("cluster") for n in nodes)), "three voters and a leader")
    for i in range(10):
        on(nodes, "SET", "k%d" % i, "before")
    settled(nodes)
    before = nodes[0].digest(SPACE)[0]

    print("BEGIN, writes from three clients, overwrites and a delete")
    check(on(nodes, "BEGIN") == "OK", "BEGIN")
    writers(nodes, "a", 100)
    for i in range(10):
        on(nodes, "SET", "k%d" % i, "during")
    on(nodes, "DEL", "k5")

    print("a follower restarted during it")
    leader = leader_of(nodes, SPACE)
    follower = next(n for n in nodes if n is not leader)
    follower.stop()
    writers(nodes, "b", 50)
    follower.start()
    check(settled(nodes), "it catches up")

    print("the leader killed during it")
    leader = leader_of(nodes, SPACE)
    leader.kill()
    rest = [n for n in nodes if n is not leader]
    check(wait_for(60, lambda: leader_of(rest, SPACE) is not None), "the other two elect a leader")
    writers(rest, "c", 20)
    leader.start()
    check(wait_for(120, lambda: settled(nodes)), "the old leader catches up")

    print("ROLLBACK on the new leader")
    # a member restarted during the transaction has its data and no mark: its ROLLBACK
    # says so, and it's rebuilt from the others. When that member leads, the client
    # hears it; every member ends the same either way. Just after an election the
    # leader can also answer UNKNOWN, a write whose commit it couldn't confirm: the
    # checks below say what happened
    try:
        got = on(nodes, "ROLLBACK")
    except redis.ResponseError as e:
        got = "ERR " + str(e)
    check(got == "OK" or "lost the mark" in str(got) or "UNKNOWN" in str(got), "ROLLBACK (%s)" % str(got)[:80])
    check(wait_for(180, lambda: settled(nodes) and len(set(n.digest(SPACE)[0] for n in nodes)) == 1
                   and nodes[0].digest(SPACE)[0] == before),
          "every member comes to the space as it was before BEGIN, rebuilt where it lost the mark")
    # a follower read waits for a known leader, which a member can be between for a
    # moment after the save ROLLBACK makes
    check(wait_for(30, lambda: all([local_get(n, "k%d" % i) for i in range(10)] == ["before"] * 10
                                   for n in nodes)), "every member holds k0..k9 as before BEGIN, k5 too")
    check(wait_for(30, lambda: all(local_get(n, "a0_0") is None and local_get(n, "c2_19") is None
                                   for n in nodes)), "and none of the keys written since")
    digests = [n.digest(SPACE)[0] for n in nodes]
    check(len(set(digests)) == 1 and digests[0] == before,
          "every member's digest is the one from before BEGIN (%s, %s)" % (before, digests))

    print("BEGIN, writes, COMMIT")
    on(nodes, "BEGIN")
    writers(nodes, "d", 20)
    on(nodes, "SET", "k0", "committed")
    check(on(nodes, "COMMIT") == "OK", "COMMIT")
    settled(nodes)
    check(wait_for(30, lambda: all(local_get(n, "k0") == "committed" and local_get(n, "d2_19") == "x"
                                   for n in nodes)),
          "every member keeps the writes")
    check(len(set(n.digest(SPACE)[0] for n in nodes)) == 1, "and their digests match")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
