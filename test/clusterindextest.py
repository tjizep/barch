#!/usr/bin/env python3
"""
An index in a replicated space is on every member - TODO 669.

INDEX CREATE, BUILD and DROP go in the space's log and every member runs them, so each
member keeps its own `<space>_ix` paths and follows the writes it applies. A member
that installs a snapshot gets the data but not those paths, so it builds its indexes
again from their definitions.

What has to hold, on a three-node cluster that snapshots every 200 entries:
  - after CREATE and BUILD on the leader, every member's own INDEX FIND answers;
  - a key written after the build is found on every member;
  - a member stopped while 1200 keys are written comes back by installing a snapshot,
    and its FIND finds those keys too;
  - with the leader killed, the new leader takes writes and every live member's FIND
    finds them.

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
BASE = scale.port(default=31300)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "ixs"
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def on(nodes, *cmd):
    """cmd on the space's leader, following it through an election"""
    for _ in range(300):
        l = leader_of(nodes, SPACE)
        if l is not None:
            try:
                return l.client(SPACE).execute_command(*cmd)
            except redis.ResponseError as e:
                if not str(e).startswith(("NOTLEADER", "TRYAGAIN")) and \
                        not isinstance(e, redis.exceptions.TryAgainError):
                    raise
            except redis.ConnectionError:
                pass
        time.sleep(0.1)
    raise RuntimeError("no leader took %s" % (cmd,))


def finds(n, value, key):
    """whether member n's own index answers `key` for field 2 = value"""
    try:
        return key in n.client(SPACE).execute_command("INDEX", "FIND", "ix", "2=" + value)
    except redis.RedisError:
        return False


def all_find(members, value, key, seconds=30):
    return wait_for(seconds, lambda: all(finds(n, value, key) for n in members))


nodes = [clusternodes.Node(i, BASE, BINARY, ["raft_snapshot_entries=200"]) for i in range(3)]
try:
    print("a replicated space with composite keys on three nodes")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    conf = nodes[0].client("configuration")
    conf.execute_command("SET", SPACE + ".key_split", "|")
    conf.execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE)
        and "learners 0" in n.group_line("cluster") for n in nodes)), "three voters and a leader")

    print("CREATE and BUILD on the leader")
    for i in range(50):
        on(nodes, "SET", "a|b|s%d|%d" % (i, i), "v")
    check(on(nodes, "INDEX", "CREATE", "ix", "COMPOSITE", "3", "PINNED", "1") == "OK", "CREATE")
    on(nodes, "INDEX", "BUILD", "ix", "ALL")
    check(all_find(nodes, "s7", "a|b|s7|7"), "every member's own FIND answers")

    print("a write after the build")
    on(nodes, "SET", "a|b|n1|100", "v")
    check(all_find(nodes, "n1", "a|b|n1|100"), "is found on every member")

    print("a member stopped while 1200 keys are written comes back from a snapshot")
    leader = leader_of(nodes, SPACE)
    follower = next(n for n in nodes if n is not leader)
    follower.stop()
    time.sleep(2)       # it holds the leader's log until it's been quiet a second - TODO 636, 645
    for i in range(1200):
        on(nodes, "SET", "a|b|f%d|%d" % (i, 1000 + i), "v")
    check(wait_for(30, lambda: leader.field(SPACE, "log_start") > 200),
          "the leader compacted its log (it starts at %d)" % leader.field(SPACE, "log_start"))
    follower.start()
    check(wait_for(90, lambda: follower.field(SPACE, "snapshots_installed") >= 1
                   and len(set(n.digest(SPACE)[0] for n in nodes)) == 1),
          "it installs a snapshot and holds the same copy (%d installed)"
          % follower.field(SPACE, "snapshots_installed"))
    check(all_find([follower], "f1000", "a|b|f1000|2000"), "its FIND finds a key from the snapshot")
    check(all_find([follower], "s7", "a|b|s7|7"), "and one from before")

    print("the leader killed")
    leader = leader_of(nodes, SPACE)
    leader.kill()
    rest = [n for n in nodes if n is not leader]
    check(wait_for(60, lambda: leader_of(rest, SPACE) is not None), "the other two elect a leader")
    on(rest, "SET", "a|b|after|1", "v")
    check(all_find(rest, "after", "a|b|after|1"), "a write on the new leader is found on both")
    check(all_find(rest, "f5", "a|b|f5|1005"), "and so is everything before")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
