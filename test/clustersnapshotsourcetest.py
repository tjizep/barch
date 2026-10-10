#!/usr/bin/env python3
"""
A snapshot's source going away while a member copies it - TODO 627.

A member that has fallen behind the leader's compacted log catches up by copying a
snapshot from the leader. NuRaft compacts that member's own log before it applies
the snapshot, so a copy that failed at the apply was fatal: NuRaft stopped the
process. A leader dying mid-copy did it, and so did a leader stopped in the
ordinary way.

BARCH_TEST_SNAPSHOT_COPY_DELAY_MS holds a member that long before it copies, so the
test can stop the source every time.

What has to hold:
  - the member copying stays up when its source is killed;
  - once another node leads, the member copies from it and catches up;
  - with the old leader back, all three hold the same copy.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of, on_leader  # noqa: E402

scale.workdir()
BASE = scale.port(default=27700)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def logged(n, text):
    try:
        return open(n.dir + ".log").read().count(text)
    except OSError:
        return 0


env = {"BARCH_TEST_SNAPSHOT_COPY_DELAY_MS": "1500"}
nodes = [clusternodes.Node(i, BASE, BINARY, ["raft_snapshot_entries=200"], env) for i in range(3)]
try:
    print("a replicated space that snapshots every 200 entries")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE)
        and "learners 0" in n.group_line("cluster") for n in nodes)), "three voters and a leader")

    print("a follower falls behind the leader's compacted log")
    leader = leader_of(nodes, SPACE)
    follower = next(n for n in nodes if n is not leader)
    third = next(n for n in nodes if n is not leader and n is not follower)
    follower.stop()
    c = leader.client(SPACE)
    for i in range(1200):
        c.execute_command("SET", "k%d" % i, "v%d" % i)
    check(wait_for(30, lambda: leader.field(SPACE, "log_start") > 200),
          "the leader compacted its log (it starts at %d)" % leader.field(SPACE, "log_start"))

    print("its source is killed while it copies the snapshot")
    before = logged(follower, "copying a snapshot")
    follower.start()
    check(wait_for(30, lambda: logged(follower, "copying a snapshot") > before),
          "the follower starts copying a snapshot")
    leader.kill()
    time.sleep(3)
    check(follower.proc is not None and follower.proc.poll() is None,
          "and stays up when the source is gone (%s)" % (follower.proc and follower.proc.poll()))
    check(wait_for(60, lambda: leader_of([follower, third], SPACE) is not None), "another node leads")
    check(wait_for(90, lambda: follower.digest(SPACE) == third.digest(SPACE)),
          "the follower copies from it and catches up")

    print("the old leader comes back")
    leader.start()
    check(wait_for(90, lambda: len(set(n.digest(SPACE)[0] for n in nodes)) == 1), "all three hold the same copy")
    check(on_leader(nodes, SPACE, lambda n: all(n.client(SPACE).execute_command("GET", "k%d" % i) == "v%d" % i
                                                for i in range(0, 1200, 37))) is True,
          "with the writes in it")
finally:
    for n in nodes:
        if n.proc and n.proc.poll() is not None:
            n.proc = None                   # it died: no stop to wait for
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
