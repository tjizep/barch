#!/usr/bin/env python3
"""
A node stopped while it copies a snapshot doesn't start another copy - TODO 638.

group_rt::stop() took the copy thread and waited for it, with NuRaft still running.
NuRaft asks for the snapshot again every 50ms until the copy's done, and each time
found no copy going, so it started a second one. That copy ran into the same shards
as the first (TSan caught one receiving files while the other cleared a shard to
install its own), and nothing waited for it.

BARCH_TEST_SNAPSHOT_COPY_DELAY_MS holds the member's copy for 4s, so the stop lands
in the middle of it every time.

What has to hold:
  - between the stop starting and the process exiting, the group whose copy is held
    starts no other copy;
  - it exits cleanly, and in time;
  - started again, it catches up and all three hold the same copy.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=29500)
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


def copies(n):
    """the groups of the copies n has logged, in order"""
    try:
        return re.findall(r"raft group (\d+) copying a snapshot", open(n.dir + ".log").read())
    except OSError:
        return []


env = {"BARCH_TEST_SNAPSHOT_COPY_DELAY_MS": "4000"}
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
    follower.stop()
    c = leader.client(SPACE)
    for i in range(1200):
        c.execute_command("SET", "k%d" % i, "v%d" % i)
    check(wait_for(30, lambda: leader.field(SPACE, "log_start") > 200),
          "the leader compacted its log (it starts at %d)" % leader.field(SPACE, "log_start"))

    print("it's stopped while it copies the snapshot")
    before = logged(follower, "copying a snapshot")
    follower.start()
    check(wait_for(30, lambda: logged(follower, "copying a snapshot") > before),
          "it starts copying (held for 4s)")
    # the delay holds only the first copy, so that's the group the stop lands in;
    # another group can still start its first copy before its own stop begins
    seen = copies(follower)
    held = seen[before]
    took = time.time()
    follower.stop()
    took = time.time() - took
    extra = copies(follower)[len(seen):].count(held)
    check(extra == 0, "group %s starts no other copy while it stops (%d did)" % (held, extra))
    check(not clusternodes.bad_exits, "and exits cleanly (%s, in %.1fs)" % (clusternodes.bad_exits, took))
    del clusternodes.bad_exits[:]

    print("started again")
    follower.start()
    check(wait_for(90, lambda: len(set(n.digest(SPACE)[0] for n in nodes)) == 1),
          "it catches up and all three hold the same copy")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
