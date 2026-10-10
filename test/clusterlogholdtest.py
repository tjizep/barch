#!/usr/bin/env python3
"""
The leader keeps the log a member copying a snapshot will need - TODO 636.

A member that's behind the leader's compacted log copies a snapshot, and then
catches up from the log after it. The leader compacted its log every snapshot no
matter who still needed it, so with writes going on, a copy that took longer than
the log the leader kept finished behind it, and the member was sent another
snapshot. Under TSan a joining node copied 77 in a row, and was only promoted once
the writes stopped.

Now the leader doesn't compact past the oldest entry a member that's answering
still needs, by up to 10 snapshots' worth of entries, and the log after a snapshot
it's sending. A member that hasn't answered for a second holds nothing.

BARCH_TEST_SNAPSHOT_COPY_DELAY_MS holds every copy of the data group
(BARCH_TEST_SNAPSHOT_COPY_DELAY_GROUP) for 3s, so the writes go past the leader's
next snapshots while it copies. Before the fix, every copy ended behind the log, and
the member never caught up while the writes went on.

What has to hold:
  - with the member down (and quiet for over a second), the leader compacts as usual:
    a member that isn't answering holds nothing;
  - with the member back and copying under writes, the leader holds its log
    (log_holds goes up), and the member catches up while the writes go on, after
    copying the data group's snapshot once, or twice: a snapshot the leader
    finishes just as a transfer starts can compact the log before the hold for
    that transfer is set, and the copy after that one is held;
  - every acknowledged write is there, and all three hold the same copy.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of, on_leader  # noqa: E402

scale.workdir()
BASE = scale.port(default=29700)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
GROUP = "1"           # the first replicated space's group
failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def copies(n):
    """the groups of the copies n has logged, in order"""
    try:
        return re.findall(r"raft group (\d+) copying a snapshot", open(n.dir + ".log").read())
    except OSError:
        return []


env = {"BARCH_TEST_SNAPSHOT_COPY_DELAY_MS": "3000", "BARCH_TEST_SNAPSHOT_COPY_DELAY_GROUP": GROUP}
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
    check(SPACE in nodes[0].group_line(SPACE) and nodes[0].group_line(SPACE).startswith("group " + GROUP + " "),
          "%s is group %s (%s)" % (SPACE, GROUP, nodes[0].group_line(SPACE)[:20]))

    print("a member is down while the leader writes")
    leader = leader_of(nodes, SPACE)
    member = next(n for n in nodes if n is not leader)
    third = next(n for n in nodes if n is not leader and n is not member)
    member.stop()
    time.sleep(2)       # a member holds the log until it's been quiet for a second
    c = leader.client(SPACE)
    for i in range(1200):
        c.execute_command("SET", "k%d" % i, "v%d" % i)
    check(wait_for(30, lambda: leader.field(SPACE, "log_start") > 200),
          "the leader compacts past it all the same (log starts at %d)" % leader.field(SPACE, "log_start"))

    print("it comes back and copies a snapshot, each copy held for 3s, while four clients write")
    w = clusternodes.Writers([leader, third], SPACE, count=4, prefix="h")
    w.start()
    holds = leader.field(SPACE, "log_holds")
    seen = len(copies(member))
    member.start()
    check(wait_for(60, lambda: GROUP in copies(member)[seen:]), "it starts copying")
    check(wait_for(60, lambda: member.field(SPACE, "applied") >= leader.field(SPACE, "log_start")
                   and member.field(SPACE, "applied") + 200 > leader.field(SPACE, "applied")),
          "it catches up while the writes go on")
    time.sleep(scale.scaled_seconds(2.0))
    w.stop()
    again = copies(member)[seen:].count(GROUP)
    check(1 <= again <= 2, "copying group %s's snapshot once or twice (%d times)" % (GROUP, again))
    held = leader.field(SPACE, "log_holds") - holds
    check(held > 0, "the leader held its log for it (%d times)" % held)

    print("what the cluster holds")
    print("    acknowledged %d, uncertain %d, told %s" % (len(w.acked), len(w.maybe), w.errors))
    missing = on_leader(nodes, SPACE, lambda n: sum(
        1 for k, v in w.acked.items() if n.client(SPACE).execute_command("GET", k) != v))
    missing = len(w.acked) if missing is None else missing
    check(len(w.acked) > 0 and missing == 0, "every acknowledged write is there (%d missing)" % missing)
    check(wait_for(60, lambda: len(set(n.digest(SPACE)[0] for n in nodes)) == 1), "all three hold the same copy")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
