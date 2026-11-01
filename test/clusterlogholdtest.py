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

BARCH_TEST_SNAPSHOT_COPY_HOLD_FILE holds the member's copy while the file is there,
and the test takes it away once the leader has held its log for the copy: the writes
have gone past one of the leader's snapshots by then, whatever the machine. The
writers are held to ~800 writes a second, so the log stays well inside the hold's cap.

The leader can also send a snapshot object again after the member answered it, and
under TSan it did, just as a copy finished: the copy was marked as gone, so the repeat
started a new copy of the same snapshot, which then ran on after the member had
installed it and gone on to the log - TODO 808. BARCH_TEST_SNAPSHOT_OBJECT_TWICE makes
the member answer the object that finds the copy done a second time, and log what
that did.

What has to hold:
  - with the member down (and quiet for over a second), the leader compacts as usual:
    a member that isn't answering holds nothing;
  - with the member back and its copy held under writes, the leader holds its log
    (log_holds goes up), and the member catches up while the writes go on, after
    copying the data group's snapshot once, or twice: a snapshot the leader
    finishes just as a transfer starts can compact the log before the hold for
    that transfer is set, and the copy after that one is held;
  - the same object sent again once the copy's done finds it done, and starts no
    other copy;
  - every acknowledged write is there, and all three hold the same copy.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import re
import sys

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
EVERY = 1000          # raft_snapshot_entries: the hold's cap is 10 times that
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




def logged(n, what):
    try:
        return what in open(n.dir + ".log").read()
    except OSError:
        return False


hold = os.path.join(os.getcwd(), "copyhold")
if os.path.exists(hold):
    os.remove(hold)
env = {"BARCH_TEST_SNAPSHOT_COPY_HOLD_FILE": hold, "BARCH_TEST_SNAPSHOT_OBJECT_TWICE": "1"}
nodes = [clusternodes.Node(i, BASE, BINARY, ["raft_snapshot_entries=%d" % EVERY], env) for i in range(3)]
try:
    print("a replicated space that snapshots every %d entries" % EVERY)
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
    c = leader.client(SPACE)
    # a member holds the log until it's been quiet for a second, so this writes a
    # snapshot's worth at a time until a snapshot compacts, not for a time
    written = 0
    for _ in range(10):
        for i in range(written, written + 2 * EVERY + 400 if written == 0 else written + EVERY):
            c.execute_command("SET", "k%d" % i, "v%d" % i)
        written = i + 1
        if leader.field(SPACE, "log_start") > EVERY:
            break
    check(wait_for(30, lambda: leader.field(SPACE, "log_start") > EVERY),
          "the leader compacts past it all the same (log starts at %d)" % leader.field(SPACE, "log_start"))

    print("it comes back and copies a snapshot, the copy held, while four clients write")
    # at most ~800 writes a second: a snapshot every second or so while the copy's
    # held, and well inside the hold's cap of 10000 - TODO 645
    w = clusternodes.Writers([leader, third], SPACE, count=4, prefix="h", pause=0.005)
    holds = leader.field(SPACE, "log_holds")
    seen = len(copies(member))
    open(hold, "w").close()
    w.start()
    member.start()
    check(wait_for(60, lambda: GROUP in copies(member)[seen:] and logged(member, "test: holding a snapshot copy")),
          "it starts copying, and the copy is held")
    check(wait_for(120, lambda: leader.field(SPACE, "log_holds") > holds),
          "the leader holds its log while it copies (%d times)" % (leader.field(SPACE, "log_holds") - holds))
    os.remove(hold)
    check(wait_for(60, lambda: member.field(SPACE, "applied") >= leader.field(SPACE, "log_start")
                   and member.field(SPACE, "applied") + EVERY > leader.field(SPACE, "applied")),
          "it catches up while the writes go on")
    # a copy started again would show by the leader's next snapshot
    snap = leader.field(SPACE, "snapshot")
    check(wait_for(120, lambda: leader.field(SPACE, "snapshot") > snap),
          "the leader makes another snapshot while the writes go on")
    w.stop()
    again = copies(member)[seen:].count(GROUP)
    check(1 <= again <= 2, "copying group %s's snapshot once or twice (%d times)" % (GROUP, again))
    check(logged(member, "test: the same object again found the copy done")
          and not logged(member, "test: the same object again started another copy"),
          "the same object again finds the copy done, and starts no other")

    print("what the cluster holds")
    print("    acknowledged %d, uncertain %d, told %s" % (len(w.acked), len(w.maybe), w.errors))
    missing = on_leader(nodes, SPACE, lambda n: sum(
        1 for k, v in w.acked.items() if n.client(SPACE).execute_command("GET", k) != v))
    missing = len(w.acked) if missing is None else missing
    check(len(w.acked) > 0 and missing == 0, "every acknowledged write is there (%d missing)" % missing)
    check(wait_for(60, lambda: len(set(n.digest(SPACE)[0] for n in nodes)) == 1), "all three hold the same copy")
finally:
    if os.path.exists(hold):
        os.remove(hold)
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
