#!/usr/bin/env python3
"""
A key-by-key copy holds whole runs from the log - TODO 674.

Members with different page layouts copy a space key by key. The copy paged through the
sender's live shards, so it could carry some of a stored function's writes and not the
rest, and the marker saying which runs are in from before or after them. The log applied
from the snapshot then ran that function again over half its writes, or not at all. Now
the sender takes the keys at one point (runs waited out, under the space write lock) and
serves the copy from that.

What has to hold: three nodes, and a fourth with another page layout joining while a
function from the log bumps 20 keys over and over. Once it's a voter and the bumps have
stopped, every member's digest is the same, and the fourth node's 20 counters are
equal to each other and to the leader's.

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
BASE = scale.port(default=35500)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "counts"
KEYS = 20
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def on_leader(members, *cmd):
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
        time.sleep(0.05)
    raise RuntimeError("no leader took %s" % (cmd,))


def counters(n):
    c = n.client(SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return [c.execute_command("GET", "c%d" % i) for i in range(KEYS)]


def settled(members):
    l = leader_of(members, SPACE)
    if l is None:
        return False
    top = l.field(SPACE, "applied")
    return wait_for(60, lambda: all(n.field(SPACE, "applied") >= top for n in members))


nodes = [clusternodes.Node(i, BASE, BINARY, ["raft_snapshot_entries=50"]) for i in range(3)]
other = clusternodes.Node(3, BASE, BINARY, ["raft_snapshot_entries=50"], env={"BARCH_TEST_LAYOUT": "test-other-layout", **({"BARCH_TEST_LIVE_EXPORT": "1"} if os.environ.get("LIVE") else {})})
try:
    print("a replicated space on three nodes, and a function from the log")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE) for n in nodes)),
        "three voters and a leader")
    on_leader(nodes, "SETF", "BUMP", '''
        function call()
            for i = 0, %d do
                local k = "c" .. i
                barch.store.set(k, tostring((tonumber(barch.store.get(k) or "0") or 0) + 1))
            end
            return 1
        end''' % (KEYS - 1))
    for _ in range(100):
        on_leader(nodes, "BUMP")

    print("a fourth node, with another page layout, joins while the function runs")
    stop = threading.Event()
    bumped = [0]

    def bumper():
        while not stop.is_set():
            try:
                on_leader(nodes, "BUMP")
                bumped[0] += 1
            except Exception:
                pass

    t = threading.Thread(target=bumper)
    t.start()
    other.start()
    other.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    everyone = nodes + [other]
    check(wait_for(180, lambda: "members 4" in other.group_line(SPACE) and "learners 0" in other.group_line(SPACE)),
          "it becomes a voter")
    time.sleep(2)
    stop.set()
    t.join()
    print("    %d calls while it joined" % bumped[0])
    check(settled(everyone), "every member has applied the log")
    got = counters(other)
    check(len(set(got)) == 1, "the fourth node's counters are equal to each other (%s)" % sorted(set(got)))
    lead = leader_of(everyone, SPACE)
    check(got == counters(lead), "and to the leader's (%s, %s)" % (got[0], counters(lead)[0]))
    digests = [n.digest(SPACE)[0] for n in everyone]
    check(len(set(digests)) == 1, "every member's digest is the same (%s)" % digests)
    logs = open(other.dir + ".log", errors="replace").read()
    check("key by key" in logs, "the fourth node copied the space key by key")
finally:
    for n in nodes + [other]:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
