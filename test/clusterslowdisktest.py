#!/usr/bin/env python3
"""
A leader keeps leading while its followers' disks are slow - TODO 641.

A follower answers new entries only once they're synced. The leader used to step
down when a quorum hadn't answered it for 300ms, the same as its read lease, so a
slow disk (206-239ms a sync on CI's runners) cost it its leadership: it cancelled
the writes in flight, which their clients were told were UNKNOWN, and the group had
no leader for half a second. Now it steps down only after twice the longest election
timeout, and reads wait out the lease instead.

BARCH_TEST_LOG_SYNC_DELAY_MS makes every log sync of the space's group
(BARCH_TEST_LOG_SYNC_DELAY_GROUP) on the two followers take 400ms longer
(BARCH_TEST_SLOW_SYNC_MS for another figure). With the old step-down, 400ms moved
the term from 1 to 10 in 8s, the leader yielded 3 times, and 37 writes were told
UNKNOWN; 250ms wasn't enough to show it here.

What has to hold, with eight clients writing:
  - node 1, the group's preferred member, leads it throughout, in the same term;
  - no write is cancelled (none told UNKNOWN);
  - every acknowledged write is there, and the copies match.

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
BASE = scale.port(default=29900)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
SLOW_MS = int(os.environ.get("BARCH_TEST_SLOW_SYNC_MS", "400"))
failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def stepped_down(n):
    try:
        return len(re.findall(r"raft group 1 .*will yield the leadership", open(n.dir + ".log").read()))
    except OSError:
        return 0


# the space's group only: slowing the cluster group too backs it up with heartbeats,
# which is another matter (TODO 642)
slow = {"BARCH_TEST_LOG_SYNC_DELAY_MS": str(SLOW_MS), "BARCH_TEST_LOG_SYNC_DELAY_GROUP": "1"}
nodes = [clusternodes.Node(i, BASE, BINARY, env=None if i == 0 else slow) for i in range(3)]
try:
    print("a replicated space whose followers' syncs take %dms longer" % SLOW_MS)
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: leader_of(nodes, SPACE) is nodes[0] and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE) for n in nodes)),
        "three voters, and node 1 leads")
    term = nodes[0].field(SPACE, "term")

    print("eight clients write")
    w = clusternodes.Writers(nodes, SPACE, count=8, prefix="s")
    w.start()
    led = []
    end = time.time() + scale.scaled_seconds(8.0, floor=6.0)
    while time.time() < end:
        led.append(nodes[0].leads(SPACE))
        time.sleep(0.1)
    w.stop()
    print("    acknowledged %d, uncertain %d, told %s" % (len(w.acked), len(w.maybe), w.errors))
    after = nodes[0].field(SPACE, "term")
    check(all(led) and after == term,
          "node 1 leads throughout, in the same term (%d then %d, %d of %d checks)"
          % (term, after, sum(led), len(led)))
    check(stepped_down(nodes[0]) == 0, "it never yields its leadership (%d times)" % stepped_down(nodes[0]))
    unknown = sum(v for k, v in w.errors.items() if k.startswith("UNKNOWN"))
    check(unknown == 0 and not w.maybe, "no write is cancelled (%d told UNKNOWN)" % unknown)

    print("what the cluster holds")
    missing = on_leader(nodes, SPACE, lambda n: sum(
        1 for k, v in w.acked.items() if n.client(SPACE).execute_command("GET", k) != v))
    missing = len(w.acked) if missing is None else missing
    check(len(w.acked) > 0 and missing == 0, "every acknowledged write is there (%d, %d missing)"
          % (len(w.acked), missing))
    check(wait_for(60, lambda: len(set(n.digest(SPACE)[0] for n in nodes)) == 1), "and the copies match")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
