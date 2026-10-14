#!/usr/bin/env python3
"""
In-place writes to an existing key are replicated - TODO 662.

setBufferAt, which setInt32At, setInt64At and incrInt32ByAt / incrInt64ByAt write
through, copied the bytes straight into a live value that was long enough and
returned, before the write path that commits to Raft. Only a missing, short or
compressed key went through Raft. So in a replicated space a counter's first
increment reached every member and the rest stayed on the leader. Now a shard bound
to a Raft group always takes the write path.

What has to hold, after 200 increments and 50 setInt64At on the leader:
  - every increment answers the running total;
  - every member ends with the same value for each key, read on that member;
  - every member's digest of the space is the same.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=30500)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
INCREMENTS = 200
SETS = 50
failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def local_get(n, key):
    """the value as this member holds it"""
    c = redis.Redis(host="127.0.0.1", port=n.port, single_connection_client=True)
    c.execute_command("USE", SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c.execute_command("GET", key)


nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]
try:
    print("a replicated space on three nodes")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE) for n in nodes)),
        "three voters and a leader")
    leader = leader_of(nodes, SPACE)
    c = leader.client(SPACE)
    c.execute_command("SETF", "incr64", """
        function call(k, by, init, off)
            return barch.current():incrInt64ByAt(k, by, init, off)
        end
    """)
    c.execute_command("SETF", "set64", """
        function call(k, v, off)
            return barch.store.setInt64At(k, v, off)
        end
    """)

    print("%d increments of a counter, %d in-place sets, on the leader" % (INCREMENTS, SETS))
    answers = [c.execute_command("incr64", "counter", 1, 1, 0)]
    for _ in range(INCREMENTS - 1):
        answers.append(c.execute_command("incr64", "counter", 1, 1, 0))
    check(answers == list(range(1, INCREMENTS + 1)), "each answers the running total (last %s)" % answers[-1])
    for i in range(SETS):
        c.execute_command("set64", "slot", i, 0)
    want_counter = INCREMENTS.to_bytes(8, "little", signed=True)
    want_slot = (SETS - 1).to_bytes(8, "little", signed=True)
    check(local_get(leader, "counter") == want_counter, "the leader holds %d" % INCREMENTS)

    print("on every member")
    check(wait_for(30, lambda: all(n.field(SPACE, "applied") >= leader.field(SPACE, "applied") for n in nodes)),
          "every member has applied the leader's log")
    got = [int.from_bytes(local_get(n, "counter") or b"", "little", signed=True) for n in nodes]
    check(all(g == INCREMENTS for g in got), "every member holds the counter at %d (%s)" % (INCREMENTS, got))
    got = [int.from_bytes(local_get(n, "slot") or b"", "little", signed=True) for n in nodes]
    check(all(g == SETS - 1 for g in got), "and the last setInt64At, %d (%s)" % (SETS - 1, got))
    check(wait_for(30, lambda: len(set(n.digest(SPACE)[0] for n in nodes)) == 1), "every member's digest matches")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
