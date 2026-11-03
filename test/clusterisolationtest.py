#!/usr/bin/env python3
"""
A follower NuRaft thinks is isolated stays up and rejoins - TODO 814.

A follower that stops hearing its leader asks the others for a pre-vote, and they
turn it down while they still hear the leader. NuRaft counts those refusals, and at
20 with no leader known it calls system_exit(N22_unrecoverable_isolation), meaning
"restarting the process might help". barch aborted on every system_exit, so one
group's follower missing heartbeats took the whole barchd down, every other group
with it. The count is only reset when the node changes role, so a follower that
misses a heartbeat now and then adds them up over its life (DONE 765 saw 20 in 55s
under heavy jitter).

Here one follower of a replicated space is made deaf to that space's group
(BARCH_TEST_DEAF_FILE: it drops every request it's sent, so it hears no leader, but
still reads the answers to its own pre-votes). There's no timing: it stays deaf until
its CLUSTER INFO says NuRaft gave up on it (`isolated`), however long that takes.
Then:
  - it's still running, and still answers;
  - its other group, the cluster's, still works there;
  - writes the space took while it was deaf reach it once it hears again.

Ports are BARCH_TEST_PORT and the 15 after it.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of, on_leader  # noqa: E402

scale.workdir()
BASE = scale.port(default=19600)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
GROUP = "1"            # the first replicated space's group
WRITES = 50
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def deaf_file(i):
    return os.path.join(os.getcwd(), "deaf%d" % (i + 1))


def running(n):
    return n.proc is not None and n.proc.poll() is None


for i in range(3):
    if os.path.exists(deaf_file(i)):
        os.remove(deaf_file(i))
nodes = [clusternodes.Node(i, BASE, BINARY, env={
    "BARCH_TEST_DEAF_FILE": deaf_file(i), "BARCH_TEST_DEAF_GROUP": GROUP}) for i in range(3)]
try:
    print("a replicated space on three nodes")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    check(wait_for(60, lambda: nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")),
          "the space is replicated")
    check(wait_for(60, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE)
        and "learners 0" in n.group_line("cluster") for n in nodes)), "three voters")
    check(nodes[0].group_line(SPACE).startswith("group %s " % GROUP),
          "the space is group %s (%s)" % (GROUP, nodes[0].group_line(SPACE)[:20]))

    print("a follower deaf to the space's group until NuRaft gives up on it")
    leader = leader_of(nodes, SPACE)
    f = next(n for n in nodes if n is not leader)
    open(deaf_file(f.i), "w").close()
    # the only way out on the old code is the process going
    wait_for(600, lambda: not running(f) or f.field(SPACE, "isolated") > 0)
    check(running(f), "it's still running")
    if running(f):
        print("    isolated %d, pre-votes refused" % f.field(SPACE, "isolated"), flush=True)
        check(f.field(SPACE, "isolated") > 0, "NuRaft gave up on it (isolated %d)" % f.field(SPACE, "isolated"))
        check(f.client().ping(), "it answers")
        # the cluster's group isn't deaf: it keeps the same leader as the others
        check(wait_for(60, lambda: len({n.field("cluster", "leader") for n in nodes}) == 1
                       and f.field("cluster", "leader") > 0), "its cluster group still has the leader")
        others = [n for n in nodes if n is not f]
        done = sum(1 for k in range(WRITES)
                   if on_leader(others, SPACE, lambda n: n.client(SPACE).execute_command("SET", "k%d" % k, k)))
        check(done == WRITES, "the other two took %d writes while it was deaf" % done)
        os.remove(deaf_file(f.i))
        target = leader_of(others, SPACE).field(SPACE, "committed")
        check(wait_for(120, lambda: f.field(SPACE, "applied") >= target), "once it hears again it has them")
        check(wait_for(60, lambda: len({n.digest(SPACE)[0] for n in nodes}) == 1), "the three copies match")
finally:
    for n in nodes:
        if os.path.exists(deaf_file(n.i)):
            os.remove(deaf_file(n.i))
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
