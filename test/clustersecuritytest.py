#!/usr/bin/env python3
"""
The cluster's shared secret - TODO 620.

What has to hold:
  - without cluster_secret, CLUSTER INIT and CLUSTER JOIN are refused;
  - a node with a different secret can't join: the leader refuses its ADMIT;
  - a member restarted with the wrong secret gets nothing from the others, which
    count its messages as refused, and catches up once it has the right one;
  - a member restarted with no secret doesn't start at all;
  - CONFIG GET says only that the secret is set, and no node's log has it.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=26300)
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


def refused(call):
    try:
        call()
        return ""
    except redis.ResponseError as e:
        return str(e)


WRONG = ["cluster_secret=not the cluster's secret"]
nodes = [clusternodes.Node(0, BASE, BINARY),
         clusternodes.Node(1, BASE, BINARY),
         clusternodes.Node(2, BASE, BINARY, WRONG),
         clusternodes.Node(3, BASE, BINARY, ["cluster_secret=off"])]
a, b, wrong, none = nodes


def refused_count(n):
    return sum(int(m.group(1)) for m in (re.search(r"\brefused (\d+)", l) for l in n.info()) if m)


try:
    for n in nodes:
        n.start()

    print("no secret, no cluster")
    said = refused(lambda: none.client().execute_command("CLUSTER", "INIT"))
    check("cluster_secret" in said, "CLUSTER INIT is refused (%s)" % said)
    said = refused(lambda: none.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(a.port)))
    check("cluster_secret" in said, "and so is CLUSTER JOIN (%s)" % said)

    print("a node with another secret can't join")
    a.client().execute_command("CLUSTER", "INIT")
    b.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(a.port))
    said = refused(lambda: wrong.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(a.port)))
    check("secret" in said, "its JOIN is refused (%s)" % said)
    check(not any(l.startswith("group ") for l in wrong.info()), "and it runs no group")
    check(wait_for(30, lambda: "members 2" in a.group_line("cluster")), "the cluster has the other two")

    print("a member restarted with the wrong secret")
    a.client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(60, lambda: leader_of([a, b], SPACE) is not None
                   and "members 2" in b.group_line(SPACE) and "learners 0" in b.group_line(SPACE)),
          "a replicated space on both")
    l = leader_of([a, b], SPACE)
    for i in range(20):
        l.client(SPACE).execute_command("SET", "before%d" % i, "x")
    check(wait_for(30, lambda: len(set(n.digest(SPACE)[0] for n in (a, b))) == 1), "both hold the same copy")
    follower = b if l is a else a
    follower.stop()
    follower.settings = WRONG
    follower.start()
    # with one of two members cut off there's no quorum, so nothing commits; what
    # matters is that the others refuse what it sends
    check(wait_for(30, lambda: refused_count(l) > 0),
          "the leader refuses its messages (refused %d)" % refused_count(l))
    check(wait_for(30, lambda: refused_count(follower) > 0),
          "and it refuses the leader's (refused %d)" % refused_count(follower))
    follower.stop()
    follower.settings = []
    follower.start()
    check(wait_for(60, lambda: leader_of([a, b], SPACE) is not None), "with the right secret again, a leader")
    l = leader_of([a, b], SPACE)
    for i in range(20):
        l.client(SPACE).execute_command("SET", "after%d" % i, "y")
    check(wait_for(60, lambda: len(set(n.digest(SPACE)[0] for n in (a, b))) == 1), "and the two agree again")

    print("a member restarted with no secret")
    follower.stop()
    follower.settings = ["cluster_secret=off"]
    try:
        follower.start()
        started = True
    except AssertionError:
        started = False
    check(not started, "doesn't start")
    log = open(follower.dir + ".log").read()
    check("cluster_secret" in log, "and says why in its log")
    follower.proc = None
    follower.settings = []
    follower.start()

    print("the secret stays out of sight")
    got = a.client().execute_command("CONFIG", "GET", "cluster_secret")
    value = got.get("cluster_secret") if isinstance(got, dict) else got[1]
    check(value == "(set)", "CONFIG GET says only that it's set (%s)" % value)
    a.client().execute_command("CONFIG", "SET", "cluster_secret", clusternodes.SECRET)
    leaked = [n.i + 1 for n in nodes
              if clusternodes.SECRET in open(n.dir + ".log").read() or "not the cluster's" in open(n.dir + ".log").read()]
    check(not leaked, "no node's log has a secret in it (%s)" % leaked)
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
