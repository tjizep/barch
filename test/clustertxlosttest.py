#!/usr/bin/env python3
"""
A transaction whose commit was sent to its home group's leader, but whose answer got
lost on the way back, answers UNKNOWN - TODO 789.

A coordinator that doesn't lead the home group sends the commit to the node that does.
If that fails, it used to answer the client with the transport error, as a refusal.
That's only true when the commit never got there. After it was sent (a timeout, the
connection dropping) the commit may well have landed, and the client was told a
transaction it made had failed.

Node 1 drops the answer to the first commit it forwards (BARCH_TEST_LOSE_FORWARD=TXCOMMIT),
after the commit has gone through. What has to hold, on three nodes:
  - that transaction's COMMITTX answers UNKNOWN, saying it may or may not have
    happened, and its write is on every member;
  - the next one from node 1 answers OK, and is on every member too.

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
BASE = scale.port(default=19580)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "lost"
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.exceptions.TryAgainError as e:
        # redis-py takes the code off the errors it has a class for
        return "ERR TRYAGAIN " + str(e)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def client(n):
    c = n.client(SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c


def everywhere(key, want):
    return wait_for(30, lambda: all(client(n).get(key) == want for n in nodes))


def commit(key, val):
    c = client(nodes[1])
    c.execute_command("BEGINTX")
    c.execute_command("SET", key, val)
    return answer(c, "COMMITTX")


ENVS = [{}, {"BARCH_TEST_LOSE_FORWARD": "TXCOMMIT"}, {}]
nodes = [clusternodes.Node(i, BASE, BINARY, env=ENVS[i]) for i in range(3)]

try:
    print("three nodes and a replicated space")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: leader_of(nodes, SPACE) is not None), "the space's group has a leader")
    if leader_of(nodes, SPACE) is nodes[1]:
        # node 1 has to forward its commits, so it can't lead; started again, its knob is fresh
        nodes[1].kill()
        check(wait_for(60, lambda: leader_of([nodes[0], nodes[2]], SPACE) is not None),
              "a leader that isn't node 1")
        nodes[1].start()
    check(wait_for(60, lambda: leader_of(nodes, SPACE) not in (None, nodes[1])
                   and nodes[1].field(SPACE, "leader") > 0 and "members 3" in nodes[1].group_line(SPACE)),
          "node 1 follows")

    print("a commit whose answer is lost on the way back")
    got = ""
    for _ in range(100):
        # node 1 may not know where the leader is yet: refused, nothing was sent, and
        # the knob is still waiting for the first commit that is
        got = str(commit("a", "1"))
        if not got.startswith(("ERR NOTLEADER", "ERR TRYAGAIN")):
            break
        time.sleep(0.2)
    check(got.startswith("ERR UNKNOWN") and "may or may not" in got, "answers UNKNOWN (%s)" % got[:60])
    check(everywhere("a", "1"), "and its write is on every member")

    print("the next one")
    got = commit("b", "2")
    check(got in ("OK", True), "answers OK (%s)" % str(got)[:60])
    check(everywhere("b", "2"), "and is on every member")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
