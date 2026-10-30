#!/usr/bin/env python3
"""
A write sent on to another group's leader, whose answer got lost on the way back,
answers UNKNOWN - TODO 790.

In a split space, a node that doesn't lead a group sends that group's writes to the
node that does: a logged command (KSPACE OPTION SET and the like) as XCOMMAND, and a
gathered batch (SETF, REMF, functions) as XFORWARD. If that failed, the client got the
transport error as a refusal. That's only true when the write never got there. After
it was sent (a timeout, the connection dropping) it may well have landed, and the
client was told a write it made had failed. TestClusterTxLost does the same for a
transaction's commit (TODO 789).

Every node drops the answer to the first XCOMMAND and XFORWARD it sends
(BARCH_TEST_LOSE_FORWARD), after the write has gone through. What has to hold, on
three nodes with a space split over two groups, from node 3, which leads neither:
  - KSPACE OPTION SET answers UNKNOWN, saying it may or may not have happened, and
    every member has the option set;
  - SETF answers UNKNOWN the same way, and every member has the function;
  - the next of each answers OK.

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
from clusternodes import wait_for  # noqa: E402

scale.workdir()
BASE = scale.port(default=19620)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "fwd"
LABELS = [SPACE, SPACE + "/1"]
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


ENV = {"BARCH_TEST_LOSE_FORWARD": "XCOMMAND,XFORWARD"}
nodes = [clusternodes.Node(i, BASE, BINARY, env=ENV) for i in range(3)]


def line_of(n, label):
    for l in n.info():
        if (" label %s" % label) in l:
            return l
    return ""


def leader_of_group(label):
    for n in nodes:
        try:
            if "(this node)" in line_of(n, label):
                return n
        except redis.RedisError:
            pass
    return None


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


def first_try(n, *cmd):
    """cmd on n, again while n doesn't know where a leader is: nothing was sent then"""
    got = ""
    for _ in range(100):
        got = str(answer(client(n), *cmd))
        if not got.startswith(("ERR NOTLEADER", "ERR TRYAGAIN")):
            break
        time.sleep(0.2)
    return got


def lost(got):
    return got.startswith("ERR UNKNOWN") and "may or may not" in got


def ok(got):
    return got in ("OK", "True", "1")


def lru_everywhere(want):
    def on(n):
        return str(client(n).execute_command("KSPACE", "OPTION", "GET", "LRU")).lower() in ("1", "true")
    return wait_for(30, lambda: all(on(n) == want for n in nodes))


def function_everywhere(name):
    return wait_for(30, lambda: all(name in client(n).execute_command("KEYSF") for n in nodes))


try:
    print("three nodes and a space split over two groups")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    conf = nodes[0].client("configuration")
    conf.execute_command("SET", SPACE + ".raft_groups", "2")
    conf.execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: all("members 3" in line_of(n, l) and re.search(r"\bleader [1-9]", line_of(n, l))
                                   for n in nodes for l in LABELS)
                   and all(leader_of_group(l) for l in LABELS)), "two groups, three voters each, led")
    # each group hands itself to the voter it prefers (TODO 612): group 1 to node 1 and
    # group 2 to node 2. Until then node 3 may yet get one, and send its writes nowhere
    check(wait_for(60, lambda: [leader_of_group(l) for l in LABELS] == nodes[:2]),
          "the leaders are spread: node 1 and node 2")
    via = nodes[2]

    print("a logged command whose answer is lost on the way back")
    got = first_try(via, "KSPACE", "OPTION", "SET", "LRU", "ON")
    check(lost(got), "KSPACE OPTION SET answers UNKNOWN (%s)" % got[:60])
    check(lru_everywhere(True), "and every member has the option on")
    got = first_try(via, "KSPACE", "OPTION", "SET", "LRU", "OFF")
    check(ok(got), "the next one answers OK (%s)" % got[:60])
    check(lru_everywhere(False), "and every member has it off")

    print("a function whose answer is lost on the way back")
    got = first_try(via, "SETF", "LOSTF", "function call() return 1 end")
    check(lost(got), "SETF answers UNKNOWN (%s)" % got[:60])
    check(function_everywhere("LOSTF"), "and every member has the function")
    got = first_try(via, "SETF", "NEXTF", "function call() return 2 end")
    check(ok(got), "the next one answers OK (%s)" % got[:60])
    check(function_everywhere("NEXTF"), "and every member has it")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
