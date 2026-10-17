#!/usr/bin/env python3
"""
A logged command in a space split over several groups - TODO 689.

A command entry in a split space goes in the first group's log, and every member runs it
there. Its writes to the other groups' shards are made outside those groups' logs, so
each member may make them at a different point against those groups' own entries, and a
key both are writing can end differently on different members.

Now, in a split space, such a function (and SETF and REMF) runs on the node it was sent
to with its writes gathered, and lands as one batch across the groups (TODO 666).

What has to hold, on a three-node cluster with a space split over two groups:
  - a stored function that bumps keys of both groups, called while clients SET the same
    keys through their groups, leaves every member's digest the same;
  - SETF from each node installs the function on every member.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import re
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for  # noqa: E402

scale.workdir()
BASE = scale.port(default=35100)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "split"
LABELS = [SPACE, SPACE + "/1"]
KEYS = ["k%d" % i for i in range(16)]
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]
by_port = {n.port: n for n in nodes}


def line_of(n, label):
    for l in n.info():
        if (" label %s" % label) in l:
            return l
    return ""


def leader_of_group(label):
    for n in nodes:
        if n.proc:
            try:
                if "(this node)" in line_of(n, label):
                    return n
            except redis.RedisError:
                pass
    return None


def following(n, *cmd):
    """cmd through redirects"""
    target = n
    for _ in range(10):
        try:
            return target.client(SPACE).execute_command(*cmd)
        except redis.exceptions.TryAgainError:
            time.sleep(0.1)         # redis-py takes the TRYAGAIN off the message
        except redis.ResponseError as e:
            m = re.match(r"NOTLEADER 127\.0\.0\.1:(\d+)", str(e))
            if m and int(m.group(1)) in by_port:
                target = by_port[int(m.group(1))]
            elif str(e).startswith(("TRYAGAIN", "NOTLEADER")):
                time.sleep(0.05)
            else:
                raise
    raise RuntimeError("no answer for %s" % (cmd,))


def settled():
    def done():
        for l in LABELS:
            tops = [int(m.group(1)) for n in nodes for m in [re.search(r"\bapplied (\d+)", line_of(n, l))] if m]
            if len(tops) != len(nodes) or len(set(tops)) != 1:
                return False
        return True
    return wait_for(60, done)


try:
    print("a space split over two groups")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    conf = nodes[0].client("configuration")
    conf.execute_command("SET", SPACE + ".raft_groups", "2")
    conf.execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: all("members 3" in line_of(n, l) and "learners 0" in line_of(n, l)
                                   for n in nodes for l in LABELS)
                   and all(leader_of_group(l) for l in LABELS)), "two groups, three voters each")
    following(nodes[0], "SETF", "BUMPALL", '''
        function call()
            for i = 0, 15 do
                local k = "k" .. i
                barch.store.set(k, tostring((tonumber(barch.store.get(k) or "0") or 0) + 1))
            end
            return 1
        end''')

    print("the function from the log while clients SET the same keys")
    stop = threading.Event()
    errors = []

    def setter(w):
        i = 0
        while not stop.is_set():
            try:
                following(nodes[w % 3], "SET", KEYS[(i * 5 + w) % len(KEYS)], str(1000 + i))
            except Exception as e:
                errors.append(str(e))
            i += 1

    def bumper():
        while not stop.is_set():
            try:
                following(nodes[0], "BUMPALL")
            except Exception as e:
                errors.append(str(e))

    ts = [threading.Thread(target=setter, args=(w,)) for w in range(3)] + [threading.Thread(target=bumper)]
    [t.start() for t in ts]
    time.sleep(8)
    stop.set()
    [t.join() for t in ts]
    print("    %d errors (%s)" % (len(errors), errors[:2]))
    check(settled(), "every member has applied both groups' logs")
    digests = [n.digest(SPACE)[0] for n in nodes]
    check(len(set(digests)) == 1, "every member's digest is the same (%s)" % digests)

    print("SETF from each node")
    for i, n in enumerate(nodes):
        got = following(n, "SETF", "F%d" % i, "function call() return %d end" % i)
        check(got in ("OK", True), "SETF on node %d (%r)" % (i + 1, got))
    def functions_on(n):
        c = n.client(SPACE)
        c.execute_command("CLUSTER", "READS", "FOLLOWER")
        return sorted(c.execute_command("KEYSF"))
    check(settled() and all(functions_on(n) == ["BUMPALL", "F0", "F1", "F2"] for n in nodes),
          "every member has the three functions (%s)" % [functions_on(n) for n in nodes])
    # BEGIN and the rest act group by group now: TestClusterSplitWhole - TODO 690
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
