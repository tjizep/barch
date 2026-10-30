#!/usr/bin/env python3
"""
CLUSTER INIT and CLUSTER JOIN can be sent again after the cluster group failed to
start - TODO 786.

Starting the cluster group writes its log file (group_0.raft) before it listens on
the Raft port. When the port was taken, the start failed and the file stayed, and
that file is how a node knows it's in a cluster: every INIT or JOIN after that was
refused with "this node is already in a cluster", so the node could never join.
It turned up as a port clash in a test run: test ports sit inside Linux's range for
outgoing connections, so a client socket can hold one.

Here a socket holds the node's Raft port while it's asked, and is closed before it's
asked again. What has to hold:
  - JOIN with the port taken is refused, saying it couldn't start;
  - JOIN again, once the port is free, joins;
  - a third node joins as usual, and the cluster group has three voters;
  - the same refusal and second try for INIT, on a fourth node of its own.

Ports are BARCH_TEST_PORT and the 15 after it.
"""
import os
import socket
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=19460)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def hold(port):
    """a listener on the port, on every address, as whatever took it would have"""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("0.0.0.0", port))
    s.listen(1)
    return s


def answer(n, *cmd):
    try:
        return str(n.client().execute_command(*cmd))
    except redis.ResponseError as e:
        return "ERR " + str(e)


def voters(n):
    line = n.group_line("cluster")
    return line if "learners 0" in line else ""


# three nodes for a cluster, and a fourth on its own for INIT
nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(4)]
cluster = nodes[:3]
try:
    print("four nodes, not in a cluster yet")
    for n in nodes:
        n.start()
    check(answer(nodes[0], "CLUSTER", "INIT") in ("OK", "True"), "node 1 starts a cluster")
    check(wait_for(30, lambda: nodes[0].leads("cluster")), "and leads its cluster group")

    print("JOIN while the joining node's Raft port is taken")
    held = hold(nodes[1].raft)
    first = answer(nodes[1], "CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    held.close()
    check("could not start" in first, "refused, saying it couldn't start (%s)" % first[:80])
    again = answer(nodes[1], "CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    check(again in ("OK", "True"), "JOIN again, once the port is free, joins (%s)" % again[:80])

    print("a third node joins as usual")
    third = answer(nodes[2], "CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    check(third in ("OK", "True"), "it joins (%s)" % third[:80])
    check(wait_for(60, lambda: leader_of(cluster, "cluster") is not None and all(
        "members 3" in voters(n) for n in cluster)),
          "three voters in the cluster group (%s)" % [voters(n)[:40] for n in cluster])

    print("INIT on the fourth node while its Raft port is taken")
    held = hold(nodes[3].raft)
    first = answer(nodes[3], "CLUSTER", "INIT")
    held.close()
    check("could not start" in first, "refused, saying it couldn't start (%s)" % first[:80])
    again = answer(nodes[3], "CLUSTER", "INIT")
    check(again in ("OK", "True"), "INIT again, once the port is free, starts it (%s)" % again[:80])
    check(wait_for(30, lambda: nodes[3].leads("cluster")), "and it leads a cluster group of its own")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
