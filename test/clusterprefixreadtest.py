#!/usr/bin/env python3
"""
A read in `space:CMD` form follows the cluster's read rules - TODO 660.

A read of a space replicated with Raft goes to its group's leader, unless the
connection asked for follower reads; then a follower answers once it has applied
what the connection has seen. The check looked the command up by its first word as
sent, and `orders:GET` isn't a command name, so in that form a follower answered
from its own copy: a client could read a value older than one it had written. The
same read after `USE orders` was redirected. Writes were refused at the shard in both
forms.

What has to hold, on a follower:
  - `orders:GET` answers NOTLEADER with the leader's address, like GET after USE;
  - so do the other single-key and multi-key reads in that form;
  - with CLUSTER READS FOLLOWER, the same reads are answered, with the leader's value;
  - a write in that form is still refused with NOTLEADER.

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
BASE = scale.port(default=30300)
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


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


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
    follower = next(n for n in nodes if n is not leader)
    lc = leader.client(SPACE)
    lc.execute_command("SET", "k", "v1")
    lc.execute_command("HSET", "h", "f", "v1")
    check(wait_for(30, lambda: follower.field(SPACE, "applied") >= leader.field(SPACE, "applied")),
          "the follower has applied the writes")
    where = "127.0.0.1:%d" % leader.port

    print("reads in space:CMD form on a follower")
    plain = redis.Redis(host="127.0.0.1", port=follower.port, decode_responses=True)
    for cmd in (["GET", "k"], ["STRLEN", "k"], ["HGET", "h", "f"], ["EXISTS", "k"], ["KEYS", "*"]):
        got = answer(plain, SPACE + ":" + cmd[0], *cmd[1:])
        check(isinstance(got, str) and got.startswith("ERR NOTLEADER " + where),
              "%s:%s redirects to the leader (%r)" % (SPACE, cmd[0], got))
    used = follower.client(SPACE)
    got = answer(used, "GET", "k")
    check(isinstance(got, str) and got.startswith("ERR NOTLEADER " + where),
          "and so does GET after USE (%r)" % got)
    got = answer(plain, SPACE + ":SET", "k", "v2")
    check(isinstance(got, str) and got.startswith("ERR NOTLEADER " + where),
          "a write in that form is refused too (%r)" % got)

    print("the same with CLUSTER READS FOLLOWER")
    fr = redis.Redis(host="127.0.0.1", port=follower.port, decode_responses=True,
                     single_connection_client=True)
    fr.execute_command("CLUSTER", "READS", "FOLLOWER")
    check(answer(fr, SPACE + ":GET", "k") == "v1", "%s:GET is answered on the follower" % SPACE)
    check(answer(fr, SPACE + ":HGET", "h", "f") == "v1", "%s:HGET too" % SPACE)
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
