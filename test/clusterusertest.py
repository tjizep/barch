#!/usr/bin/env python3
"""
Users in a cluster - TODO 676.

A node's users, their secrets and the per-space rules KSPACE ACL writes were its auth
file, which nothing replicated: a user made on one node couldn't log in on another.
In a cluster they're the cluster's now: one meta key in the `cluster` space, which
CLUSTER INIT fills from the first node's users, and which ACL SETUSER, ACL DEL and
KSPACE ACL change through the cluster group's log, on every member.

What has to hold, on a three-node cluster:
  - a user made on the cluster group's leader can log in on every member, with the
    rights it was given; ACL SETUSER on a follower answers NOTLEADER;
  - the first node's own users carry on: `default` logs in everywhere;
  - a KSPACE ACL rule set on the leader holds on every member: the user can't write
    that space anywhere, and can elsewhere;
  - ACL DEL on the leader: the user can't log in anywhere;
  - a member restarted, and one rebuilt from a snapshot after more changes, have the
    cluster's users.

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
BASE = scale.port(default=18820)
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


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def login(n, user, secret):
    """AUTH on a fresh connection to `n`: the connection, or the error"""
    c = n.client()
    got = answer(c, "AUTH", user, secret)
    return c if got in ("OK", True) else str(got)


def can_login(n, user, secret):
    return not isinstance(login(n, user, secret), str)


def writes_own(n, user, secret, space):
    """can `user` SET in its own (not replicated) `space` on `n`"""
    c = login(n, user, secret)
    if isinstance(c, str):
        return c
    c.execute_command("USE", space)
    got = answer(c, "SET", "k", "v")
    return got in ("OK", True) or str(got)


def cluster_leader(members):
    return leader_of(members, "cluster")


nodes = [clusternodes.Node(i, BASE, BINARY, ["raft_snapshot_entries=20"]) for i in range(3)]
try:
    print("three nodes, the first with a user of its own")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("ACL", "SETUSER", "early", "on", ">e1", "+read", "+write", "+keys", "+data")
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    check(wait_for(90, lambda: cluster_leader(nodes) is not None and all(
        "members 3" in n.group_line("cluster") and "learners 0" in n.group_line("cluster") for n in nodes)),
        "three voters")
    check(wait_for(30, lambda: all(can_login(n, "early", "e1") for n in nodes)),
          "the first node's own user logs in on every member")
    check(all(can_login(n, "default", "empty") for n in nodes), "and so does default")

    print("a user made on the cluster leader")
    lead = cluster_leader(nodes)
    follower = next(n for n in nodes if n is not lead)
    got = answer(follower.client(), "ACL", "SETUSER", "nope", "on", ">x", "+read")
    check(str(got).startswith("ERR NOTLEADER"), "ACL SETUSER on a follower: NOTLEADER (%r)" % str(got)[:50])
    check(answer(lead.client(), "ACL", "SETUSER", "alice", "on", ">a1", "+read", "+write", "+keys", "+data")
          in ("OK", True), "ACL SETUSER alice on the leader")
    check(wait_for(30, lambda: all(can_login(n, "alice", "a1") for n in nodes)), "alice logs in on every member")
    check(not any(can_login(n, "alice", "wrong") for n in nodes), "not with the wrong secret")
    check(all(writes_own(n, "alice", "a1", "mine") is True for n in nodes),
          "and writes a space of each node's own (%s)" % [writes_own(n, "alice", "a1", "mine") for n in nodes])

    print("a per-space rule, on the leader")
    check(answer(lead.client(), "KSPACE", "ACL", "locked", "SETUSER", "alice", "on", "-write") in ("OK", True),
          "KSPACE ACL locked SETUSER alice on -write")
    got = [writes_own(n, "alice", "a1", "locked") for n in nodes]
    check(wait_for(30, lambda: all(writes_own(n, "alice", "a1", "locked") is not True for n in nodes)),
          "alice can't write locked on any member (%s)" % [str(g)[:30] for g in got])
    check(all(writes_own(n, "alice", "a1", "mine") is True for n in nodes), "and still can elsewhere")

    print("a member restarted, and one rebuilt from a snapshot")
    nodes[2].stop()
    nodes[2].start()
    check(wait_for(60, lambda: can_login(nodes[2], "alice", "a1")), "the restarted member has alice")
    lead = cluster_leader(nodes)
    behind = next(n for n in nodes if n is not lead)
    behind.stop()
    for i in range(30):
        answer(lead.client(), "ACL", "SETUSER", "bulk%d" % i, "on", ">b", "+read")
    behind.start()
    check(wait_for(90, lambda: can_login(behind, "bulk29", "b") and can_login(behind, "alice", "a1")),
          "a member that missed 30 changes past the snapshot interval has them")

    print("ACL DEL on the leader")
    lead = cluster_leader(nodes)
    check(answer(lead.client(), "ACL", "DEL", "alice") in ("OK", True), "ACL DEL alice")
    check(wait_for(30, lambda: not any(can_login(n, "alice", "a1") for n in nodes)),
          "alice logs in nowhere")
    check(all(can_login(n, "default", "empty") and can_login(n, "early", "e1") for n in nodes),
          "and the others still do")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
