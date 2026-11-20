#!/usr/bin/env python3
"""
The cluster's users seed word, and a client that names it - TODO 946.

The seed entry's word was written "\\x01AUTHSEED". The escape takes the A as a hex
digit, so the word was 0x1a then "UTHSEED". The check that refuses a client's call
to a name starting with \\x01 missed it. A stored function of that name, called in a
Raft space, went to the log, and every member's runner took it for the seed.

What has to hold, on a three-node cluster:
  - a stored function named 0x1a UTHSEED, called in a Raft space, runs as the
    function on every member. With no arguments, no member fails the entry and the
    space isn't rebuilt. With one, the users stay as they were;
  - the first node's own user logs in on every member: the seed in the new spelling;
  - and in the old spelling, as a node from before the fix logs it
    (BARCH_TEST_FAULT=old_seed).

Ports are BARCH_TEST_PORT and the 19 after it, then the 20 after those.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=18960)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

OLD = "\x1aUTHSEED"
SPACE = "seedfn"
FUNCTION = "function call(...) barch.store.set('ran', 'yes') return 'mine' end\n"

failures = 0


def check(ok, what):
    global failures
    print("  %-74s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def can_login(n, user, secret):
    return answer(n.client(), "AUTH", user, secret) in ("OK", True)


def cluster(base, env=None):
    """three voters in the cluster group, the first node with a user of its own"""
    nodes = [clusternodes.Node(i, base, BINARY, [], env=env) for i in range(3)]
    for n in nodes:
        n.start()
    if "no cluster" in str(answer(nodes[0].client(), "CLUSTER", "INFO")):
        for n in nodes:
            n.stop()
        print("SKIP: this barchd has no cluster")
        sys.exit(0)
    nodes[0].client().execute_command("ACL", "SETUSER", "early", "on", ">e1", "+read", "+write", "+keys", "+data")
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    check(wait_for(90, lambda: leader_of(nodes, "cluster") is not None and all(
        "members 3" in n.group_line("cluster") and "learners 0" in n.group_line("cluster") for n in nodes)),
        "three voters")
    return nodes


def stop(nodes):
    for n in nodes:
        n.stop()


nodes = []
try:
    print("a stored function named like the old seed word, in a Raft space")
    nodes = cluster(BASE)
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE) for n in nodes)),
        "a replicated space")
    lead = leader_of(nodes, SPACE)
    c = lead.client(SPACE)
    check(answer(c, "SETF", OLD, FUNCTION) in ("OK", True), "SETF")
    got = answer(c, OLD)
    check(got == "mine", "called with nothing: it runs as the function (%r)" % (str(got)[:70],))
    check(answer(c, "GET", "ran") == "yes", "its write is there")
    check(wait_for(30, lambda: len(set(str(n.digest(SPACE)[0]) for n in nodes)) == 1),
          "every member the same")
    fails = [n.field(SPACE, "apply_failures") for n in nodes]
    check(all(f <= 0 for f in fails), "no member failed the entry (%s)" % fails)
    got = answer(c, OLD, "x")
    check(got == "mine", "called with one: it runs as the function (%r)" % (str(got)[:70],))
    check(all(can_login(n, "early", "e1") for n in nodes), "the users are as they were")
    check(all(not can_login(n, "x", "") for n in nodes), "and nobody new")

    print("the seed, in the new spelling")
    check(wait_for(30, lambda: all(can_login(n, "early", "e1") for n in nodes)),
          "the first node's own user logs in on every member")
    stop(nodes)

    print("the seed, in the old spelling, as a node from before the fix logs it")
    nodes = cluster(BASE + 20, env={"BARCH_TEST_FAULT": "old_seed"})
    check(wait_for(30, lambda: all(can_login(n, "early", "e1") for n in nodes)),
          "the first node's own user logs in on every member")
finally:
    stop(nodes)

print("authseedtest:", "FAIL" if failures else "OK", flush=True)
sys.exit(1 if failures else 0)
