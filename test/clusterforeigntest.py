#!/usr/bin/env python3
"""
A foreign source on a replicated space - TODO 685.

FOREIGN (FOREIGN FAKE, the test source a space can read through) and FOREIGN_MISS
were refused in a replicated space. The fake source is each member's memory, so the
commands that change it are logged and every member runs them. A cached miss is a
tomb leaf in the tree, written on the leader alone; now a miss a fetch finds goes in
the space's log, with the leader's deadline, and every member stores the same tomb. A
follower asked through a follower read doesn't query the source, since it couldn't
keep the answer: it says NOTLEADER.

What has to hold, on a three-node cluster with a replicated space whose source is
the fake one:
  - FOREIGN FAKE SET on the leader: every member's source holds it, so a read on the
    leader fills the key from it, once, and every member holds the value;
  - a read of a key the source doesn't have is a cached miss on every member, with
    matching digests, and a second read doesn't query again;
  - a follower read of a key nobody has fetched answers NOTLEADER, and no member
    queries its source for it;
  - the leader killed: the new leader's source has what was set on the old one, and
    it fills another key from it.

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
BASE = scale.port(default=18740)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
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


def local(n):
    c = n.client(SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c


def queries(n):
    return n.client(SPACE).execute_command("FOREIGN", "FAKE", "QUERIES")


def settled(members):
    l = leader_of(members, SPACE)
    if l is None:
        return False
    top = l.field(SPACE, "applied")
    return wait_for(60, lambda: all(n.field(SPACE, "applied") >= top for n in members))


def same(members):
    return len(set(n.digest(SPACE)[0] for n in members)) == 1


nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]
try:
    print("a replicated space reading through the fake source, on three nodes")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    conf = nodes[0].client("configuration")
    conf.execute_command("SET", SPACE + ".foreign", "fake")
    conf.execute_command("SET", SPACE + ".missing_ttl", "600")
    conf.execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE)
        and "learners 0" in n.group_line("cluster") for n in nodes)), "three voters and a leader")
    leader = leader_of(nodes, SPACE)
    lc = leader.client(SPACE)

    print("the source set on the leader, and a read that fills from it")
    check(lc.execute_command("FOREIGN", "FAKE", "SET", "sku", "widget") == "OK", "FOREIGN FAKE SET")
    lc.execute_command("FOREIGN", "FAKE", "SET", "later", "gadget")
    check(lc.execute_command("GET", "sku") == "widget", "GET on the leader fills it")
    check(queries(leader) == 1, "with one query")
    check(settled(nodes) and all(local(n).execute_command("GET", "sku") == "widget" for n in nodes),
          "every member holds the value")

    print("a key the source doesn't have")
    check(lc.execute_command("GET", "nope") is None, "GET answers nil")
    check(lc.execute_command("GET", "nope") is None and queries(leader) == 2,
          "and a second GET doesn't query again (%d)" % queries(leader))
    check(settled(nodes) and same(nodes), "every member's digest matches, the cached miss included")
    check(all(local(n).execute_command("GET", "nope") is None for n in nodes),
          "and a follower read of it is nil, from the tomb")
    check(all(queries(n) == 0 for n in nodes if n is not leader), "no follower queried its source")

    print("a follower read of a key nobody has fetched")
    follower = next(n for n in nodes if n is not leader)
    got = answer(local(follower), "GET", "unseen")
    check(str(got).startswith("ERR NOTLEADER"), "answers NOTLEADER (%r)" % str(got)[:60])
    check(queries(follower) == 0, "without a query")

    print("the leader killed")
    leader.kill()
    rest = [n for n in nodes if n is not leader]
    check(wait_for(60, lambda: leader_of(rest, SPACE) is not None), "the other two elect a leader")
    new = leader_of(rest, SPACE)
    got = None
    for _ in range(100):
        got = answer(new.client(SPACE), "GET", "later")
        if not str(got).startswith("ERR"):
            break
    check(got == "gadget", "the new leader's source has what was set on the old one (%r)" % (got,))
    leader.start()
    check(wait_for(120, lambda: settled(nodes) and same(nodes)), "the old leader comes back to the same digest")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
