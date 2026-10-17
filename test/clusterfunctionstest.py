#!/usr/bin/env python3
"""
A stored function says whether it writes, and a replicated space routes it by that -
TODO 672.

A call to a stored function isn't a builtin, so Raft's gate didn't see it: it ran on
whatever node it was sent to, read that node's copy without the read rules, and on a
follower had its writes refused one by one partway through. Now a function may write
unless its `--@barch` header says `"write": false`. One that may write goes to the
leader before it runs; a read-only one is a read, so a follower answers it only for a
connection with follower reads on; and a read-only one can't write at all.

What has to hold, on a three-node cluster:
  - a writing function sent to a follower answers NOTLEADER and doesn't run, by name
    or through CALLF;
  - a read-only function sent to a follower answers NOTLEADER too, unless the
    connection asked for follower reads; then it runs there and sees a write the
    leader made, once the connection says it has seen that write (CLUSTER AFTER);
  - a read-only function that writes through the store, runs a write command through
    barch.call, or calls a writing function is refused, and nothing is written;
  - a read-only function can still run reads through barch.call.

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
BASE = scale.port(default=31700)
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
    """what a command answers, an error as its text"""
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


FUNCTIONS = {
    # writes when asked to, and answers "ran" either way
    "wfn": "function call(k, mode)\n"
           "    if mode == 'write' then barch.store.set(k, 'w') end\n"
           "    return 'ran'\n"
           "end\n",
    "rofn": '--@barch {"write": false}\n'
            "function call(k) return barch.store.get(k) end\n",
    "rofn_set": '--@barch {"write": false}\n'
                "function call(k) barch.store.set(k, 'bad') return 'wrote' end\n",
    "rofn_call": '--@barch {"write": false}\n'
                 "function call(k) barch.call('SET', k, 'bad') return 'wrote' end\n",
    "rofn_callw": '--@barch {"write": false}\n'
                  "function call(k) barch.call('CALLF', 'wfn', k, 'write') return 'wrote' end\n",
    "rofn_get": '--@barch {"write": false}\n'
                "function call(k) return barch.call('GET', k) end\n",
}

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
    c = leader.client(SPACE)
    for name, source in FUNCTIONS.items():
        c.execute_command("SETF", name, source)
    c.execute_command("SET", "k", "v1")
    top = leader.field(SPACE, "applied")
    check(wait_for(30, lambda: all(n.field(SPACE, "applied") >= top for n in nodes)), "every member has them")

    print("a writing function on a follower")
    f = follower.client(SPACE)
    got = answer(f, "wfn", "k", "read")
    check(str(got).startswith("ERR NOTLEADER"), "by name, answers NOTLEADER before it runs (%r)" % got)
    got = answer(f, "CALLF", "wfn", "k", "read")
    check(str(got).startswith("ERR NOTLEADER"), "through CALLF too (%r)" % got)
    check(answer(c, "wfn", "k", "read") == "ran", "and runs on the leader")

    print("a read-only function on a follower")
    got = answer(f, "rofn", "k")
    check(str(got).startswith("ERR NOTLEADER"), "without follower reads, NOTLEADER (%r)" % got)
    c.execute_command("SET", "k", "v2")
    seen = c.execute_command("CLUSTER", "INDEX", SPACE)
    fr = follower.client(SPACE)
    fr.execute_command("CLUSTER", "READS", "FOLLOWER")
    fr.execute_command("CLUSTER", "AFTER", SPACE, str(seen))
    got = answer(fr, "rofn", "k")
    check(got == "v2", "with follower reads, runs there and sees the leader's write (%r)" % got)

    print("a read-only function that writes")
    for name, why in (("rofn_set", "through the store"), ("rofn_call", "a SET through barch.call"),
                      ("rofn_callw", "a writing function through barch.call CALLF")):
        got = answer(c, name, "k")
        check(str(got).startswith("ERR") and "write" in str(got), "%s is refused (%r)" % (why, str(got)[:90]))
    check(c.execute_command("GET", "k") == "v2", "and nothing was written")
    got = answer(c, "rofn_get", "k")
    check(got == "v2", "a read through barch.call still works (%r)" % got)
    check(wait_for(30, lambda: len(set(n.digest(SPACE)[0] for n in nodes)) == 1), "every member's digest matches")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
