#!/usr/bin/env python3
"""
barch.time() and barch.uuid() answer the same on every member of a Raft space - TODO 763.

A stored function that may write runs from the space's log, on every member (TODO 673).
A local clock would give each member a different time, and a function that stores it
would leave them holding different data. So the node that commits the entry puts its ms
in it, barch.time() in the run answers that, and barch.uuid() is built from it and the
entry's index.

What has to hold, on a three-node cluster:
  - 20 calls that store barch.time() and two barch.uuid()s leave every member the same;
  - each call's time is near now, and its uuids start with that time and are in order;
  - no uuid repeats, across calls or in a CALLF nested in one;
  - outside the log (a function that only reads) barch.time() is still the clock.

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
BASE = scale.port(default=19220)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "stamps"
CALLS = 20
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


def local_get(n, key):
    """the value as this member holds it"""
    c = redis.Redis(host="127.0.0.1", port=n.port, single_connection_client=True, decode_responses=True)
    c.execute_command("USE", SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c.execute_command("GET", key)


def uuid_ms(u):
    return int(u.replace("-", "")[:12], 16)


FUNCTIONS = {
    "stamp": "function call(k)\n"
             "    local t = barch.time()\n"
             "    local u = barch.uuid() .. ' ' .. barch.uuid()\n"
             "    barch.store.set(k .. ':t', string.format('%d', t))\n"
             "    barch.store.set(k .. ':u', u)\n"
             "    return string.format('%d', t) .. ' ' .. u\n"
             "end\n",
    "inner": "function call(k)\n"
             "    local u = barch.uuid()\n"
             "    barch.store.set(k .. ':inner', u)\n"
             "    return u\n"
             "end\n",
    "outer": "function call(k)\n"
             "    local before = barch.uuid()\n"
             "    local inner = barch.call('CALLF', 'inner', k)\n"
             "    local after = barch.uuid()\n"
             "    barch.store.set(k .. ':outer', before .. ' ' .. after)\n"
             "    return before .. ' ' .. inner .. ' ' .. after\n"
             "end\n",
    "clock": '--@barch {"write": false}\n'
             "function call() return string.format('%d', barch.time()) end\n",
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
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE)
        and "learners 0" in n.group_line("cluster") for n in nodes)), "three voters and a leader")
    leader = leader_of(nodes, SPACE)
    c = leader.client(SPACE)
    for name, source in FUNCTIONS.items():
        check(answer(c, "SETF", name, source) in (b"OK", "OK"), "SETF " + name)

    print("a function that stores the time and uuids, from the log")
    said = []
    before = int(time.time() * 1000)
    for i in range(CALLS):
        r = answer(c, "CALLF", "stamp", "s%d" % i)
        said.append(r.decode() if isinstance(r, bytes) else r)
    after = int(time.time() * 1000)
    check(all(not s.startswith("ERR") for s in said), "every call ran (%s)" % next(
        (s for s in said if s.startswith("ERR")), "no errors"))
    r = answer(c, "CALLF", "outer", "n")
    nested = (r.decode() if isinstance(r, bytes) else r).split()
    check(len(nested) == 3, "a call with a nested CALLF ran (%s)" % nested)
    top = leader.field(SPACE, "applied")
    check(wait_for(30, lambda: all(n.field(SPACE, "applied") >= top for n in nodes)), "every member applied them")

    keys = ["s%d:t" % i for i in range(CALLS)] + ["s%d:u" % i for i in range(CALLS)] + ["n:inner", "n:outer"]
    held = {n.port: [local_get(n, k) for k in keys] for n in nodes}
    first = held[nodes[0].port]
    check(all(v is not None for v in first), "the leader's reads found every key")
    check(all(held[n.port] == first for n in nodes), "every member holds the same times and uuids")

    times = [int(s.split()[0]) if not s.startswith("ERR") else 0 for s in said]
    check(all(before - 5000 <= t <= after + 5000 for t in times),
          "each time is the clock when it was logged (%s..%s, now %d)" % (min(times), max(times), after))
    check(times == sorted(times), "and they don't go back")
    pairs = [s.split()[1:] for s in said if not s.startswith("ERR")]
    check(all(len(p) == 2 and uuid_ms(p[0]) == t and p[0] < p[1] for p, t in zip(pairs, times)),
          "each call's uuids start with its time and are in order")
    every = [u for p in pairs for u in p] + nested
    check(len(every) == len(set(every)) == 2 * CALLS + 3, "no uuid repeats, nested CALLF included (%d)" % len(every))
    check(len(nested) == 3 and nested[0] < nested[2], "the call's own uuids are in order around the nested one")

    print("outside the log")
    def clock():
        r = answer(c, "CALLF", "clock")
        return int(r) if str(r).isdigit() else 0

    a = clock()
    time.sleep(0.05)
    b = clock()
    now = int(time.time() * 1000)
    check(now - 5000 <= a < b <= now + 5000, "a read-only call reads the clock (%d, %d, now %d)" % (a, b, now))
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
