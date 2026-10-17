#!/usr/bin/env python3
"""
A stored function that may write runs from the space's log, on every member - TODO 673.

A call to it goes in the space's Raft log as one entry, and every member runs it as the
entry commits. So its reads and writes can't be split by another call, and it lands
whole. A run has to do the same on every member: math.random is seeded from the log
index, there's no wall clock limit (function_log_insns counts interrupts instead), and
whatever reaches outside the data is refused unless the function's header says
`"replicate": "effects"`, which keeps the old way: run on the leader, writes replicated
one by one. A member applies its log again from its snapshot after a restart, over
files that may be ahead of it, so each space keeps the last run its files hold and a
run isn't repeated.

What has to hold, on a three-node cluster:
  - 1600 calls of a naive `set(k, get(k) + 1)`, from clients on the leader and on a
    follower (which follow NOTLEADER), leave 1600 on every member;
  - a function that writes math.random values leaves every member the same;
  - one that reads this node's storage stats, or another space, from the log is
    refused and writes nothing; with "replicate": "effects" the same function runs;
  - one that loops forever stops at function_log_insns, the same on every member,
    and the group goes on taking writes;
  - a follower saved after more calls and then restarted ends with the same count as
    the others, so no run was applied twice.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=31900)
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


def local_get(n, key):
    """the value as this member holds it"""
    c = redis.Redis(host="127.0.0.1", port=n.port, single_connection_client=True, decode_responses=True)
    c.execute_command("USE", SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c.execute_command("GET", key)


def settled(nodes):
    l = leader_of(nodes, SPACE)
    top = l.field(SPACE, "applied")
    return wait_for(30, lambda: all(n.field(SPACE, "applied") >= top for n in nodes))


INC = "function call(k)\n" \
      "    local v = tonumber(barch.store.get(k) or '0')\n" \
      "    barch.store.set(k, tostring(v + 1))\n" \
      "    return v + 1\n" \
      "end\n"
STATS_BODY = "function call(k)\n" \
             "    barch.store.stats(function() end)\n" \
             "    barch.store.set(k, 'x')\n" \
             "    return 'ok'\n" \
             "end\n"
FUNCTIONS = {
    "inc": INC,
    "rnd": "function call(k) barch.store.set(k, tostring(math.random(1, 1000000000))) return 1 end\n",
    "statsfn": STATS_BODY,
    "statsfx": '--@barch {"replicate": "effects"}\n' + STATS_BODY,
    "otherfn": "function call(k)\n"
               "    local v = barch.space.configuration['x']\n"
               "    barch.store.set(k, 'x')\n"
               "    return 'ok'\n"
               "end\n",
    "spin": "function call() while true do end end\n",
}

nodes = [clusternodes.Node(i, BASE, BINARY, ["raft_snapshot_entries=200"]) for i in range(3)]
by_port = {n.port: n for n in nodes}
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
    follower = next(n for n in nodes if n is not leader)
    c = leader.client(SPACE)
    for name, source in FUNCTIONS.items():
        c.execute_command("SETF", name, source)

    print("1600 naive increments from the leader and a follower")
    errors = []

    def run(node):
        target = node.client(SPACE)
        for _ in range(100):
            while True:
                try:
                    target.execute_command("inc", "n")
                    break
                except redis.exceptions.TryAgainError:
                    continue
                except redis.RedisError as e:
                    msg = str(e)
                    if msg.startswith("NOTLEADER"):
                        target = by_port[int(msg.split()[1].split(":")[1])].client(SPACE)
                        continue
                    errors.append(msg[:80])
                    break

    ts = [threading.Thread(target=run, args=(n,)) for n in [leader] * 8 + [follower] * 8]
    [t.start() for t in ts]
    [t.join() for t in ts]
    check(not errors, "every call answered (%s)" % sorted(set(errors))[:3])
    settled(nodes)
    got = [local_get(n, "n") for n in nodes]
    check(got == ["1600"] * 3, "every member holds 1600 (%s)" % got)

    print("math.random agrees across members")
    for i in range(20):
        c.execute_command("rnd", "r%d" % i)
    settled(nodes)
    got = [local_get(n, "r7") for n in nodes]
    check(len(set(got)) == 1 and got[0] is not None, "the same random value everywhere (%s)" % got)
    check(len(set(n.digest(SPACE)[0] for n in nodes)) == 1, "and the same digest")

    print("what reaches outside the data")
    got = answer(c, "statsfn", "s1")
    check(str(got).startswith("ERR") and "replicate" in str(got),
          "this node's storage stats are refused from the log (%r)" % str(got)[:100])
    got = answer(c, "otherfn", "s2")
    check(str(got).startswith("ERR") and "replicate" in str(got),
          "another space is refused from the log (%r)" % str(got)[:100])
    settled(nodes)
    check(all(local_get(n, "s1") is None and local_get(n, "s2") is None for n in nodes), "and nothing was written")
    got = answer(c, "statsfx", "s3")
    check(got == "ok", "with \"replicate\": \"effects\" it runs (%r)" % got)
    settled(nodes)
    check(all(local_get(n, "s3") == "x" for n in nodes), "and its write reaches every member")

    print("a function that never ends")
    for n in nodes:
        n.client().execute_command("CONFIG", "SET", "function_log_insns", "200000")
    got = answer(c, "spin")
    check(str(got).startswith("ERR") and "function_log_insns" in str(got),
          "stops at function_log_insns (%r)" % str(got)[:100])
    check(answer(c, "inc", "after") == 1, "and the group still takes writes")
    settled(nodes)
    check(len(set(n.digest(SPACE)[0] for n in nodes)) == 1, "every member the same")

    print("a follower saved, then restarted, runs nothing twice")
    for _ in range(50):
        c.execute_command("inc", "n")
    settled(nodes)
    follower = next(n for n in nodes if n is not leader_of(nodes, SPACE))
    follower.client(SPACE).execute_command("SAVE")
    follower.stop()
    follower.start()
    check(wait_for(90, lambda: local_get(follower, "n") == "1650"),
          "it comes back holding 1650 (%s)" % local_get(follower, "n"))
    check(wait_for(30, lambda: len(set(n.digest(SPACE)[0] for n in nodes)) == 1), "every member the same")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
