#!/usr/bin/env python3
"""
The cluster's tick keeps going while the cluster group is slow - TODO 795.

Each node's tick thread runs the cluster's bookkeeping every 250ms: membership changes,
shard checks, batch and transaction recovery, and this node's heartbeat. The heartbeat
was sent from there too, and on the cluster group's leader it's a Raft commit for
`node:<id>:seen` plus one per space, each waited for in turn. With slow syncs that's
seconds, the tick's own reads of the cluster space queued behind them as well, and
ticks of 6-18s showed up in TestClusterReadIndex and TestClusterSlowHeartbeat.

Every log sync here is 700ms slower, on every node, and writers keep a space busy. What
has to hold, once the groups are set up:
  - no node logs a tick of 2s or more. With the heartbeat moved off the tick, the
    leader's ticks were still 1-2.6s: its HGETALLs of each node's record took the
    write latch, and waited at the gate behind every commit of their shard;
  - no commit in the cluster group times out. The batch is applied on the leader too,
    under its shards' latches, and an HSET that keeps its latch through its own commit
    and comes after it in the log held the latch the batch needed: both waited out the
    3s timeout, and a CLUSTER JOIN failed with UNKNOWN in the suite;
  - and the cluster space has each node's record for the space, with its applied
    index, term and whether it leads, and one of them leading. The leader now writes a
    heartbeat as one batch, applied from the log on every member, itself included, so
    this checks the batch lands whole.

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
BASE = scale.port(default=24100)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
SYNC_MS = 700
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def log_size(n):
    return os.path.getsize(n.dir + ".log")


def as_dict(v):
    if isinstance(v, dict):
        return v
    return dict(zip(v[0::2], v[1::2]))


def records():
    """the `node:<id>:space:<SPACE>` records, from whichever node answers for the cluster space"""
    for n in nodes:
        try:
            c = n.client("cluster")
            return [as_dict(c.execute_command("HGETALL", k))
                    for k in c.execute_command("KEYS", "node:*:space:" + SPACE)]
        except redis.ResponseError:
            continue        # NOTLEADER: reads of the cluster space go to its leader
    return []


def records_whole():
    got = records()
    return (len(got) == 3 and all(set(r) == {"applied", "term", "leader"} for r in got)
            and [r["leader"] for r in got].count("1") == 1)


def timeouts(n):
    """commits in the cluster group (group 0) that waited out their timeout"""
    with open(n.dir + ".log", "rb") as f:
        text = f.read().decode(errors="replace")
    return len(re.findall(r"raft group 0 \[NOT OK\] commit_ret_cv .* result code -2", text))


def ticks_since(n, offset):
    """the lengths of the long ticks a node logged after `offset`, in ms"""
    with open(n.dir + ".log", "rb") as f:
        f.seek(offset)
        text = f.read().decode(errors="replace")
    return [int(m) for m in re.findall(r"a tick took (\d+) ms", text)]


nodes = [clusternodes.Node(i, BASE, BINARY, env={"BARCH_TEST_LOG_SYNC_DELAY_MS": str(SYNC_MS)})
         for i in range(3)]
try:
    print("three nodes and a space, every log sync %dms" % SYNC_MS)
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(120, lambda: all(
        "members 3" in n.group_line(g) and "learners 0" in n.group_line(g)
        for n in nodes for g in ("cluster", SPACE))), "three voters in both groups")

    print("writers for 15s")
    offsets = [log_size(n) for n in nodes]
    w = clusternodes.Writers(nodes, SPACE, count=4, prefix="k")
    w.start()
    time.sleep(15)
    w.stop()
    check(len(w.acked) > 0, "writes acknowledged (%d)" % len(w.acked))
    longest = [max(ticks_since(n, o) or [0]) for n, o in zip(nodes, offsets)]
    check(max(longest) < 2000, "no tick took 2s or more (longest on each node: %s ms)" % longest)
    got = [timeouts(n) for n in nodes]
    check(sum(got) == 0, "no commit in the cluster group timed out (%s)" % got)
    check(wait_for(30, records_whole),
          "the cluster space has each node's record for the space, one leading")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
