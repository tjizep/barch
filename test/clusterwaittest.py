#!/usr/bin/env python3
"""
Writes waiting on Raft don't hold up other clients - TODO 626.

A write to a replicated space waits for its commit. It used to wait on the RESP
thread its connection lives on, and every other connection on that thread waited
with it, whatever space it used: clusterbench measured reads of a space that isn't
replicated going from 325k/s to 52/s beside writers.

What has to hold:
  - reads of a space that isn't replicated, on the leader, are about as quick beside
    32 writers to a replicated space as without them;
  - every write those writers were told succeeded is there;
  - a pipelined SET and CLUSTER INDEX on one connection: the index covers the SET;
  - MULTI/EXEC on the replicated space still works.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=27300)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
PLAIN = "plain"
WRITERS = 32
failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def read_latencies(node, seconds):
    """GETs of the plain space for that long, one at a time; their times in ms"""
    c = node.client(PLAIN)
    out = []
    end = time.time() + seconds
    i = 0
    while time.time() < end:
        a = time.perf_counter()
        c.execute_command("GET", "p%d" % (i % 1000))
        out.append((time.perf_counter() - a) * 1000)
        i += 1
    out.sort()
    return out


def median(xs):
    return xs[len(xs) // 2] if xs else float("inf")


nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]
try:
    print("a replicated space and a plain one")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(60, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE) for n in nodes)),
        "three voters and a leader")
    time.sleep(1)
    leader = leader_of(nodes, SPACE)
    p = leader.client(PLAIN).pipeline(transaction=False)
    for i in range(1000):
        p.execute_command("SET", "p%d" % i, "v")
    p.execute()

    print("reads of the plain space, alone and beside %d writers" % WRITERS)
    alone = read_latencies(leader, scale.scaled_seconds(2.0))
    stop = threading.Event()
    acked = {}
    guard = threading.Lock()

    def writer(w):
        c = leader.client(SPACE)
        n = 0
        while not stop.is_set():
            key = "w%d-%d" % (w, n)
            n += 1
            try:
                c.execute_command("SET", key, key)
                with guard:
                    acked[key] = key
            except redis.RedisError:
                time.sleep(0.01)

    threads = [threading.Thread(target=writer, args=(w,)) for w in range(WRITERS)]
    for t in threads:
        t.start()
    time.sleep(scale.scaled_seconds(1.0))
    beside = read_latencies(leader, scale.scaled_seconds(3.0))
    stop.set()
    for t in threads:
        t.join()
    print("    alone: %d reads, median %.3f ms; beside: %d reads, median %.3f ms, p99 %.3f ms; %d writes" % (
        len(alone), median(alone), len(beside), median(beside), beside[int(len(beside) * 0.99)] if beside else -1,
        len(acked)))
    check(len(acked) > 0, "the writers wrote")
    # relative, so it holds under a sanitizer too: blocked, the median was 50ms and
    # more against a few hundredths of a millisecond alone
    check(median(beside) < 10 * median(alone) + 5,
          "reads beside the writers are about as quick (median %.3f ms against %.3f)"
          % (median(beside), median(alone)))
    c = leader.client(SPACE)
    missing = sum(1 for k, v in acked.items() if c.execute_command("GET", k) != v)
    check(missing == 0, "every acknowledged write is there (%d, %d missing)" % (len(acked), missing))

    print("one connection's own order and state")
    # redis-py runs a pipeline on a connection of its own, not the one that ran USE,
    # so each says where it is: USE in the pipeline, and a space: prefix inside MULTI
    c = leader.client(SPACE)
    p = c.pipeline(transaction=False)
    p.execute_command("USE", SPACE)
    p.execute_command("CLUSTER", "INDEX", SPACE)
    p.execute_command("SET", "pipelined", "1")
    p.execute_command("CLUSTER", "INDEX", SPACE)
    p.execute_command("GET", "pipelined")
    r = p.execute()
    check(r[2] in ("OK", True) and int(r[3]) > int(r[1]) and r[4] == "1",
          "a pipelined SET, CLUSTER INDEX and GET: the index covers the SET (%s)" % r[1:])
    t = c.pipeline(transaction=True)
    t.execute_command(SPACE + ":SET", "m1", "a")
    t.execute_command(SPACE + ":SET", "m2", "b")
    r = t.execute()
    check(len(r) == 2 and c.execute_command("GET", "m1") == "a" and c.execute_command("GET", "m2") == "b",
          "MULTI/EXEC on the replicated space (%s)" % r)
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
