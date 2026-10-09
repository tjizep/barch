#!/usr/bin/env python3
"""
Single-key writes that commit with the shard latch let go - TODO 632.

A single-key write to a replicated space used to hold its shard's latch until it
committed, so a shard had one write in flight. Now it works out its record under the
latch, takes it back out of the tree, marks its key pending, and lets the latch go
while the entry commits; the group's commit thread applies it, on the leader as on
every member. A space with 4 shards shows it.

What has to hold:
  - 32 writers overlap past 4: each follower's log syncs cover more than 4 entries
    on average (a follower syncs once per batch it's sent, and a batch can only hold
    the writes in flight);
  - 16 threads INCR one key: its value is the number of INCRs acknowledged, on every
    member;
  - hashes and lists (composite commands, which still hold the latch through their
    commit) written beside plain SETs on the same 4 shards: nothing deadlocks, every
    acknowledged write is there, and the copies match;
  - the leader paused with SIGSTOP under writes (so some are refused, some unknown):
    every acknowledged write is there afterwards and the copies match.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import re
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=29300)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "narrow"
failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def counters(n):
    line = n.group_line(SPACE)
    vals = [int(m.group(1)) if m else 0 for m in (re.search(r"\b%s (\d+)" % k, line)
                                                 for k in ("log_appended", "log_syncs"))]
    return tuple(vals)


def same_copies(nodes):
    return lambda: len(set(n.digest(SPACE)[0] for n in nodes)) == 1


def run(threads, seconds):
    stop = threading.Event()
    ts = [threading.Thread(target=t, args=(stop,)) for t in threads]
    for t in ts:
        t.start()
    time.sleep(seconds)
    stop.set()
    for t in ts:
        t.join(60)
    return not any(t.is_alive() for t in ts)


nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]
try:
    print("a replicated space with 4 shards")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    conf = nodes[0].client("configuration")
    conf.execute_command("SET", SPACE + ".shards", "4")
    conf.execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE) for n in nodes)),
        "three voters and a leader")
    check(all(n.field(SPACE, "space_shards") == 4 for n in nodes), "with 4 shards on each")
    leader = leader_of(nodes, SPACE)
    followers = [n for n in nodes if n is not leader]

    print("32 writers on 4 shards")
    acked = {}
    guard = threading.Lock()

    def setter(w):
        def go(stop):
            c = leader.client(SPACE)
            i = 0
            while not stop.is_set():
                k = "s%d-%d" % (w, i)
                i += 1
                try:
                    c.execute_command("SET", k, k)
                    with guard:
                        acked[k] = k
                except redis.RedisError:
                    time.sleep(0.01)
        return go

    before = {n.i: counters(n) for n in followers}
    check(run([setter(w) for w in range(32)], scale.scaled_seconds(4.0, floor=3.0)), "they all finish")
    per_sync = []
    for n in followers:
        a0, s0 = before[n.i]
        a1, s1 = counters(n)
        per_sync.append((a1 - a0) / max(1, s1 - s0))
    print("    %d writes; entries per sync on the followers: %s" % (len(acked), ["%.1f" % p for p in per_sync]))
    check(min(per_sync) > 4, "the writes overlap past the 4 shards (%s per sync)" % ["%.1f" % p for p in per_sync])

    print("16 threads INCR one key")
    incrs = [0]

    def incr(stop):
        c = leader.client(SPACE)
        while not stop.is_set():
            try:
                c.execute_command("INCR", "counter")
                with guard:
                    incrs[0] += 1
            except redis.RedisError:
                time.sleep(0.01)

    check(run([incr] * 16, scale.scaled_seconds(3.0, floor=2.0)), "they all finish")
    check(wait_for(30, same_copies(nodes)), "the copies match")
    vals = []
    for n in nodes:
        c = n.client(SPACE)
        c.execute_command("CLUSTER", "READS", "FOLLOWER")
        vals.append(int(c.execute_command("GET", "counter") or 0))
    check(all(v == incrs[0] for v in vals), "counter is the %d acknowledged, on every member (%s)" % (incrs[0], vals))

    print("hashes and lists beside plain SETs, on the same shards")
    composite_ok = {"h": 0, "l": 0}

    def hasher(w):
        def go(stop):
            c = leader.client(SPACE)
            i = 0
            while not stop.is_set():
                try:
                    c.execute_command("HSET", "hash%d" % (w % 4), "f%d-%d" % (w, i), "v")
                    with guard:
                        composite_ok["h"] += 1
                    i += 1
                except redis.RedisError:
                    time.sleep(0.01)
        return go

    def lister(w):
        def go(stop):
            c = leader.client(SPACE)
            while not stop.is_set():
                try:
                    c.execute_command("LPUSH", "list%d" % (w % 4), "x")
                    with guard:
                        composite_ok["l"] += 1
                except redis.RedisError:
                    time.sleep(0.01)
        return go

    acked.clear()
    finished = run([setter(100 + w) for w in range(16)] + [hasher(w) for w in range(8)] + [lister(w) for w in range(8)],
                   scale.scaled_seconds(4.0, floor=3.0))
    check(finished, "nothing deadlocks: every writer finishes")
    print("    %d SETs, %d HSETs, %d LPUSHes" % (len(acked), composite_ok["h"], composite_ok["l"]))
    check(composite_ok["h"] > 0 and composite_ok["l"] > 0, "the composite writes went through too")
    c = leader.client(SPACE)
    missing = sum(1 for k, v in acked.items() if c.execute_command("GET", k) != v)
    lens = sum(int(c.execute_command("LLEN", "list%d" % w)) for w in range(4))
    check(missing == 0, "every acknowledged SET is there (%d missing)" % missing)
    check(lens == composite_ok["l"], "and every acknowledged LPUSH (%d, %d)" % (lens, composite_ok["l"]))
    check(wait_for(30, same_copies(nodes)), "the copies match")

    print("the leader is paused while 16 clients write")
    w = clusternodes.Writers(nodes, SPACE, count=16, prefix="p")
    w.start()
    time.sleep(scale.scaled_seconds(1.0))
    leader.pause()
    check(wait_for(30, lambda: leader_of(followers, SPACE) is not None), "another node leads")
    time.sleep(0.5)
    leader.resume()
    time.sleep(scale.scaled_seconds(2.0))
    w.stop()
    print("    acknowledged %d, uncertain %d, told %s" % (len(w.acked), len(w.maybe), w.errors))
    check(wait_for(60, lambda: leader_of(nodes, SPACE) is not None), "a leader")
    l = leader_of(nodes, SPACE)
    c = l.client(SPACE)
    missing = sum(1 for k, v in w.acked.items() if c.execute_command("GET", k) != v)
    check(len(w.acked) > 0 and missing == 0, "every acknowledged write is there (%d missing)" % missing)
    check(wait_for(60, same_copies(nodes)), "and the copies match")
finally:
    for n in nodes:
        try:
            n.resume()
        except Exception:
            pass
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
