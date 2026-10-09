#!/usr/bin/env python3
"""
Three barchd processes in a Raft cluster, and the leader of a replicated space
killed while clients write to it - TODO 610.

What has to hold:
  - every write the cluster acknowledged is there after the leader is gone, on the
    leader the other two elect;
  - a follower refuses reads with NOTLEADER and the leader's address;
  - the killed node comes back from its files and its log, catches up, and every
    member then holds the same copy of the space.

Writes are each to a key of their own, so "acknowledged" means "that key holds that
value". A write whose connection died with the leader, or that came back UNKNOWN,
may or may not have happened; it only has to agree across the members.

Ports are BARCH_TEST_PORT and the 11 after it: three RESP ports, and three raft
bases with two groups each.
"""
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402

scale.workdir()
BASE = scale.port(default=24600)
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



def Node(i):
    return clusternodes.Node(i, BASE, BINARY)


wait_for = clusternodes.wait_for
leader_of = clusternodes.leader_of


nodes = [Node(i) for i in range(3)]
try:
    print("a cluster of three")
    for n in nodes:
        n.start()
    check(nodes[0].client().execute_command("CLUSTER", "INIT") == "OK", "node 1 starts a cluster")
    for n in nodes[1:]:
        ok = n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port)) == "OK"
        check(ok, "node %d joins it" % (n.i + 1))
    check(wait_for(20, lambda: all("members 3" in n.group_line("cluster") for n in nodes)),
          "and every member lists three")
    check(wait_for(20, lambda: all("learners 0" in n.group_line("cluster") for n in nodes)),
          "and the two that joined are voters")

    print("a replicated space")
    c = nodes[0].client("configuration")
    c.execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(30, lambda: leader_of(nodes, SPACE) is not None
                   and all("members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE)
                           for n in nodes)),
          "%s.raft on gives it a group of three voters" % SPACE)
    leader = leader_of(nodes, SPACE)
    follower = [n for n in nodes if n is not leader][0]
    try:
        follower.client(SPACE).execute_command("GET", "x")
        check(False, "a follower refuses a read")
    except redis.ResponseError as e:
        check(str(e) == "NOTLEADER 127.0.0.1:%d %d" % (leader.port, leader.field(SPACE, "term")),
              "a follower refuses a read, naming the leader and its epoch (%s)" % e)

    print("the leader is killed while four clients write")
    writers = clusternodes.Writers(nodes, SPACE)
    acked, maybe, errors = writers.acked, writers.maybe, writers.errors
    writers.start()
    time.sleep(scale.scaled_seconds(2.0))
    before = len(acked)
    killed = leader
    killed.kill()
    check(before > 0, "writes were acknowledged before the kill (%d)" % before)
    check(wait_for(30, lambda: (lambda l: l is not None and l is not killed)(leader_of(nodes, SPACE))),
          "the other two elect a new leader")
    time.sleep(scale.scaled_seconds(2.0))
    writers.stop()
    after = len(acked)
    print("    what writers were told:", errors)
    check(after > before, "and take writes again (%d more)" % (after - before))

    new_leader = leader_of(nodes, SPACE)
    cl = new_leader.client(SPACE)
    missing = [k for k, v in acked.items() if cl.execute_command("GET", k) != v]
    check(not missing, "every acknowledged write is on the new leader (%d checked, %d missing)"
          % (len(acked), len(missing)))
    if missing:
        print("    missing, first few:", missing[:10])

    print("the killed node comes back")
    killed.start()
    live = [n for n in nodes]

    def caught_up():
        lines = [n.group_line(SPACE) for n in live]
        applied = [int(re.search(r"applied (\d+)", l).group(1)) for l in lines if "applied" in l]
        return len(applied) == 3 and len(set(applied)) == 1

    check(wait_for(60, caught_up), "it rejoins and every member has applied the same log")
    digests = [n.digest(SPACE)[0] for n in live]
    check(len(set(digests)) == 1, "and every member holds the same copy (%s)" % digests[0])
    keys = int(digests[0].split()[1])
    check(len(acked) <= keys <= len(acked) + len(maybe),
          "which has every acknowledged key, and at most the uncertain ones besides (%d, %d, +%d)"
          % (keys, len(acked), len(maybe)))

    print("the cluster space")
    # read on the cluster group's leader, which a client is sent to like any other
    boss = leader_of(nodes, "cluster")
    check(boss is not None, "the cluster group has a leader")
    cc = boss.client("cluster")
    ids = {}
    for n in nodes:
        first = n.info()[0].split()
        ids[n] = first[1]
    # read again each time: a leader handing over (TODO 612) adds an entry
    def stats_follow():
        for n in nodes:
            h = cc.execute_command("HGETALL", "node:%s:space:%s" % (ids[n], SPACE))
            m = dict(zip(h[::2], h[1::2])) if isinstance(h, list) else h
            if int(m.get("applied", "0")) != n.field(SPACE, "applied"):
                return False
        return True

    check(wait_for(15, stats_follow), "node:<id>:space:%s holds each member's applied index" % SPACE)
    seen = [int(cc.execute_command("GET", "node:%s:seen" % ids[n]) or 0) for n in nodes]
    check(all(time.time() * 1000 - s < 5000 for s in seen), "and node:<id>:seen is recent for all three")

    try:
        cc.execute_command("SET", "node:nobody", "x")
        check(False, "a client can't write the cluster space")
    except redis.ResponseError as e:
        check(True, "a client can't write the cluster space (%s)" % str(e)[:40])

    print("a second replicated space")
    boss.client("configuration").execute_command("SET", "second.raft", "on")
    check(wait_for(30, lambda: sorted(cc.execute_command("KEYS", "space:*")) == ["space:" + SPACE, "space:second"]),
          "gets a group of its own - TODO 612")
    check(wait_for(30, lambda: all("members 3" in n.group_line("second") for n in nodes)),
          "with all three in it")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
