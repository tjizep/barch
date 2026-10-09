#!/usr/bin/env python3
"""
Splitting a replicated space's group while clients write to it - TODO 616.

What has to hold:
  - CLUSTER SPLIT on a group hands the upper half of its shards to a new group,
    which every member starts and which gets a leader of its own;
  - a second split, of the new group, works the same way;
  - clients writing through redirects the whole time lose no acknowledged write,
    and every member ends with the same copy;
  - the groups' runs cover every shard once, and CLUSTER ROUTES and the space's
    record in the cluster space agree;
  - a node restarted after the splits comes back with the new groups.

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
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=25900)
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


nodes = [clusternodes.Node(i, BASE, BINARY, ["raft_snapshot_entries=200"]) for i in range(3)]
by_port = {n.port: n for n in nodes}


def groups_of(n):
    """label -> (group, from, to, line) for the space's groups on node n"""
    out = {}
    for l in n.info():
        if not l.startswith("group ") or (" spaces %s" % SPACE) not in l:
            continue
        num = int(l.split()[1])
        m = re.search(r"shards (\d+)-(\d+) label (\S+)", l)
        if m:
            out[m.group(3)] = (num, int(m.group(1)), int(m.group(2)), l)
        else:
            out[SPACE] = (num, None, None, l)
    return out


def all_voters(labels):
    def done():
        for n in nodes:
            g = groups_of(n)
            for lab in labels:
                if lab not in g or "members 3" not in g[lab][3] or "learners 0" not in g[lab][3]:
                    return False
        return True
    return done


def led(label):
    return any("(this node)" in groups_of(n).get(label, (0, 0, 0, ""))[3] for n in nodes if n.proc)


def get_following(key, start):
    target = start
    for _ in range(8):
        try:
            return target.client(SPACE).execute_command("GET", key)
        except redis.ResponseError as e:
            m = re.match(r"NOTLEADER 127\.0\.0\.1:(\d+)", str(e))
            if m:
                target = by_port[int(m.group(1))]
            else:
                time.sleep(0.1)
    raise AssertionError("no answer for %s" % key)


def same_copies():
    return len(set(n.digest(SPACE)[0] for n in nodes)) == 1


try:
    print("a replicated space in one group")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(60, all_voters([SPACE])) and wait_for(30, lambda: led(SPACE)), "three voters and a leader")

    print("split twice while four clients write")
    w = clusternodes.Writers(nodes, SPACE, count=4, prefix="w")
    w.start()
    time.sleep(scale.scaled_seconds(1.5))
    boss = leader_of(nodes, "cluster")
    said = boss.client().execute_command("CLUSTER", "SPLIT", SPACE)
    check(said.startswith("OK %s/1" % SPACE), "CLUSTER SPLIT %s answers the new group (%s)" % (SPACE, said))
    check(wait_for(60, all_voters([SPACE, SPACE + "/1"])) and wait_for(30, lambda: led(SPACE + "/1")),
          "every member runs it, and it has a leader")
    time.sleep(scale.scaled_seconds(1.5))
    boss = leader_of(nodes, "cluster")
    said = boss.client().execute_command("CLUSTER", "SPLIT", SPACE + "/1")
    check(said.startswith("OK %s/2" % SPACE), "and splitting that one too (%s)" % said)
    labels = [SPACE, SPACE + "/1", SPACE + "/2"]
    check(wait_for(60, all_voters(labels)) and wait_for(30, lambda: all(led(l) for l in labels)),
          "three groups, each with a leader")
    time.sleep(scale.scaled_seconds(1.5))
    w.stop()
    print("    acknowledged %d, uncertain %d, told %s" % (len(w.acked), len(w.maybe), w.errors))

    runs = sorted((v[1], v[2]) for v in groups_of(nodes[0]).values())
    shards = runs[-1][1] + 1
    covered = [s for f, t in runs for s in range(f, t + 1)]
    check(covered == list(range(shards)), "the runs cover every shard once (%s)" % runs)
    routes = nodes[2].client().execute_command("CLUSTER", "ROUTES")
    check(all(any(r.startswith(l + " ") and "shards" in r for r in routes) for l in labels),
          "CLUSTER ROUTES lists all three with their runs")
    record = leader_of(nodes, "cluster").client("cluster").execute_command("HGET", "space:" + SPACE, "runs")
    check(record is not None and all(l in record for l in labels), "and so does the space's record (%s)" % record)

    missing = sum(1 for k, v in w.acked.items() if get_following(k, nodes[0]) != v)
    check(len(w.acked) > 0 and missing == 0,
          "every acknowledged write is there (%d, %d missing)" % (len(w.acked), missing))
    check(wait_for(60, same_copies), "every member holds the same copy")

    print("a node restarts after the splits")
    nodes[2].stop()
    nodes[2].start()
    check(wait_for(60, all_voters(labels)), "it comes back in all three groups")
    after = {l: (v[1], v[2]) for l, v in groups_of(nodes[2]).items()}
    before = {l: (v[1], v[2]) for l, v in groups_of(nodes[0]).items()}
    check(after == before, "with the same runs (%s)" % after)
    check(wait_for(60, same_copies), "and the same copy")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
