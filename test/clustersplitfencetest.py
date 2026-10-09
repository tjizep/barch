#!/usr/bin/env python3
"""
The write fence and the retry of a live split - TODO 617.

Two test knobs make the narrow cases wide:
  - BARCH_TEST_COMMIT_DELAY_MS holds every data write for that long between the
    fence check and the log, so a hand-off always lands among writes in flight;
  - BARCH_TEST_LOSE_HANDOFF turns the first hand-off's answer into UNKNOWN after
    it has committed.

What has to hold:
  - no write for a handed-off shard reaches the old group's log after the
    hand-off: no member applies an entry for a shard its group no longer owns
    ("strays" in CLUSTER INFO), every acknowledged write is there, and every
    member holds the same copy;
  - a SPLIT whose answer was UNKNOWN is finished by running it again: the second
    answer names the group the first one made, and no third group appears;
  - the space's record lists the two groups' runs, which cover every shard once.

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
BASE = scale.port(default=26100)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
NEW = SPACE + "/1"
DELAY_MS = 150
failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


env = {"BARCH_TEST_COMMIT_DELAY_MS": str(DELAY_MS), "BARCH_TEST_LOSE_HANDOFF": "1"}
nodes = [clusternodes.Node(i, BASE, BINARY, ["raft_snapshot_entries=200"], env) for i in range(3)]
by_port = {n.port: n for n in nodes}


def groups_of(n):
    """label -> (group, from, to, line) for the space's groups on node n"""
    out = {}
    for l in n.info():
        if not l.startswith("group ") or (" spaces %s" % SPACE) not in l:
            continue
        m = re.search(r"shards (\d+)-(\d+) label (\S+)", l)
        if m:
            out[m.group(3)] = (int(l.split()[1]), int(m.group(1)), int(m.group(2)), l)
        else:
            out[SPACE] = (int(l.split()[1]), None, None, l)
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


def strays(n):
    return sum(int(m.group(1)) for m in (re.search(r"\bstrays (\d+)", l) for l in n.info()) if m)


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


def split(label):
    try:
        return leader_of(nodes, "cluster").client().execute_command("CLUSTER", "SPLIT", label)
    except redis.exceptions.TryAgainError as e:
        return "TRYAGAIN %s" % e          # redis-py takes the word off
    except redis.ResponseError as e:
        return str(e)


try:
    print("a replicated space whose writes wait %dms before the log" % DELAY_MS)
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(60, all_voters([SPACE])) and wait_for(30, lambda: led(SPACE)), "three voters and a leader")

    print("split while eight clients write, and lose the answer")
    w = clusternodes.Writers(nodes, SPACE, count=8, prefix="f")
    w.start()
    time.sleep(scale.scaled_seconds(1.5))
    first = split(SPACE)
    check(first.startswith("UNKNOWN"), "the first SPLIT answers UNKNOWN (%s)" % first)
    again = first
    for _ in range(300):
        again = split(SPACE)
        if not again.startswith("TRYAGAIN"):
            break
        time.sleep(0.1)
    check(again.startswith("OK %s group " % NEW), "SPLIT again finishes it (%s)" % again)
    check(wait_for(60, all_voters([SPACE, NEW])) and wait_for(30, lambda: led(NEW)),
          "every member runs the new group, and it has a leader")
    time.sleep(scale.scaled_seconds(1.5))
    w.stop()
    print("    acknowledged %d, uncertain %d, told %s" % (len(w.acked), len(w.maybe), w.errors))

    print("what the split left")
    groups = groups_of(nodes[0])
    check(sorted(groups) == [SPACE, NEW], "two groups, no third (%s)" % sorted(groups))
    number = int(again.split()[-1]) if again.startswith("OK") else -1
    check(groups.get(NEW, (None,))[0] == number, "the new one is the group SPLIT named (%d)" % number)
    runs = sorted((v[1], v[2]) for v in groups.values())
    shards = runs[-1][1] + 1
    check([s for f, t in runs for s in range(f, t + 1)] == list(range(shards)),
          "their runs cover every shard once (%s)" % runs)
    record = leader_of(nodes, "cluster").client("cluster").execute_command("HGET", "space:" + SPACE, "runs")
    check(record is not None and SPACE + ":" in record and NEW + ":" in record and record.count(";") == 1,
          "and the space's record lists both (%s)" % record)

    print("the fence")
    found = [strays(n) for n in nodes]
    check(found == [0, 0, 0], "no member applied an entry for a shard handed off (%s)" % found)
    missing = sum(1 for k, v in w.acked.items() if get_following(k, nodes[0]) != v)
    check(len(w.acked) > 0 and missing == 0,
          "every acknowledged write is there (%d, %d missing)" % (len(w.acked), missing))
    check(wait_for(60, lambda: len(set(n.digest(SPACE)[0] for n in nodes)) == 1),
          "every member holds the same copy")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
