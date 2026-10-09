#!/usr/bin/env python3
"""
A replicated space split over three Raft groups - TODO 615.

What has to hold:
  - <space>.raft_groups 3 gives the space three groups, each owning a run of its
    shards, led by three different nodes;
  - a single-key read on a node is answered there when the key's group is led
    there, and redirected to that group's leader when it isn't;
  - a read over the whole space needs every group: refused by a node that leads
    only some of them, answered with follower reads;
  - clearing the space in one step is refused;
  - with clients writing through redirects, killing a node loses no acknowledged
    write, and once it's back every member holds the same copy.

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
BASE = scale.port(default=25700)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
LABELS = ["%s/%d" % (SPACE, k) for k in range(3)]
failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


nodes = [clusternodes.Node(i, BASE, BINARY, ["raft_snapshot_entries=200"]) for i in range(3)]
by_port = {n.port: n for n in nodes}


def line_of(n, label):
    for l in n.info():
        if (" label %s" % label) in l:
            return l
    return ""


def leader_of_group(label):
    for n in nodes:
        if n.proc:
            try:
                if "(this node)" in line_of(n, label):
                    return n
            except redis.RedisError:
                pass
    return None


def get_following(key, start):
    """GET through redirects; the value and the node that answered"""
    target = start
    for _ in range(5):
        try:
            return target.client(SPACE).execute_command("GET", key), target
        except redis.ResponseError as e:
            m = re.match(r"NOTLEADER 127\.0\.0\.1:(\d+)", str(e))
            if not m:
                time.sleep(0.1)
                continue
            target = by_port[int(m.group(1))]
    raise AssertionError("no answer for %s" % key)


def same_copies(group):
    def done():
        return len(set(n.digest(SPACE)[0] for n in group)) == 1
    return done


try:
    print("a space split over three groups")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    conf = nodes[0].client("configuration")
    conf.execute_command("SET", SPACE + ".raft_groups", "3")
    conf.execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(60, lambda: all("members 3" in line_of(n, l) and "learners 0" in line_of(n, l)
                                   for n in nodes for l in LABELS)),
          "three groups, three voters each")
    check(wait_for(60, lambda: len({leader_of_group(l) for l in LABELS} - {None}) == 3),
          "led by three different nodes")
    runs = [re.search(r"shards (\d+-\d+)", line_of(nodes[0], l)).group(1) for l in LABELS]
    print("    shard runs:", runs)
    routes = nodes[1].client().execute_command("CLUSTER", "ROUTES")
    check(all(any(r.startswith(l + " ") for r in routes) for l in LABELS), "CLUSTER ROUTES lists each group")

    print("single-key reads")
    w = clusternodes.Writers(nodes, SPACE, count=1, prefix="s")
    w.start()
    time.sleep(1)
    w.stop()
    check(len(w.acked) > 50, "a client writes through redirects (%d keys)" % len(w.acked))
    a = nodes[0]
    here, sent = 0, 0
    wrong = 0
    for key in list(w.acked)[:200]:
        value, answered = get_following(key, a)
        if value != key:
            wrong += 1
        if answered is a:
            here += 1
        else:
            sent += 1
    check(wrong == 0, "every key reads back (%d wrong)" % wrong)
    check(here > 0 and sent > 0,
          "node 1 answers its groups' keys and redirects the rest (%d here, %d sent)" % (here, sent))

    print("reads over the whole space")
    try:
        a.client(SPACE).execute_command("KEYS", "*")
        check(False, "KEYS on a node that leads one group is refused")
    except redis.ResponseError as e:
        check(str(e).startswith("NOTLEADER"), "KEYS on a node that leads one group is refused (%s)" % e)
    c = a.client(SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    keys = c.execute_command("KEYS", "*")
    check(set(w.acked) <= set(keys), "and answered with follower reads (%d keys)" % len(keys))
    try:
        leader_of_group(LABELS[0]).client(SPACE).execute_command("FLUSHDB")
        check(False, "FLUSHDB is refused")
    except redis.ResponseError as e:
        check("split" in str(e), "FLUSHDB is refused (%s)" % e)

    print("a node is killed while clients write")
    w = clusternodes.Writers(nodes, SPACE, count=4, prefix="k")
    w.start()
    time.sleep(scale.scaled_seconds(2.0))
    victim = nodes[1]
    led = [l for l in LABELS if leader_of_group(l) is victim]
    victim.kill()
    check(wait_for(30, lambda: all(leader_of_group(l) not in (None, victim) for l in LABELS)),
          "every group has a leader among the other two (node 2 led %s)" % led)
    time.sleep(scale.scaled_seconds(2.0))
    w.stop()
    print("    acknowledged %d, uncertain %d, told %s" % (len(w.acked), len(w.maybe), w.errors))
    live = [n for n in nodes if n is not victim]
    missing = 0
    for key, v in w.acked.items():
        got, _ = get_following(key, live[0])
        if got != v:
            missing += 1
    check(len(w.acked) > 0 and missing == 0,
          "every acknowledged write is there (%d, %d missing)" % (len(w.acked), missing))
    victim.start()
    check(wait_for(90, same_copies(nodes)), "node 2 comes back and the three copies match")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
