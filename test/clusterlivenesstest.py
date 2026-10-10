#!/usr/bin/env python3
"""
A group keeps its leader when a member is down or busy - TODO 628.

What has to hold:
  - with the node a group prefers as its leader killed, the others lead it and
    don't hand it to the dead node: its term stays put for 25s with nothing written.
    The leader judged the dead node by the log index it last heard from it, which
    still looked caught up, and handed it the group every 10s, leaving the group
    without a leader each time;
  - a member copying a snapshot (made to take 4s with a test setting) while it and
    the leader are the only two up doesn't cost the leader its lease. The copy used
    to run inside NuRaft's lock, so the member answered nothing until it was done;
  - the member catches up, and with everyone back all three hold the same copy.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of, on_leader  # noqa: E402

scale.workdir()
BASE = scale.port(default=27900)
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


def term(n):
    m = re.search(r"\bterm (\d+)", n.group_line(SPACE))
    return int(m.group(1)) if m else -1


def raft_leader(n):
    """(leader id, term) as n's Raft server has them, ready to serve or not"""
    l = n.group_line(SPACE)
    a = re.search(r"\bleader (-?\d+)", l)
    b = re.search(r"\bterm (\d+)", l)
    return (int(a.group(1)) if a else -1, int(b.group(1)) if b else -1)


def logged(n, text):
    try:
        return open(n.dir + ".log").read().count(text)
    except OSError:
        return 0


env = {"BARCH_TEST_SNAPSHOT_COPY_DELAY_MS": "8000"}
nodes = [clusternodes.Node(i, BASE, BINARY, ["raft_snapshot_entries=200"], env) for i in range(3)]
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
    # the group's preferred leader is the voter with the lowest number, node 1
    check(wait_for(30, lambda: leader_of(nodes, SPACE) is nodes[0]), "node 1, its preferred member, leads it")

    print("the preferred leader is killed")
    nodes[0].kill()
    rest = nodes[1:]
    check(wait_for(30, lambda: leader_of(rest, SPACE) is not None), "another node leads")
    time.sleep(2)
    first = max(term(n) for n in rest)
    end = time.time() + 25
    gaps = 0
    while time.time() < end:
        if leader_of(rest, SPACE) is None:
            gaps += 1
        time.sleep(0.25)
    last = max(term(n) for n in rest)
    check(last == first, "its term stays put for 25s (%d then %d)" % (first, last))
    check(gaps == 0, "and it always has a leader (%d checks without one)" % gaps)
    handed = sum(logged(n, "hands its leadership to node 1") for n in rest)
    check(handed == 0, "nobody hands it to the dead node (%d times)" % handed)
    nodes[0].start()
    # and leads again, as preferred, before the next part writes to whoever leads
    check(wait_for(90, lambda: all("members 3" in n.group_line(SPACE) for n in nodes)
                   and leader_of(nodes, SPACE) is nodes[0]), "node 1 comes back, and leads again")
    time.sleep(1)

    print("a member copies a snapshot while it and the leader are the only two up")
    leader = leader_of(nodes, SPACE)
    follower = next(n for n in nodes if n is not leader)
    third = next(n for n in nodes if n is not leader and n is not follower)
    follower.stop()
    w = clusternodes.Writers([leader, third], SPACE, count=2, prefix="k")
    w.start()
    wait_for(60, lambda: len(w.acked) >= 1200)
    w.stop()
    check(wait_for(30, lambda: leader.field(SPACE, "log_start") > 200),
          "the leader compacted its log (it starts at %d)" % leader.field(SPACE, "log_start"))
    before = logged(follower, "copying a snapshot")
    follower.start()
    check(wait_for(60, lambda: logged(follower, "copying a snapshot") > before),
          "the member starts copying (for 8s)")
    # only now, with the copy going: killed before the member was up, the third left
    # the leader alone for longer than its 300ms lease (a sanitizer build takes
    # seconds to start), and it stepped down for that, not for the copy - TODO 633
    third.kill()
    # the leader can't serve until the member has caught up - its first entry needs
    # the member to commit - so what's watched is Raft's own leader and term: a
    # leader that loses its lease steps down, and the next election moves the term
    # once there is one: with the third node gone, the two left may need an
    # election or two first, and under a sanitizer those take a while
    wait_for(30, lambda: raft_leader(leader)[0] > 0)
    start = raft_leader(leader)
    changed = 0
    seen = set()
    end = time.time() + 3.0
    while time.time() < end:
        now = raft_leader(leader)
        seen.add(now)
        if now != start:
            changed += 1
        time.sleep(0.1)
    check(start[0] > 0 and changed == 0,
          "the leader keeps its leadership through the copy (%s, then %s)" % (start, sorted(seen)))
    check(wait_for(90, lambda: follower.digest(SPACE) == on_leader([leader, follower], SPACE,
                                                                    lambda n: n.digest(SPACE), seconds=1)),
          "the member catches up")
    third.start()
    check(wait_for(90, lambda: len(set(n.digest(SPACE)[0] for n in nodes)) == 1), "and all three hold the same copy")
    missing = on_leader(nodes, SPACE, lambda n: sum(
        1 for k, v in w.acked.items() if n.client(SPACE).execute_command("GET", k) != v))
    missing = len(w.acked) if missing is None else missing
    check(missing == 0, "with every acknowledged write in it (%d, %d missing)" % (len(w.acked), missing))
finally:
    for n in nodes:
        if n.proc and n.proc.poll() is not None:
            n.proc = None
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
