#!/usr/bin/env python3
"""
Locks with fencing tokens in a space replicated with Raft - TODO 614.

What has to hold:
  - LOCK grants a free lock with a token above 0, answers 0 while another owner
    holds it, and renews for its owner with a higher token; UNLOCK only releases
    for the owner; LOCKINFO names the holder;
  - a lock whose time is up can be taken by someone else;
  - eight clients contending for one lock never hold it at the same time, and the
    tokens they're granted only go up;
  - a lock survives its space's leader being killed: the new leader still says
    who holds it, and its next token is higher than any before.

Ports are BARCH_TEST_PORT and the 15 after it.
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
BASE = scale.port(default=25500)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "locks"
failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]


def on_leader():
    l = leader_of(nodes, SPACE)
    return l.client(SPACE) if l else None


try:
    print("a replicated space for locks")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(60, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE)
        and "learners 0" in n.group_line("cluster") for n in nodes)), "three voters")
    time.sleep(2)

    print("grant, refuse, renew, release")
    c = on_leader()
    t1 = c.execute_command("LOCK", "a", "alice", "60000")
    check(t1 > 0, "a free lock is granted with a token (%d)" % t1)
    check(c.execute_command("LOCK", "a", "bob", "60000") == 0, "and refused to another owner")
    check(c.execute_command("LOCKINFO", "a")[0] == "alice", "LOCKINFO names the holder")
    t2 = c.execute_command("LOCK", "a", "alice", "60000")
    check(t2 > t1, "the holder renews it with a higher token (%d)" % t2)
    check(c.execute_command("UNLOCK", "a", "bob") == 0, "UNLOCK by another owner does nothing")
    check(c.execute_command("UNLOCK", "a", "alice") == 1, "UNLOCK by the holder releases it")
    check(c.execute_command("LOCKINFO", "a") is None, "and then nobody holds it")
    t3 = c.execute_command("LOCK", "a", "bob", "60000")
    check(t3 > t2, "the next owner gets a higher token (%d)" % t3)
    c.execute_command("SET", "plain", "x")
    check(c.execute_command("KEYS", "*") == ["plain"], "a lock isn't a key a client sees")
    try:
        nodes[0].client("unreplicated").execute_command("LOCK", "a", "x", "100")
        check(False, "LOCK in a space without Raft is refused")
    except redis.ResponseError as e:
        check("Raft" in str(e), "LOCK in a space without Raft is refused")

    print("a lock whose time is up")
    check(c.execute_command("LOCK", "b", "alice", "300") > 0, "alice takes it for 300ms")
    check(c.execute_command("LOCK", "b", "bob", "60000") == 0, "bob can't yet")
    time.sleep(0.5)
    check(c.execute_command("LOCKINFO", "b") is None, "after 500ms nobody holds it")
    check(c.execute_command("LOCK", "b", "bob", "60000") > 0, "and bob takes it")

    print("eight clients contend for one lock")
    inside = [0]
    overlaps = [0]
    grants = []
    guard = threading.Lock()
    stop = threading.Event()

    def contender(w):
        cl = on_leader()
        me = "c%d" % w
        while not stop.is_set():
            token = cl.execute_command("LOCK", "hot", me, "5000")
            if not token:
                time.sleep(0.002)
                continue
            with guard:
                inside[0] += 1
                if inside[0] > 1:
                    overlaps[0] += 1
                grants.append(token)
            time.sleep(0.005)
            with guard:
                inside[0] -= 1
            cl.execute_command("UNLOCK", "hot", me)

    threads = [threading.Thread(target=contender, args=(w,)) for w in range(8)]
    for t in threads:
        t.start()
    time.sleep(scale.scaled_seconds(3.0))
    stop.set()
    for t in threads:
        t.join()
    check(len(grants) > 20, "the lock changed hands (%d grants)" % len(grants))
    check(overlaps[0] == 0, "never held by two at once (%d overlaps)" % overlaps[0])
    check(grants == sorted(grants) and len(set(grants)) == len(grants),
          "and every token is higher than the one before")

    print("the leader is killed")
    c = on_leader()
    before = c.execute_command("LOCK", "c", "alice", "60000")
    old = leader_of(nodes, SPACE)
    old.kill()
    check(wait_for(30, lambda: leader_of(nodes, SPACE) not in (None, old)), "another node leads")
    c = on_leader()
    check(wait_for(10, lambda: on_leader().execute_command("LOCKINFO", "c") is not None),
          "the new leader answers")
    check(c.execute_command("LOCKINFO", "c")[0] == "alice", "and still says alice holds it")
    check(c.execute_command("LOCK", "c", "bob", "60000") == 0, "so bob is refused")
    after = c.execute_command("LOCK", "c", "alice", "60000")
    check(after > before, "and alice's renewal has a higher token (%d > %d)" % (after, before))
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
