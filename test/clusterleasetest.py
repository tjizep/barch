#!/usr/bin/env python3
"""
A leader's lease counts from when it sent, not from when it was answered - TODO 782.

A follower that takes a request from the leader won't help elect anyone else for an
election timeout (400ms at least) after it got it. The lease (300ms) has to end
before that, so it has to count from when the request went out. It counted from
when the answer came in. An answer can come in late: the follower's disk is slow,
or the leader was stopped and the answer sat in its socket. Then the lease outlived
the followers' promise, a new leader could be elected and take a write, and the old
one still answered reads with the value from before it. TestClusterRoute hit that
once under TSan.

To make that happen every time, the followers' log syncs take 150ms, and the old
leader is deaf while it's stopped and for a moment after (BARCH_TEST_DEAF_FILE): it
drops every request it's sent and sends none, but still reads the answers already
on their way. So it doesn't hear of the new leader, and the late answers are all it
has to go on. Each round:
  - the leader takes a write and is stopped 50ms later, before the followers'
    answers to it come in;
  - the other two elect a new leader, which takes a write of its own;
  - the old leader resumes, and for half a second it's asked for the key that
    write changed, on a connection opened before the pause.
It must never answer with the old value. A round counts once the old leader refused
with TRYAGAIN, so it still thought it led; one that said NOTLEADER found out too
soon and is run again.

Ports are BARCH_TEST_PORT and the 15 after it.
"""
import os
import socket
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=37000)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
GROUP = "1"            # the first replicated space's group
ROUNDS = 6             # tries at a round that counts
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def deaf_file(i):
    return os.path.join(os.getcwd(), "deaf%d" % (i + 1))


def resp(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a.encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


for i in range(3):
    if os.path.exists(deaf_file(i)):
        os.remove(deaf_file(i))
nodes = [clusternodes.Node(i, BASE, BINARY, env={
    "BARCH_TEST_LOG_SYNC_DELAY_MS": "150", "BARCH_TEST_LOG_SYNC_DELAY_GROUP": GROUP,
    "BARCH_TEST_DEAF_FILE": deaf_file(i), "BARCH_TEST_DEAF_GROUP": GROUP}) for i in range(3)]
try:
    print("a replicated space on three nodes, every log sync 150ms")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(60, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE)
        and "learners 0" in n.group_line("cluster") for n in nodes)), "three voters")
    check(nodes[0].group_line(SPACE).startswith("group %s " % GROUP),
          "the space is group %s (%s)" % (GROUP, nodes[0].group_line(SPACE)[:20]))
    time.sleep(2)

    print("the leader stopped with answers on their way")
    counted, stale = 0, 0
    for r in range(ROUNDS):
        old = leader_of(nodes, SPACE)
        if old is None:
            wait_for(30, lambda: leader_of(nodes, SPACE) is not None)
            continue
        others = [n for n in nodes if n is not old]
        term = old.field(SPACE, "term")
        held = old.client(SPACE)            # opened before the pause, used after it
        held.execute_command("SET", "lease", "old%d" % r)
        # a write the followers answer 150ms late; its own reply isn't waited for
        w = socket.create_connection(("127.0.0.1", old.port))
        w.sendall(resp("USE", SPACE) + resp("SET", "pad", str(r)))
        time.sleep(0.05)
        open(deaf_file(old.i), "w").close()
        old.pause()
        paused = time.time()
        try:
            got_new = wait_for(30, lambda: (lambda l: l is not None and l is not old)(leader_of(others, SPACE)))
            new = leader_of(others, SPACE)
            new.client(SPACE).execute_command("SET", "lease", "new%d" % r)
            took = time.time() - paused
        finally:
            old.resume()
        answers = {}
        end = time.time() + 0.5
        while time.time() < end:
            try:
                a = str(held.execute_command("GET", "lease"))
            except redis.exceptions.TryAgainError:
                a = "TRYAGAIN"
            except redis.ResponseError as e:
                a = str(e).split(" ")[0]
            answers[a] = answers.get(a, 0) + 1
        os.remove(deaf_file(old.i))
        w.close()
        wait_for(30, lambda: old.field(SPACE, "term") > term)
        olds = answers.get("old%d" % r, 0)
        stale += olds
        print("    round %d: stopped %.2fs, new leader %s, answers %s" % (r, took, got_new, answers), flush=True)
        if olds or answers.get("TRYAGAIN"):
            counted += 1
        if counted >= 3:
            break
        time.sleep(1)
    check(counted > 0, "a round where the old leader still thought it led (%d)" % counted)
    check(stale == 0, "it never answered with the value from before the new write (%d did)" % stale)
finally:
    for n in nodes:
        if os.path.exists(deaf_file(n.i)):
            os.remove(deaf_file(n.i))
        if n.proc:
            n.resume()
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
