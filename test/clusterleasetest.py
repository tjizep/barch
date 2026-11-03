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

To make that happen every time, the old leader is deaf while it's stopped and for a
moment after (BARCH_TEST_DEAF_FILE): it drops every request it's sent and sends none,
but still reads the answers already on their way. So it doesn't hear of the new
leader, and the late answers are all it has to go on. Each round:
  - the leader takes a write, which its followers log but can't answer yet: their
    log syncs wait on a file (BARCH_TEST_LOG_SYNC_HOLD_FILE);
  - once both have it, the leader is made deaf and stopped, and only then are the
    syncs let go, so the answers land in the stopped leader's socket;
  - the other two elect a new leader, which takes a write of its own;
  - the old leader resumes, and once it has read the late answers (it commits the
    held write) it's asked for the key the new leader's write changed, on a
    connection opened before the pause.
It must never answer with the old value. A round counts once the old leader still
thought it led: it refused with TRYAGAIN, or (TODO 785) a read it couldn't cover
with its lease asked for a read index, which no quorum confirmed (its CLUSTER INFO
reads_unconfirmed went up). A round where the leader found out before the read is
run again.

The old leader never steps down by its own clock here (BARCH_TEST_STEP_DOWN_MS=-1,
TODO 815). Otherwise a pause longer than the step-down time (1.6s) made it give up
leading the moment it resumed, before it was asked, and how long the pause takes is
up to how fast the other two elect a leader: 0.4 to 2.4s under 700ms jitter, so no
round counted. Deaf, it can only hear of a newer term in an answer already on its
way (then it says NOTLEADER, and the round doesn't count unless a read index went
unconfirmed first). Otherwise its read index gets nobody's answer and the read is
refused with TRYAGAIN once the client timeout runs out. One read is enough. It finds
out it lost once it can hear again, after the read.

Before the stop, all three have to name the old leader in the term it took the
round's first write in (TODO 815). The followers' log_appended going up is what says
they have the held write, but under jitter the elections churn, and a new leader's
first entry raised it just as well. The followers can't move to a newer term while
their syncs are held, since they're waiting inside NuRaft's append, under its lock.

There's no sleep and no time window here - TODO 813. A write can land as leadership
moves (a loaded machine can miss an election timeout), so every write is tried
again on the leader of the moment until it's known to have committed, and a round
whose leader moved before the stop is run again.

Ports are BARCH_TEST_PORT and the 15 after it.
"""
import os
import socket
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of, on_leader  # noqa: E402

scale.workdir()
BASE = scale.port(default=19380)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
GROUP = "1"            # the first replicated space's group
ROUNDS = 20            # tries at a round that counts; it stops at 3 counted
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def deaf_file(i):
    return os.path.join(os.getcwd(), "deaf%d" % (i + 1))


def hold_file(i):
    return os.path.join(os.getcwd(), "hold%d" % (i + 1))


def touch(f):
    open(f, "w").close()


def write(among, key, value):
    """SET key on whichever of `among` leads, until one says it committed; that node"""
    def f(n):
        n.client(SPACE).execute_command("SET", key, value)
        return n
    return on_leader(among, SPACE, f)


def join(n):
    """JOIN, asked again while it answers UNKNOWN. Under jitter the cluster group's
    leader can change while the JOIN's own writes commit. Asking again is safe:
    before the node starts its group it just asks again, and after that it's
    refused as "already in a cluster", and the wait for three voters settles it."""
    try:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
        return True
    except redis.ResponseError as e:
        if "already in a cluster" in str(e):
            return True
        if str(e).startswith("UNKNOWN"):
            return False
        raise


def settled():
    """all three agree on the space's leader and term, and have applied everything"""
    lines = [n.group_line(SPACE) for n in nodes]
    if not all(lines):
        return False
    fields = [{k: line.split()[line.split().index(k) + 1] for k in ("leader", "term", "committed", "applied")}
              for line in lines]
    return (leader_of(nodes, SPACE) is not None
            and len({(f["leader"], f["term"], f["committed"]) for f in fields}) == 1
            and all(f["applied"] == f["committed"] for f in fields))


def resp(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a.encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


for i in range(3):
    for f in (deaf_file(i), hold_file(i)):
        if os.path.exists(f):
            os.remove(f)
nodes = [clusternodes.Node(i, BASE, BINARY, env={
    "BARCH_TEST_LOG_SYNC_HOLD_FILE": hold_file(i), "BARCH_TEST_LOG_SYNC_HOLD_GROUP": GROUP,
    "BARCH_TEST_DEAF_FILE": deaf_file(i), "BARCH_TEST_DEAF_GROUP": GROUP,
    "BARCH_TEST_STEP_DOWN_MS": "-1", "BARCH_TEST_STEP_DOWN_GROUP": GROUP}) for i in range(3)]
try:
    print("a replicated space on three nodes")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        check(wait_for(120, lambda: join(n)), "node %d joined" % n.i)
    check(wait_for(60, lambda: nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")),
          "the space is replicated")
    check(wait_for(60, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE)
        and "learners 0" in n.group_line("cluster") for n in nodes)), "three voters")
    check(nodes[0].group_line(SPACE).startswith("group %s " % GROUP),
          "the space is group %s (%s)" % (GROUP, nodes[0].group_line(SPACE)[:20]))
    check(wait_for(60, settled), "the three agree")

    print("the leader stopped with answers on their way")
    counted, stale = 0, 0
    for r in range(ROUNDS):
        old = write(nodes, "lease", "old%d" % r)
        if old is None:
            check(False, "a leader took the round's first write")
            break
        others = [n for n in nodes if n is not old]
        term = old.field(SPACE, "term")
        unconfirmed = max(old.field(SPACE, "reads_unconfirmed"), 0)
        held = old.client(SPACE)            # opened before the pause, used after it
        logged = [n.field(SPACE, "log_appended") for n in others]
        committed = old.field(SPACE, "committed")
        for n in others:
            touch(hold_file(n.i))
        # a write the followers log and can't answer; its own reply isn't waited for
        w = socket.create_connection(("127.0.0.1", old.port))
        w.sendall(resp("USE", SPACE) + resp("SET", "pad", str(r)))
        got = wait_for(30, lambda: all(n.field(SPACE, "log_appended") > a for n, a in zip(others, logged)))
        # it still leads, and the followers' new entry is its own: they're in its term
        still = old.leads(SPACE) and all(
            (n.field(SPACE, "leader"), n.field(SPACE, "term")) == (old.field(SPACE, "leader"), term) for n in nodes)
        touch(deaf_file(old.i))
        old.pause()
        try:
            for n in others:
                os.remove(hold_file(n.i))
            new = write(others, "lease", "new%d" % r) if got and still else None
        finally:
            old.resume()
        # it commits the held write once it reads the late answers, and only then is
        # it asked: the answers are what an old lease counted from
        late = new is not None and wait_for(30, lambda: old.field(SPACE, "committed") > committed)
        answers = {}
        if late:
            # once: it can't find out it lost while it's deaf, so a TRYAGAIN would
            # only be followed by more of them
            try:
                a = str(held.execute_command("GET", "lease"))
            except redis.exceptions.TryAgainError:
                a = "TRYAGAIN"
            except redis.ResponseError as e:
                a = str(e).split(" ")[0]
            answers[a] = 1
        os.remove(deaf_file(old.i))
        w.close()
        held.close()
        wait_for(30, lambda: old.field(SPACE, "term") > term)
        olds = answers.get("old%d" % r, 0)
        stale += olds
        refused = max(old.field(SPACE, "reads_unconfirmed"), 0) - unconfirmed
        print("    round %d: followers had the write %s, it still led %s, new leader %s, late answers read %s,"
              " answers %s, unconfirmed reads %d" % (r, got, still, new is not None, late, answers, refused), flush=True)
        if late and (olds or answers.get("TRYAGAIN") or refused > 0):
            counted += 1
        if counted >= 3:
            break
        check(wait_for(60, settled), "the three agree again")
    check(counted > 0, "a round where the old leader still thought it led (%d)" % counted)
    check(stale == 0, "it never answered with the value from before the new write (%d did)" % stale)
finally:
    for n in nodes:
        for f in (deaf_file(n.i), hold_file(n.i)):
            if os.path.exists(f):
                os.remove(f)
        if n.proc:
            n.resume()
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
