#!/usr/bin/env python3
"""
A leader read the lease can't cover waits for a round of answers - TODO 785.

The lease (TODO 782) counts from when a request went out, and on a slow disk a
follower answers new entries only after its sync. So while a leader takes writes,
its lease is short by about a sync, and with syncs much over 150ms a read on a busy
leader got TRYAGAIN. Now such a read takes a read index instead (the Raft
dissertation, section 6.4): it notes the commit index, waits until a majority has
answered a request sent after the read came in, waits until that index is applied,
and then reads. No clock is involved, so it can't read stale, and it costs about a
sync instead of a refusal.

Every log sync here takes 700ms, on every node, and writers keep each space busy.
What has to hold:
  - readers on the leader, each writing a key of its own and reading it back, get
    every GET answered, with the value they wrote, and none get TRYAGAIN;
  - a pipelined SET and GET on the leader are both answered;
  - the same in a space split over two groups, where a GET is checked under its
    shard's latch, can't wait there, and runs again where it can: on the Raft pool
    if it came in alone, in place if it was queued there already;
  - CLUSTER INFO counts the confirmed reads.

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
BASE = scale.port(default=19420)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
SPLIT = "ledger"
SYNC_MS = 700
SECONDS = 8            # how long the readers run in each space
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def by_port(port):
    for n in nodes:
        if n.port == port:
            return n
    return None


class Readers:
    """
    Clients on whichever node leads their key, following NOTLEADER, each setting a
    key of its own and reading it back on the same connection. A GET has to answer
    what was just written; TRYAGAIN is what this is about, so it's counted, not
    retried quietly.
    """

    def __init__(self, space, count=4):
        self.space = space
        self.lock = threading.Lock()
        self.stopping = threading.Event()
        self.answered, self.wrong, self.waits = 0, 0, []
        self.errors = {}
        self.threads = [threading.Thread(target=self.run, args=(r,), daemon=True) for r in range(count)]

    def note(self, kind):
        with self.lock:
            self.errors[kind] = self.errors.get(kind, 0) + 1

    def run(self, r):
        key = "r%d" % r
        target = leader_of(nodes, self.space) or nodes[0]
        cl = None
        i = 0
        while not self.stopping.is_set():
            try:
                if cl is None:
                    cl = target.client(self.space)
                cl.execute_command("SET", key, str(i))
                began = time.time()
                got = cl.execute_command("GET", key)
                took = time.time() - began
                with self.lock:
                    self.answered += 1
                    self.waits.append(took)
                    if got != str(i):
                        self.wrong += 1
                i += 1
            except redis.exceptions.TryAgainError:
                self.note("TRYAGAIN")
                time.sleep(0.05)
            except redis.ResponseError as e:
                self.note(str(e).split(" ")[0])
                m = re.match(r"NOTLEADER 127\.0\.0\.1:(\d+)", str(e))
                if m and by_port(int(m.group(1))):
                    target = by_port(int(m.group(1)))
                    cl = None
                time.sleep(0.05)
            except (redis.ConnectionError, redis.TimeoutError) as e:
                self.note(type(e).__name__)
                cl = None
                time.sleep(0.1)

    def go(self, seconds):
        for t in self.threads:
            t.start()
        time.sleep(seconds)
        self.stopping.set()
        for t in self.threads:
            t.join()
        w = sorted(self.waits) or [0.0]
        return w[len(w) // 2], w[-1]


def readers_on(space, writers):
    w = clusternodes.Writers(nodes, space, count=4, prefix="w" + space)
    w.start()
    time.sleep(1)
    r = Readers(space)
    mid, top = r.go(SECONDS)
    w.stop()
    print("    %s: %d GETs, %d writes, GET %.2fs median %.2fs slowest, errors %s, writers %s"
          % (space, r.answered, len(w.acked), mid, top, r.errors, w.errors), flush=True)
    check(r.answered > 0 and not r.errors.get("TRYAGAIN"),
          "%s: leader GETs under writes are answered, no TRYAGAIN (%d)" % (space, r.errors.get("TRYAGAIN", 0)))
    check(r.wrong == 0, "%s: each with the value just written (%d weren't)" % (space, r.wrong))
    # a read waits for a round: about a sync, two if a round was already out. Not a
    # refusal's worth of client timeouts
    check(mid < 3.0, "%s: in about a sync (%.2fs median)" % (space, mid))


def piped(space):
    """
    SET, GET, GET in one write, to the node that leads the key's group (a NOTLEADER
    says which). The GETs queue behind the SET on the Raft pool, so each is checked
    off the I/O thread; in a split space, at its shard's latch first, where it can't
    wait, and then again where it can.
    """
    w = clusternodes.Writers(nodes, space, count=4, prefix="p" + space)
    w.start()
    time.sleep(1)
    got, at = None, leader_of(nodes, space) or nodes[0]
    for attempt in range(6):
        c = redis.Redis(host="127.0.0.1", port=at.port, decode_responses=True, socket_timeout=30)
        p = c.pipeline(transaction=False)
        p.execute_command("USE", space)
        p.execute_command("SET", "piped", str(attempt))
        p.execute_command("GET", "piped")
        p.execute_command("GET", "piped")
        got = p.execute(raise_on_error=False)
        c.close()
        m = [re.match(r"NOTLEADER 127\.0\.0\.1:(\d+)", str(a)) for a in got if isinstance(a, redis.ResponseError)]
        m = [x for x in m if x]
        if not m:
            break
        at = by_port(int(m[0].group(1))) or at
        time.sleep(0.2)
    w.stop()
    check(got is not None and got[1] is True and got[2] == got[3] == str(attempt),
          "%s: both answered, with the value (%s)" % (space, got and [str(a) for a in got]))


nodes = [clusternodes.Node(i, BASE, BINARY, env={"BARCH_TEST_LOG_SYNC_DELAY_MS": str(SYNC_MS)})
         for i in range(3)]
try:
    print("three nodes, a space and a space split over two groups, every log sync %dms" % SYNC_MS)
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    conf = nodes[0].client("configuration")
    conf.execute_command("SET", SPACE + ".raft", "on")
    conf.execute_command("SET", SPLIT + ".raft_groups", "2")
    conf.execute_command("SET", SPLIT + ".raft", "on")
    check(wait_for(120, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE)
        and "learners 0" in n.group_line("cluster")
        and sum(1 for l in n.info() if l.startswith("group ") and " label %s" % SPLIT in l) == 2
        and all("members 3" in l and "learners 0" in l for l in n.info()
                if l.startswith("group ") and " label %s" % SPLIT in l) for n in nodes)),
          "three voters in every group")
    if failures:
        for n in nodes:
            print("    node %d: %s" % (n.i + 1, [l for l in n.info() if l.startswith("group ")]), flush=True)
    time.sleep(2)

    print("readers on the leader while writers keep it busy")
    readers_on(SPACE, 4)

    print("a pipelined SET and GET on the leader, under writes")
    piped(SPACE)

    print("readers in the split space, on each key's leader")
    readers_on(SPLIT, 4)

    print("a pipelined SET and GET on the key's leader in the split space, under writes")
    piped(SPLIT)

    confirmed = sum(max(n.field(SPACE, "reads_confirmed"), 0) for n in nodes)
    check(confirmed > 0, "CLUSTER INFO counts the confirmed reads (%d)" % confirmed)
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
