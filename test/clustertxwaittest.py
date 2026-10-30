#!/usr/bin/env python3
"""
Waiting, pushing and deadlock - TODO 696. See docs/TRANSACTIONS.md. INFO transactions
showing who holds what and who waits on whom is TODO 720.

A transaction whose prepare finds a key another one holds tries again for about 50 ms.
Then, if it's the older of the two (ids start with their BEGINTX time) or the other has
been pending a second at its home (a quiet coordinator), it pushes the other aside:
aborts it at its home, then where it's in the way, and tries once more. Otherwise it
answers TXCONFLICT.

Node 1 pauses three seconds between its prepares and its commit
(BARCH_TEST_TX_PAUSE_MS), so a transaction there plays a coordinator that has stopped
while holding its keys; the other two don't pause. On three nodes and a space split
over two groups, what has to hold:
  - two clients on two nodes writing the same 16 keys in opposite orders, 30
    transactions each, all commit within the time allowed: nothing hangs;
  - a younger transaction meeting the paused one's keys is refused at first, and run
    again commits once the paused one has been pending a second, well before its three
    seconds are up, and the paused one answers TXABORTED;
  - an older one meeting them commits within a fraction of a second, the same way;
  - a younger one meeting them while they're under a second old answers TXCONFLICT
    quickly, and the paused one then commits;
  - every member's digest is the same at the end.

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
from clusternodes import wait_for  # noqa: E402

scale.workdir()
BASE = scale.port(default=19180)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "stock"
LABELS = [SPACE, SPACE + "/1"]
KEYS = ["k%d" % i for i in range(16)]
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


nodes = [clusternodes.Node(i, BASE, BINARY, env={"BARCH_TEST_TX_PAUSE_MS": "3000"} if i == 0 else None)
         for i in range(3)]
paused, quick, other = nodes


def line_of(n, label):
    for l in n.info():
        if (" label %s" % label) in l:
            return l
    return ""


def leader_of_group(label):
    for n in nodes:
        try:
            if "(this node)" in line_of(n, label):
                return n
        except redis.RedisError:
            pass
    return None


def settled():
    def done():
        for l in LABELS:
            tops = [int(m.group(1)) for n in nodes for m in [re.search(r"\bapplied (\d+)", line_of(n, l))] if m]
            if len(tops) != len(nodes) or len(set(tops)) != 1:
                return False
        return True
    return wait_for(60, done)


def local(n):
    c = n.client(SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c


def same():
    return settled() and len(set(n.digest(SPACE)[0] for n in nodes)) == 1


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.exceptions.TryAgainError as e:
        return "TRYAGAIN " + str(e)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def writes(c, value, keys=KEYS):
    c.execute_command("BEGINTX")
    for k in keys:
        c.execute_command("SET", k, value)


def committed(c, value, keys):
    """a transaction setting `keys` to `value`, run again until it commits: (runs, conflicts)"""
    runs = conflicts = 0
    until = time.time() + 60
    while time.time() < until:
        runs += 1
        writes(c, value, keys)
        got = answer(c, "COMMITTX")
        if got == "OK":
            return runs, conflicts
        if "TXCONFLICT" in str(got):
            conflicts += 1
        elif not any(w in str(got) for w in ("TXABORTED", "TRYAGAIN", "UNKNOWN", "NOTLEADER")):
            raise RuntimeError("COMMITTX: %s" % got)
        time.sleep(0.01)
    raise RuntimeError("never committed")


def timed_commit(c):
    started = time.time()
    got = answer(c, "COMMITTX")
    return got, time.time() - started


def listed(n):
    """INFO transactions' line for each open transaction on n, as redis-py reads it - TODO 720"""
    info = n.client(SPACE).execute_command("INFO", "transactions")
    return [v for k, v in info.items() if k[:2] == "tx" and k[2:].isdigit()]


def background(f):
    out = []
    t = threading.Thread(target=lambda: out.append(f()), daemon=True)
    t.start()
    return t, out


try:
    print("three nodes and a space split over two groups")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    conf = nodes[0].client("configuration")
    conf.execute_command("SET", SPACE + ".raft_groups", "2")
    conf.execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: all("members 3" in line_of(n, l) and "learners 0" in line_of(n, l)
                                   for n in nodes for l in LABELS)
                   and all(leader_of_group(l) for l in LABELS)), "two groups, three voters each")
    # the leaders settle where they'll stay before the timings below
    time.sleep(5)

    print("the same keys in opposite orders, from two nodes")
    stats = {}

    def loop(n, name, keys):
        c = n.client(SPACE)
        runs = conflicts = 0
        for i in range(30):
            r, x = committed(c, "%s%d" % (name, i), keys)
            runs += r
            conflicts += x
        stats[name] = (runs, conflicts)

    started = time.time()
    ts = [threading.Thread(target=loop, args=(quick, "a", KEYS)),
          threading.Thread(target=loop, args=(other, "b", list(reversed(KEYS))))]
    [t.start() for t in ts]
    [t.join(120) for t in ts]
    took = time.time() - started
    check(not any(t.is_alive() for t in ts) and len(stats) == 2,
          "all 60 commit, in %.1f s (runs and client conflicts: %s)" % (took, stats))
    check(wait_for(30, lambda: same() and len(set(local(nodes[0]).execute_command("MGET", *KEYS))) == 1),
          "every member's the same, and every key from one transaction")

    print("a younger transaction meets a paused coordinator's keys")
    held = paused.client(SPACE)
    writes(held, "paused")
    t, out = background(lambda: timed_commit(held))
    time.sleep(0.3)                     # prepared, and pausing
    # INFO transactions on its node: it holds the keys, prepared in both groups - TODO 720
    rows = listed(paused)
    row = rows[0] if len(rows) == 1 else {}
    shown = str(row.get("held", "")).split("|")
    check(row.get("state") == "committing" and row.get("holds") == len(KEYS)
          and len(shown) == len(KEYS) and set(shown) <= set(KEYS),
          "INFO transactions: the paused one holds its %d keys (%s, %s)"
          % (len(KEYS), row.get("state"), row.get("holds")))
    paused_id = row.get("id")
    seen = []
    watching = [True]

    def watch():
        while watching[0]:
            seen.extend(listed(quick))
            time.sleep(0.01)
    w = threading.Thread(target=watch, daemon=True)
    w.start()
    c = quick.client(SPACE)
    began = time.time()
    first = None
    for _ in range(100):                # as a client would: again on TXCONFLICT
        writes(c, "younger")
        got, _ = timed_commit(c)
        first = first or got
        if got == "OK":
            break
        time.sleep(0.05)
    took = time.time() - began
    watching[0] = False
    w.join(10)
    waits = [r for r in seen if r.get("state") == "waiting"]
    check(waits and all(r.get("waiting_on") == paused_id and str(r.get("waiting", "")).startswith("a key in group")
                        for r in waits),
          "and the younger one is seen waiting on it (%d of %d looks, on %s)"
          % (len(waits), len(seen), waits[0].get("waiting_on") if waits else None))
    t.join(10)
    check("TXCONFLICT" in str(first), "it's refused at first, the paused one being older (%s)" % str(first)[:40])
    check(got == "OK" and 0.5 < took < 2.7,
          "run again, it commits once the paused one is a second old (%s in %.2f s)" % (got, took))
    check(out and "TXABORTED" in str(out[0][0]), "and the paused one answers TXABORTED (%s)" % out)
    check(wait_for(30, lambda: all(local(n).execute_command("MGET", *KEYS) == ["younger"] * 16 for n in nodes)),
          "every member holds the younger one")

    print("an older transaction meets them")
    older = quick.client(SPACE)
    older.execute_command("BEGINTX")    # its id has the earlier time
    time.sleep(0.05)
    held = paused.client(SPACE)
    writes(held, "paused")
    t, out = background(lambda: timed_commit(held))
    time.sleep(0.3)
    for k in KEYS:
        older.execute_command("SET", k, "older")
    got, took = timed_commit(older)
    t.join(10)
    check(got == "OK" and took < 0.8, "it commits at once (%s in %.2f s)" % (got, took))
    check(out and "TXABORTED" in str(out[0][0]), "and the paused one answers TXABORTED (%s)" % out)
    check(wait_for(30, lambda: all(local(n).execute_command("MGET", *KEYS) == ["older"] * 16 for n in nodes)),
          "every member holds the older one")

    print("a younger one meets them while they're under a second old")
    held = paused.client(SPACE)
    writes(held, "kept")
    t, out = background(lambda: timed_commit(held))
    time.sleep(0.2)
    c = other.client(SPACE)
    writes(c, "refused")
    got, took = timed_commit(c)
    t.join(10)
    check("TXCONFLICT" in str(got) and took < 0.7, "it answers TXCONFLICT quickly (%s in %.2f s)"
          % (str(got)[:40], took))
    check(out and out[0][0] == "OK", "and the paused one commits (%s)" % out)
    check(wait_for(30, lambda: all(local(n).execute_command("MGET", *KEYS) == ["kept"] * 16 for n in nodes)),
          "every member holds it")

    logs = "".join(open(n.dir + ".log", errors="replace").read() for n in nodes)
    check("pushed transaction" in logs, "the pushes are logged")
    check(same(), "every member's digest is the same")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
