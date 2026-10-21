#!/usr/bin/env python3
"""
Reads in a transaction, and conflicts - TODO 695. See docs/TRANSACTIONS.md.

Inside BEGINTX a read sees the transaction's own writes first, then the store, and each
key read from the store is kept with a hash of what it held. COMMITTX's prepare checks
every one is still as it was (the answer goes to the node that wrote it, which aborts
on a change), and holds the keys read as shared locks until the commit, so no other
transaction writes them in between. That's choice 3 of the design: serializable.

What has to hold, on three nodes and a space split over two groups (so transactions
run on any node, and keys are in both groups):
  - two clients on two nodes, each running INCR on one key in a transaction 40 times,
    running each again on TXCONFLICT: the key ends at 80, so no increment is lost;
  - the same with INCRBY over 16 keys at once in each transaction: every key ends at
    the sum of what was added;
  - write skew: two transactions each read all of 16 keys and, if none is set, set
    their own; both read before either commits. Serializable lets at most one of them
    commit, so after each round at most one key is set - over 20 rounds;
  - a read-only transaction whose key changes before COMMITTX answers TXCONFLICT;
  - a COMMITTX that answers UNKNOWN (a leader handed over as the cluster balances its
    groups after a start) may have landed, so one run again after it may add twice:
    the sums allow one more step for each;
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
BASE = scale.port(default=36100)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "counts"
LABELS = [SPACE, SPACE + "/1"]
KEYS = ["k%d" % i for i in range(16)]
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]


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


# COMMITTXs that answered UNKNOWN, by what they added: a leader handing over (the cluster
# balances its groups' leaders after a start) cancels one that may have landed all the same
unknown = {}


def committed(c, body, adds=0):
    """BEGINTX, body(c), COMMITTX, again on a conflict: how many runs it took"""
    said = {}
    until = time.time() + 60
    run = 0
    while time.time() < until:
        run += 1
        c.execute_command("BEGINTX")
        body(c)
        got = answer(c, "COMMITTX")
        if got == "OK":
            return run
        said[str(got)[:60]] = said.get(str(got)[:60], 0) + 1
        if "UNKNOWN" in str(got):
            unknown[adds] = unknown.get(adds, 0) + 1
        time.sleep(0.02 * min(run, 10))     # a leader moving holds keys for a moment
        if not any(w in str(got) for w in ("TXCONFLICT", "TXABORTED", "TRYAGAIN", "UNKNOWN", "NOTLEADER")):
            raise RuntimeError("COMMITTX: %s" % got)
    raise RuntimeError("never committed: %s" % said)


def in_parallel(*fs):
    out = [None] * len(fs)
    errs = []

    def run(i, f):
        try:
            out[i] = f()
        except Exception as e:      # noqa: BLE001 - reported below
            errs.append(e)
    ts = [threading.Thread(target=run, args=(i, f)) for i, f in enumerate(fs)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    if errs:
        raise errs[0]
    return out


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

    print("INCR on one key from two nodes")

    def incr_loop(n):
        c = n.client(SPACE)
        runs = 0
        for _ in range(40):
            runs += committed(c, lambda c: c.execute_command("INCR", "counter"), adds=1)
        return runs

    runs = in_parallel(lambda: incr_loop(nodes[0]), lambda: incr_loop(nodes[1]))
    # one that answered UNKNOWN and ran again may have landed twice
    most = 80 + unknown.get(1, 0)

    def counter_ok():
        got = [local(n).execute_command("GET", "counter") for n in nodes]
        return len(set(got)) == 1 and 80 <= int(got[0]) <= most
    check(wait_for(30, counter_ok),
          "the key ends at 80 on every member (%s, %d runs for 80, %d unknown)"
          % ([local(n).execute_command("GET", "counter") for n in nodes], sum(runs), most - 80))
    final = local(nodes[0]).execute_command("GET", "counter")
    check(sum(runs) > 80, "and some ran again on a conflict (%d runs)" % sum(runs))

    print("INCRBY over 16 keys at once from two nodes")

    def incrby_loop(n, by):
        c = n.client(SPACE)
        for _ in range(15):
            committed(c, lambda c: [c.execute_command("INCRBY", k, by) for k in KEYS], adds=by)

    in_parallel(lambda: incrby_loop(nodes[1], 1), lambda: incrby_loop(nodes[2], 2))
    extra = unknown.get(2, 0) * 2 + (unknown.get(1, 0) - (most - 80))

    def sums_ok():
        got = [local(n).execute_command("MGET", *KEYS) for n in nodes]
        return (all(g == got[0] for g in got) and len(set(got[0])) == 1
                and 45 <= int(got[0][0]) <= 45 + extra)
    check(wait_for(30, sums_ok),
          "every key ends at 45 on every member (%s)" % local(nodes[0]).execute_command("MGET", *KEYS)[:4])

    print("write skew: each reads every key, and sets its own if none is set")
    skewed = 0
    for r in range(20):
        flags = ["f%d_%d" % (r, i) for i in range(16)]
        a, b = nodes[0].client(SPACE), nodes[2].client(SPACE)
        for c, mine in ((a, flags[0]), (b, flags[15])):
            c.execute_command("BEGINTX")
            if all(v is None for v in c.execute_command("MGET", *flags)):
                c.execute_command("SET", mine, "1")
        got = in_parallel(lambda: answer(a, "COMMITTX"), lambda: answer(b, "COMMITTX"))
        if not wait_for(30, lambda: all(len([v for v in local(n).execute_command("MGET", *flags) if v]) <= 1
                                        for n in nodes)) or got.count("OK") > 1:
            skewed += 1
    check(skewed == 0, "no round ends with two set (%d of 20 did)" % skewed)

    print("a read-only transaction whose key changes before COMMITTX")
    c = nodes[0].client(SPACE)
    c.execute_command("BEGINTX")
    seen = c.execute_command("GET", "counter")
    other = nodes[1].client(SPACE)
    other.execute_command("SET", "counter", "changed")
    got = answer(c, "COMMITTX")
    check(seen == final and "TXCONFLICT" in str(got), "COMMITTX answers TXCONFLICT (%s)" % str(got)[:60])
    c.execute_command("BEGINTX")
    seen = c.execute_command("GET", "counter")
    got = answer(c, "COMMITTX")
    check(seen == "changed" and got == "OK", "and run again it commits (%s, %s)" % (seen, got))

    check(same(), "every member's digest is the same")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
