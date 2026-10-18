#!/usr/bin/env python3
"""
Transactions in one group, writes only - TODO 693. See docs/TRANSACTIONS.md.

BEGINTX gathers a connection's writes without making them; COMMITTX puts them in the
space's group as a transaction record with an intent per key (prepare), then makes them
all at once (commit), under a gate every read command holds. A key held by another
transaction's intent refuses a second one. A transaction prepared and never committed
(its coordinator gone) is aborted by the group's leader after five seconds.

The nodes pause one second between prepare and commit (BARCH_TEST_TX_PAUSE_MS), so the
window is easy to hit. What has to hold, on three nodes and one replicated space:
  - a transaction's writes are on every member after COMMITTX, with matching digests;
  - a reader running MGET over the keys throughout, on the leader and with follower
    reads, never sees some of a transaction's writes without the rest;
  - ABORTTX, and a connection closed in a transaction, leave nothing;
  - reads inside a transaction see its own writes, and a command it doesn't take
    (HSET) is refused, saying why;
  - a second transaction on a key the first holds answers TXCONFLICT;
  - the leader killed between prepare and commit: the transaction is aborted on every
    member, its keys unchanged, and a new transaction on them commits.

Ports are BARCH_TEST_PORT and the 19 after it.
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
BASE = scale.port(default=35900)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "accounts"
KEYS = ["k%d" % i for i in range(10)]
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def local(n):
    c = n.client(SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c


def transaction(c, value):
    """BEGINTX, SET every key to `value`, COMMITTX: COMMITTX's answer"""
    c.execute_command("BEGINTX")
    for k in KEYS:
        c.execute_command("SET", k, value)
    return answer(c, "COMMITTX")


def settled(members):
    l = leader_of(members, SPACE)
    if l is None:
        return False
    top = l.field(SPACE, "applied")
    return wait_for(60, lambda: all(n.field(SPACE, "applied") >= top for n in members))


def values(n):
    return local(n).execute_command("MGET", *KEYS)


nodes = [clusternodes.Node(i, BASE, BINARY, env={"BARCH_TEST_TX_PAUSE_MS": "1000"}) for i in range(3)]
stop = threading.Event()
try:
    print("three nodes and a replicated space")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE) for n in nodes)),
        "three voters and a leader")
    leader = leader_of(nodes, SPACE)

    print("a transaction")
    c = leader.client(SPACE)
    check(transaction(c, "v1") == "OK", "COMMITTX answers OK")
    check(settled(nodes) and all(values(n) == ["v1"] * 10 for n in nodes), "every member holds its writes")
    check(len(set(n.digest(SPACE)[0] for n in nodes)) == 1, "with matching digests")

    print("a reader throughout five more")
    seen = []
    mixed = []

    def reader(n):
        while not stop.is_set():
            try:
                got = values(n)
            except redis.RedisError:
                continue
            seen.append(1)
            if len(set(got)) != 1:
                mixed.append(got)

    rs = [threading.Thread(target=reader, args=(n,), daemon=True) for n in nodes]
    [t.start() for t in rs]
    got = [transaction(c, "t%d" % i) for i in range(5)]
    stop.set()
    [t.join() for t in rs]
    check(got == ["OK"] * 5, "each answers OK (%s)" % got)
    check(not mixed and len(seen) > 50, "no read saw part of one (%d reads, %s)" % (len(seen), mixed[:2]))
    check(settled(nodes) and all(values(n) == ["t4"] * 10 for n in nodes), "every member holds the last")

    print("ABORTTX, and a connection closed in a transaction")
    c.execute_command("BEGINTX")
    c.execute_command("SET", "k0", "aborted")
    check(answer(c, "ABORTTX") == "OK", "ABORTTX answers OK")
    gone = leader.client(SPACE)
    gone.execute_command("BEGINTX")
    gone.execute_command("SET", "k1", "never")
    gone.close()
    check(settled(nodes) and all(values(n) == ["t4"] * 10 for n in nodes), "and nothing of either is anywhere")

    print("reads in a transaction see its own writes (TODO 695)")
    c.execute_command("BEGINTX")
    got = [answer(c, "GET", "k0"), answer(c, "SET", "k0", "mine"), answer(c, "GET", "k0"),
           answer(c, "INCR", "n"), answer(c, "INCRBY", "n", "4"), answer(c, "GET", "n")]
    c.execute_command("ABORTTX")
    check(got == ["t4", True, "mine", 1, 5, "5"], "GET, SET, GET, INCR, INCRBY, GET (%s)" % got)
    check(settled(nodes) and all(local(n).execute_command("MGET", "k0", "n") == ["t4", None] for n in nodes),
          "and ABORTTX leaves none of it")

    print("what a transaction doesn't take yet")
    c.execute_command("BEGINTX")
    got = answer(c, "HSET", "h", "f", "v")
    c.execute_command("ABORTTX")
    check(str(got).startswith("ERR") and "isn't taken in a transaction" in str(got),
          "HSET is refused, saying why (%s)" % str(got)[:50])

    print("two transactions on one key")
    out = []

    def first():
        out.append(transaction(leader.client(SPACE), "first"))

    t = threading.Thread(target=first)
    t.start()
    time.sleep(0.5)                     # the first is prepared, and pausing before its commit
    second = transaction(leader.client(SPACE), "second")
    t.join()
    check(out == ["OK"] and "TXCONFLICT" in str(second),
          "the first commits, the second answers TXCONFLICT (%s, %s)" % (out, str(second)[:60]))

    print("the leader killed between prepare and commit")
    before = values(leader)
    out = []
    def lost():
        try:
            out.append(transaction(leader.client(SPACE), "lost"))
        except redis.ConnectionError:
            out.append("gone")       # the leader it was on is killed under it

    t = threading.Thread(target=lost)
    t.start()
    time.sleep(0.5)
    leader.kill()
    t.join(30)
    rest = [n for n in nodes if n is not leader]
    check(wait_for(60, lambda: leader_of(rest, SPACE) is not None), "the other two elect a leader")
    new = leader_of(rest, SPACE)
    # its transaction is aborted once it has been pending five seconds, which frees the keys
    got = None
    for _ in range(60):
        got = transaction(new.client(SPACE), "after")
        if got == "OK":
            break
        time.sleep(0.5)
    check(got == "OK", "a new transaction on the same keys commits once it's aborted (%s)" % str(got)[:60])
    logs = "".join(open(n.dir + ".log", errors="replace").read() for n in rest)
    check("aborted transaction" in logs or "pushed transaction" in logs, "the one left prepared was aborted, or pushed aside (TODO 696)")
    leader.start()
    check(settled(nodes) and all(values(n) == ["after"] * 10 for n in nodes),
          "every member holds the new one and none of the lost one (was %s)" % before[0])
    check(len(set(n.digest(SPACE)[0] for n in nodes)) == 1, "with matching digests")
finally:
    stop.set()
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
