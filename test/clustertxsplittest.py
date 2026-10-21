#!/usr/bin/env python3
"""
Transactions over a space split into two groups - TODO 694. See docs/TRANSACTIONS.md.

COMMITTX prepares every group with a write in the transaction, its home (the lowest
numbered) first, then commits the home, which decides it, then the rest. A group whose
transaction stays pending five seconds asks the home's leader: committed there, it
commits too (a roll forward); aborted, or never prepared there, it aborts. While a
node has a transaction committed in some groups and not the rest, reads of the space
wait for it.

The nodes pause one second between the prepares and the home's commit
(BARCH_TEST_TX_PAUSE_MS), and one more before the other groups' commits
(BARCH_TEST_TX_SPREAD_PAUSE_MS), so each window is easy to hit. What has to hold, on
three nodes and a space split over two groups (16 keys, over both):
  - a transaction's writes are on every member, with matching digests;
  - readers running MGET over all the keys on every node, during five more, never see
    some of a transaction's writes without the rest;
  - a second transaction on keys the first holds answers TXCONFLICT;
  - MULTI, SET of every key, EXEC runs as one transaction (TODO 710): readers on every
    node never see part of one, and every member holds the last;
  - the coordinator killed after the prepares, before the home's commit: aborted
    everywhere, the keys as they were, and a new transaction on them commits;
  - the coordinator killed after the home's commit, before the rest: committed
    everywhere (rolled forward), readers on the other two never seeing a mix;
  - after each, once the killed node is back, every member's digest is the same;
  - CLUSTER SPLIT of a group while a transaction is prepared with keys in it answers
    TRYAGAIN, and goes through once the transaction is in, with nothing lost.

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
BASE = scale.port(default=36000)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "ledger"
LABELS = [SPACE, SPACE + "/1"]
KEYS = ["k%d" % i for i in range(16)]
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


ENV = {"BARCH_TEST_TX_PAUSE_MS": "1000", "BARCH_TEST_TX_SPREAD_PAUSE_MS": "1000"}
nodes = [clusternodes.Node(i, BASE, BINARY, env=ENV) for i in range(3)]


def line_of(n, label):
    for l in n.info():
        if (" label %s" % label) in l:
            return l
    return ""


def leader_of_group(label, among=None):
    for n in among or nodes:
        if n.proc:
            try:
                if "(this node)" in line_of(n, label):
                    return n
            except redis.RedisError:
                pass
    return None


def settled(members=None):
    members = members or nodes

    def done():
        for l in LABELS:
            tops = [int(m.group(1)) for n in members for m in [re.search(r"\bapplied (\d+)", line_of(n, l))] if m]
            if len(tops) != len(members) or len(set(tops)) != 1:
                return False
        return True
    return wait_for(60, done)


def local(n):
    c = n.client(SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c


def values(n):
    return local(n).execute_command("MGET", *KEYS)


def same():
    return settled() and len(set(n.digest(SPACE)[0] for n in nodes)) == 1


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.exceptions.TryAgainError as e:
        return "TRYAGAIN " + str(e)     # redis-py takes the TRYAGAIN off the message
    except redis.ResponseError as e:
        return "ERR " + str(e)


def transaction(n, value):
    """BEGINTX, SET every key to `value`, COMMITTX on node `n`: COMMITTX's answer"""
    c = n.client(SPACE)
    c.execute_command("BEGINTX")
    for k in KEYS:
        c.execute_command("SET", k, value)
    return answer(c, "COMMITTX")


def exec_all(n, value):
    """MULTI, SET every key to `value`, EXEC on node `n`: EXEC's answer"""
    c = n.client(SPACE)
    c.execute_command("MULTI")
    for k in KEYS:
        c.execute_command("SET", k, value)
    return answer(c, "EXEC")


def until_committed(n, value):
    got = None
    for _ in range(60):
        try:
            got = transaction(n, value)
        except redis.RedisError as e:
            got = "ERR " + str(e)
        if got == "OK":
            break
        time.sleep(0.5)
    return got


class Readers:
    """MGET over every key on each of `members`, until stopped; any mix is kept"""

    def __init__(self, members):
        self.stop = threading.Event()
        self.seen = 0
        self.mixed = []
        self.threads = [threading.Thread(target=self.read, args=(n,), daemon=True) for n in members]
        [t.start() for t in self.threads]

    def read(self, n):
        while not self.stop.is_set():
            try:
                got = values(n)
            except redis.RedisError:
                time.sleep(0.01)
                continue
            self.seen += 1
            if len(set(got)) != 1:
                self.mixed.append(got)

    def done(self):
        self.stop.set()
        [t.join() for t in self.threads]
        return self


def killed_during(coordinator, value, at):
    """a transaction on `coordinator`, which is killed `at` seconds in"""
    out = []

    def run():
        try:
            out.append(transaction(coordinator, value))
        except redis.ConnectionError:
            out.append("gone")

    t = threading.Thread(target=run, daemon=True)
    t.start()
    time.sleep(at)
    coordinator.kill()
    t.join(30)
    return out


readers = None
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

    print("a transaction over both")
    check(transaction(nodes[1], "v1") == "OK", "COMMITTX answers OK")
    check(wait_for(30, lambda: same() and all(values(n) == ["v1"] * 16 for n in nodes)),
          "every member holds its writes, with matching digests")

    print("readers throughout five more")
    readers = Readers(nodes)
    got = [transaction(nodes[i % 3], "t%d" % i) for i in range(5)]
    r = readers.done()
    readers = None
    check(got == ["OK"] * 5, "each answers OK (%s)" % [str(g)[:40] for g in got])
    check(not r.mixed and r.seen > 50, "no read saw part of one (%d reads, %s)" % (r.seen, r.mixed[:2]))
    check(wait_for(30, lambda: same() and all(values(n) == ["t4"] * 16 for n in nodes)),
          "every member holds the last")

    print("EXEC over both, with readers")
    readers = Readers(nodes)
    got = [exec_all(nodes[i % 3], "e%d" % i) for i in range(3)]
    r = readers.done()
    readers = None
    check(all(isinstance(g, list) and len(g) == 16 for g in got),
          "each answers its 16 replies (%s)" % [str(g)[:40] for g in got])
    check(not r.mixed and r.seen > 30, "no read saw part of one (%d reads, %s)" % (r.seen, r.mixed[:2]))
    check(wait_for(30, lambda: same() and all(values(n) == ["e2"] * 16 for n in nodes)),
          "every member holds the last")

    print("two transactions on the same keys")
    out = []
    t = threading.Thread(target=lambda: out.append(transaction(nodes[0], "first")))
    t.start()
    time.sleep(0.5)                     # the first is prepared, and pausing before its commit
    second = transaction(nodes[2], "second")
    t.join()
    check(out == ["OK"] and "TXCONFLICT" in str(second),
          "the first commits, the second answers TXCONFLICT (%s, %s)" % (out, str(second)[:60]))
    check(wait_for(30, lambda: same() and all(values(n) == ["first"] * 16 for n in nodes)),
          "every member holds the first")

    print("the coordinator killed after the prepares, before the home's commit")
    victim = nodes[1]
    rest = [n for n in nodes if n is not victim]
    out = killed_during(victim, "lost", 0.5)
    check(wait_for(60, lambda: all(leader_of_group(l, rest) for l in LABELS)), "both groups have a leader")
    got = until_committed(rest[0], "after")
    check(got == "OK", "a new transaction on the same keys commits once it's aborted (%s)" % str(got)[:60])
    logs = "".join(open(n.dir + ".log", errors="replace").read() for n in rest)
    check("aborted transaction" in logs or "pushed transaction" in logs, "the one left prepared was aborted, or pushed aside (TODO 696)")
    victim.start()
    check(wait_for(60, lambda: same() and all(values(n) == ["after"] * 16 for n in nodes)),
          "every member holds the new one and none of the lost one")

    print("the coordinator killed after the home's commit, before the rest")
    victim = nodes[2]
    rest = [n for n in nodes if n is not victim]
    # the node back from the last part can leave a group's leader unsure of its lease for a moment
    got = until_committed(victim, "warm")
    check(got == "OK", "a transaction on the node to be killed commits first (%s)" % str(got)[:60])
    readers = Readers(rest)
    out = killed_during(victim, "rolled", 1.6)
    # the other group's leader asks the home after five seconds, and commits it there
    check(wait_for(60, lambda: all(values(n) == ["rolled"] * 16 for n in rest)),
          "the other two hold all of it (%s)" % out)
    r = readers.done()
    readers = None
    check(not r.mixed and r.seen > 20, "no read on them saw part of it (%d reads, %s)" % (r.seen, r.mixed[:2]))
    logs = "".join(open(n.dir + ".log", errors="replace").read() for n in rest)
    check("committed transaction" in logs, "a group's leader rolled it forward from its home")
    victim.start()
    check(wait_for(60, lambda: same() and all(values(n) == ["rolled"] * 16 for n in nodes)),
          "every member holds it, with matching digests")

    print("a group split while a transaction holds keys in it")
    cl = leader_of_group("cluster") or nodes[0]
    out = []
    t = threading.Thread(target=lambda: out.append(until_committed(nodes[0], "split")))
    t.start()
    said = None
    for _ in range(40):                 # until it lands in the window between prepare and commit
        time.sleep(0.1)
        said = answer(cl.client(), "CLUSTER", "SPLIT", SPACE + "/1")
        if "transaction" in str(said) or str(said).startswith("OK"):
            break
    check("TRYAGAIN" in str(said) and "transaction" in str(said),
          "SPLIT while it's prepared answers TRYAGAIN (%s)" % str(said)[:70])
    t.join()
    check(out == ["OK"], "the transaction commits (%s)" % out)
    for _ in range(40):
        if str(said).startswith("OK"):
            break
        said = answer(cl.client(), "CLUSTER", "SPLIT", SPACE + "/1")
        time.sleep(0.5)
    check(str(said).startswith("OK"), "then SPLIT goes through (%s)" % str(said)[:60])
    m = re.match(r"OK (\S+)", str(said))
    if m:
        LABELS.append(m.group(1))
    check(wait_for(60, lambda: same() and all(values(n) == ["split"] * 16 for n in nodes)),
          "every member holds the transaction's writes over three groups")
finally:
    if readers:
        readers.done()
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
