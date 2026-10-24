#!/usr/bin/env python3
"""
RANGE, COUNT, KEYS and SCAN in a transaction in a Raft space - TODO 701.

A transaction's snapshot is the node it runs on's own: what that node had applied at
BEGINTX. Every member applies through a shard's insert and remove, so the node keeps the
before-images its snapshots need, and a walk merges overlay, layers and space as it does
in a space without Raft. At COMMITTX the node checks its own layers first; then each
group's prepare decides, as the reads' hashes do: a range walked is sent as a word per
shard with a hash of that shard's [lo, hi) as the snapshot saw it, which the group
compares with its own shard, and keeps as a lock while the transaction is pending. A
write intent inside a range another pending transaction walked, or a range walked over
another's write intent, is a TXCONFLICT.

Node 1 skips the local check (BARCH_TEST_TX_NO_LOCAL_CHECK), so what it runs is decided
by the group alone. Node 2 pauses between prepare and commit (BARCH_TEST_TX_PAUSE_MS),
so a transaction it runs stays pending long enough to be in another's way. What has to
hold, on three nodes:
  - on a follower, RANGE, COUNT, KEYS and SCAN see the keys as at BEGINTX after the
    leader adds one in the range and deletes another, and its own SET and DEL;
  - one that walked that range and writes answers TXCONFLICT, from its own layers and
    from the group alone, and one that walked a range nothing was written into commits
    on every member;
  - a script's barch.store.range and min in a transaction are checked the same way,
    on the node that skips its local check and on a follower;
  - write skew: two transactions that each COUNT a range the other writes into can't
    both commit, whichever prepares first;
  - a range lock is on every member: with the leader killed while a transaction that
    walked a range is pending, it and a write into the range on the new leader can't
    both commit (the write may push it aside, if the election outlasts its patience);
  - in a space split over two groups, KEYS sees the snapshot of both, a phantom in
    either answers TXCONFLICT, and one with no phantom commits;
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
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=27400)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "walks"
SPLIT = "walks2"
LABELS = [SPLIT, SPLIT + "/1"]
FIVE = ["r1", "r2", "r3", "r4", "r5"]
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


ENVS = [{}, {"BARCH_TEST_TX_NO_LOCAL_CHECK": "1"}, {"BARCH_TEST_TX_PAUSE_MS": "2000"}]
nodes = [clusternodes.Node(i, BASE, BINARY, env=ENVS[i]) for i in range(3)]


def line_of(n, label):
    """the group's line. Only a split group's line has a label, and a split space's
    name alone means its first group, /0. An unsplit group is found by its spaces:
    looking for " label walks" found walks2/0, so waits on walks didn't wait - TODO 753"""
    want = label if "/" in label else label + "/0"
    for l in n.info():
        if not l.startswith("group "):
            continue
        if " label " in l:
            if (" label %s " % want) in l:
                return l
            continue
        m = re.match(r"group \d+ spaces (.*?) (?:shards|stopped|leader)\b", l)
        if m and label in m.group(1).split():
            return l
    return ""


def applied(n, label):
    m = re.search(r"\bapplied (\d+)", line_of(n, label))
    return int(m.group(1)) if m else -1


def settled(labels, members=None):
    members = [n for n in (members or nodes) if n.proc]

    def done():
        for l in labels:
            tops = [applied(n, l) for n in members]
            if min(tops) < 0 or len(set(tops)) != 1:
                return False
        return True
    return wait_for(60, done)


def client(n, space=SPACE):
    c = n.client(space)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c


def on_leader(space, *cmd):
    """a plain write, sent to each node until one leads its key's group, over an election too"""
    end = time.time() + 60
    while True:
        for n in nodes:
            if not n.proc:
                continue
            try:
                return n.client(space).execute_command(*cmd)
            except redis.RedisError:
                if time.time() > end:
                    raise
        time.sleep(0.2)


def reset():
    on_leader(SPACE, "DEL", "r1", "r2", "r2x", "r3", "r4", "r5", "r6", "w", "s1")
    on_leader(SPACE, "MSET", *[x for k in FIVE for x in (k, "v")])
    settled([SPACE])


def scan_all(c, pattern):
    cursor, out = "0", set()
    for _ in range(1000):
        cursor, keys = c.execute_command("SCAN", cursor, "MATCH", pattern, "COUNT", 100)
        out.update(keys)
        if str(cursor) == "0":
            break
    return sorted(out)


try:
    print("three nodes and two replicated spaces, one split over two groups")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    conf = nodes[0].client("configuration")
    conf.execute_command("SET", SPACE + ".raft", "on")
    conf.execute_command("SET", SPLIT + ".raft_groups", "2")
    conf.execute_command("SET", SPLIT + ".raft", "on")
    check(wait_for(90, lambda: all("members 3" in line_of(n, l) and "learners 0" in line_of(n, l)
                                   for n in nodes for l in [SPACE] + LABELS)
                   and leader_of(nodes, SPACE) is not None), "three voters in each group, and leaders")
    leader = leader_of(nodes, SPACE)
    followers = [n for n in nodes if n is not leader]

    print("walks on a follower, as of BEGINTX")
    reset()
    f = followers[0]
    tx = client(f)
    tx.execute_command("BEGINTX")
    first = [list(tx.execute_command("RANGE", "r0", "r9")), tx.execute_command("COUNT", "r0", "r9")]
    on_leader(SPACE, "SET", "r2x", "new")
    on_leader(SPACE, "DEL", "r4")
    check(settled([SPACE]) and client(f).execute_command("COUNT", "r0", "r9") == 5,
          "the follower has the leader's writes")
    again = [list(tx.execute_command("RANGE", "r0", "r9")), tx.execute_command("COUNT", "r0", "r9"),
             sorted(tx.execute_command("KEYS", "r*")), scan_all(tx, "r*")]
    check(first == [FIVE, 5] and again == [FIVE, 5, FIVE, FIVE],
          "RANGE, COUNT, KEYS and SCAN see the keys as they were (%s)" % again)
    tx.execute_command("SET", "r6", "mine")
    tx.execute_command("DEL", "r1")
    got = [list(tx.execute_command("RANGE", "r0", "r9")), sorted(tx.execute_command("KEYS", "r*"))]
    mine = ["r2", "r3", "r4", "r5", "r6"]
    check(got == [mine, mine], "and its own SET and DEL (%s)" % got)
    got = answer(tx, "COMMITTX")
    check("TXCONFLICT" in str(got), "it answers TXCONFLICT (%s)" % str(got)[:60])
    check(settled([SPACE]) and all(client(n).get("r6") is None for n in nodes), "and nothing of it is anywhere")

    print("a phantom the group finds alone")
    for what, walk in [("RANGE", ("RANGE", "r0", "r9")), ("COUNT", ("COUNT", "r0", "r9")),
                       ("KEYS", ("KEYS", "r*"))]:
        reset()
        tx = client(nodes[1])
        tx.execute_command("BEGINTX")
        tx.execute_command(*walk)
        on_leader(SPACE, "SET", "r2x", "new")
        settled([SPACE])
        tx.execute_command("SET", "w", "1")
        got = answer(tx, "COMMITTX")
        check("TXCONFLICT" in str(got), "after %s, a key added to the range (%s)" % (what, str(got)[:50]))
    reset()
    tx = client(nodes[1])
    tx.execute_command("BEGINTX")
    tx.execute_command("COUNT", "r0", "r9")
    on_leader(SPACE, "DEL", "r3")
    settled([SPACE])
    tx.execute_command("SET", "w", "1")
    got = answer(tx, "COMMITTX")
    check("TXCONFLICT" in str(got), "a key deleted from it (%s)" % str(got)[:50])

    print("a range nothing was written into")
    for n in nodes:
        reset()
        tx = client(n)
        tx.execute_command("BEGINTX")
        tx.execute_command("RANGE", "r0", "r9")
        on_leader(SPACE, "SET", "s1", "outside")
        settled([SPACE])
        tx.execute_command("SET", "w", "node%d" % nodes.index(n))
        got = answer(tx, "COMMITTX")
        check(got in ("OK", True) and settled([SPACE])
              and all(client(m).get("w") == "node%d" % nodes.index(n) for m in nodes),
              "commits on node %d, on every member (%s)" % (nodes.index(n), got))

    print("a script's barch.store range reads")
    fx = '--@barch {"replicate": "effects"}\n'
    on_leader(SPACE, "SETF", "srange", fx + "function call(lo, hi) return table.concat(barch.store.range(lo, hi, 100), ',') end\n")
    on_leader(SPACE, "SETF", "smin", fx + "function call() return barch.store.min() or 'nil' end\n")
    settled([SPACE])
    for n, what, call, write, conflict in [
            (nodes[1], "range, by the group alone", ("srange", "r0", "r9"), ("SET", "r2x", "new"), True),
            (nodes[1], "min, by the group alone", ("smin",), ("SET", "a0", "new"), True),
            (followers[0], "range on a follower", ("srange", "r0", "r9"), ("SET", "r2x", "new"), True),
            (followers[0], "range on a follower, written past", ("srange", "r0", "r9"), ("SET", "s1", "x"), False)]:
        reset()
        on_leader(SPACE, "DEL", "a0")
        tx = client(n)
        tx.execute_command("BEGINTX")
        seen = answer(tx, "CALLF", *call)
        on_leader(SPACE, *write)
        settled([SPACE])
        tx.execute_command("SET", "w", "1")
        got = answer(tx, "COMMITTX")
        ok = ("TXCONFLICT" in str(got)) if conflict else got in ("OK", True)
        check(ok and seen in (",".join(FIVE), "r1"), "%s: %s (%s, %s)" % (
            what, "TXCONFLICT" if conflict else "commits", str(seen)[:16], str(got)[:40]))
    on_leader(SPACE, "DEL", "a0")

    print("write skew over two ranges")
    for k in ["a1", "b1"]:
        on_leader(SPACE, "DEL", k)
    settled([SPACE])
    results = {}

    def first_tx():
        c = client(nodes[2])                # pauses after its prepare
        c.execute_command("BEGINTX")
        results["first count"] = c.execute_command("COUNT", "a0", "a9")
        c.execute_command("SET", "b1", "from the first")
        results["first"] = answer(c, "COMMITTX")

    t = threading.Thread(target=first_tx)
    t.start()
    time.sleep(0.8)                         # its prepare is in, its commit isn't
    second = client(nodes[0])
    second.execute_command("BEGINTX")
    n = second.execute_command("COUNT", "b0", "b9")
    second.execute_command("SET", "a1", "from the second")
    results["second"] = answer(second, "COMMITTX")
    t.join()
    settled([SPACE])
    landed = [client(nodes[0]).get("a1"), client(nodes[0]).get("b1")]
    check(results["first"] in ("OK", True) and "TXCONFLICT" in str(results["second"])
          and landed == [None, "from the first"],
          "a range walked over a pending write intent: one commits (%s, %s, %s)"
          % (results["first"], str(results["second"])[:50], landed))

    on_leader(SPACE, "DEL", "c5")
    settled([SPACE])

    def walker():
        c = client(nodes[2])
        c.execute_command("BEGINTX")
        c.execute_command("COUNT", "c0", "c9")
        c.execute_command("SET", "d1", "walked")
        results["walker"] = answer(c, "COMMITTX")

    t = threading.Thread(target=walker)
    t.start()
    time.sleep(0.8)
    writer = client(nodes[0])
    writer.execute_command("BEGINTX")
    writer.execute_command("SET", "c5", "into the range")
    results["writer"] = answer(writer, "COMMITTX")
    t.join()
    check(results["walker"] in ("OK", True) and "TXCONFLICT" in str(results["writer"]),
          "a write intent into a range a pending one walked: refused (%s)" % str(results["writer"])[:60])

    print("a range lock across a leader change")
    leader = leader_of(nodes, SPACE)
    if leader is nodes[2]:
        # the pausing node coordinates below, so it can't be the one killed
        nodes[2].kill()
        check(wait_for(60, lambda: leader_of(nodes[:2], SPACE) is not None), "a leader that isn't node 2")
        nodes[2].start()
        check(wait_for(60, lambda: "members 3" in line_of(nodes[2], SPACE)), "node 2 back")
        settled([SPACE] + LABELS)
        leader = leader_of(nodes, SPACE)
    on_leader(SPACE, "DEL", "e5")
    settled([SPACE])

    def held():
        c = client(nodes[2])
        c.execute_command("BEGINTX")
        c.execute_command("COUNT", "e0", "e9")
        c.execute_command("SET", "f1", "held")
        results["held"] = answer(c, "COMMITTX")

    t = threading.Thread(target=held)
    t.start()
    time.sleep(0.8)
    leader.kill()
    rest = [n for n in nodes if n is not leader]
    check(wait_for(60, lambda: leader_of(rest, SPACE) is not None), "a new leader")
    c = client([n for n in rest if n is not nodes[2]][0])
    got = None
    for _ in range(50):
        c.execute_command("BEGINTX")
        c.execute_command("SET", "e5", "into the range")
        got = answer(c, "COMMITTX")
        if "TXCONFLICT" in str(got) or got in ("OK", True):
            break
        time.sleep(0.2)                     # an election still going
    t.join()
    # the election can take longer than a pending one's patience (tx_quiet_ms), and then
    # the write pushes it aside, which is fine: what can't happen is both committing
    wrote = got in ("OK", True)
    walked = results["held"] in ("OK", True)
    check(wrote != walked and (walked or "TXABORTED" in str(results["held"]))
          and (wrote or ("TXCONFLICT" in str(got) and "range" in str(got))),
          "a write into the range on the new leader and the pending walk: one commits (%s, %s)"
          % (str(got)[:50], str(results["held"])[:50]))
    settled([SPACE], rest)
    check(all(client(n).get("e5") == ("into the range" if wrote else None) for n in rest)
          and all(client(n).get("f1") == ("held" if walked else None) for n in rest),
          "and every member holds that one's write alone")
    leader.start()
    check(wait_for(60, lambda: "members 3" in line_of(leader, SPACE)), "the old leader is back")

    print("a space split over two groups")
    keys = ["k%d" % i for i in range(16)]

    def split_reset():
        for k in keys + ["k16", "w"]:
            on_leader(SPLIT, "DEL", k)
        for k in keys:
            on_leader(SPLIT, "SET", k, "v")
        settled(LABELS)

    split_reset()
    tx = client(nodes[1], SPLIT)
    tx.execute_command("BEGINTX")
    first = sorted(tx.execute_command("KEYS", "k*"))
    on_leader(SPLIT, "SET", "k16", "new")
    on_leader(SPLIT, "DEL", "k3")
    settled(LABELS)
    again = sorted(tx.execute_command("KEYS", "k*"))
    tx.execute_command("SET", "w", "1")
    got = answer(tx, "COMMITTX")
    check(first == sorted(keys) and again == sorted(keys), "KEYS sees the snapshot of both groups")
    check("TXCONFLICT" in str(got), "and a phantom answers TXCONFLICT (%s)" % str(got)[:50])
    caught = 0
    for k in ["k%d" % i for i in range(16, 24)]:
        # one at a time, so some land in each group
        split_reset()
        tx = client(nodes[1], SPLIT)
        tx.execute_command("BEGINTX")
        tx.execute_command("KEYS", "k*")
        on_leader(SPLIT, "SET", k, "new")
        settled(LABELS)
        tx.execute_command("SET", "w", "1")
        caught += "TXCONFLICT" in str(answer(tx, "COMMITTX"))
        on_leader(SPLIT, "DEL", k)
    check(caught == 8, "a phantom in either group is caught (%d of 8)" % caught)
    split_reset()
    tx = client(nodes[1], SPLIT)
    tx.execute_command("BEGINTX")
    tx.execute_command("KEYS", "k*")
    tx.execute_command("SET", "w", "1")
    got = answer(tx, "COMMITTX")
    check(got in ("OK", True) and settled(LABELS) and all(client(n, SPLIT).get("w") == "1" for n in nodes),
          "one with no phantom commits, on every member (%s)" % got)

    print("every member the same")
    check(settled([SPACE] + LABELS) and len(set(n.digest(SPACE)[0] for n in nodes)) == 1
          and len(set(n.digest(SPLIT)[0] for n in nodes)) == 1, "digests match")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
