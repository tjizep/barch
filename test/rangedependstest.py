#!/usr/bin/env python3
"""
KSPACE DEPENDS over range sharded spaces - TODO 715, TODO 716.

A dependant's shard N reads its source's shard N. A range sharded space puts a key in
whichever shard its span is in, and its rebalancer keeps moving the spans, so most reads
through a dependant missed: 3558 of 4000 GETs, over 8 shards. TODO 715 refused it. Now
two range sharded spaces can: the dependant takes its source's boundaries at DEPENDS,
its own keys move to fit them, and the source doesn't rebalance while it has dependants.
A range sharded space and a hash sharded one still can't, since they can't put a key in
the same shard.

Checked against one barchd, pairs of 8 shard spaces:
  - a range sharded source under a hash sharded dependant, and the other way round: each
    DEPENDS answers an error saying why, and leaves the dependant with no source;
  - two hash sharded spaces: DEPENDS answers OK, KSPACE DEPENDANTS names the source, and
    every key reads through;
  - two range sharded spaces, each rebalanced on its own first, the dependant holding
    keys of its own, some of them over the source's:
      - every key of both reads through the dependant, its own winning;
      - both spaces put every key in the same shard;
      - a key deleted in the dependant stays hidden, though the source still has it;
      - the source, written lopsided, doesn't rebalance while the dependant lasts, and
        everything still reads through, RANGE and COUNT included;
      - a SAVE and RELOAD of the source keeps its boundaries, even one below the
        lowest key of its shard, so a key written into that gap after still reads
        through;
      - after RELEASE the dependant still has its own keys, and the source rebalances
        again;
      - DEPENDS onto a second source with other boundaries moves the dependant's
        tombstones too, so its deleted keys stay hidden;
  - MERGE of a range sharded dependant into its source: the source has the merged keys
    and its deletes, and every key is still found after it rebalances again;
  - a chain of three range sharded spaces reads through to the bottom.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14805)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-74s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


DATA = os.path.join(os.getcwd(), "rangedepends_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "rangedepends.log"), "a")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

TAG = str(int(time.time()))
KEYS = ["k%05d" % i for i in range(4000)]
WAIT = scale.scaled_seconds(30.0, floor=10.0)

try:
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)

    def space(name, ranged):
        for k, v in ((".ordered", "1"), (".shards", "8"), (".range_sharded", "1" if ranged else "0")):
            c.execute_command("configuration:SET", name + "_" + TAG + k, v)
        name = name + "_" + TAG
        c.execute_command("USE", name)
        c.execute_command("SETF", "sn", "function call(k) return barch.store.shardNumber(k) end\n")
        return name

    def answer(*cmd):
        try:
            return c.execute_command(*cmd)
        except redis.ResponseError as e:
            return "ERR " + str(e)

    def put(name, keys, value):
        c.execute_command("USE", name)
        for i in range(0, len(keys), 500):
            c.execute_command("MSET", *[x for k in keys[i:i + 500] for x in (k, value)])

    def gets(name, keys):
        c.execute_command("USE", name)
        out = []
        for i in range(0, len(keys), 500):
            out += c.execute_command("MGET", *keys[i:i + 500])
        return out

    def shard(name, key):
        c.execute_command("USE", name)
        return int(c.execute_command("CALLF", "sn", key))

    def spread(name, lo, hi):
        """wait for the rebalancer to spread lo..hi over every shard"""
        deadline = time.time() + WAIT
        while time.time() < deadline:
            if shard(name, lo) == 0 and shard(name, hi) == 7:
                return True
            time.sleep(0.2)
        return False

    def layout(name, keys):
        return [shard(name, k) for k in keys]

    print("a range sharded space and a hash sharded one")
    for source_ranged in (True, False):
        s = space("rds%d" % source_ranged, source_ranged)
        put(s, KEYS, "v")
        d = space("rdd%d" % source_ranged, not source_ranged)
        got = answer("KSPACE", "DEPENDS", d, "ON", s)
        check("ERR" in str(got) and "range sharded" in str(got), "DEPENDS is refused (%s)" % str(got)[4:60])
        check(answer("KSPACE", "DEPENDANTS", d) is None, "and the dependant has no source")
        check(gets(d, KEYS[:1]) == [None], "nothing reads through")

    print("two hash sharded spaces")
    s = space("rdsh", False)
    put(s, KEYS, "v")
    d = space("rddh", False)
    check(answer("KSPACE", "DEPENDS", d, "ON", s) in (True, "OK"), "DEPENDS answers OK")
    check(answer("KSPACE", "DEPENDANTS", d) == s, "DEPENDANTS names the source")
    missed = sum(1 for v in gets(d, KEYS) if v != "v")
    check(missed == 0, "every key reads through (%d missed)" % missed)

    print("two range sharded spaces")
    s = space("rdsr", True)
    put(s, KEYS, "v")
    check(spread(s, KEYS[0], KEYS[-1]), "the source spread its keys over its shards")
    d = space("rddr", True)
    OWN = ["j%05d" % i for i in range(3000)]           # below the source's keys
    SHADOWED = KEYS[1000:1200]                          # over some of the source's
    put(d, OWN, "own")
    put(d, SHADOWED, "mine")
    check(spread(d, OWN[0], SHADOWED[-1]), "the dependant spread its own keys over its shards")
    check(answer("KSPACE", "DEPENDS", d, "ON", s) in (True, "OK"), "DEPENDS answers OK")
    check(answer("KSPACE", "DEPENDANTS", d) == s, "DEPENDANTS names the source")
    want = {k: "v" for k in KEYS}
    want.update({k: "own" for k in OWN})
    want.update({k: "mine" for k in SHADOWED})
    ALL = sorted(want)

    def through(what):
        got = gets(d, ALL)
        wrong = [k for k, v in zip(ALL, got) if v != want.get(k)]
        check(not wrong, "%s: every key reads through the dependant (%d wrong, %s)" % (what, len(wrong), wrong[:3]))

    through("at DEPENDS")
    sample = KEYS[::97] + OWN[::211] + SHADOWED[::37]
    check(layout(d, sample) == layout(s, sample), "both spaces put every key in the same shard")

    GONE = KEYS[2000:2100]
    c.execute_command("USE", d)
    c.execute_command("DEL", *GONE)
    for k in GONE:
        del want[k]
    through("after deletes in the dependant")
    check(gets(s, GONE[:1]) == ["v"], "the source still has the deleted keys")

    before = layout(s, KEYS[::250])
    TOP = ["z%05d" % i for i in range(8000)]            # all in the source's top shard
    put(s, TOP, "top")
    want.update({k: "top" for k in TOP})
    ALL = sorted(want)
    time.sleep(scale.scaled_seconds(4.0, floor=3.0))
    check(layout(s, KEYS[::250]) == before and shard(s, TOP[-1]) == 7,
          "the source doesn't rebalance while it has a dependant")
    through("after the source grew lopsided")
    c.execute_command("USE", d)
    in_d = sorted(k for k in want if "k" <= k < "l")
    got = c.execute_command("RANGE", "k", "l", 100000)
    check(got == in_d, "RANGE through the dependant (%d of %d keys)" % (len(got), len(in_d)))
    check(c.execute_command("COUNT", "j", "{") == len(want),
          "COUNT through the dependant (%s of %d)" % (c.execute_command("COUNT", "j", "{"), len(want)))

    # the bottom keys of one of the source's shards go, so its boundary sits below its
    # lowest key. A table rebuilt from the keys at RELOAD would move it up, and a key
    # written into the gap after would go to the shard below, where the dependant
    # doesn't look
    lay = layout(s, KEYS)
    at = next(i for i in range(1, len(KEYS) - 20) if lay[i] != lay[i - 1]
              and not set(KEYS[i:i + 20]) & (set(SHADOWED) | set(GONE)))
    GAP = KEYS[at:at + 20]
    c.execute_command("USE", s)
    c.execute_command("DEL", *GAP)
    for k in GAP:
        del want[k]
    check(answer("SAVE") in (True, "OK"), "SAVE the source")
    check(answer("RELOAD") in (True, "OK"), "RELOAD the source")
    c.execute_command("USE", s)
    c.execute_command("SET", GAP[5], "back")
    want[GAP[5]] = "back"
    ALL = sorted(set(want) | set(GAP))
    through("after the source reloaded and %s was written again" % GAP[5])

    check(answer("KSPACE", "RELEASE", s, "FROM", d) in (True, "OK"), "RELEASE answers OK")
    check(answer("KSPACE", "DEPENDANTS", d) is None, "the dependant has no source")
    mine = {k: v for k, v in want.items() if v in ("own", "mine")}
    got = gets(d, sorted(mine))
    check(got == [mine[k] for k in sorted(mine)], "the dependant still has its own keys")
    deadline = time.time() + WAIT
    while time.time() < deadline and layout(s, KEYS[::250]) == before:
        time.sleep(0.2)
    check(layout(s, KEYS[::250]) != before, "the source rebalances again")

    # onto a source whose boundaries sit elsewhere: the dependant's tombstones move to
    # fit them with everything else, and still hide their keys, as on a hash pair
    s2 = space("rdsr2", True)
    LOW = ["c%05d" % i for i in range(4000)]
    put(s2, LOW + KEYS, "s2")
    check(spread(s2, LOW[0], KEYS[-1]), "a second source spread its keys over its shards")
    check(layout(s2, GONE[::10]) != layout(d, GONE[::10]), "with the deleted keys in other shards than the dependant has them")
    check(answer("KSPACE", "DEPENDS", d, "ON", s2) in (True, "OK"), "DEPENDS on it answers OK")
    want = {k: "s2" for k in LOW + KEYS}
    want.update(mine)
    for k in GONE:
        del want[k]
    ALL = sorted(set(want) | set(GONE))
    got = gets(d, ALL)
    wrong = [k for k, v in zip(ALL, got) if v != want.get(k)]
    check(not wrong, "every key reads through, the deleted ones hidden (%d wrong, %s)" % (len(wrong), wrong[:3]))

    print("MERGE a range sharded dependant into its source")
    s = space("rdsm", True)
    put(s, KEYS, "v")
    check(spread(s, KEYS[0], KEYS[-1]), "the source spread its keys over its shards")
    d = space("rddm", True)
    check(answer("KSPACE", "DEPENDS", d, "ON", s) in (True, "OK"), "DEPENDS answers OK")
    ADDED = ["m%05d" % i for i in range(6000)]          # above the source's keys
    put(d, ADDED, "added")
    put(d, KEYS[500:700], "changed")
    c.execute_command("USE", d)
    c.execute_command("DEL", *KEYS[3000:3100])
    want = {k: "v" for k in KEYS}
    want.update({k: "added" for k in ADDED})
    want.update({k: "changed" for k in KEYS[500:700]})
    for k in KEYS[3000:3100]:
        del want[k]
    check(answer("KSPACE", "MERGE", d, "INTO", s) in (True, "OK"), "MERGE answers OK")
    ALL = sorted(want)
    got = gets(s, ALL)
    wrong = [k for k, v in zip(ALL, got) if v != want[k]]
    check(not wrong, "the source has every merged key (%d wrong, %s)" % (len(wrong), wrong[:3]))
    check(gets(s, KEYS[3000:3001]) == [None], "and the dependant's deletes")
    check(answer("KSPACE", "RELEASE", s, "FROM", d) in (True, "OK"), "RELEASE answers OK")
    # the merge piled the added keys into the top shard: wait for them to spread out
    deadline = time.time() + WAIT
    while time.time() < deadline and shard(s, ADDED[0]) == 7:
        time.sleep(0.2)
    check(shard(s, ADDED[0]) < 7, "the source rebalances the merged keys out of its top shard")
    got = gets(s, ALL)
    wrong = [k for k, v in zip(ALL, got) if v != want[k]]
    check(not wrong, "every key is still found after (%d wrong, %s)" % (len(wrong), wrong[:3]))

    print("a chain of three range sharded spaces")
    bottom = space("rdc0", True)
    put(bottom, KEYS, "v")
    check(spread(bottom, KEYS[0], KEYS[-1]), "the bottom spread its keys over its shards")
    middle = space("rdc1", True)
    top = space("rdc2", True)
    check(answer("KSPACE", "DEPENDS", middle, "ON", bottom) in (True, "OK"), "DEPENDS the middle on the bottom")
    check(answer("KSPACE", "DEPENDS", top, "ON", middle) in (True, "OK"), "DEPENDS the top on the middle")
    put(middle, KEYS[100:200], "middle")
    want = {k: "v" for k in KEYS}
    want.update({k: "middle" for k in KEYS[100:200]})
    got = gets(top, KEYS)
    wrong = [k for k, v in zip(KEYS, got) if v != want[k]]
    check(not wrong, "every key reads through to the bottom (%d wrong, %s)" % (len(wrong), wrong[:3]))
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
