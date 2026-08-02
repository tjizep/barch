# Ordered range sharding: that keys are routed by the range they fall in, that the
# rebalancer keeps the shards even, and that neither of those loses a key.
#
# The option and its plumbing are tested next door in rangeshardtest.py. What is tested
# here is the algorithm - see DONE 31 and test/rangeshard_prototype.cpp, where it was
# settled before it was written against real shards.
#
# The thing worth being careful about in this test is that rebalancing runs on the key
# space's maintenance thread, so keys move *while* the writes below are happening. Every
# read back is therefore also a test of the race between routing a key and locking the
# shard it routed to.
import time

import redis
import barch

PORT = 14073
SHARDS = 8
KEYS = 20000

barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

conf = barch.KeyValue("configuration")


def shard_sizes(conn, space, shards):
    """the size of every shard of `space`, in shard order"""
    conn.execute_command("USE", space)
    out = []
    for s in range(shards):
        # the # form asks for a shard by number. Without it the argument is taken as a
        # key and hashed, which answers about some other shard entirely
        raw = conn.execute_command("INFO SHARD #%d" % s)
        if isinstance(raw, bytes):
            raw = raw.decode()
        size = None
        for line in raw.split("\n"):
            if line.startswith("size:"):
                size = int(line.split(":", 1)[1])
        assert size is not None, raw
        out.append(size)
    return out


def settle(conn, space, shards, expect, timeout=20.0):
    """wait for the maintenance thread to finish balancing, and report what it reached

    Two things have to come true, and both need the retry. The sizes are read one shard
    at a time and nothing holds the space still while that happens, so a key the sweep
    moves between two reads is counted twice or not at all - the total only settles once
    the sweep has nothing left to do. And the balance itself is the sweep's own work,
    which is on the maintenance thread rather than on the writes that caused it."""
    deadline = time.time() + timeout
    best = None
    while time.time() < deadline:
        sizes = shard_sizes(conn, space, shards)
        total = sum(sizes)
        ratio = max(sizes) / (total / len(sizes)) if total else 1.0
        best = (total, ratio, sizes)
        # the sweep aims for 1.25x and stops there, so anything at or under it is done
        if total == expect and ratio <= 1.30:
            return sizes
        time.sleep(0.1)
    assert False, "never settled: %d keys at %.2fx, expected %d: %s" % (best + (expect,))


def decoded(items):
    return [i.decode() if isinstance(i, bytes) else i for i in items]


def rng(conn, space, lo, hi, count=-1):
    """B.RANGE over `space`, as strings.

    The key space is a property of the connection, so it is named on every call here
    rather than left to whatever the previous one happened to select"""
    conn.execute_command("USE", space)
    return decoded(conn.execute_command("RANGE", lo, hi, count))


print("start range routing test")

conf.set("rs_route.ordered", "1")
conf.set("rs_route.shards", str(SHARDS))
conf.set("rs_route.range_sharded", "1")
rs = barch.KeyValue("rs_route")
assert rs.getRangeSharded()

r = redis.Redis(host="127.0.0.1", port=PORT, protocol=2)

# --- ascending keys, which is the worst case for an ordered partition ---------------
# every new key lands at the top, so one key has to cross every boundary to keep the
# shards even. It is the workload that exercises the rebalancer hardest and the one that
# used to leave every key in the last shard when shedding only went upwards
def key(i):
    return "k%08d" % i


for i in range(KEYS):
    rs.set(key(i), "v%d" % i)

# --- nothing was lost on the way ----------------------------------------------------
# keys were moving between shards on the maintenance thread throughout the loop above,
# so a key that reads back is a key whose route and whose lock agreed
for i in range(KEYS):
    got = rs.get(key(i))
    assert got == "v%d" % i, "key %s came back as %r" % (key(i), got)

# --- the rebalancer actually ran ------------------------------------------------------
sizes = settle(r, "rs_route", SHARDS, KEYS)
assert sizes.count(0) == 0, "every shard should have a span by now: %s" % sizes
ratio = max(sizes) / (sum(sizes) / len(sizes))
assert ratio <= 1.30, "shards are %.2fx out of balance: %s" % (ratio, sizes)

# --- and the keys are in shard order --------------------------------------------------
# this is the invariant the whole thing rests on: shard i holds only keys below every key
# in shard j, for i < j. RANGE reads the space in key order by walking the shards in
# shard order, so a sorted, complete answer is that invariant seen from outside
everything = rng(r, "rs_route", key(0), "k~")
assert len(everything) == KEYS, "RANGE returned %d of %d keys" % (len(everything), KEYS)
assert everything == sorted(everything), "RANGE did not come back in key order"
assert everything == [key(i) for i in range(KEYS)]

# a range that only overlaps some of the shards is the case the walk can stop early on
window = rng(r, "rs_route", key(5000), key(5100))
assert window == [key(i) for i in range(5000, 5100)], window[:5]

# a count limits the answer without changing where it starts
limited = rng(r, "rs_route", key(5000), key(9999), 10)
assert limited == [key(i) for i in range(5000, 5010)], limited

# --- the ordered single answers -------------------------------------------------------
assert rs.min() == key(0), rs.min()
assert rs.max() == key(KEYS - 1), rs.max()
assert rs.lowerBound(key(1234)) == key(1234)
assert rs.lowerBound("k000012345") == key(1235)

# --- random order keys, which the sweep barely has to touch ---------------------------
conf.set("rs_rand.ordered", "1")
conf.set("rs_rand.shards", str(SHARDS))
conf.set("rs_rand.range_sharded", "1")
rand = barch.KeyValue("rs_rand")

import random
order = list(range(KEYS))
random.Random(12345).shuffle(order)
for i in order:
    rand.set(key(i), "v%d" % i)
for i in range(KEYS):
    assert rand.get(key(i)) == "v%d" % i, key(i)
rand_sizes = settle(r, "rs_rand", SHARDS, KEYS)
assert rand_sizes.count(0) == 0, rand_sizes

# --- deletes route the same way inserts did -------------------------------------------
for i in range(0, KEYS, 2):
    rand.erase(key(i))
for i in range(KEYS):
    got = rand.get(key(i))
    if i % 2 == 0:
        assert not got, "erased key %s still reads back as %r" % (key(i), got)
    else:
        assert got == "v%d" % i, key(i)
settle(r, "rs_rand", SHARDS, KEYS // 2)

# --- a reload rebuilds the routing table from the shards -------------------------------
# the table is never persisted - it is nothing but each shard's first key - so a reload
# derives it again rather than reading it back. If that did not happen, or happened
# wrongly, the reads below would route into the wrong shard and find nothing
before_reload = settle(r, "rs_route", SHARDS, KEYS)
assert rs.save()
assert rs.reload()
assert shard_sizes(r, "rs_route", SHARDS) == before_reload
for i in range(0, KEYS, 7):
    assert rs.get(key(i)) == "v%d" % i, "after reload: %s" % key(i)
reloaded = rng(r, "rs_route", key(0), "k~")
assert reloaded == [key(i) for i in range(KEYS)], len(reloaded)

# --- a hash sharded space is untouched by any of this ----------------------------------
# the same keys, the same shard count, routed by hash: it must still answer for every one
# of them, and its shards must not be in key order
conf.set("rs_hash.ordered", "1")
conf.set("rs_hash.shards", str(SHARDS))
hashed = barch.KeyValue("rs_hash")
assert not hashed.getRangeSharded()
for i in range(2000):
    hashed.set(key(i), "v%d" % i)
for i in range(2000):
    assert hashed.get(key(i)) == "v%d" % i
hash_range = rng(r, "rs_hash", key(0), "k~")
assert hash_range == [key(i) for i in range(2000)], len(hash_range)

r.close()
barch.stop()
print("complete range routing test")
