# Sorted-set writes beside other writers on the same shards - TODO 463, TODO 467.
#
# ZREMRANGEBYSCORE, ZRANGESTORE and the ZUNIONSTORE / ZINTERSTORE / ZDIFFSTORE store
# step used to write with try_lock(), which fails when another thread holds the shard,
# and then write anyway with no lock. This puts them on a space of two shards next to
# writers that ZADD and ZREM into the very sets they work on, so they keep meeting on
# the same trees.
#
# A member is two keys: the score-ordered one ZRANGE walks, and the member index ZSCORE
# reads. A write that lands half way leaves one without the other, so after the run
# every set is checked for that. Under TSan a race fails the process outright.
import random
import threading
import time

import scale

import redis
import barch

scale.workdir()
PORT = scale.port(default=14230)
SECONDS = scale.env_float("BARCH_ZSET_RACE_SECONDS", 4.0, 1.5)

MEMBERS = ["m%02d" % i for i in range(60)]
SOURCES = ["s%d" % i for i in range(4)]   # ZADD / ZREM by writers, read by storers
TRIMMED = ["r%d" % i for i in range(4)]   # ZADD by writers, trimmed by removers
STORED = ["d%d" % i for i in range(4)]    # written only by the store commands

print("start zset race test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

# two shards, so a command and a writer are nearly always on the same one
conf = barch.KeyValue("configuration")
conf.set("zr.shards", "2")


def conn():
    c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
    c.execute_command("USE", "zr")
    return c


seed = conn()
seed.execute_command("FLUSHDB")
for k in SOURCES + TRIMMED:
    for i, m in enumerate(MEMBERS):
        seed.execute_command("ZADD", k, i, m)

stop = threading.Event()
errors = []
counts = {"writes": 0, "trims": 0, "stores": 0}


def writer(n):
    c = conn()
    rnd = random.Random(n)
    try:
        while not stop.is_set():
            m = rnd.choice(MEMBERS)
            c.execute_command("ZADD", rnd.choice(SOURCES), rnd.randint(0, 100), m)
            c.execute_command("ZADD", rnd.choice(TRIMMED), rnd.randint(0, 100), m)
            c.execute_command("ZREM", rnd.choice(SOURCES), rnd.choice(MEMBERS))
            c.execute_command("ZINCRBY", rnd.choice(TRIMMED), 1, rnd.choice(MEMBERS))
            counts["writes"] += 4
    except Exception as e:
        errors.append("writer %d: %r" % (n, e))


def remover(n):
    c = conn()
    rnd = random.Random(100 + n)
    try:
        while not stop.is_set():
            lo = rnd.randint(0, 80)
            # the exclusive form too - it goes down the same path
            c.execute_command("ZREMRANGEBYSCORE", rnd.choice(TRIMMED),
                              "(%d" % lo if rnd.random() < 0.5 else lo, lo + 20)
            counts["trims"] += 1
    except Exception as e:
        errors.append("remover %d: %r" % (n, e))


def storer(n):
    c = conn()
    rnd = random.Random(200 + n)
    try:
        while not stop.is_set():
            dest = rnd.choice(STORED)
            a, b = rnd.sample(SOURCES, 2)
            which = rnd.randint(0, 3)
            if which == 0:
                c.execute_command("ZUNIONSTORE", dest, 2, a, b)
            elif which == 1:
                c.execute_command("ZINTERSTORE", dest, 2, a, b)
            elif which == 2:
                c.execute_command("ZDIFFSTORE", dest, 2, a, b)
            else:
                c.execute_command("ZRANGESTORE", dest, a, 0, -1)
            counts["stores"] += 1
    except Exception as e:
        errors.append("storer %d: %r" % (n, e))


def check_whole(c, key):
    """every member has both of its keys, and they agree on the score"""
    rows = c.execute_command("ZRANGE", key, 0, -1, "WITHSCORES")
    ranged = {}
    for i in range(0, len(rows), 2):
        m = rows[i].decode()
        assert m not in ranged, "%s: %s listed twice by ZRANGE" % (key, m)
        ranged[m] = float(rows[i + 1])
    assert c.execute_command("ZCARD", key) == len(ranged), \
        "%s: ZCARD %s, ZRANGE lists %d" % (key, c.execute_command("ZCARD", key), len(ranged))
    for m in MEMBERS:
        s = c.execute_command("ZSCORE", key, m)
        if m in ranged:
            assert s is not None, "%s: %s in ZRANGE but ZSCORE has none" % (key, m)
            assert float(s) == ranged[m], \
                "%s: %s scores %s in ZRANGE, %s in ZSCORE" % (key, m, ranged[m], s)
        else:
            assert s is None, "%s: ZSCORE has %s for %s, ZRANGE doesn't list it" % (key, s, m)
    return ranged


try:
    threads = [threading.Thread(target=writer, args=(n,)) for n in range(4)] + \
              [threading.Thread(target=remover, args=(n,)) for n in range(2)] + \
              [threading.Thread(target=storer, args=(n,)) for n in range(2)]
    for t in threads:
        t.start()
    time.sleep(SECONDS)
    stop.set()
    for t in threads:
        t.join(timeout=30)
    assert not any(t.is_alive() for t in threads), "a thread is still running"
    assert not errors, errors[:5]
    assert counts["trims"] > 0 and counts["stores"] > 0, counts
    print("  %(writes)d writes, %(trims)d ZREMRANGEBYSCORE, %(stores)d stores" % counts,
          flush=True)

    c = conn()
    sets = {k: check_whole(c, k) for k in SOURCES + TRIMMED + STORED}
    print("  %d sets whole after the race" % len(sets), flush=True)

    # and each command still gives the right answer once it's quiet
    a, b = sets["s0"], sets["s1"]
    c.execute_command("ZUNIONSTORE", "d0", 2, "s0", "s1")
    want = dict(a)
    for m, s in b.items():
        want[m] = want.get(m, 0) + s
    assert check_whole(c, "d0") == want, "ZUNIONSTORE"

    c.execute_command("ZINTERSTORE", "d1", 2, "s0", "s1")
    assert check_whole(c, "d1") == {m: a[m] + b[m] for m in a if m in b}, "ZINTERSTORE"

    c.execute_command("ZDIFFSTORE", "d2", 2, "s0", "s1")
    assert check_whole(c, "d2") == {m: s for m, s in a.items() if m not in b}, "ZDIFFSTORE"

    c.execute_command("ZRANGESTORE", "d3", "s0", 0, -1)
    assert check_whole(c, "d3") == a, "ZRANGESTORE"

    r0 = sets["r0"]
    removed = c.execute_command("ZREMRANGEBYSCORE", "r0", "(20", 60)
    kept = {m: s for m, s in r0.items() if not (20 < s <= 60)}
    assert removed == len(r0) - len(kept), "ZREMRANGEBYSCORE removed %d" % removed
    assert check_whole(c, "r0") == kept, "ZREMRANGEBYSCORE"
    print("zset race test complete", flush=True)
finally:
    barch.stop()
