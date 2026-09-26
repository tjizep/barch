# Keys that share a prefix of 256 bytes or more - TODO 473.
#
# A tree node kept its prefix length in one byte. When two keys shared 256 bytes or
# more, the length was stored modulo 256 while the children sat at the full depth, so
# every lookup that walked past that node went down the wrong branch. A sorted set's
# keys all share its name, so a set named about 250 bytes long was the first to show
# it: ZCARD said 0 or 1 right after adding three members, ZREM and DEL removed nothing,
# and a STORE into it couldn't clear it. Hashes hit it at some lengths too.
#
# Each kind of key is tried at every length across the boundary and at a few long
# ones, with more than one member so the keys really do share the prefix.
import scale

import redis
import barch

scale.workdir()
PORT = scale.port(default=14240)

print("start long prefix test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("USE", "lp")
r.execute_command("FLUSHDB")

LENGTHS = list(range(240, 272)) + [300, 511, 512, 513, 1000, 4000]
failures = []


def expect(ok, what):
    if not ok:
        failures.append(what)


try:
    for n in LENGTHS:
        for fill in "de":
            name = fill * n

            # a sorted set: add, count, read, remove one, remove a range, delete
            r.zadd(name, {"a": 1, "b": 2, "c": 3, "d": 4})
            expect(r.zcard(name) == 4, "ZCARD %s*%d" % (fill, n))
            expect(r.zscore(name, "c") == 3.0, "ZSCORE %s*%d" % (fill, n))
            expect([m for m, _ in r.zrange(name, 0, -1, withscores=True)]
                   == [b"a", b"b", b"c", b"d"], "ZRANGE %s*%d" % (fill, n))
            expect(r.zrem(name, "a") == 1 and r.zcard(name) == 3, "ZREM %s*%d" % (fill, n))
            expect(r.zremrangebyscore(name, 2, 2) == 1 and r.zcard(name) == 2,
                   "ZREMRANGEBYSCORE %s*%d" % (fill, n))
            expect(r.delete(name) == 1 and r.zcard(name) == 0, "DEL zset %s*%d" % (fill, n))

            # a STORE into the long name replaces what was there
            r.zadd(name, {"old": 9})
            r.zadd("src", {"x": 1, "y": 2})
            expect(r.zunionstore(name, ["src"]) == 2, "ZUNIONSTORE count %s*%d" % (fill, n))
            expect([m for m, _ in r.zrange(name, 0, -1, withscores=True)] == [b"x", b"y"]
                   and r.zscore(name, "old") is None, "ZUNIONSTORE replaces %s*%d" % (fill, n))
            r.delete(name, "src")

            # a hash with several fields
            h = "h" + name
            r.hset(h, mapping={"f1": "1", "f2": "2", "f3": "3"})
            expect(r.hlen(h) == 3 and r.hget(h, "f2") == b"2", "hash %s*%d" % (fill, n))
            expect(r.delete(h) == 1 and r.hlen(h) == 0, "DEL hash %s*%d" % (fill, n))

            # plain keys sharing the whole name as a prefix
            for s in "abc":
                r.set(name + s, s)
            expect([r.get(name + s) for s in "abc"] == [b"a", b"b", b"c"],
                   "plain %s*%d" % (fill, n))
            expect(r.delete(name + "a", name + "b", name + "c") == 3, "DEL plain %s*%d" % (fill, n))

    expect(r.dbsize() == 0, "everything deleted, dbsize %d" % r.dbsize())
    if failures:
        print("  %d failed: %s" % (len(failures), failures[:8]), flush=True)
    assert not failures, failures[:8]
    print("  %d lengths, from %d to %d bytes" % (len(LENGTHS), LENGTHS[0], LENGTHS[-1]),
          flush=True)
    print("long prefix test complete", flush=True)
finally:
    barch.stop()
