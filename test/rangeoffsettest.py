# RANGE lo hi LIMIT offset count, and ZRANGE's LIMIT and index ranges, reaching
# the offset through the node counts (iterator::skip) instead of walking to it.
# Every answer is checked against a slice of the same range read without an
# offset, so nothing here depends on knowing barch's key order. See TODO 369.
import random
import time

import scale

import redis
import barch

scale.workdir()
PORT = scale.port(default=14140)

print("start range offset test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")

rnd = random.Random(369)
failures = []


def check(ok, what):
    if not ok:
        print("  FAIL " + what, flush=True)
        failures.append(what)


def conf(key, value):
    r.execute_command("configuration:SET", key, value)


def keys_for_every_node_size():
    """Groups whose fan-out lands in each ART node size - 4, 16, 48 and 256 - at
    more than one depth, plus random keys of mixed length on top."""
    out = set()
    alphabet = [chr(c) for c in range(33, 127) if chr(c) not in "\\\"'"]
    for depth in (1, 2, 3):
        for fan in (2, 4, 5, 15, 16, 17, 47, 48, 49, 90):
            base = "g%d_%d_" % (depth, fan) + "x" * depth
            for i in range(fan):
                out.add(base + alphabet[i % len(alphabet)] + ("y" * (i % 3)))
    while len(out) < scale.scaled(6000, 1500):
        n = rnd.randint(1, 12)
        out.add("k" + "".join(rnd.choice("abcdefghijklmnop") for _ in range(n)))
    return sorted(out)


def load(space, keys):
    p = r.pipeline(transaction=False)
    for i, k in enumerate(keys):
        p.execute_command(space + ":SET", k, "v")
        if i % 1000 == 999:
            p.execute()
    p.execute()


def check_space(space, lo="\x01", hi="\x7f", probes=300):
    full = r.execute_command(space + ":RANGE", lo, hi, -1)
    n = len(full)
    check(n > 0, "%s: the range holds something" % space)
    offsets = list(range(0, min(n, 60))) + [rnd.randrange(n) for _ in range(probes)] \
        + [n - 1, n, n + 5]
    for off in offsets:
        cnt = rnd.choice((1, 2, 7, 50, 1000))
        got = r.execute_command(space + ":RANGE", lo, hi, "LIMIT", off, cnt)
        if got != full[off:off + cnt]:
            check(False, "%s: LIMIT %d %d gave %d keys starting %r, wanted %d starting %r"
                  % (space, off, cnt, len(got), got[:1], len(full[off:off + cnt]),
                     full[off:off + 1]))
            break
    # an inner window, so the skip starts somewhere other than the first key
    a, b = sorted(rnd.sample(range(n), 2))
    inner = r.execute_command(space + ":RANGE", full[a], full[b], -1)
    check(inner == full[a:b], "%s: an inner range reads the same as the slice" % space)
    for off in (0, 1, (b - a) // 2, b - a - 1, b - a):
        got = r.execute_command(space + ":RANGE", full[a], full[b], "LIMIT", off, 25)
        check(got == inner[off:off + 25], "%s: inner LIMIT %d 25" % (space, off))
    # the old count-only form is unchanged
    check(r.execute_command(space + ":RANGE", lo, hi, 10) == full[:10],
          "%s: RANGE lo hi count still works" % space)
    print("  %s: %d keys, %d offsets checked" % (space, n, len(offsets)), flush=True)
    return full


try:
    keys = keys_for_every_node_size()

    print("one shard: the skip on its own", flush=True)
    conf("one.shards", "1")
    load("one", keys)
    check_space("one")

    print("hash sharded, 7 shards", flush=True)
    conf("seven.shards", "7")
    load("seven", keys)
    check_space("seven")

    print("hash sharded, the default shard count", flush=True)
    load("dflt", keys)
    check_space("dflt", probes=100)

    print("range sharded", flush=True)
    conf("ranged.ordered", "1")
    conf("ranged.shards", "4")
    conf("ranged.range_sharded", "1")
    load("ranged", keys)
    check_space("ranged")

    print("composite keys beside plain ones", flush=True)
    conf("mixed.key_split", ":")
    mixed = [k for k in keys[:3000]] + ["c%d:%d" % (i % 37, i) for i in range(800)]
    load("mixed", mixed)
    check_space("mixed", probes=150)

    print("bad LIMIT arguments", flush=True)
    for args, want in [(("LIMIT", -1, 5), "negative"), (("LIMT", 1, 5), "syntax")]:
        try:
            r.execute_command("one:RANGE", "a", "z", *args)
            check(False, "RANGE %r should have been refused" % (args,))
        except redis.ResponseError as e:
            check(want in str(e), "RANGE %r refused with %r" % (args, str(e)))
    check(r.execute_command("one:RANGE", "\x01", "\x7f", "LIMIT", 3, 0) == [],
          "LIMIT n 0 is nothing")

    print("ZRANGE LIMIT and index ranges", flush=True)
    members = ["m%05d" % i for i in range(scale.scaled(5000, 1500))]
    scores = {m: rnd.choice((rnd.randint(0, 50), rnd.random() * 100)) for m in members}
    p = r.pipeline(transaction=False)
    for i, m in enumerate(members):
        p.execute_command("ZADD", "zs", scores[m], m)
        if i % 1000 == 999:
            p.execute()
    p.execute()
    zfull = r.execute_command("ZRANGEBYSCORE", "zs", "-inf", "+inf")
    check(len(zfull) == len(members), "every member is in the set")
    zn = len(zfull)
    for off in list(range(0, 20)) + [rnd.randrange(zn) for _ in range(150)] + [zn - 1, zn]:
        cnt = rnd.choice((1, 3, 10, 400))
        got = r.execute_command("ZRANGEBYSCORE", "zs", "-inf", "+inf", "LIMIT", off, cnt)
        if got != zfull[off:off + cnt]:
            check(False, "ZRANGEBYSCORE LIMIT %d %d" % (off, cnt))
            break
        got = r.execute_command("ZRANGE", "zs", "-inf", "+inf", "BYSCORE", "LIMIT", off, cnt)
        if got != zfull[off:off + cnt]:
            check(False, "ZRANGE BYSCORE LIMIT %d %d" % (off, cnt))
            break
    # a bounded score range, open at the bottom, so the skip starts past excluded members
    part = r.execute_command("ZRANGEBYSCORE", "zs", "(10", "60")
    for off in (0, 1, 5, len(part) // 2, len(part) - 1, len(part)):
        got = r.execute_command("ZRANGEBYSCORE", "zs", "(10", "60", "LIMIT", off, 20)
        check(got == part[off:off + 20], "ZRANGEBYSCORE (10 60 LIMIT %d 20" % off)
    got = r.execute_command("ZRANGEBYSCORE", "zs", "-inf", "+inf", "WITHSCORES", "LIMIT", 7, 3)
    ws = r.execute_command("ZRANGEBYSCORE", "zs", "-inf", "+inf", "WITHSCORES")
    check(got == ws[14:20], "WITHSCORES LIMIT 7 3")
    # REV still slices the whole range, but has to give the same answers
    rev = r.execute_command("ZREVRANGEBYSCORE", "zs", "+inf", "-inf")
    check(r.execute_command("ZREVRANGEBYSCORE", "zs", "+inf", "-inf", "LIMIT", 100, 5)
          == rev[100:105], "ZREVRANGEBYSCORE LIMIT 100 5")

    r.execute_command("DEL", "zl")
    lexm = sorted({"".join(rnd.choice("abcdefgh") for _ in range(rnd.randint(1, 6)))
                   for _ in range(2000)})
    p = r.pipeline(transaction=False)
    for m in lexm:
        p.execute_command("ZADD", "zl", 0, m)
    p.execute()
    lfull = r.execute_command("ZRANGEBYLEX", "zl", "-", "+")
    check(len(lfull) == len(lexm), "every lex member is in the set")
    for off in (0, 1, 2, 17, 500, len(lfull) - 1, len(lfull)):
        got = r.execute_command("ZRANGEBYLEX", "zl", "-", "+", "LIMIT", off, 9)
        check(got == lfull[off:off + 9], "ZRANGEBYLEX - + LIMIT %d 9" % off)
    lpart = r.execute_command("ZRANGEBYLEX", "zl", "(b", "[f")
    for off in (0, 3, len(lpart) // 2, len(lpart)):
        got = r.execute_command("ZRANGEBYLEX", "zl", "(b", "[f", "LIMIT", off, 11)
        check(got == lpart[off:off + 11], "ZRANGEBYLEX (b [f LIMIT %d 11" % off)

    idx_checks = 0
    for start, stop in [(0, -1), (0, 0), (5, 9), (-10, -1), (zn - 3, zn + 10), (-zn - 5, 2),
                        (10, 5), (zn, zn + 3)] + \
            [tuple(sorted(rnd.sample(range(zn), 2))) for _ in range(40)]:
        py_start = start + zn if start < 0 else start
        py_stop = stop + zn if stop < 0 else stop
        py_start = max(py_start, 0)
        want = zfull[py_start:py_stop + 1] if py_start <= py_stop else []
        got = r.execute_command("ZRANGE", "zs", start, stop)
        check(got == want, "ZRANGE %d %d" % (start, stop))
        got = r.execute_command("ZRANGE", "zs", start, stop, "REV")
        rwant = list(reversed(zfull))[py_start:py_stop + 1] if py_start <= py_stop else []
        check(got == rwant, "ZRANGE %d %d REV" % (start, stop))
        idx_checks += 2
    got = r.execute_command("ZRANGE", "zs", 3, 5, "WITHSCORES")
    check(got == ws[6:12], "ZRANGE 3 5 WITHSCORES")
    print("  %d score, %d lex members; %d index ranges" % (zn, len(lfull), idx_checks),
          flush=True)

    # The point of it: an offset deep into a big range shouldn't cost the walk.
    # One shard, so a key count that fits the test is still one deep tree.
    print("a deep offset is not a walk", flush=True)
    conf("big.shards", "1")
    big = scale.scaled(200000, 40000)
    p = r.pipeline(transaction=False)
    for i in range(big):
        p.execute_command("big:SET", "b%07d" % i, "v")
        if i % 5000 == 4999:
            p.execute()
    p.execute()
    deep = big - 20

    def timed(*args):
        t0 = time.perf_counter()
        out = r.execute_command(*args)
        return out, time.perf_counter() - t0

    got, t_skip = timed("big:RANGE", "b", "c", "LIMIT", deep, 10)
    check(got == [b"b%07d" % i for i in range(deep, deep + 10)], "the deep page is right")
    _, t_walk = timed("big:RANGE", "b", "c", deep + 10)
    print("  offset %d: %.2f ms; walking to it: %.2f ms" % (deep, t_skip * 1000, t_walk * 1000),
          flush=True)
    check(t_skip * 5 < t_walk, "the skip is well under the walk (%.2f vs %.2f ms)"
          % (t_skip * 1000, t_walk * 1000))

    # the same keys across the default hash shards: the offset has to be found
    # across every shard at once, which is select_start rather than one skip
    p = r.pipeline(transaction=False)
    for i in range(big):
        p.execute_command("bigh:SET", "b%07d" % i, "v")
        if i % 5000 == 4999:
            p.execute()
    p.execute()
    for off in (deep, big // 2, 12345):
        got, t_hskip = timed("bigh:RANGE", "b", "c", "LIMIT", off, 10)
        check(got == [b"b%07d" % i for i in range(off, off + 10)],
              "the hash sharded page at %d is right" % off)
    _, t_hwalk = timed("bigh:RANGE", "b", "c", deep + 10)
    print("  hash sharded, offset %d: %.2f ms; walking to it: %.2f ms"
          % (deep, t_hskip * 1000, t_hwalk * 1000), flush=True)
    check(t_hskip * 5 < t_hwalk, "the hash sharded offset is found, not walked (%.2f vs %.2f ms)"
          % (t_hskip * 1000, t_hwalk * 1000))

    conf("bigz.shards", "1")
    p = r.pipeline(transaction=False)
    for i in range(big):
        p.execute_command("bigz:ZADD", "z", i, "m%07d" % i)
        if i % 5000 == 4999:
            p.execute()
    p.execute()
    got, t_zskip = timed("bigz:ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", deep, 5)
    check(got == [b"m%07d" % i for i in range(deep, deep + 5)], "the deep ZRANGE page is right")
    got, t_zidx = timed("bigz:ZRANGE", "z", deep, deep + 4)
    check(got == [b"m%07d" % i for i in range(deep, deep + 5)], "the deep index page is right")
    _, t_zwalk = timed("bigz:ZRANGEBYSCORE", "z", "-inf", "+inf")
    print("  ZRANGEBYSCORE LIMIT %d: %.2f ms; ZRANGE by index: %.2f ms; whole set: %.2f ms"
          % (deep, t_zskip * 1000, t_zidx * 1000, t_zwalk * 1000), flush=True)
    check(t_zskip * 5 < t_zwalk, "ZRANGE LIMIT skips rather than walks")
    check(t_zidx * 5 < t_zwalk, "ZRANGE by index skips rather than collects the set")

    if failures:
        raise AssertionError("%d checks failed, first: %s" % (len(failures), failures[0]))
    print("complete range offset test")
finally:
    try:
        barch.stop()
    except Exception:
        pass
