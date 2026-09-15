# lower_bound, checked against python's own bisect - TODO 322, DONE 310.
#
# `inner_lower_bound` is the middle of the ART: every ordered operation, every
# range and every directory listing goes through it, and DONE 310 changed how it
# descends. An argument that a change there is correct is not evidence, so this is
# the evidence: build a key set shaped like the cases the descent has to get right
# - shared prefixes, keys that are prefixes of other keys, deep paths, bytes
# either side of '/' - and compare every answer against bisect over the same set.
#
# Integer looking keys are kept out on purpose. barch encodes those as integers
# and orders them numerically, so a bisect over strings is not a model of it: the
# first run of this reported 116 mismatches that were all the harness's fault.
#
# **One shard.** The first version of this ran against the default space and
# passed 20,317 probes while a real lower_bound bug was live - see DONE 312 -
# because `LB` takes the minimum across every shard, so one shard answering past
# its keys is covered for by another. A per shard error is only visible when
# there is one shard. The many shard case is checked too, through the ranges at
# the end, which is where that bug did show.
import bisect, os, random, string, sys
sys.path.insert(0, "/home/test/barch/test")
import scale, redis, barch

scale.workdir()
PORT = scale.port(default=14970)
barch.start("0.0.0.0", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHALL")
r.execute_command("USE", "configuration")
r.set("one.shards", "1")
r.execute_command("USE", "one")

# every key starts with a letter and holds no all-digit form, because barch
# encodes an integer-looking key as an integer and its order is then numeric, not
# byte order - a bisect over strings is not a model of that
rnd = random.Random(7)
keys = set()
# shapes chosen to exercise the paths the fix touches: shared prefixes, deep
# paths, keys that are prefixes of other keys, bytes either side of '/'
tops = ["catalog", "cat", "catalogue", "img", "meta", "zz"]
mid = ["arts-crafts", "automotive", "baby", "beading", "b", "a"]
for t in tops:
    keys.add(f"fs:n:/{t}")
    keys.add(f"fs:n:/{t}/")
    for m in mid:
        keys.add(f"fs:n:/{t}/{m}")
        keys.add(f"fs:n:/{t}/{m}/")
        for i in range(40):
            keys.add(f"fs:n:/{t}/{m}/{''.join(rnd.choices(string.ascii_uppercase + string.digits, k=10))}")
for i in range(2000):
    keys.add("k" + ''.join(rnd.choices(string.ascii_letters + string.digits + "/.-_", k=rnd.randint(1, 24))))
keys = sorted(keys)
pipe = r.pipeline(transaction=False)
for k in keys:
    pipe.set(k, "v")
pipe.execute()
assert r.dbsize() == len(keys), (r.dbsize(), len(keys))
print(f"{len(keys)} keys loaded")

def probes():
    for k in keys:                      # every key itself
        yield k
    for k in keys:                      # and the neighbourhoods around it
        yield k + "0"
        yield k + "/"
        if len(k) > 2:
            yield k[:-1]
            yield k[:-1] + chr(min(ord(k[-1]) + 1, 126))
    for i in range(3000):
        yield "k" + ''.join(rnd.choices(string.ascii_letters + string.digits + "/.-_", k=rnd.randint(1, 20)))

bad = 0
n = 0
for p in probes():
    n += 1
    i = bisect.bisect_left(keys, p)
    want = keys[i].encode() if i < len(keys) else None
    got = r.execute_command("LB", p)
    if got != want:
        bad += 1
        if bad < 8:
            print(f"  MISMATCH probe={p!r} want={want!r} got={got!r}")
print(f"{n} probes on one shard, {bad} mismatches")

# and the same keys in a space with the default shard count, checked through
# bounded ranges - a limit and a `hi` are what the sharded walk gets wrong when it
# gets anything wrong (DONE 311, DONE 312)
r.execute_command("USE", "many")
r.execute_command("FLUSHDB")
pipe = r.pipeline(transaction=False)
for k in keys:
    pipe.set(k, "v")
pipe.execute()
assert r.dbsize() == len(keys)

rbad = 0
for i in range(400):
    a, b = rnd.choice(keys), rnd.choice(keys)
    lo, hi = min(a, b), max(a, b)
    for limit in (10, 50, 300, 0):
        want = [k.encode() for k in keys if lo <= k < hi]
        if limit:
            want = want[:limit]
        got = list(r.execute_command("RANGE", lo, hi, limit) or [])
        if got != want:
            rbad += 1
            if rbad < 4:
                d = next((j for j, (x, y) in enumerate(zip(want, got)) if x != y), None)
                print(f"  RANGE MISMATCH {lo!r}..{hi!r} limit={limit}: "
                      f"want {len(want)} got {len(got)}, first difference at {d}")
print(f"1600 bounded ranges over {r.dbsize()} keys, {rbad} mismatches")

# the ten keys of the one shard that lost them, on their own - the reduced case
# from DONE 312. A prefix longer than a node can store, and a search key that
# diverges inside the part the node never wrote down
r.execute_command("USE", "configuration")
r.set("ten.shards", "1")
r.execute_command("USE", "ten")
r.execute_command("FLUSHDB")
ten = ["fs:n:/catalogue/a", "fs:n:/catalogue/b/VDOWWMBEUG",
       "fs:n:/catalogue/baby/73UL7NDLZ7", "k7", "kAWqUB.4o6DwjlcozT7T./5t",
       "kKLAAGf4", "kPDu-m9ds0", "kRGY3on", "kWS_paHUu-nQ4_zWjHiZ", "kinufis"]
for k in ten:
    r.set(k, "v")
LO, HI = "fs:n:/catalog/baby/A6P3O5QFAT", "fs:n:/catalogue/baby/PI8LX3O1KD"
want = sorted(k.encode() for k in ten if LO <= k < HI)
assert r.execute_command("LB", LO) == want[0], \
    f"LB overshot: {r.execute_command('LB', LO)!r} for {LO!r}"
got = list(r.execute_command("RANGE", LO, HI, 0) or [])
assert got == want, f"the reduced case lost keys: {got!r}"

if bad:
    raise AssertionError(f"{bad} of {n} lower_bound answers disagree with bisect")
if rbad:
    raise AssertionError(f"{rbad} of 1600 ranges disagree with the same set")
print("lower bound and range ok")
