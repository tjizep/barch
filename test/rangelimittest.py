# RANGE with a limit must return the keys it was asked for - TODO 323, DONE 311.
#
# This was the reproducer for a walk that stopped early and filled the answer up
# from beyond the window: the right number of keys, in order, without duplicates,
# and four of the thousand missing. The count being right is what made it quiet,
# so this checks membership at several limits and not the count.
import random, string, sys
sys.path.insert(0, "/home/test/barch/test")
import scale, redis, barch
scale.workdir()
PORT = scale.port(default=14977)
barch.start("0.0.0.0", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.config_set("internal_shards", "1")
r.execute_command("USE", "skip")
r.execute_command("FLUSHDB")
rnd = random.Random(11)
keys = sorted({"k%05d" % i for i in range(1000)})
p = r.pipeline(transaction=False)
for k in keys: p.set(k, "v")
p.execute()
print("keys", r.dbsize())
for limit in (255, 512, 1000):
    want = [k.encode() for k in keys][:limit]
    got = list(r.execute_command("RANGE", keys[0], "l", limit) or [])
    missing = [i for i,(a,b) in enumerate(zip(want,got)) if a!=b]
    lost = sorted(set(want) - set(got))
    dups = len(got) - len(set(got))
    print(f"  limit={limit:4d}: got {len(got)} ({dups} duplicates), first mismatch "
          f"at {missing[0] if missing else None}, lost {len(lost)}")
    assert dups == 0, f"limit {limit} returned {dups} duplicates"
    assert not lost, f"limit {limit} lost {len(lost)} keys: {[x.decode() for x in lost[:6]]}"
    if missing:
        i = missing[0]
        print(f"    want[{i-2}:{i+4}] = {[x.decode() for x in want[i-2:i+4]]}")
        print(f"    got [{i-2}:{i+4}] = {[x.decode() for x in got[i-2:i+4]]}")
        print(f"    monotonic: {all(got[j] <= got[j+1] for j in range(len(got)-1))}")

print("range limits ok")
