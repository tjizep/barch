import scale
import barch
import time

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()
MAXK = 1000000
barch.clear()
barch.save()
barch.setConfiguration("max_memory_bytes","300m")
barch.setConfiguration("eviction_policy","allkeys-lru")
k = barch.KeyValue()
for i in range(MAXK):
    k.set(str(i),str(i))
print(barch.size())
assert(barch.size() == MAXK)
barch.setConfiguration("max_memory_bytes","1m")
print("sleeping")
for i in range(1):
    time.sleep(3.5)
    print(barch.size())
assert(barch.size() < MAXK)
for i in range(MAXK+1,MAXK + 100000):
    try:
        k.set(str(i),str(i))
    except Exception:
        pass

stats = barch.stats()
print(stats.oom_avoided_inserts)
assert(stats.oom_avoided_inserts > 0)
assert(barch.size() <  MAXK)