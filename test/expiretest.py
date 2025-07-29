import barch
import time
MAXK = 1000000
barch.clear()
print("cleared",barch.size(),barch.stats().logical_allocated)
barch.save()
print("saved",barch.size(),barch.stats().logical_allocated)

barch.setConfiguration("max_memory_bytes","400m")
barch.setConfiguration("active_defrag","on")
k = barch.KeyValue()
st1 = barch.stats()
print(barch.size(),barch.stats().logical_allocated)
for i in range(MAXK):
    k.set(str(i),str(i))
    k.expire(str(i),4)
    assert(k.ttl(str(i))>=2)
    if i%10000 == 0:
        st1 = barch.stats()
        print(barch.size(),st1.logical_allocated)

barch.setConfiguration("max_memory_bytes","20m")
barch.setConfiguration("maintenance_poll_delay","1")
print("sleeping")
st1 = barch.stats()
while st1.logical_allocated > 25*1024*1024:
    time.sleep(0.5)
    st1 = barch.stats()
    print(barch.size(),st1.logical_allocated)

stats = barch.stats()
print(stats.oom_avoided_inserts)
print(f"stats.keys_evicted {stats.keys_evicted}")
assert(stats.keys_evicted > MAXK/3)
assert(barch.size() <  MAXK)