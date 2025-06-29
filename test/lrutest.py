import barch
import time
MAXK = 1000000
barch.setConfiguration("max_memory_bytes","100m")
barch.setConfiguration("eviction_policy","allkeys-lru")
k = barch.KeyValue()
for i in range(MAXK):
    k.set(str(i),str(i))
assert(barch.size() == MAXK)
barch.setConfiguration("max_memory_bytes","1m")
print("sleeping")
for i in range(10):
    time.sleep(1)
    print(barch.size())
assert(barch.size() <  MAXK)