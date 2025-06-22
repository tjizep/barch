import barch
import time
print("keyset size:",barch.size())
k = barch.KeyValue()

stime = time.time()
l = 0
for i in range(1000000):
    si = str(i)
    if k.get(si)==si:
        l+=1
print("read",time.time()-stime,l)
