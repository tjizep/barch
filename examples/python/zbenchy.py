import barch
import time
k = barch.KeyValue()
k.set("0","0")

stime = time.time()

for i in range(1000000):
    si = str(i)
    k.set(si,si)
l = 0
for i in range(1000000):
    si = str(i)
    if k.get(si)==si:
        l+=1
print(time.time()-stime,l)
barch.save()