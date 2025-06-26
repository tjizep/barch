import time
from sortedcontainers import SortedDict
k = SortedDict()
stime = time.time()
for i in range(1000000):
    si = str(i)
    k[si] = si
l = 0
for i in range(1000000):
    si = str(i)
    if k.get(si)==si:
        l+=1
print(time.time()-stime,l)
