
import barch
import redis
import time

MAXK = 20000

barch.start("0.0.0.0", 15000)
gr = redis.Redis(host="127.0.0.0", port=15000, db=0)
gr.flushdb()
def test():
    r = redis.Redis(host="127.0.0.0", port=15000, db=0)
    r.flushdb()
    for i in range(MAXK):
        r.set(f"KEY:{str(i)}",str(i))
        if i%1000 == 0:
            print(barch.size(),i)
    count = 0
    for key in r.scan_iter("KEY:*"):
        print(key,count)
        count += 1
    print(count)
    assert count == MAXK
test()