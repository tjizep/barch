
import barch
import redis
import time
import random

MAXK = 20000

barch.start("0.0.0.0", 15000)
gr = redis.Redis(host="127.0.0.0", port=15000, db=0, protocol=2)
gr.flushdb()
def test():
    r = redis.Redis(host="127.0.0.0", port=15000, db=0, protocol=2)
    r.flushdb()
    r.execute_command("USE a")
    for i in range(MAXK):
        r.set(f"KEY:{str(i)}",str(i))
        if i%1000 == 0:
            print(barch.size(),i)
    count = 0
    for key in r.scan_iter("KEY:*",count=10):# maxk is divisible by 10 on purpose
        print(key,count)
        count += 1
    print(count)
    assert count == MAXK
    count = 0
    for key in r.scan_iter("KEY:*",count=random.randint(1,243)):
        if count%1000 == 0:
            print(key,count)
        count += 1
    print(count)
    assert count == MAXK
    count = 0
    r.execute_command("USE b")
    r.execute_command("SPACES DEPENDS b ON a")
    i = 1
    r.set(f"KEY:{str(i)}",str(i))
    for key in r.scan_iter("KEY:*",count=random.randint(1,333)):
        if count%1000 == 0:
            print(key,count)
        count += 1
    print(count)
    assert count == MAXK
    count = 0

    i = 3

    r.delete(f"KEY:{str(i)}")
    for key in r.scan_iter("KEY:*",count=random.randint(1,553)):
        if count%1000 == 0:
            print(key,count)
        count += 1
    print(count)
    assert count == MAXK - 1
test()