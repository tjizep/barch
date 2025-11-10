import redis
import barch
import threading
import time
def btest(num):
    r = redis.Redis("127.0.0.1", 11000, 0)
    time.sleep(1)
    r.lpush("testkey","l2")
    time.sleep(1)
    r.lpush("testkey1","l3")
    time.sleep(1)
    r.lpush("testkey2","l4")
def ctest(num):
    r = redis.Redis("127.0.0.1", 11000, 0)
    popped = r.blpop(["testkey1"],10)
    print ("c",popped)
    assert (popped == None or (popped[0] == b'testkey1' and popped[1] == b'l3'))

barch.start("0.0.0.0", 11000)

bt = threading.Thread(target=btest, args=(1,))
ct = threading.Thread(target=ctest, args=(1,))

rp = redis.Redis("127.0.0.1", 11000, 0)
bt.start()
ct.start()
popped = rp.blpop(["testkey"],10)
print("a",popped)
assert (popped[0] == b'testkey' and popped[1] == b'l2')
popped = rp.blpop(["testkey1","testkey"],10)
print("b",popped)
assert (popped == None or (popped[0] == b'testkey1' and popped[1] == b'l3'))
assert (rp.blpop(["nonekey"],1)== None)