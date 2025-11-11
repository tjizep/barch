import redis
import barch
import threading
import time
def btest(num):
    r = redis.Redis("127.0.0.1", 11000, 0)
    time.sleep(.1)
    r.lpush("testkey","l2")
    time.sleep(.1)
    r.lpush("testkey1","l3")
    time.sleep(.1)
    r.lpush("testkey2","l4")
def ctest(num):
    r = redis.Redis("127.0.0.1", 11000, 0)
    popped = r.blpop(["testkey1"],10)
    print ("c",popped)
    assert (popped == None or (popped[0] == b'testkey1' and popped[1] == b'l3'))
def tloss(num):
    r = redis.Redis("127.0.0.1", 11000, 0)
    for i in range(1,1000):
        r.lpush("testloss",f"l{i}")
        #print(f"Pushed {i}")
barch.clear()
barch.save()
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
assert (rp.blpop(["nonekey"],0.1)== None)
tl = threading.Thread(target=tloss, args=(1,))
tl.start()
i = 0
for i in range(1,1000):
    pp = rp.brpop(["testloss"],0.1)
    if(not pp == None):
        assert(f"l{i}" == pp[1].decode('utf-8'))
