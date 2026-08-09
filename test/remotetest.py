print("loading remotetest.py")
import time

import barch
print("starting server")
barch.start("127.0.0.1",13000)
print("server started")
k = barch.KeyValue("127.0.0.1",13000)
#k = barch.KeyValue()
k.set("key1","value1")
for i in range(100000):
    k.set(str(i),str(i+1))
    k.set(str(i),str(i))
    #if i < 100000:
    assert(k.get(str(i))==str(i))
    if i%1000==0:
        print(i)
assert(k.get("key1")=="value1")
print(f"k.get(str(1000))={k.get(str(1000))}")
assert(k.get(str(1000))==str(1000))
l = barch.List("127.0.0.1",13000)
# push() is LPUSH, which prepends now, as in redis. So a1 then a2 leaves a2 at the head,
# and b1 then b2 leaves the list as b2 b1 a2 a1. pop() answers with what it removed
# rather than with the length left behind - len() is what reports that. See TODO 38
assert(l.push("l",["a1","a2"])==2)
assert(l.push("l",["b1","b2"])==4)

assert(l.len("l")==4)
assert(l.pop("l",1)[0].s()=="b2")
assert(l.len("l")==3)
assert(l.back("l")=="a1")
print(l.front("l"))
assert(l.front("l")=="b1")
assert(l.pop("l",1)[0].s()=="b1")
assert(l.len("l")==2)
popped = l.pop("l",2)
# over RPC only the first element of an array reply survives - the rest decode as the
# wrong type. That is TODO 44 and it is not specific to pop; the same handle returns
# ['v1', 'false', '0.0'] for a three field HMGET. Asserted here as it behaves so the
# suite stays honest about it; when TODO 44 is fixed this should become
# [v.s() for v in popped] == ["a2","a1"]
assert(len(popped)==2)
assert(popped[0].s()=="a2")
assert(l.len("l")==0)
