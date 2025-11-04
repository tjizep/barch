import redis
import barch
print("start merge test")
barch.start("0.0.0.0", 14000)
# connect redis client to barch running inside this process
r = redis.Redis(host="127.0.0.0", port=14000, db=0)
r1 = redis.Redis(host="127.0.0.0", port=14000, db=0)
r.execute_command("USE dest")
r.set("a","desta")
r.set("b","destb")
r.set("c","destc")
r.set("d","destd")
assert(r.get("c") == b'destc')
r.execute_command("USE src")
r.execute_command("SPACES DEPENDS src ON dest")
r.set("a","srca")
assert(r.get("c") != None)
r.execute_command("REM c")
assert(r.get("c") == None)
r.execute_command("USE dest")
assert(r.get("c") == b'destc')
r.execute_command("SPACES MERGE src INTO dest")
assert(r.get("a") == b'srca')
assert(r.get("c") == None)
a = r
b = r1
a.execute_command("USE a")
b.execute_command("USE b")
assert (a.dbsize()==0)
assert (b.dbsize()==0)
for i in range(1,1000):
    a.set(f"{i}",f"${i}")
for i in range(1001,2000):
    b.set(f"{i}",f"${i}")
t = 0
for i in range(1,1000):
    assert(b.get(f"{i}") == None)
    t = t + 1
assert(t == 999)
a.execute_command("SPACES DEPENDS b ON a")
t = 0
for i in range(1,1000):
    assert(r1.get(f"{i}") != None)
    t = t + 1
assert (t == 999)
assert(b.dbsize() == 1998)
assert(a.dbsize() == 999)
print(b.dbsize())
assert(b.execute_command("COUNT 0 2000") == 1998)
assert(len(b.execute_command("RANGE 0 1000 100")) == 100)
#assert ( )
assert(a.execute_command("SPACES RELEASE a FROM b") == b'OK')
assert(a.execute_command("SPACES RELEASE dest FROM src") == b'OK')
assert(a.execute_command("SPACES DROP a") == b'OK')
assert(a.execute_command("SPACES DROP b") == b'OK')
assert(a.execute_command("SPACES DROP src") == b'OK')
assert(a.execute_command("SPACES DROP dest") == b'OK')
print("complete merge test")