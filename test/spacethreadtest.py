import threading
import barch
import redis
import time

barch.start("0.0.0.0", 15000)
gr = redis.Redis(host="127.0.0.0", port=15000, db=0)
gr.execute_command("USE g")
gr.set("g","vg")
def testspace(num):
    r = redis.Redis(host="127.0.0.0", port=15000, db=0)
    r.flushdb()

    for i in range(1,1000):
        r.execute_command(f"USE t{num}")
        r.execute_command(f"SPACES DEPENDS t{num} ON g")
        r.set(f"t{i}",f"{i}")
        assert (r.dbsize() > 0)
        assert (r.execute_command("g:GET g") == b'vg')
        assert (r.get("g") == b'vg')
    assert (r.execute_command(f"SPACES EXIST t{num}") == 1)
    assert(r.execute_command(f"SPACES DROP t{num}")==b'OK')
    print(f"exit thread {num}")

t = [
threading.Thread(target=testspace, args=(1,)),
threading.Thread(target=testspace, args=(2,)),
threading.Thread(target=testspace, args=(3,)),
threading.Thread(target=testspace, args=(4,)),
threading.Thread(target=testspace, args=(5,)),
]

for i in t:
    i.start()

time.sleep(1)

for i in t:
    i.join()
r = redis.Redis(host="127.0.0.0", port=15000, db=0)

assert(r.execute_command("SPACES EXIST t1") == 0)
assert(r.execute_command("SPACES EXIST g") == 1)

