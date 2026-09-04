
import threading

import scale
import barch
import redis
import time

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

PORT = scale.port(default=15000)
print("start thread test")
barch.start("0.0.0.0", PORT)
gr = redis.Redis(host="127.0.0.0", port=PORT, db=0, protocol=2)
gr.flushdb()
gr.select(0)
gr.select("g") #Yes! we can select strings too
gr.set("g","vg")
def testspace(num):
    r = redis.Redis(host="127.0.0.0", port=PORT, db=0, protocol=2)
    for i in range(1, scale.scaled(1000, floor=50)):
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

time.sleep(3)

for i in t:
    i.join()
r = redis.Redis(host="127.0.0.0", port=PORT, db=0, protocol=2)

