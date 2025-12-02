import threading

import barch
import redis
import time
import os
print(f'running {__file__}')
exec(open(f"{os.path.dirname(os.path.realpath(__file__))}/test_data.py").read())

barch.start("0.0.0.0", 15000)
gr = redis.Redis(host="127.0.0.0", port=15000, db=0)
gr.config_set("compression", "zstd")
gr.flushdb()
def test(num):
    r = redis.Redis(host="127.0.0.0", port=15000, db=0)
    r.flushdb()
    tr = 512000
    for w in words:
        left = r.execute_command(f'TRAIN {train_set[w]}')
        assert (left < tr)
        tr = left
        print (left)
    assert (r.execute_command(f'TRAIN') == 0) # this will save a file called barch_dict.dat in the current dir


    for w in words:
        print(f"collecting test data for: {w}")
        r.set(w, test_set[w])
    r.execute_command('SAVE')
    for w in words:
        print(f"testing:{w}")
        assert r.get(w) == test_set[w]
    print(f"exit thread {num}")

t = [
    threading.Thread(target=test, args=(1,))
]

for i in t:
    i.start()

time.sleep(1)

for i in t:
    i.join()

assert(barch.stats().value_bytes_compressed > 0)
