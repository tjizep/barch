import scale
import threading

import barch
import redis
import time
import os

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

PORT = scale.port(default=15000)
print(f'running {__file__}')
exec(open(f"{os.path.dirname(os.path.realpath(__file__))}/test_data.py").read())

barch.start("0.0.0.0", PORT)
gr = redis.Redis(host="127.0.0.0", port=PORT, db=0, protocol=2)
gr.config_set("compression", "zstd")
gr.flushdb()
def test(num):
    r = redis.Redis(host="127.0.0.0", port=PORT, db=0, protocol=2)
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

# Compression is no longer part of SET - a write stores what it was given and the
# background pass in shard.cpp compresses cold keys on a later maintenance tick.
# See TODO 300. So this has to wait for a tick rather than assert straight away,
# which is what it did while SET compressed inline.
deadline = time.time() + 30
while time.time() < deadline and barch.stats().value_bytes_compressed == 0:
    time.sleep(0.25)
compressed = barch.stats().value_bytes_compressed
print(f"background pass compressed {compressed} bytes")
assert compressed > 0, "the background compression pass never ran"

# and the values still read back, compressed or not
r = redis.Redis(host="127.0.0.0", port=PORT, db=0, protocol=2)
for w in words:
    assert r.get(w) == test_set[w], f"value changed for {w}"

