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


# DICTIONARY GET/SET - TODO 415. A backup that copies only the shard files would
# lose the dictionary, and with it every compressed value, so it can be read out
# and put back.
d = r.execute_command("DICTIONARY", "GET")
assert isinstance(d, bytes) and len(d) > 0, d
# the same one again is fine, a different one is refused: the values compressed
# here need this one
assert r.execute_command("DICTIONARY", "SET", d) == b"OK"
try:
    r.execute_command("DICTIONARY", "SET", d[:-1] + bytes([d[-1] ^ 0xFF]))
    assert False, "a different dictionary was accepted"
except redis.ResponseError as e:
    assert "different dictionary" in str(e), e

# a space that never trained has none, until one is put back
other = redis.Redis(host="127.0.0.0", port=PORT, db=0, protocol=2)
other.execute_command("USE", "dictcopy")
assert other.execute_command("DICTIONARY", "GET") is None
assert other.execute_command("DICTIONARY", "SET", d) == b"OK"
assert other.execute_command("DICTIONARY", "GET") == d
# saved the way training saves, so it survives a restart
assert os.path.exists("barch_dict_dictcopy_.dat")
try:
    other.execute_command("DICTIONARY", "SET", b"")
    assert False, "an empty dictionary was accepted"
except redis.ResponseError:
    pass
print("DICTIONARY GET/SET ok")
