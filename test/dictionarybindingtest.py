# barch.store.getDictionary() / setDictionary(buffer) - the space's zstd
# dictionary from inside a stored function. The same pair the DICTIONARY command
# reads and writes, so a backup taken by a script can carry it. Both answer nil
# when compression is off. See TODO 458.
import scale

import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

PORT = scale.port(default=15060)

print("start dictionary binding test", flush=True)
barch.start("0.0.0.0", PORT)

r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.response_callbacks = {}
r.execute_command("FLUSHDB")

SRC = '''
function call(which, arg)
    if which == "get" then
        return barch.store.getDictionary()
    elseif which == "set" then
        return barch.store.setDictionary(arg)
    end
    return nil
end
'''
assert r.execute_command("SETF", "dict", SRC) == b"OK"

# --- compression off: both answer nil -----------------------------------------
assert r.execute_command("CALLF", "dict", "get") is None, \
    "getDictionary should be nil with compression off"
assert r.execute_command("CALLF", "dict", "set", b"not a dictionary") is None, \
    "setDictionary should be nil with compression off"

# --- compression on, and a dictionary trained ---------------------------------
assert r.execute_command("CONFIG", "SET", "compression", "zstd") == b"OK"
# on, but nothing trained yet: still nil rather than an empty buffer
assert r.execute_command("DICTIONARY", "GET") is None, "no dictionary before training"
assert r.execute_command("CALLF", "dict", "get") is None, \
    "getDictionary should be nil before anything is trained"
# zstd's trainer refuses a corpus that is one repeated string, so the samples
# vary a little as they go in
left = 512000
n = 0
while left > 0:
    chunk = f"sample {n} ".encode() + bytes([(n * 7 + i) % 251 + 1 for i in range(30000)])
    left = r.execute_command("TRAIN", chunk)
    n += 1
    assert n < 60, "the dictionary never trained"

d = r.execute_command("DICTIONARY", "GET")
assert isinstance(d, bytes) and len(d) > 0, d

got = r.execute_command("CALLF", "dict", "get")
assert got == d, "getDictionary should be the same bytes DICTIONARY GET answers"

# putting the same one back is accepted
assert r.execute_command("CALLF", "dict", "set", d) == 1, \
    "setDictionary should accept the space's own dictionary"

# a different one is refused: the compressed values here need this one
try:
    bad = d[:-1] + bytes([d[-1] ^ 0xFF])
    r.execute_command("CALLF", "dict", "set", bad)
    assert False, "a different dictionary was accepted"
except redis.exceptions.ResponseError:
    pass

# a write via the binding does not disturb the dictionary or the data
assert r.execute_command("DICTIONARY", "GET") == d
assert r.execute_command("SET", "after", "value") == b"OK"
assert r.execute_command("GET", "after") == b"value"

print("dictionary binding test passed")
