# A stored file goes whole, or not at all - TODO 302.
#
# A file in the store is four kinds of key: the name record, the inode, one key per
# chunk, and the space's fs:layout marker. The key level eviction sweep works one
# leaf at a time and knows nothing about that, so left alone it will happily take a
# chunk out of the middle of a live file. Two of the ways it can pick are worse than
# ordinary data loss:
#
#   - a chunk goes and the name and inode still promise it, so every read of that
#     file fails from then on;
#   - the name record goes and the inode and every chunk are stranded with nothing
#     able to name them, so the smallest key of the set is freed and all the big
#     ones stay resident. Under memory pressure it makes memory pressure worse.
#
# So the sweep is refused fs: keys outright (shard.cpp may_evict), and whole file
# eviction happens on the space maintenance thread instead, where the name, the
# inode and every chunk go into one staged commit.
#
# Its own process, like functionevicttest.py and for the same reason: it drops
# maxmemory far enough that the sweeper takes almost everything, which no other test
# would survive.
import time

import scale
import redis
import barch

scale.workdir()

PORT = scale.port(default=14320)
KEYS = 500
CHUNK = 64                      # small, so a hand written file is many chunk keys

barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0)

print("start fs eviction test", flush=True)


def stat(name):
    s = r.execute_command("STATS")
    for i in range(0, len(s) - 1, 2):
        k = s[i].decode() if isinstance(s[i], bytes) else str(s[i])
        if k.lstrip("$") == name:
            return int(s[i + 1])
    raise AssertionError("no such stat: " + name)


conf = redis.Redis(host="127.0.0.1", port=PORT, db=0)
conf.execute_command("USE", "configuration")
conf.execute_command("SET", "evfs.ordered", "1")
conf.execute_command("SET", "evfs.fs_source", "gen")

r.execute_command("USE", "evfs")
r.execute_command("SETF", "gen", """function call(path)
    if string.sub(path, 1, 5) == "/gen/" then return string.rep("g", 4000) end
    return nil
end""")

# the one that must come through untouched: written by hand, so nothing can produce
# it again, and long enough to be many chunks rather than one
MINE = "".join("%04d|" % n for n in range(400))
r.execute_command("SETF", "put", """function call(path, body, chunk)
    return barch.fs.put(path, body, "text/plain", tonumber(chunk))
end""")
r.execute_command("put", "/mine/big.txt", MINE, str(CHUNK))
assert r.execute_command("FS", "GET", "/mine/big.txt").decode() == MINE

# and a set of fetched ones, which are the source's copies and so may go
for n in range(12):
    assert r.execute_command("FS", "FETCH", "/gen/f%d" % n) is not None
    time.sleep(0.01)                             # so the fetch stamps differ
held_files = int(r.execute_command("GET", "fs:cache"))
assert held_files == 12 * 4000, held_files

for i in range(KEYS):
    r.execute_command("SET", "plain%d" % i, "x" * 64)

# Eviction only runs once logical_allocated passes max_memory * pre_evict_thresh, so
# the ceiling comes down under what is already held rather than allocating past it.
# Halved, the way functionevicttest.py does it, so the sweeper takes almost
# everything and every fs: key it could have picked is on the table. LRU is set on
# this space rather than through CONFIG SET, which only reaches the default space's
# shards.
print("under pressure hard enough to take almost everything", flush=True)
r.execute_command("KSPACE", "OPTION", "SET", "LRU", "ON")
held = stat("logical_allocated")
r.execute_command("CONFIG", "SET", "maxmemory", str(max(held // 2, 4096)))

# wait for the sweep to actually take something, so a run where eviction never fired
# fails rather than passing for the wrong reason
before = stat("keys_evicted")
deadline = time.time() + 30
while time.time() < deadline and stat("keys_evicted") == before:
    time.sleep(0.2)
took = stat("keys_evicted") - before
assert took > 0, "eviction never ran, so this test proved nothing"

left = sum(1 for i in range(KEYS) if r.execute_command("EXISTS", "plain%d" % i))
assert left < KEYS, "the sweeper took nothing: %d of %d plain keys left" % (left, KEYS)

# files went too, and by the only route that is allowed to take one: the whole file
# pass on the maintenance thread. Note this happens with every write past the hard
# limit - freeing a file has to remove keys, and removing them allocates nothing,
# which is why that pass does its own removes instead of going through the staged
# path a user facing delete uses
deadline = time.time() + 30
while time.time() < deadline and stat("files_evicted") == 0:
    time.sleep(0.2)
files = stat("files_evicted")
assert files > 0, "no file was ever evicted, so the maintenance pass never ran"

# THE POINT. Not one of the files that is still here lost a piece. A listing entry
# with no readable body, or a short one, is exactly the half evicted state the key
# level sweep would have produced if it were allowed near an fs: key.
names = [x.decode().split()[-1] for x in r.execute_command("FS", "LS", "/gen")]
assert len(names) < 12, "no file went, so nothing was proved about how they go"
for name in names:
    got = r.execute_command("FS", "GET", "/gen/" + name)
    assert got is not None and len(got) == 4000, (name, got and len(got))

# and the hand written one was never a candidate: nothing can produce it again, so
# dropping it would be deletion rather than eviction
body = r.execute_command("FS", "GET", "/mine/big.txt")
assert body is not None, "the hand written file was evicted - it has no other copy"
assert body.decode() == MINE, "the hand written file came back short or changed"

print("evicted %d keys and %d whole files; %d of %d plain keys and %d of 12 files "
      "left, every one of them readable in full"
      % (took, files, left, KEYS, len(names)), flush=True)
r.close()
conf.close()
barch.stop()
print("complete fs eviction test", flush=True)
