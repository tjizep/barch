import os
import shutil
import scale
import time
import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()
# and start it empty, so a run never depends on what the last one saved - TODO 529
for entry in os.listdir("."):
    if os.path.isdir(entry):
        shutil.rmtree(entry)
    else:
        os.remove(entry)

# A stored function must never be evicted. It is a command, not data: losing one to
# memory pressure deletes a command, and because a session keeps whatever it compiled,
# the connections that already ran it would carry on while new ones met "unknown
# command". Defragmenting one is fine and has to keep working - see TODO 98.
#
# This runs in a process of its own because it drops maxmemory far enough to make the
# sweeper take almost everything, which no other test would survive.

PORT = scale.port(default=14083)
KEYS = 500

barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0)

print("start function eviction test")
# One shard, so the second phase below can empty the space. Each shard checks the
# global threshold before it takes a page, and with 17 of them the last to check
# finds memory already under it and rightly keeps its 20 to 30 keys - which could
# be the shard the function is on - TODO 530.
conf = redis.Redis(host="127.0.0.1", port=PORT, db=0)
conf.execute_command("USE", "configuration")
conf.execute_command("SET", "evspace.shards", "1")
r.execute_command("USE", "evspace")


def stat(name):
    s = r.execute_command("STATS")
    for i in range(0, len(s) - 1, 2):
        k = s[i].decode() if isinstance(s[i], bytes) else str(s[i])
        if k.lstrip("$") == name:
            return int(s[i + 1])
    raise AssertionError("no such stat: " + name)


r.execute_command("SETF", "survivor", 'function call() return "alive" end')
for i in range(KEYS):
    r.execute_command("SET", "plain%d" % i, "x" * 64)

# eviction only runs once logical_allocated passes max_memory * pre_evict_thresh, so
# the ceiling comes down under what is already held rather than allocating past it
r.execute_command("KSPACE", "OPTION", "SET", "LRU", "ON")
held = stat("logical_allocated")
r.execute_command("CONFIG", "SET", "maxmemory", str(max(held // 2, 4096)))

# maintenance sweeps every 80ms; wait for it to actually take something, so a run
# where eviction never fired fails rather than passing for the wrong reason
before = stat("keys_evicted")
deadline = time.time() + 30
while time.time() < deadline and stat("keys_evicted") == before:
    time.sleep(0.2)
took = stat("keys_evicted") - before
assert took > 0, "eviction never ran, so this test proved nothing"

left = sum(1 for i in range(KEYS) if r.execute_command("EXISTS", "plain%d" % i))
assert left < KEYS, f"the sweeper took nothing: {left} of {KEYS} plain keys left"

# The sweep above stops once memory is back under half, so it may just miss the
# function - TODO 529. So the ceiling goes as low as it will and the sweep runs until
# no plain key is left: on one shard, an evictable function would have gone too.
r.execute_command("CONFIG", "SET", "maxmemory", "4096")
deadline = time.time() + 60
remaining = KEYS
while time.time() < deadline:
    remaining = sum(1 for i in range(KEYS) if r.execute_command("EXISTS", "plain%d" % i))
    if remaining == 0:
        break
    time.sleep(0.5)
assert remaining == 0, f"{remaining} plain keys outlived the sweep, so it proved nothing" \
                       " about the function"

# and through all of that the function is untouched: still stored, still listed, and
# still runnable
assert r.execute_command("GETF", "survivor") is not None, "the function was evicted"
assert r.execute_command("survivor").decode() == "alive", "the function stopped working"
assert [k.decode() for k in r.execute_command("KEYSF")] == ["SURVIVOR"]

# every plain key went and nothing else did, so that's the count. Defrag lifts a key
# out and puts it back, and that used to be counted as an eviction too: the function
# moved once and the count said 501 - TODO 535
counted = stat("keys_evicted") - before
assert counted == KEYS, f"keys_evicted went up by {counted} for {KEYS} evicted keys"

print("evicted %d keys, %d of %d plain keys left, the function survived" % (took, left, KEYS))
r.close()
conf.close()
barch.stop()
print("complete function eviction test")
