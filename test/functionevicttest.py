import time
import redis
import barch

# A stored function must never be evicted. It is a command, not data: losing one to
# memory pressure deletes a command, and because a session keeps whatever it compiled,
# the connections that already ran it would carry on while new ones met "unknown
# command". Defragmenting one is fine and has to keep working - see TODO 98.
#
# This runs in a process of its own because it drops maxmemory far enough to make the
# sweeper take almost everything, which no other test would survive.

PORT = 14083
KEYS = 500

barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0)

print("start function eviction test")
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

# and through all of that the function is untouched: still stored, still listed, and
# still runnable
assert r.execute_command("GETF", "survivor") is not None, "the function was evicted"
assert r.execute_command("survivor").decode() == "alive", "the function stopped working"
assert [k.decode() for k in r.execute_command("KEYSF")] == ["SURVIVOR"]

print("evicted %d keys, %d of %d plain keys left, the function survived" % (took, left, KEYS))
r.close()
barch.stop()
print("complete function eviction test")
