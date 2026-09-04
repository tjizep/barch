# SCAN, and the promise it makes while the key space is being written to.
#
# Hand written rather than translated. valkey's unit/scan.tcl is twenty tests and the
# translator in test/valkeytrans gets none of them, because every one is built out of
# while loops, `lappend` and `populate` - see TODO 40. Half of the file is SSCAN, HSCAN
# and ZSCAN, which barch does not implement, and a good part of the rest is multiplied
# out over valkey's internal encodings (intset, listpack, hashtable, skiplist), which say
# nothing about barch. So this is the part of that file that is about a promise barch
# actually makes, written out by hand.
#
# The one that matters is the last: a full iteration has to report every key that was
# there when it started and is still there when it ends, however much is written in the
# meantime. That is what a cursor is for, and barch has real cursor machinery behind it -
# the cursor is split between the connection and the store (DONE 18), a connection may
# only hold so many at once, and an abandoned one holds a page buffer until the
# connection closes. None of that was covered by a test.
import scale
import time

import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

PORT = scale.port(default=14099)
SPACE = "scantest"

barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

r = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True, socket_timeout=15)
r.execute_command("USE", SPACE)
r.execute_command("FLUSHDB")

print("start scan test")

checks = 0


# redis-py recognises SCAN and runs its own parser over the reply, which turns the cursor
# into an int - the wire carries a bulk string, `$5\r\n65536`. So the end of an iteration
# is `str(cursor) == "0"`, not `cursor == "0"`, which quietly never matches and walks the
# key space forever. redis-py does the same kind of thing to INFO
def finished(cursor):
    return str(cursor) == "0"


def scan_all(*options):
    """every key a full iteration reports, and how many round trips it took"""
    cursor = "0"
    seen = []
    trips = 0
    while True:
        reply = r.execute_command("SCAN", cursor, *options)
        cursor, keys = reply[0], reply[1]
        seen.extend(keys)
        trips += 1
        if finished(cursor):
            return seen, trips
        assert trips < 10000, "SCAN did not finish - cursor never came back to 0"


def populate(prefix, n):
    for i in range(n):
        r.execute_command("SET", "%s%d" % (prefix, i), str(i))


def check(what, condition):
    global checks
    assert condition, what
    checks += 1
    print("  ok: %s" % what)


# --- a full iteration reports everything ------------------------------------------
populate("key:", 1000)
seen, trips = scan_all()
check("a full iteration reports all 1000 keys, in %d round trips" % trips,
      len(set(seen)) == 1000)
check("and reports nothing that was not there", set(seen) == {"key:%d" % i for i in range(1000)})

# --- COUNT is a hint about page size, not a limit on the whole walk ----------------
small, small_trips = scan_all("COUNT", 5)
check("COUNT 5 still reports all 1000 keys", len(set(small)) == 1000)
check("and takes more round trips than the default did (%d vs %d)" % (small_trips, trips),
      small_trips > trips)

# --- MATCH filters, and does not lose anything that matches ------------------------
matched, _ = scan_all("MATCH", "key:1?")
expected = {"key:1%d" % d for d in range(10)}
check("MATCH key:1? reports exactly the ten keys that match",
      set(matched) == expected)

# --- expired keys are not reported -------------------------------------------------
r.execute_command("SET", "gone:1", "v")
r.execute_command("SET", "gone:2", "v")
r.execute_command("EXPIRE", "gone:1", "1")
r.execute_command("EXPIRE", "gone:2", "1")
time.sleep(1.5)
after, _ = scan_all("MATCH", "gone:*")
check("keys that have expired are not reported by a later scan", after == [])

# --- the guarantee, which is the reason this file exists ---------------------------
# Start with a known set, then write to the space on every round trip. Everything that was
# there at the start and is still there at the end has to be reported, whatever else the
# walk happens to pick up along the way.
r.execute_command("FLUSHDB")
populate("stable:", 100)

# COUNT 10 on purpose. barch's default page is 128, so a hundred keys come back in a
# single round trip and the writes below never happen while a walk is in progress - the
# check would pass without testing anything. valkey's version relies on redis defaulting
# to 10; here it has to be asked for
cursor = "0"
seen = []
iterations = 0
while True:
    reply = r.execute_command("SCAN", cursor, "COUNT", 10)
    cursor, keys = reply[0], reply[1]
    seen.extend(keys)
    iterations += 1
    if finished(cursor):
        break
    # ten new keys per round trip, names that cannot collide with the stable set
    for j in range(10):
        r.execute_command("SET", "added:%d:%d" % (iterations, j), "foo")
    assert iterations < 10000, "SCAN did not finish under write load"

stable_seen = {k for k in seen if k.startswith("stable:")}
missing = {"stable:%d" % i for i in range(100)} - stable_seen
check("the walk really did page, so the writes landed mid-iteration (%d round trips)"
      % iterations, iterations > 3)
check("all 100 keys present throughout are reported despite writes on every "
      "round trip (%d iterations, %d keys seen)" % (iterations, len(set(seen))),
      not missing)

r.execute_command("FLUSHDB")
r.close()
print("scan test ok - %d checks" % checks)

# Not covered, deliberately:
#
#   - SSCAN, HSCAN and ZSCAN. barch registers none of them, so there is nothing to test.
#   - SCAN TYPE. The option parses, but there is no TYPE command to establish what a key's
#     type is, so an assertion here would only be describing barch to itself.
#   - valkey's encoding variants. intset against hashtable and listpack against skiplist
#     are its internal representations; barch does not have them and never will.
#   - the cluster slot case, for the same reason.
