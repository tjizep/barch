# A key past its expiry is gone, whether or not the sweep has taken it yet - TODO 980.
#
# Expiry was only enforced by the sweep in shard maintenance, which samples keys at
# random. Until it got to one, an overdue key still answered GET and EXISTS, and PTTL
# said 0. barch.store.get in a function already said nil. A cache built on barch
# couldn't rely on expiry alone.
#
# No waiting: SET ... PXAT 1 and PEXPIREAT k 1 store a deadline already in the past,
# and the sweep is held off so it can't be what hides the key.
import scale
import redis
import barch

scale.workdir()
PORT = scale.port(default=14903)

print("start overdue test", flush=True)
barch.setConfiguration("maintenance_poll_delay", "3600000")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=None)
r.execute_command("FLUSHDB")
r.execute_command("SETF", "fnget", "function call(k) return barch.store.get(k) end")

checks = 0


def gone(k):
    global checks
    assert r.get(k) is None, ("GET", k, r.get(k))
    assert r.exists(k) == 0, ("EXISTS", k)
    assert r.pttl(k) == -2, ("PTTL", k, r.pttl(k))
    assert r.ttl(k) == -2, ("TTL", k, r.ttl(k))
    assert r.execute_command("fnget", k) is None, ("barch.store.get", k)
    checks += 5


print("a deadline in the past hides the key", flush=True)
assert r.execute_command("SET", "a", "v", "PXAT", "1")
gone("a")
assert r.set("b", "v")
assert r.execute_command("PEXPIREAT", "b", "1") in (0, 1)
gone("b")

print("a write over an overdue key starts afresh", flush=True)
assert r.execute_command("SET", "c", "old", "PXAT", "1")
# NX sees no key, so it writes
assert r.execute_command("SET", "c", "new", "NX")
assert r.get("c") == b"new" and r.pttl("c") == -1, (r.get("c"), r.pttl("c"))
assert r.execute_command("SET", "d", "1", "PXAT", "1")
# INCR starts from nothing, not from the overdue value
assert r.incr("d") == 1
checks += 3

print("a live key with a deadline is still there", flush=True)
assert r.set("e", "v", px=3600000)
assert r.get("e") == b"v" and r.exists("e") == 1 and 0 < r.pttl("e") <= 3600000
checks += 1

print("overdue test complete, %d checks" % checks, flush=True)
barch.stop()
