import time

import scale
import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

# A locked region that reads a range - TODO 497.
#
# barch.store.locked(key, fn) write-locks the key's shard, or every shard when no key
# is named, and everything inside fn is meant to see that and skip its own lock. The
# point reads did. min, max and range didn't: they lock every shard shared, and a
# shared lock on a shard this thread holds for writing waited on its own write for
# the full lock timeout (60 s), holding the shard the whole time, then threw.
#
# Now the two lock types ask the region first. A shard it holds is skipped. Another
# shard of the same space is the cross shard case, and fails straight away rather
# than waiting, because a region on one shard waiting for the rest can deadlock with
# anything that takes the whole space in order. So:
#   1. in a region on the whole space, min, max and range answer, and quickly
#   2. what they answer is what they answer outside it
#   3. in a region on one key they need the other shards too, so they're refused
#      quickly, naming the locked region
#   4. either way the shard isn't left held: another connection can write to it

PORT = scale.port(default=14000)
# well under the 60 s the old code waited, and well over what an answer takes
QUICK = 5.0

print("start locked read test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=20)
    r.flushdb()
    for i in range(100):
        r.set(f"lr{i:03d}", str(i))

    reads = {
        "min": "return barch.store.min()",
        "max": "return barch.store.max()",
        "range": 'return table.concat(barch.store.range("lr010", "lr013", 10), ",")',
    }
    for name, body in reads.items():
        # the same read three ways: outside a region, in a region on one key, and in
        # a region on the whole space
        assert r.execute_command("SETF", f"plain{name}", f"""
            function call(k) {body} end
        """) == b"OK"
        assert r.execute_command("SETF", f"lockedkey{name}", f"""
            function call(k)
                return barch.store.locked(k, function() {body} end)
            end
        """) == b"OK"
        assert r.execute_command("SETF", f"lockedall{name}", f"""
            function call(k)
                return barch.store.locked(function() {body} end)
            end
        """) == b"OK"

    for name in reads:
        want = r.execute_command(f"plain{name}", "lr050")
        assert want, f"{name} outside a region answered nothing"
        for form in ("lockedkey", "lockedall"):
            started = time.monotonic()
            got, refusal = None, None
            try:
                got = r.execute_command(f"{form}{name}", "lr050")
            except redis.exceptions.ResponseError as e:
                refusal = str(e)
            except redis.exceptions.TimeoutError as e:
                raise AssertionError(f"{form} {name} timed out after "
                                     f"{time.monotonic() - started:.1f}s: {e}")
            took = time.monotonic() - started
            assert took < QUICK, f"{form} {name} took {took:.1f}s ({refusal or got!r})"
            if form == "lockedall":
                assert refusal is None, f"{form} {name} was refused: {refusal}"
                assert got == want, f"{form} {name} answered {got!r}, outside it was {want!r}"
            else:
                assert refusal and "second shard" in refusal, \
                    f"{form} {name} should be refused as a second shard, got {refusal or got!r}"
            # the region let go of its shard: a write from another connection lands
            other = redis.Redis(host="127.0.0.1", port=PORT, db=0, socket_timeout=QUICK)
            other.set("lr050", "50")
            other.close()
        print(f"  {name}: ok")

    print("locked read test passed")
finally:
    barch.stop()
