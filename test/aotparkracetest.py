# Hookless-native park race - TODO 393.
#
# Shape: an AOT function whose native frame parks (barch.store.fetch on a
# foreign miss) while a per-resume watchdog is armed for its deadline. The
# park path (`job->parked`, re-entered pump_call) never disarms the watchdog
# (watch_done only fires at the restore block, which a park return skips),
# so the watchdog can fire *while parked*: it writes cb.interrupt back onto
# the shared state mid-park. The completion then resumes the coroutine with
# the hook unexpectedly up; the resumed frame runs interpreted-speed, and a
# second park spawns a *second* watchdog on the same job without cancelling
# the first - two detached threads racing to write one state's hook pointer,
# one of them potentially after the call finished and the thread was reused.
#
# What this pins: the racy interleaving must not kill the server. Park,
# re-park before the first watchdog fires, let the deadline pass mid-park,
# then keep using the session. A silent death (connection reset, no reply)
# is the TODO 393 signature; anything else - timeout errors, fills, slow
# answers - is the race resolving safely.
import scale
import time
import redis
import barch

scale.workdir()

PORT = scale.port(default=14099)

print("start hookless park race test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")
conf = barch.KeyValue("configuration")


def refused(*args):
    try:
        r.execute_command(*args)
        return None
    except redis.exceptions.ResponseError as e:
        return str(e)


# the fill script: slow enough (~300ms of arithmetic) that a 150ms function
# deadline passes *while the fetch is parked*, so the watchdog re-arms the
# hook mid-park
FILL = ("function call(key, space)\n"
        "    local s = 0\n"
        "    for i = 1, 3000000 do s = s + i end\n"
        "    return 'filled:' .. key\n"
        "end\n")
conf.set("race.foreign_script", "filler")
conf.set("race.foreign", "luau")
conf.save()

try:
    r.execute_command("USE", "race")
    assert r.execute_command("SETF", "filler", FILL) == b"OK"
    # hookless native frame that parks on a miss, then computes
    assert r.execute_command("SETF", "racen",
        "function call(k) local v = barch.store.fetch(k) local s = 0 "
        "for i = 1, 500000 do s = s + i end return tostring(v) .. ':' .. s end",
        "AOT") == b"OK"

    # deadline inside the fill: the fetch parks, the watchdog fires mid-park
    r.execute_command("CONFIG SET function_deadline_ms 150")
    try:
        for i in range(6):
            # fresh key every time so every call really parks (no cached fill)
            e = refused("racen", "key%d" % i)
            # timeout, fill, or slow answer are all safe outcomes - the race
            # may resolve either way depending on thread timing
            assert e is None or isinstance(e, str), e
    finally:
        r.execute_command("CONFIG SET function_deadline_ms 1000")

    # ...and the session is still alive afterwards. This is the assertion
    # that fails on the TODO 393 signature (connection reset = dead server).
    got = r.execute_command("racen", "afterwards")
    if isinstance(got, bytes):
        got = got.decode()
    assert got.startswith("filled:afterwards"), got

    print("complete hookless park race test")
finally:
    try:
        barch.stop()
    except Exception:
        pass
