import scale
import os
import threading
import time
import redis
import barch

# SETF ... AOT: the experiment flag for native code - TODO 392. Same source
# installed plain and with AOT must answer the same, and the flag in either
# position (with RELOAD or without) must be accepted while anything else is
# refused.

scale.workdir()

PORT = scale.port(default=14098)

print("start aot test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")


def refused(*args):
    try:
        r.execute_command(*args)
        return None
    except redis.exceptions.ResponseError as e:
        return str(e)


SRC = "function call(n) local s = 0; for i = 1, tonumber(n) do s = s + i end; return s end"

try:
    assert r.execute_command("SETF", "aotplain", SRC) == b"OK"
    assert r.execute_command("SETF", "aotfast", SRC, "AOT") == b"OK"
    assert r.execute_command("SETF", "aotboth", SRC, "RELOAD", "AOT") == b"OK"
    assert r.execute_command("SETF", "aotswap", SRC, "AOT", "RELOAD") == b"OK"
    e = refused("SETF", "aotbad", SRC, "BOGUS")
    assert e and "SETF" in e, e

    # same source, same answer, whichever flag it was installed with
    for name in ("aotplain", "aotfast", "aotboth", "aotswap"):
        assert r.execute_command(name, "200000") == 20000100000, name

    # `--!native` in the header asks for what the flag asks for, and it is
    # kept with the source, so it lasts where the in-memory flag does not -
    # through a restart, a git sync, a SETF that leaves the flag off. Only
    # the header counts, the way Luau reads hot comments. Each name is new,
    # so its first call is a cold compile and counts once if it went native.
    # TODO 413.
    def native():
        return int(r.info("memory").get("luau_native_compiled", 0))

    for name, src, want in (
            ("aotmark", "--!native\n" + SRC, 1),
            ("aotmarkhdr", "--[[ a header ]]\n-- more\n  --!native  \n" + SRC, 1),
            ("aotmarklate", "local x = 1\n--!native\n" + SRC, 0),
            ("aotmarknone", SRC, 0)):
        assert r.execute_command("SETF", name, src) == b"OK"
        before = native()
        assert r.execute_command(name, "200000") == 20000100000, name
        assert native() - before == want, (name, native() - before)

    # and it still works after REMF + reinstall without the flag
    assert r.execute_command("REMF", "aotfast") == 1
    assert r.execute_command("SETF", "aotfast", SRC) == b"OK"
    assert r.execute_command("aotfast", "200000") == 20000100000

    # a hookless runaway still dies on the deadline: the watchdog re-arms
    # the hook past it, so the timeout lands one loop edge late, not never
    assert r.execute_command("SETF", "aotslow",
        "function call() local s = 0 while true do s = s + 1 end return s end",
        "AOT") == b"OK"
    r.execute_command("CONFIG SET function_deadline_ms 300")
    try:
        t0 = time.time()
        e = refused("aotslow")
        dt = time.time() - t0
        assert e and "timeout" in e.lower(), e
        assert dt < 5, "the watchdog did not stop the runaway: %.2fs" % dt
    finally:
        r.execute_command("CONFIG SET function_deadline_ms 1000")

    # ...and the session is still usable afterwards (no wedged worker, no
    # stray hook breaking a later call on a reused thread)
    assert r.execute_command("aotplain", "200000") == 20000100000

    # a parking AOT function keeps the hook and still parks: fetch reaches
    # the fill script rather than running hookless past it
    assert r.execute_command("SETF", "aotfill",
        "function call(key, space) return 'filled:' .. key end") == b"OK"
    assert r.execute_command("SETF", "aotfetch",
        "function call(k) return barch.store.fetch(k) end", "AOT") == b"OK"

    # a native frame that parks and then computes: the yield goes through
    # the native frame (CALL_FALLBACK_YIELD) and the resume continues native
    assert r.execute_command("SETF", "aotparkn",
        "function call(k) local v = barch.store.fetch(k) local s = 0 "
        "for i = 1, 1000000 do s = s + i end return s end", "AOT") == b"OK"

    # a user coroutine.yield inside AOT answers, not wedges
    assert r.execute_command("SETF", "aotyield",
        "function call() local co = coroutine.wrap(function() "
        "coroutine.yield(42) end) return co() end", "AOT") == b"OK"
    assert r.execute_command("aotyield") == 42

    # The deadline watch is one thread for the whole server. It used to be a
    # detached thread per hookless resume that slept out the whole deadline
    # whether the call ended or not, so the server carried a thread per AOT
    # call made in the last function_deadline_ms - 2000 calls left 2000
    # threads, and once the limit was reached std::thread threw and took the
    # process with it. TODO 393, 394.
    if os.path.exists("/proc/self/status"):
        def threads():
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("Threads:"):
                        return int(line.split()[1])
            return -1

        assert r.execute_command("SETF", "aotburst", SRC, "AOT") == b"OK"
        r.execute_command("aotburst", "10")      # the one the watch starts on
        base = threads()
        for _ in range(1000):
            r.execute_command("aotburst", "10")
        grew = threads() - base
        assert grew <= 4, "1000 AOT calls left %d threads behind" % grew

    # A parked AOT call gives the interrupt hook back. It used to save the
    # hook at the top of each resume and put that back at the end, and the
    # resume after a park saw a null one - so the session ended up with no
    # hook at all and the next runaway on it spun for ever instead of timing
    # out. Same connection for both calls: the hook is per session.
    r2 = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=15)
    assert r2.execute_command("SETF", "aotresp",
        'function call() return resp.connect("127.0.0.1", %d):call("PING") end' % PORT,
        "AOT") == b"OK"
    assert r2.execute_command("SETF", "spin",
        "function call() local s = 0 while true do s = s + 1 end return s end") == b"OK"
    assert r2.execute_command("aotresp") == b"PONG"
    r.execute_command("CONFIG SET function_deadline_ms 300")
    try:
        t0 = time.time()
        try:
            r2.execute_command("spin")
            assert False, "the runaway returned instead of timing out"
        except redis.exceptions.TimeoutError:
            assert False, "no hook after a parked AOT call: the runaway never stopped"
        except redis.exceptions.ResponseError as e:
            assert "timeout" in str(e).lower(), str(e)
        assert time.time() - t0 < 5, "the deadline took too long to land"
    finally:
        r.execute_command("CONFIG SET function_deadline_ms 1000")

    # transport() methods are native too, and they run hookless the way
    # call() does. Compiling only call() got its inner functions and nothing
    # else, so a function whose work is all behind methods got nothing from
    # the flag at all. TODO 396.
    TRANS = """
local function ADD(a, b) return tonumber(a) + tonumber(b) end
local function SPIN() local s = 0 while true do s = s + 1 end return s end
function call() return "usage" end
function transport()
    return {
        kind = "resp",
        methods = {PFXADD = ADD, PFXSPIN = SPIN},
        categories = {PFXADD = {"read"}, PFXSPIN = {"read"}},
        arity = {PFXADD = 2, PFXSPIN = 0},
    }
end
"""
    assert r.execute_command("SETF", "aotnat", TRANS.replace("PFX", "NAT"),
                             "AOT", "RELOAD") == b"OK"
    assert r.execute_command("SETF", "aotint", TRANS.replace("PFX", "INT"),
                             "RELOAD") == b"OK"
    # same answers through a native method as through an interpreted one
    assert r.execute_command("NATADD", "2", "3") == 5
    assert r.execute_command("INTADD", "2", "3") == 5
    assert r.execute_command("aotnat") == b"usage"

    # and the deadline reaches inside a native method, which is what the
    # watch is for - a hookless method that never returns is otherwise a
    # wedged worker
    r.execute_command("CONFIG SET function_deadline_ms 300")
    try:
        t0 = time.time()
        e = refused("NATSPIN")
        dt = time.time() - t0
        assert e and "timeout" in e.lower(), e
        assert dt < 5, "a native method ran past its deadline: %.2fs" % dt
    finally:
        r.execute_command("CONFIG SET function_deadline_ms 1000")

    # the session is still good afterwards
    assert r.execute_command("NATADD", "40", "2") == 42

    # A native call gives its thread back. It runs with the hook down, so
    # nothing counts instructions for it and it used to hold the thread it
    # started on until it returned - eight of them took all eight RESP service
    # threads and a PING from anyone else waited over a second. The deadline
    # watch now asks for a yield after a slice's worth of time as well, so the
    # work moves to the script pool and the service threads stay free.
    # TODO 398.
    assert r.execute_command("SETF", "aothog",
        "function call(n) local s = 0 for i = 1, tonumber(n) do "
        "s = s + i * 1.000001 end return s end", "AOT") == b"OK"
    r.execute_command("CONFIG SET function_deadline_ms 120000")
    try:
        t0 = time.time()
        r.execute_command("aothog", "400000000")
        one = time.time() - t0

        def hog():
            c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2,
                            socket_timeout=300)
            try:
                c.execute_command("aothog", "400000000")
            finally:
                c.close()

        hogs = [threading.Thread(target=hog) for _ in range(8)]
        for h in hogs:
            h.start()
        time.sleep(one / 4)
        other = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2,
                            socket_timeout=300)
        t0 = time.time()
        other.execute_command("PING")
        ping = time.time() - t0
        other.close()
        for h in hogs:
            h.join()
        assert ping < max(0.35, one / 3), (
            "a PING waited %.2fs behind eight native calls of %.2fs each" % (ping, one))
    finally:
        r.execute_command("CONFIG SET function_deadline_ms 1000")

    print("complete aot test")
finally:
    try:
        barch.stop()
    except Exception:
        pass
