import scale
import threading
import time
import redis
import barch

# Function overtime - TODO 836. A call past function_deadline_ms goes on in an
# overtime pool instead of failing, while that pool has room. It fails at the deadline
# when function_overtime_max is 0 (the old way), when the pool is full, when the
# function says "overtime": false, and when it's run inline by another function. In
# overtime it fails at function_overtime_max_ms.
#
# No timing: each function spins until the test sets a key, and the test sets it only
# once INFO says the call is in overtime. A call that finishes therefore got past its
# deadline.

scale.workdir()

PORT = scale.port(default=14162)

print("start overtime test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")

try:
    from wasmmodule import waits_for  # noqa: E402
    HAS_WASM = b"OK" == r.execute_command("SETF", "wprobe", waits_for("probe").build())
except redis.exceptions.ResponseError:
    HAS_WASM = False
print("with WASM" if HAS_WASM else "without WASM")


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, single_connection_client=True)


def stat(field):
    return int(r.info("memory").get(field, 0))


def reaches(field, want):
    """waits for a counter to reach `want`; the cap is only so a broken build fails"""
    end = time.time() + 30
    while time.time() < end:
        if stat(field) >= want:
            return True
        time.sleep(0.01)
    return False


def refused(*args):
    try:
        got = client().execute_command(*args)
    except redis.exceptions.ResponseError as e:
        return str(e)
    raise AssertionError(f"{args} answered {got!r}")


def in_background(*args):
    out = {}

    def run():
        try:
            out["v"] = client().execute_command(*args)
        except Exception as ex:
            out["v"] = ex

    t = threading.Thread(target=run)
    t.start()
    return t, out


LUAU_WAIT = "function call(k) while not barch.store.exists(k) do end; return 1 end"


def setf(name, src, *flags):
    assert r.execute_command("SETF", name, src, *flags) == b"OK", name


def finishes(fn, *args, key="go"):
    """runs fn, sees it enter overtime, lets it go, and answers what it answered"""
    r.delete(key)
    entered = stat("function_overtimes")
    t, out = in_background(fn, *args)
    assert reaches("function_overtimes", entered + 1), f"{fn} never went into overtime"
    assert stat("function_overtime_running") >= 1
    assert "v" not in out, out
    r.set(key, "1")
    t.join()
    return out["v"]


try:
    setf("ot", LUAU_WAIT)
    setf("otno", '--@barch {"overtime": false}\n' + LUAU_WAIT)
    setf("otaot", LUAU_WAIT, "AOT")
    setf("otrun", "function call() while true do end end")
    setf("otouter", 'function call(k) return barch.call("CALLF", "ot", k) end')
    if HAS_WASM:
        assert r.execute_command("SETF", "wot", waits_for("go").build()) == b"OK"
        assert r.execute_command("SETF", "wotno",
                                 waits_for("go", b'{"overtime": false}').build()) == b"OK"

    for name in ("function_overtime_max", "function_overtime_threads", "function_overtime_max_ms"):
        assert name in r.config_get(name), name
    threads = int(r.config_get("function_overtime_threads")["function_overtime_threads"])
    assert threads >= 1, threads
    assert r.config_get("function_overtime_max")["function_overtime_max"] == "0"

    r.execute_command("CONFIG SET function_deadline_ms 20")

    # with the limit at 0, a call past its deadline fails, as it always did
    r.delete("go")
    entered = stat("function_overtimes")
    e = refused("ot", "go")
    assert "FUNCTION timeout" in e, e
    assert stat("function_overtimes") == entered
    if HAS_WASM:
        e = refused("wot")
        assert "FUNCTION timeout" in e, e
        assert stat("function_overtimes") == entered

    # with room, it goes into overtime and finishes
    r.execute_command("CONFIG SET function_overtime_max 2")
    assert finishes("ot", "go") == 1
    assert finishes("otaot", "go") == 1
    if HAS_WASM:
        assert finishes("wot") == 1
    assert reaches("function_overtime_running", 0)
    assert stat("function_overtime_running") == 0

    # two at once fill the two overtime places
    r.delete("go")
    entered = stat("function_overtimes")
    a = in_background("ot", "go")
    b = in_background("ot", "go")
    assert reaches("function_overtimes", entered + 2)
    assert stat("function_overtime_running") == 2

    # the pool is full: the next call past its deadline fails, and is counted
    full = stat("function_overtime_full")
    e = refused("ot", "go")
    assert "FUNCTION timeout" in e, e
    assert stat("function_overtime_full") == full + 1
    if HAS_WASM:
        e = refused("wot")
        assert "FUNCTION timeout" in e, e
        assert stat("function_overtime_full") == full + 2
    r.set("go", "1")
    for t, out in (a, b):
        t.join()
        assert out["v"] == 1, out
    assert stat("function_overtime_running") == 0

    # a function that says "overtime": false fails at its deadline
    r.delete("go")
    entered = stat("function_overtimes")
    e = refused("otno", "go")
    assert "FUNCTION timeout" in e, e
    if HAS_WASM:
        e = refused("wotno")
        assert "FUNCTION timeout" in e, e
    # run inline by another function, it can't go on its own: it fails too
    e = refused("otouter", "go")
    assert "FUNCTION timeout" in e, e
    assert stat("function_overtimes") == entered

    # a runaway stops at function_overtime_max_ms and gives its place back
    r.execute_command("CONFIG SET function_overtime_max 1")
    r.execute_command("CONFIG SET function_overtime_max_ms 50")
    entered = stat("function_overtimes")
    e = refused("otrun")
    assert "FUNCTION timeout" in e, e
    assert stat("function_overtimes") == entered + 1
    assert stat("function_overtime_running") == 0
    r.execute_command("CONFIG SET function_overtime_max_ms 30000")
    assert finishes("ot", "go") == 1

    # back to 0, the old way again
    r.execute_command("CONFIG SET function_overtime_max 0")
    r.delete("go")
    e = refused("ot", "go")
    assert "FUNCTION timeout" in e, e

    # what the settings refuse
    for name, bad in (("function_overtime_max", "x"), ("function_overtime_threads", "0"),
                      ("function_overtime_max_ms", "-1")):
        try:
            r.execute_command("CONFIG SET", name, bad)
            raise AssertionError(f"{name} took {bad}")
        except redis.exceptions.ResponseError:
            pass
    print("overtime test passed")
finally:
    barch.stop()
