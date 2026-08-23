import redis
import barch

# Stored Luau functions as keys. A function lives under art::tfunction in whichever key
# space the command ran in, so SETF/GETF/REMF/KEYSF are SET/GET/REM/KEYS with a
# different type byte and their own ACL category. See TODO 98.
#
# No calling yet - this covers the storage half: what gets written, what gets refused,
# and that the function range and the ordinary key range cannot see each other.

PORT = 14000

GREET = 'function call(argv) return "hello " .. argv[1] end'
COUNT = 'function call(argv) return #argv end'

print("start function test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)


def names(*args):
    return [n.decode() for n in r.execute_command("KEYSF", *args)]


def refused(*args):
    try:
        r.execute_command(*args)
        return None
    except redis.exceptions.ResponseError as e:
        return str(e)


try:
    # --- a function goes in and comes back ---------------------------------------
    assert r.execute_command("SETF", "greet", GREET) == b"OK"
    assert r.execute_command("GETF", "greet").decode() == GREET
    assert "greet" in names(), f"KEYSF should list greet, got {names()}"

    # --- what SETF refuses ---------------------------------------------------------
    # a script that will not compile
    e = refused("SETF", "broken", "function call(argv) return end end")
    assert e, "a script that does not compile should be refused"
    assert refused("GETF", "broken") is None and r.execute_command("GETF", "broken") is None, \
        "a refused SETF must not have written anything"

    # a script that compiles but declares no call()
    e = refused("SETF", "noentry", 'function other() return 1 end')
    assert e and "call" in e, f"the refusal should name call(), said: {e}"

    # a name that is already a command. Built-ins are never overloaded
    e = refused("SETF", "GET", GREET)
    assert e and "command" in e, f"SETF GET should say the name is a command, said: {e}"
    e = refused("SETF", "get", GREET)
    assert e, "the built-in check is case insensitive"

    # --- functions and ordinary keys are different ranges ---------------------------
    r.execute_command("SET", "greet", "an ordinary value")
    assert r.execute_command("GET", "greet").decode() == "an ordinary value"
    assert r.execute_command("GETF", "greet").decode() == GREET, \
        "SET must not have touched the function of the same name"
    r.execute_command("DEL", "greet")
    assert r.execute_command("GETF", "greet").decode() == GREET, \
        "DEL of the string must not remove the function"

    # --- a function key is an ordinary key, and behaves like one --------------------
    # this is the trade the tfunction lead buys: functions are contiguous and cheap to
    # list, but they are also in everything that walks the space. Asserted rather than
    # described, because each one is a behaviour change to a command that has nothing
    # to do with functions
    r.execute_command("SET", "plain", "v")
    keys = {k.decode() for k in r.execute_command("KEYS", "*")}
    assert {"plain", "greet"} <= keys, f"KEYS * should show functions, got {sorted(keys)}"
    _, items = r.execute_command("SCAN", "0")
    assert "greet" in {i.decode() for i in items}, "SCAN should show functions"
    # tfunction is 12, after every other lead, so the functions sort last in the space
    assert r.execute_command("MAX").decode() == "greet", \
        "a function key sorts after ordinary keys, so MAX finds it"
    r.execute_command("DEL", "plain")

    # --- KEYSF filters, and sorts -----------------------------------------------
    assert r.execute_command("SETF", "counter", COUNT) == b"OK"
    assert names() == sorted(names()), "KEYSF should answer in order"
    assert set(names()) >= {"greet", "counter"}
    assert names("gr*") == ["greet"], f"KEYSF gr* gave {names('gr*')}"
    assert names("nothing*") == []

    # --- a redefinition replaces, it does not double up ---------------------------
    assert r.execute_command("SETF", "greet", COUNT) == b"OK"
    assert r.execute_command("GETF", "greet").decode() == COUNT
    assert names().count("greet") == 1

    # --- REMF ---------------------------------------------------------------------
    assert r.execute_command("REMF", "greet") == 1
    assert r.execute_command("GETF", "greet") is None
    assert r.execute_command("REMF", "greet") == 0, "removing it twice is not an error"
    assert "greet" not in names()

    # --- calling one --------------------------------------------------------------
    assert r.execute_command("SETF", "greet", GREET) == b"OK"
    assert r.execute_command("CALLF", "greet", "world").decode() == "hello world"
    # the arguments are 1-based and carry neither the command nor the function name
    assert r.execute_command("CALLF", "counter", "a", "b", "c") == 3
    assert r.execute_command("CALLF", "counter") == 0

    e = refused("CALLF", "nosuchfunction")
    assert e and "no such function" in e, f"said: {e}"

    # --- what a return value becomes ------------------------------------------------
    shapes = {
        "r_nil":    ("function call() return nil end", None),
        "r_int":    ("function call() return 7 end", 7),
        # RESP2 has no double, so a non-integral number is a bulk string here. The
        # RESP3 half of that rule is asserted further down
        "r_float":  ("function call() return 2.5 end", b"2.5"),
        "r_true":   ("function call() return true end", 1),
        "r_false":  ("function call() return false end", None),
        "r_str":    ('function call() return "text" end', b"text"),
        "r_array":  ('function call() return {1, "two", 3} end', [1, b"two", 3]),
        "r_ok":     ('function call() return {ok = "FINE"} end', b"FINE"),
        "r_nested": ("function call() return {1, {2, 3}} end", [1, [2, 3]]),
    }
    for name, (src, want) in shapes.items():
        assert r.execute_command("SETF", name, src) == b"OK"
        got = r.execute_command("CALLF", name)
        assert got == want, f"{name} answered {got!r}, expected {want!r}"

    # the same function over RESP3, where a double has a wire type of its own
    r3 = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=3)
    assert r3.execute_command("CALLF", "r_float") == 2.5, \
        "a non-integral number should be a real double on RESP3"
    assert r3.execute_command("CALLF", "r_int") == 7
    r3.close()

    # a table with an err field is that error, verbatim, as it is in redis
    assert r.execute_command("SETF", "r_err", 'function call() return {err = "MYFAULT bad"} end') == b"OK"
    e = refused("CALLF", "r_err")
    assert e == "MYFAULT bad", f"the script's own error should come through, said: {e}"

    # --- a script that misbehaves is stopped, not tolerated -------------------------
    assert r.execute_command("SETF", "spin", "function call() while true do end end") == b"OK"
    e = refused("CALLF", "spin")
    assert e and ("budget" in e or "timeout" in e), f"a spin should be cut off, said: {e}"
    # and the connection is still usable afterwards - a script that was cut off must
    # not leave the session out of step with its own replies. redis-py turns +PONG
    # into True through its own response callback, hence not comparing to the string
    assert r.execute_command("PING") is True
    assert r.execute_command("GETF", "greet").decode() == GREET

    assert r.execute_command("SETF", "boom", 'function call() error("went wrong") end') == b"OK"
    e = refused("CALLF", "boom")
    assert e and "went wrong" in e, f"a script error should say what it was, said: {e}"

    # --- a function belongs to the space it was written in -------------------------
    assert r.execute_command("fspace:SETF", "counter", GREET) == b"OK"
    assert r.execute_command("fspace:GETF", "counter").decode() == GREET
    assert r.execute_command("GETF", "counter").decode() == COUNT, \
        "the other space's function must not be visible here"
finally:
    for n in ("greet", "counter", "broken", "noentry", "spin", "boom", "r_err",
              "r_nil", "r_int", "r_float", "r_true", "r_false", "r_str", "r_array",
              "r_ok", "r_nested"):
        try:
            r.execute_command("REMF", n)
            r.execute_command("fspace:REMF", n)
        except redis.exceptions.ResponseError:
            pass
    r.close()
    barch.stop()

print("complete function test")
