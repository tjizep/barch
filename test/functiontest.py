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
    # a function name is a command name: case insensitive, stored folded, so KEYSF
    # answers the canonical form rather than however it was typed
    assert "GREET" in names(), f"KEYSF should list GREET, got {names()}"
    assert r.execute_command("GETF", "GREET").decode() == GREET, "the name folds on read too"

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
    assert {"plain", "GREET"} <= keys, f"KEYS * should show functions, got {sorted(keys)}"
    _, items = r.execute_command("SCAN", "0")
    assert "GREET" in {i.decode() for i in items}, "SCAN should show functions"
    # tfunction is 12, after every other lead, so the functions sort last in the space
    assert r.execute_command("MAX").decode() == "GREET", \
        "a function key sorts after ordinary keys, so MAX finds it"
    r.execute_command("DEL", "plain")

    # --- KEYSF filters, and sorts -----------------------------------------------
    assert r.execute_command("SETF", "counter", COUNT) == b"OK"
    assert names() == sorted(names()), "KEYSF should answer in order"
    assert set(names()) >= {"GREET", "COUNTER"}
    assert names("GR*") == ["GREET"], f"KEYSF GR* gave {names('GR*')}"
    assert names("nothing*") == []

    # --- a redefinition replaces, it does not double up ---------------------------
    assert r.execute_command("SETF", "greet", COUNT) == b"OK"
    assert r.execute_command("GETF", "greet").decode() == COUNT
    assert names().count("GREET") == 1

    # --- REMF ---------------------------------------------------------------------
    assert r.execute_command("REMF", "greet") == 1
    assert r.execute_command("GETF", "greet") is None
    assert r.execute_command("REMF", "greet") == 0, "removing it twice is not an error"
    assert "GREET" not in names()

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

    # --- calling one by its own name ----------------------------------------------
    assert r.execute_command("SETF", "greet", GREET) == b"OK"
    assert r.execute_command("greet", "world").decode() == "hello world"
    # the name is a command name, so it folds case the way every other one does
    assert r.execute_command("GREET", "world").decode() == "hello world"
    # and it is not confused with CALLF's argument order: argv[1] is the first argument
    assert r.execute_command("counter", "a", "b") == 2

    # a name that is not a function is still an unknown command, and stays answerable
    # after the function is created - the resolution must not be cached on a miss
    e = refused("laterfn", "x")
    assert e and "unknown command" in e.lower(), f"said: {e}"
    assert r.execute_command("SETF", "laterfn", COUNT) == b"OK"
    assert r.execute_command("laterfn", "x") == 1, \
        "a name that missed before the function existed must resolve once it does"

    # --- a dotted name says which space the definition comes from -------------------
    assert r.execute_command("fspace:SETF", "dotted", GREET) == b"OK"
    assert r.execute_command("fspace.dotted", "there").decode() == "hello there"
    # the space half of a dotted name keeps its case, since space names are not folded
    assert refused("FSPACE.dotted", "there") is not None
    # an unknown space is not a function, it is an unknown command
    e = refused("nosuchspace.dotted", "x")
    assert e and "unknown command" in e.lower(), f"said: {e}"
    r.execute_command("fspace:REMF", "dotted")

    # --- arity, declared by the script and checked before it runs -------------------
    assert r.execute_command("SETF", "exactly2",
                             "arity = 2\nfunction call(argv) return #argv end") == b"OK"
    assert r.execute_command("exactly2", "a", "b") == 2
    for bad in ([], ["a"], ["a", "b", "c"]):
        e = refused("exactly2", *bad)
        assert e and "wrong number of arguments" in e, f"{bad} said: {e}"

    # a negative arity is a minimum, as it is in redis
    assert r.execute_command("SETF", "atleast1",
                             "arity = -1\nfunction call(argv) return #argv end") == b"OK"
    assert r.execute_command("atleast1", "a") == 1
    assert r.execute_command("atleast1", "a", "b", "c") == 3
    assert refused("atleast1") is not None

    # and a script that declares none takes whatever it is given
    assert r.execute_command("counter", "a", "b", "c", "d") == 4

    # --- one function cannot leave globals behind for the next ----------------------
    # each is loaded on a sandboxed thread with its own globals table, so a write goes
    # there rather than into the state the whole space shares
    assert r.execute_command("SETF", "setsglobal",
                             'function call() leaked = "yes" return "set" end') == b"OK"
    assert r.execute_command("SETF", "readsglobal",
                             'function call() return leaked == nil and "clean" or "dirty" end') == b"OK"
    assert r.execute_command("setsglobal").decode() == "set"
    assert r.execute_command("readsglobal").decode() == "clean", \
        "a global set by one function must not be visible to another"

    # --- the compiled function is held for the life of the connection ---------------
    # nothing invalidates it, so a redefinition reaches new connections and not this
    # one. That is the deal in TODO 98 C, and 137 is where it gets revisited
    assert r.execute_command("SETF", "version", 'function call() return "one" end') == b"OK"
    assert r.execute_command("version").decode() == "one"
    assert r.execute_command("SETF", "version", 'function call() return "two" end') == b"OK"
    assert r.execute_command("version").decode() == "one", \
        "this connection keeps what it compiled"
    fresh = redis.Redis(host="127.0.0.1", port=PORT, db=0)
    assert fresh.execute_command("version").decode() == "two", \
        "a new connection gets the new code"
    fresh.close()

    # --- require, between functions in a space -------------------------------------
    # what comes back is the required function's globals table, so a module can offer
    # helpers as well as its own call()
    assert r.execute_command("SETF", "helpers", '''
        function double(n) return n * 2 end
        function call(argv) return "helpers" end
    ''') == b"OK"
    assert r.execute_command("SETF", "usesHelpers", '''
        local h = require("helpers")
        function call(argv) return h.double(tonumber(argv[1])) end
    ''') == b"OK"
    assert r.execute_command("usesHelpers", "21") == 42

    # the required name folds like any other
    assert r.execute_command("SETF", "usesUpper", '''
        local h = require("HELPERS")
        function call(argv) return h.double(1) end
    ''') == b"OK"
    assert r.execute_command("usesUpper") == 2

    # a require is resolved when the function is stored, not when it is first called,
    # so requiring something that is not there is refused by SETF. The price of that
    # is that a function has to be stored after the ones it requires
    e = refused("SETF", "usesMissing",
                'local m = require("nosuchmodule")\nfunction call() return 1 end')
    assert e and "nosuchmodule" in e.lower(), f"said: {e}"
    assert r.execute_command("GETF", "usesMissing") is None, "and nothing was stored"

    # --- a cycle is refused, with the path that made it -----------------------------
    # it cannot be built in one go, because the first half would require something
    # that is not there yet. It takes a redefinition, which is exactly the case that
    # would otherwise recurse until the stack gave out
    assert r.execute_command("SETF", "cycleb", 'function call() return 2 end') == b"OK"
    assert r.execute_command("SETF", "cyclea",
                             'local b = require("cycleb")\nfunction call() return 1 end') == b"OK"
    e = refused("SETF", "cycleb",
                'local a = require("cyclea")\nfunction call() return 2 end')
    assert e and "cycle" in e.lower(), f"a cycle should be refused, said: {e}"
    assert "CYCLEA" in e and "CYCLEB" in e, f"the message should name the path, said: {e}"
    # the refused redefinition did not land, so the old one still works
    assert r.execute_command("cycleb") == 2
    # and the server is still standing rather than having run out of stack
    assert r.execute_command("PING") is True

    # a function requiring itself is the shortest cycle there is
    e = refused("SETF", "selfref",
                'local me = require("selfref")\nfunction call() return 1 end')
    assert e and "cycle" in e.lower(), f"said: {e}"

    # --- barch.call, into the ordinary commands -------------------------------------
    assert r.execute_command("SETF", "roundtrip", '''
        function call(argv)
            barch.call("SET", argv[1], argv[2])
            return barch.call("GET", argv[1])
        end
    ''') == b"OK"
    assert r.execute_command("roundtrip", "fromscript", "written").decode() == "written"
    assert r.execute_command("GET", "fromscript").decode() == "written", \
        "the write a script made is an ordinary write"

    # numbers go over as the wire would carry them, without the script saying tostring
    assert r.execute_command("SETF", "counterup", '''
        function call(argv)
            barch.call("SET", "n", 0)
            barch.call("INCRBY", "n", 41)
            return barch.call("INCR", "n")
        end
    ''') == b"OK"
    assert r.execute_command("counterup") == 42

    # an array reply becomes a table, and a missing key becomes nil
    assert r.execute_command("SETF", "readsmany", '''
        function call(argv)
            barch.call("SET", "a", "1")
            barch.call("SET", "b", "2")
            local got = barch.call("MGET", "a", "b")
            local missing = barch.call("GET", "nothinghere")
            return {got[1], got[2], missing == nil and "nil" or "notnil"}
        end
    ''') == b"OK"
    assert r.execute_command("readsmany") == [b"1", b"2", b"nil"]

    # --- what barch.call refuses ----------------------------------------------------
    # a name each, not one name redefined: this connection keeps whatever it compiled
    # first, so reusing the name would run the first body every time - see TODO 98 C
    refusers = []

    def refuses(name, body):
        refusers.append(name)
        assert r.execute_command("SETF", name, "function call(argv) %s end" % body) == b"OK"
        return refused(name)

    e = refuses("ref_block", 'return barch.call("BLPOP", "nolist", "0")')
    assert e and "blocks" in e, f"a blocking command should be refused, said: {e}"
    e = refuses("ref_async", 'return barch.call("KEYS", "*")')
    assert e and "asynchronous" in e, f"an asynchronous command should be refused, said: {e}"
    e = refuses("ref_multi", 'return barch.call("MULTI")')
    assert e and "MULTI" in e, f"a transaction should be refused, said: {e}"
    e = refuses("ref_unknown", 'return barch.call("NOSUCHCOMMAND")')
    assert e and "unknown command" in e, f"said: {e}"

    # a refused call is a Lua error, so a script that wants to carry on can pcall it
    assert r.execute_command("SETF", "survives", '''
        function call(argv)
            local ok, e = pcall(function() return barch.call("BLPOP", "nolist", "0") end)
            return ok and "ran" or "caught"
        end
    ''') == b"OK"
    assert r.execute_command("survives").decode() == "caught"
    # and the connection is fine afterwards
    assert r.execute_command("PING") is True

    r.execute_command("DEL", "fromscript"); r.execute_command("DEL", "n")
    r.execute_command("DEL", "a"); r.execute_command("DEL", "b")

    # --- barch.store, and three built-ins written in Luau ---------------------------
    # this is the test TODO 98 F asks for: reimplement commands of different shapes
    # against the interface and see what it cannot do
    for i, v in enumerate(["alpha", "bravo", "charlie", "delta"]):
        r.execute_command("SET", "sk%d" % i, v)

    # GETRANGE, on barch.store.get
    assert r.execute_command("SETF", "myGetrange", '''
        function call(argv)
            local v = barch.store.get(argv[1])
            if v == nil then return nil end
            local from = tonumber(argv[2]) + 1
            local to = tonumber(argv[3]) + 1
            return string.sub(v, from, to)
        end
    ''') == b"OK"
    assert r.execute_command("myGetrange", "sk2", "0", "2").decode() == "cha"
    assert r.execute_command("myGetrange", "sk2", "0", "2").decode() == \
        r.execute_command("GETRANGE", "sk2", "0", "2").decode()

    # COUNT, on barch.store.count
    assert r.execute_command("SETF", "myCount", '''
        function call(argv) return barch.store.count(argv[1], argv[2]) end
    ''') == b"OK"
    assert r.execute_command("myCount", "sk0", "sk9") == 4

    # a KEYS-shaped walk, bounded, on barch.store.range
    assert r.execute_command("SETF", "myKeys", '''
        function call(argv)
            local out = {}
            for _, k in ipairs(barch.store.range(argv[1], argv[2], 100)) do
                out[#out + 1] = k
            end
            return out
        end
    ''') == b"OK"
    assert [k.decode() for k in r.execute_command("myKeys", "sk0", "sk9")] == \
        ["sk0", "sk1", "sk2", "sk3"]

    # min, max and exists
    assert r.execute_command("SETF", "myBounds", '''
        function call(argv)
            return {barch.store.min(), barch.store.max(),
                    barch.store.exists("sk0") and "y" or "n",
                    barch.store.exists("nothing") and "y" or "n"}
        end
    ''') == b"OK"
    bounds = r.execute_command("myBounds")
    assert bounds[2] == b"y" and bounds[3] == b"n", f"exists gave {bounds}"

    # the space describes itself, by the name a client would address it with. The
    # default space has no name in that vocabulary - `undecorate("node")` is "" and
    # there is no `space:` prefix for it - so that is what a function there sees
    assert r.execute_command("SETF", "mySpace", '''
        function call(argv) local s = barch.space() return {s.name, s.ordered} end
    ''') == b"OK"
    assert r.execute_command("mySpace") == [b"", b"1"], \
        f"the default space has no prefix name, got {r.execute_command('mySpace')}"
    # in a named one it is the name the prefix uses
    assert r.execute_command("fspace:SETF", "mySpace", '''
        function call(argv) return barch.space().name end
    ''') == b"OK"
    assert r.execute_command("fspace:mySpace").decode() == "fspace"

    # a range walk is bounded whatever the script asks for, so one function cannot
    # copy the whole space into a single reply
    assert r.execute_command("SETF", "myHuge", '''
        function call(argv) return #barch.store.range(argv[1], argv[2], 100000000) end
    ''') == b"OK"
    assert r.execute_command("myHuge", "sk0", "sk9") == 4

    # a composite key comes back the way it was written - the components rejoined
    # with the space's split character, which is what KEYS answers too, because a
    # script and a client have to agree about the same key.
    #
    # It also sits in a different part of the key order: a key holding the split is
    # stored under tplain and a plain string under tstring, so a range over string
    # bounds does not contain it. That is not this interface being odd, it is what
    # the built-in RANGE does, and the test says so both ways
    r.execute_command("SET", "part one", "composite")
    r.execute_command("SET", "partplain", "plain")
    assert r.execute_command("SETF", "myComposite", '''
        function call(argv)
            -- bounds holding the split are composites too, so they bracket one
            local found = barch.store.range("part a", "part z", 10)
            local plain = barch.store.range("part", "partz", 10)
            return {found[1], barch.store.get(found[1]), #plain, plain[1]}
        end
    ''') == b"OK"
    got = r.execute_command("myComposite")
    assert got[0] == b"part one", f"a composite key should render as written, got {got[0]!r}"
    assert got[1] == b"composite", "and read back through that same rendering"
    assert got[0] in r.execute_command("KEYS", "*"), "the script and KEYS should agree"
    # a range over plain string bounds does not reach it, because a composite is a
    # different lead and sorts elsewhere. The built-in does the same
    assert got[2] == 1 and got[3] == b"partplain", f"the plain range gave {got[2:]}"
    assert [k.decode() for k in r.execute_command("RANGE", "part", "partz")] == ["partplain"], \
        "and the built-in agrees with the script"
    r.execute_command("DEL", "part one"); r.execute_command("DEL", "partplain")

    for i in range(4):
        r.execute_command("DEL", "sk%d" % i)

    # --- an export carries functions, and puts them back as functions --------------
    # they used to be dropped: a function is not a container and not a string, so it
    # fell through to the plain branch, where re-encoding the name found no key and
    # nothing was written. An export is a backup, so a hole in it is the whole problem
    export_path = "/tmp/functiontest-export.txt"
    r.execute_command("SETF", "greet", GREET)
    r.execute_command("SET", "greet", "an ordinary value")
    r.execute_command("EXPORT", export_path)
    r.execute_command("REMF", "greet")
    r.execute_command("DEL", "greet")
    assert r.execute_command("GETF", "greet") is None
    r.execute_command("IMPORT", export_path)
    assert r.execute_command("GETF", "greet").decode() == GREET, \
        "the export should have carried the function"
    # and the string of the same name came back too - they are different ranges, so an
    # export has to carry both rather than let one stand in for the other
    assert r.execute_command("GET", "greet").decode() == "an ordinary value"
    r.execute_command("DEL", "greet")

    # --- a function belongs to the space it was written in -------------------------
    assert r.execute_command("fspace:SETF", "counter", GREET) == b"OK"
    assert r.execute_command("fspace:GETF", "counter").decode() == GREET
    assert r.execute_command("GETF", "counter").decode() == COUNT, \
        "the other space's function must not be visible here"
finally:
    for n in ("greet", "counter", "broken", "noentry", "spin", "boom", "r_err",
              "r_nil", "r_int", "r_float", "r_true", "r_false", "r_str", "r_array",
              "r_ok", "r_nested", "laterfn", "dotted", "exactly2", "atleast1",
              "setsglobal", "readsglobal", "version", "helpers", "usesHelpers",
              "usesUpper", "usesMissing", "cyclea", "cycleb", "selfref",
              "roundtrip", "counterup", "readsmany", "survives",
              "ref_block", "ref_async", "ref_multi", "ref_unknown",
              "myGetrange", "myCount", "myKeys", "myBounds", "mySpace", "myHuge",
              "myComposite"):
        try:
            r.execute_command("REMF", n)
            r.execute_command("fspace:REMF", n)
        except redis.exceptions.ResponseError:
            pass
    r.close()
    barch.stop()

print("complete function test")
