import time

import redis
import barch

# Stored Luau functions as keys. A function lives under art::tfunction in whichever key
# space the command ran in, so SETF/GETF/REMF/KEYSF are SET/GET/REM/KEYS with a
# different type byte and their own ACL category. See TODO 98.
#
# No calling yet - this covers the storage half: what gets written, what gets refused,
# and that the function range and the ordinary key range cannot see each other.

PORT = 14000

GREET = 'function call(who) return "hello " .. who end'
COUNT = 'function call(...) return select("#", ...) end'

print("start function test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
# the build directory is shared, so a previous test's save is still here
r.execute_command("FLUSHDB")


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
    e = refused("SETF", "broken", "function call(...) return end end")
    assert e, "a script that does not compile should be refused"
    assert refused("GETF", "broken") is None and r.execute_command("GETF", "broken") is None, \
        "a refused SETF must not have written anything"

    # a script that compiles but declares no call()
    e = refused("SETF", "noentry", 'function other() return 1 end')
    assert e and "call" in e, f"the refusal should name call(), said: {e}"

    # a stored GET does not overload GET. The dotted form is how you call it.
    assert r.execute_command("SETF", "GET", GREET) == b"OK"
    assert r.execute_command("GET", "no-such-key") is None, \
        "bare GET must stay the builtin"
    r.execute_command("REMF", "GET")

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

    # colon is the builtin in that space; a dotted name is the stored function
    # against whatever space is current. See TODO 160.
    assert r.execute_command("fspace:SETF", "SET", '''
        function call(v)
            barch.store.set("fromfn", v)
            barch.current().seen = "yes"
            return barch.running()
        end
    ''') == b"OK"
    assert r.execute_command("fspace:SETF", "CLOSEST", '''
        function call(q)
            return barch.store.get("fromfn")
        end
    ''') == b"OK"
    r.execute_command("USE", "myspace")
    assert r.set("a", "b") is True
    assert r.get("a") == b"b"
    got = r.execute_command("fspace.SET", "alpha")
    assert got == b"myspace" or (isinstance(got, bytes) and got.decode() == "myspace"), got
    assert r.get("fromfn") == b"alpha"
    assert r.get("seen") == b"yes"
    assert r.execute_command("fspace:SET", "alpha", "beta") == b"OK"
    assert r.execute_command("fspace:GET", "alpha") == b"beta"
    assert r.get("alpha") is None, "colon SET must not have written in the current space"
    assert r.execute_command("fspace.CLOSEST", "alfa") == b"alpha"
    r.execute_command("USE", "")
    # colon then dot, no USE: myspace:fspace.SET runs fspace's SET in myspace
    got = r.execute_command("myspace:fspace.SET", "gamma")
    assert got == b"myspace", got
    assert r.get("fromfn") is None, "must not have written in the default space"
    assert r.execute_command("myspace:GET", "fromfn") == b"gamma"
    assert r.execute_command("myspace:fspace.CLOSEST", "gmma") == b"gamma"
    r.execute_command("fspace:REMF", "SET")
    r.execute_command("fspace:REMF", "CLOSEST")

    # --- arity, declared by the script and checked before it runs -------------------
    assert r.execute_command("SETF", "exactly2",
                             'arity = 2\nfunction call(...) return select("#", ...) end') == b"OK"
    assert r.execute_command("exactly2", "a", "b") == 2
    for bad in ([], ["a"], ["a", "b", "c"]):
        e = refused("exactly2", *bad)
        assert e and "wrong number of arguments" in e, f"{bad} said: {e}"

    # a negative arity is a minimum, as it is in redis
    assert r.execute_command("SETF", "atleast1",
                             'arity = -1\nfunction call(...) return select("#", ...) end') == b"OK"
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
        function call(...) return "helpers" end
    ''') == b"OK"
    assert r.execute_command("SETF", "usesHelpers", '''
        local h = require("helpers")
        function call(n)
            return h.double(tonumber(n)) end
    ''') == b"OK"
    assert r.execute_command("usesHelpers", "21") == 42

    # the required name folds like any other
    assert r.execute_command("SETF", "usesUpper", '''
        local h = require("HELPERS")
        function call(...) return h.double(1) end
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

    # --- require("SPACE.NAME"): another space, nested helpers stay there ------------
    e = refused("SETF", "foo.bar", GREET)
    assert e and "." in e, f"a function name must not contain a dot, said: {e}"

    r.execute_command("SETF", "helpers", '''
        function who() return "from-default" end
        function call() return who() end
    ''')
    r.execute_command("SETF", "ghost", 'function call() return "ghost" end')
    r.execute_command("otherspace:SETF", "helpers", '''
        function who() return "from-other" end
        function call() return who() end
    ''')
    r.execute_command("otherspace:SETF", "more", '''
        function who() return "more-other" end
        function call() return who() end
    ''')
    r.execute_command("otherspace:SETF", "mod", '''
        local h = require("helpers")
        local m = require("more")
        function call()
            return {h.who(), m.who()}
        end
    ''')
    assert r.execute_command("SETF", "requses", '''
        local g = require("otherspace.mod")
        function call() return g.call() end
    ''') == b"OK"
    got = r.execute_command("requses")
    assert got == [b"from-other", b"more-other"], got

    # space half keeps its case
    e = refused("SETF", "reqcase",
                'local g = require("OTHERSPACE.mod")\nfunction call() return 1 end')
    assert e and "no function" in e.lower(), f"said: {e}"
    assert r.execute_command("SETF", "reqcase", '''
        local g = require("otherspace.mod")
        function call() return g.call() end
    ''') == b"OK"
    assert r.execute_command("reqcase") == [b"from-other", b"more-other"]

    # a dotted miss does not pick up a global of the same name
    e = refused("SETF", "reqghost",
                'local g = require("otherspace.ghost")\nfunction call() return 1 end')
    assert e and "ghost" in e.lower(), f"said: {e}"

    # an unknown space is not created
    assert r.execute_command("KSPACE", "EXIST", "nosuchreq") == 0
    e = refused("SETF", "reqnosuch",
                'local g = require("nosuchreq.mod")\nfunction call() return 1 end')
    assert e, f"said: {e}"
    assert r.execute_command("KSPACE", "EXIST", "nosuchreq") == 0, \
        "require of an unknown space must not build one"

    # unqualified require still falls through to a global
    r.execute_command("emptyreq:SETF", "reqglob", '''
        local h = require("helpers")
        function call() return h.who() end
    ''')
    assert r.execute_command("emptyreq:reqglob") == b"from-default"

    r.execute_command("REMF", "requses")
    r.execute_command("REMF", "reqcase")
    r.execute_command("REMF", "ghost")
    r.execute_command("REMF", "helpers")
    for n in ("mod", "more", "helpers"):
        r.execute_command("otherspace:REMF", n)
    r.execute_command("emptyreq:REMF", "reqglob")

    # --- barch.call, into the ordinary commands -------------------------------------
    assert r.execute_command("SETF", "roundtrip", '''
        function call(...)
            local argv = {...}
            barch.call("SET", argv[1], argv[2])
            return barch.call("GET", argv[1])
        end
    ''') == b"OK"
    assert r.execute_command("roundtrip", "fromscript", "written").decode() == "written"
    assert r.execute_command("GET", "fromscript").decode() == "written", \
        "the write a script made is an ordinary write"

    # numbers go over as the wire would carry them, without the script saying tostring
    assert r.execute_command("SETF", "counterup", '''
        function call(...)
            barch.call("SET", "n", 0)
            barch.call("INCRBY", "n", 41)
            return barch.call("INCR", "n")
        end
    ''') == b"OK"
    assert r.execute_command("counterup") == 42

    # an array reply becomes a table, and a missing key becomes nil
    assert r.execute_command("SETF", "readsmany", '''
        function call(...)
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
        assert r.execute_command("SETF", name, "function call(...) %s end" % body) == b"OK"
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
        function call(...)
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
        function call(key, from, to)
            local v = barch.store.get(key)
            if v == nil then return nil end
            return string.sub(v, tonumber(from) + 1, tonumber(to) + 1)
        end
    ''') == b"OK"
    assert r.execute_command("myGetrange", "sk2", "0", "2").decode() == "cha"
    assert r.execute_command("myGetrange", "sk2", "0", "2").decode() == \
        r.execute_command("GETRANGE", "sk2", "0", "2").decode()

    # COUNT, on barch.store.count
    assert r.execute_command("SETF", "myCount", '''
        function call(lo, hi) return barch.store.count(lo, hi) end
    ''') == b"OK"
    assert r.execute_command("myCount", "sk0", "sk9") == 4

    # a KEYS-shaped walk, bounded, on barch.store.range
    assert r.execute_command("SETF", "myKeys", '''
        function call(...)
            local argv = {...}
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
        function call(...)
            return {barch.store.min(), barch.store.max(),
                    barch.store.exists("sk0") and "y" or "n",
                    barch.store.exists("nothing") and "y" or "n"}
        end
    ''') == b"OK"
    bounds = r.execute_command("myBounds")
    assert bounds[2] == b"y" and bounds[3] == b"n", f"exists gave {bounds}"

    # the space describes itself through barch.config(); `barch.space` is now the
    # key space itself rather than a function describing one. The
    # default space has no name in that vocabulary - `undecorate("node")` is "" and
    # there is no `space:` prefix for it - so that is what a function there sees
    assert r.execute_command("SETF", "mySpace", '''
        function call() local s = barch.config() return {s.name, s.ordered} end
    ''') == b"OK"
    assert r.execute_command("mySpace") == [b"", b"1"], \
        f"the default space has no prefix name, got {r.execute_command('mySpace')}"
    # in a named one it is the name the prefix uses
    assert r.execute_command("fspace:SETF", "mySpace", '''
        function call() return barch.config().name end
    ''') == b"OK"
    assert r.execute_command("fspace:mySpace").decode() == "fspace"

    # a range walk is bounded whatever the script asks for, so one function cannot
    # copy the whole space into a single reply
    assert r.execute_command("SETF", "myHuge", '''
        function call(lo, hi)
            return #barch.store.range(lo, hi, 100000000) end
    ''') == b"OK"
    assert r.execute_command("myHuge", "sk0", "sk9") == 4

    # --- barch.art(): a private one-shard space, same methods as a space handle ------
    assert r.execute_command("SETF", "scratchArt", '''
        function call()
            local q = barch.art()
            if q:size() ~= 0 or q:min() ~= nil or q:get("x") ~= nil then
                return "empty?"
            end
            q["a"] = "1"
            q:set("b", "2")
            if q:get("a") ~= "1" or q["b"] ~= "2" or q:size() ~= 2 then
                return "set"
            end
            q["a"] = nil
            if q:exists("a") or not q:remove("b") or q:size() ~= 0 then
                return "remove"
            end
            q["2"] = "n2"
            q["10"] = "n10"
            if q:min() ~= "2" or q:max() ~= "10" then
                return "numeric " .. tostring(q:min()) .. " " .. tostring(q:max())
            end
            local d = barch.art()
            d["0.2 b"] = "db"
            d["0.1 a"] = "da"
            if d:min() ~= "0.1 a" then
                return "dist " .. tostring(d:min())
            end
            local k, v = d:popmin()
            if k ~= "0.1 a" or v ~= "da" or d:min() ~= "0.2 b" then
                return "popmin"
            end
            q["x"] = "mine"
            if d:get("x") ~= nil then
                return "leak"
            end
            local r = d:range("0", "9", 100000000)
            if #r > 10000 then
                return "cap"
            end
            return "ok"
        end
    ''') == b"OK"
    assert r.execute_command("scratchArt").decode() == "ok", \
        f"scratch art: {r.execute_command('scratchArt')!r}"
    # a second call does not see the first call's tree
    assert r.execute_command("SETF", "scratchAgain", '''
        function call()
            return barch.art():get("x")
        end
    ''') == b"OK"
    assert r.execute_command("scratchAgain") is None

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
        function call(...)
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

    # --- reading the store directly answers to the same rights as a command ---------
    # a function must not be a way round the check a client would have failed. This
    # is the barch.call check one layer down, where there is no command to read the
    # categories off, so store_for decides them from the equivalent command's
    r.execute_command("SET", "aclkey", "visible")
    assert r.execute_command("SETF", "aclreads",
                             'function call() return barch.store.get("aclkey") end') == b"OK"
    # the default user holds every right, so it reads
    assert r.execute_command("aclreads").decode() == "visible"

    # a user with +function and nothing else may call a function, and must still be
    # refused at the read inside it
    r.execute_command("ACL", "SETUSER", "fnreader", "on", ">s3cret", "+function")
    try:
        restricted = redis.Redis(host="127.0.0.1", port=PORT, db=0)
        # redis-py maps AUTH's +OK to True through its own response callback
        assert restricted.execute_command("AUTH", "fnreader", "s3cret") is True
        try:
            got = restricted.execute_command("aclreads")
            assert False, f"the read should have been refused, answered {got!r}"
        except redis.exceptions.ResponseError as e:
            assert "not authorized" in str(e), f"said: {e}"
        # and the same user is refused the equivalent command, which is the point:
        # the two routes to the same key answer to the same rights
        try:
            restricted.execute_command("GET", "aclkey")
            assert False, "GET should have been refused too"
        except redis.exceptions.ResponseError as e:
            assert "authorized" in str(e), f"said: {e}"
        restricted.close()
    finally:
        r.execute_command("ACL", "DEL", "fnreader")
    r.execute_command("DEL", "aclkey")

    # --- who can reach a function at all --------------------------------------------
    # a walk inside a script always belongs to someone holding +function, because
    # invoking one needs that category before a line of Luau runs. So the store's
    # own may_see_functions guard is belt and braces rather than the gate. What is
    # *not* covered is the command level - KEYS and MAX still name function keys to
    # any reader - see TODO 98
    # and a range cannot reach them anyway: bounds are encoded like any key, so they
    # are tstring led and the function range is tfunction led, which sorts past every
    # string bound there is. `max()` is the one entry point that can surface one
    r.execute_command("SET", "walkkey", "v")
    assert r.execute_command("SETF", "walker", '''
        function call(...)
            local ks = barch.store.range("", "\255", 1000)
            local top = barch.store.max()
            return {#ks, top == nil and "nil" or top}
        end
    ''') == b"OK"
    walked = r.execute_command("walker")
    assert walked[0] >= 1, f"the walk should see plain keys, got {walked}"
    # the default user holds +function, so the top of the space is visible to it
    assert walked[1] == b"WALKER", f"max should reach the function range, got {walked}"
    r.execute_command("DEL", "walkkey")

    r.execute_command("SET", "blindkey", "v")
    r.execute_command("ACL", "SETUSER", "fnblind", "on", ">s3cret", "+read", "+keys")
    try:
        blind = redis.Redis(host="127.0.0.1", port=PORT, db=0)
        assert blind.execute_command("AUTH", "fnblind", "s3cret") is True
        for attempt in (("walker",), ("CALLF", "walker"), ("GETF", "walker")):
            try:
                blind.execute_command(*attempt)
                assert False, f"{attempt[0]} should need +function"
            except redis.exceptions.ResponseError as e:
                assert "authorized" in str(e), f"{attempt}: {e}"

        # and the commands do not name them either: a walk for this user behaves as
        # though the function range is not there
        blind_keys = {k.decode() for k in blind.execute_command("KEYS", "*")}
        assert "blindkey" in blind_keys, f"ordinary keys are still answered: {blind_keys}"
        assert not any(k in blind_keys for k in ("WALKER", "GREET", "COUNTER")), \
            f"a user without +function saw function keys: {sorted(blind_keys)}"
        _, scanned = blind.execute_command("SCAN", "0")
        assert not any(s.decode() in ("WALKER", "GREET") for s in scanned), \
            "SCAN named a function key"
        # MAX answers the largest key below the function range rather than a null.
        # The range is the top of the key order, so the unfiltered maximum always
        # lands in it once a space holds a function
        top = blind.execute_command("MAX")
        assert top is not None, "MAX should answer a real key, not null"
        assert top.decode() == "blindkey", f"MAX gave {top!r}"
        # and the holder still gets the true maximum, which is a function
        assert r.execute_command("MAX").decode() in ("WALKER", "GREET", "COUNTER",
                                                     "EXACTLY2", "SURVIVOR", "VERSION",
                                                     "ACLREADS", "MYCOMPOSITE"), \
            f"the holder should see the function range: {r.execute_command('MAX')!r}"
        # the default user, holding +function, still sees them
        assert any(k in {k.decode() for k in r.execute_command("KEYS", "*")}
                   for k in ("WALKER", "GREET")), "the holder should still see them"
        blind.close()
    finally:
        r.execute_command("ACL", "DEL", "fnblind")
    r.execute_command("DEL", "blindkey")

    # --- a parked call keeps its place in a pipeline --------------------------------
    # a function does not run on the connection's thread any more: it parks, the script
    # runs in slices on the foreign pool, and the reply is written when it wakes. The
    # replies behind it must not overtake it. See TODO 98 H
    assert r.execute_command("SETF", "slowish",
                             'function call() local n=0 for i=1,2000000 do n=n+i end return "fn" end') == b"OK"
    r.execute_command("SET", "pipea", "1")
    pipe = r.pipeline(transaction=False)
    pipe.execute_command("SET", "pipea", "2")
    pipe.execute_command("slowish")
    pipe.execute_command("GET", "pipea")
    pipe.execute_command("slowish")
    pipe.execute_command("PING")
    assert pipe.execute() == [True, b"fn", b"2", b"fn", True], "the pipeline came back out of order"
    # and with an asynchronous command in front, which forces the batch path
    pipe2 = r.pipeline(transaction=False)
    pipe2.execute_command("KEYS", "pipea")
    pipe2.execute_command("slowish")
    pipe2.execute_command("GET", "pipea")
    out2 = pipe2.execute()
    assert out2[1] == b"fn" and out2[2] == b"2", f"batched pipeline gave {out2}"
    r.execute_command("DEL", "pipea")

    # --- 64 bit integers and buffers, for scripts that compute ---------------------
    # Luau numbers are doubles, so a 64 bit identifier - an H3 geo cell, a hash, a
    # snowflake - could only be carried as two halves through bit32 without these.
    # The conversion has to keep every bit: lua_tointegerx is the 32 bit accessor and
    # silently kept the low word, which turned 2^62 into 0. See TODO 98
    assert r.execute_command("SETF", "bigint", '''
        function call()
            local v = integer.lshift(integer.create(1), integer.create(62))
            return integer.add(v, integer.create(12345))
        end
    ''') == b"OK"
    assert r.execute_command("bigint") == 2**62 + 12345, \
        f"a 64 bit value lost bits: {r.execute_command('bigint')}"

    # the bit work an H3 style cell id is made of, packed into the high bits where
    # truncation would have hidden itself
    assert r.execute_command("SETF", "packcell", '''
        function call(...)
            local argv = {...}
            local res = tonumber(argv[1])
            local cell = integer.bor(integer.create(0),
                                     integer.lshift(integer.create(1), integer.create(59)))
            cell = integer.bor(cell, integer.lshift(integer.create(res), integer.create(52)))
            return cell
        end
    ''') == b"OK"
    packed = r.execute_command("packcell", "9")
    assert packed == (1 << 59) | (9 << 52), f"cell packing gave {packed}"

    # buffers, for a lookup table that is not a table of boxed numbers
    assert r.execute_command("SETF", "bufwork", '''
        function call()
            local b = buffer.create(1024)
            for i = 0, 255 do buffer.writeu32(b, i * 4, i * i) end
            return buffer.readu32(b, 200)
        end
    ''') == b"OK"
    assert r.execute_command("bufwork") == 2500

    # --- a key space as a value ----------------------------------------------------
    # barch.space.NAME.key reads, assigns and removes, and iterates. See TODO 98 F2
    r.execute_command("SET", "sv1", "one")
    r.execute_command("SET", "sv2", "two")
    assert r.execute_command("SETF", "spaceval", '''
        function call(name)
            local sp = barch.space[name]
            local before = sp.sv1
            sp.sv3 = "three"           -- write
            sp.sv2 = nil               -- remove
            return {before, sp.sv3, sp.sv2 == nil and "gone" or "still there"}
        end
    ''') == b"OK"
    assert r.execute_command("spaceval", "") == [b"one", b"three", b"gone"], \
        f"got {r.execute_command('spaceval', '')}"
    # and the write a script made is an ordinary key
    assert r.execute_command("GET", "sv3").decode() == "three"
    assert r.execute_command("GET", "sv2") is None

    # the dotted form and the bracket form are the same thing
    assert r.execute_command("SETF", "spacedot", '''
        function call() return barch.space[""].sv3 end
    ''') == b"OK"
    assert r.execute_command("spacedot").decode() == "three"

    # a name that is not a key space is refused, and touching it does not create one
    assert r.execute_command("SETF", "spacebad", '''
        function call()
            local ok, e = pcall(function() return barch.space["nosuchspace"].k end)
            return ok and "opened" or "refused"
        end
    ''') == b"OK"
    assert r.execute_command("spacebad").decode() == "refused"
    assert r.execute_command("KSPACE", "EXIST", "nosuchspace") == 0, \
        "mentioning a space must not build one"

    # iterating: every row says what it is, so a walk can skip what it does not want
    assert r.execute_command("SETF", "spacewalk", '''
        function call()
            local keys, fns = 0, 0
            for row in barch.space[""] do
                if row.type == "function" then fns = fns + 1
                elseif row.type == "key" then keys = keys + 1 end
            end
            return {keys, fns}
        end
    ''') == b"OK"
    walked = r.execute_command("spacewalk")
    assert walked[0] >= 2, f"the walk should see the plain keys, got {walked}"
    assert walked[1] >= 1, f"and the functions, got {walked}"

    r.execute_command("DEL", "sv1"); r.execute_command("DEL", "sv3")

    # --- containers: a hash, a list and an ordered set from a script ----------------
    # a list, hash and ordered set are the same key shape with a different lead, so one
    # handle serves all three once the kind is known. See TODO 98 F
    r.execute_command("HSET", "ch", "one", "1", "two", "2", "three", "3")
    r.execute_command("ZADD", "cz", "1.5", "alpha", "2.5", "beta")
    r.execute_command("RPUSH", "cl", "x", "y")

    assert r.execute_command("SETF", "ckind", '''
        function call(n) return barch.space[""]:kind(n) end
    ''') == b"OK"
    assert r.execute_command("ckind", "ch").decode() == "hash"
    assert r.execute_command("ckind", "cz").decode() == "orderedset"
    assert r.execute_command("ckind", "cl").decode() == "list"
    assert r.execute_command("ckind", "nosuchthing") is None

    # read, write and remove a field, the way a space handle does a key
    assert r.execute_command("SETF", "cfield", '''
        function call(n, f)
            local h = barch.space[""]:container(n)
            local before = h[f]
            h.added = "new"
            h.two = nil
            return {before, h.added, h.two == nil and "gone" or "there"}
        end
    ''') == b"OK"
    assert r.execute_command("cfield", "ch", "one") == [b"1", b"new", b"gone"], \
        f"got {r.execute_command('cfield', 'ch', 'one')}"
    # and the built-in agrees about what the script did
    assert r.execute_command("HGET", "ch", "added").decode() == "new"
    assert r.execute_command("HGET", "ch", "two") is None

    # HRANDFIELD, which section F said could not be written against this interface
    assert r.execute_command("SETF", "myHrandfield", '''
        function call(n)
            local fields = {}
            for member in barch.space[""]:container(n) do
                fields[#fields + 1] = member
            end
            if #fields == 0 then return nil end
            return fields[(math.random(#fields))]
        end
    ''') == b"OK"
    got = r.execute_command("myHrandfield", "ch")
    assert got.decode() in ("one", "three", "added"), f"HRANDFIELD gave {got!r}"

    # ZRANGEBYSCORE, the other one
    assert r.execute_command("SETF", "myZrangebyscore", '''
        function call(n, lo, hi)
            local out = {}
            for member, score in barch.space[""]:container(n) do
                local s = tonumber(score)
                if s and s >= tonumber(lo) and s <= tonumber(hi) then
                    out[#out + 1] = member
                end
            end
            return out
        end
    ''') == b"OK"
    assert r.execute_command("myZrangebyscore", "cz", "1.0", "2.0") == [b"alpha"], \
        f"got {r.execute_command('myZrangebyscore', 'cz', '1.0', '2.0')}"
    assert r.execute_command("myZrangebyscore", "cz", "0", "9") == [b"alpha", b"beta"]

    # a name that is not a container is refused rather than invented
    assert r.execute_command("SETF", "cbad", '''
        function call()
            local ok = pcall(function() return barch.space[""]:container("nope").f end)
            return ok and "opened" or "refused"
        end
    ''') == b"OK"
    assert r.execute_command("cbad").decode() == "refused"

    # --- the slice and the deadline are the function's own ---------------------------
    # they used to be borrowed from foreign - see 98 I.2. Both answer the server
    # setting here, since this space overrides neither
    assert r.execute_command("KSPACE", "OPTION", "GET", "FUNCTION_SLICE") == 1000000
    assert r.execute_command("KSPACE", "OPTION", "GET", "FUNCTION_DEADLINE") == 1000

    # --- a key is a string, in and out, TODO 145 ------------------------------------
    # keys enter and leave barch as text, so that is what a script sees too - no
    # guessing at a stored type and no two ways to name one key. What matters is the
    # round trip: what a walk hands back must reach the same key when handed back
    r.execute_command("SET", "4242", "int key")
    r.execute_command("SET", "1.5", "double key")
    r.execute_command("SET", "plainstr", "string key")
    r.execute_command("SET", "two part", "composite value")
    assert r.execute_command("SETF", "keytypes", """
        function call()
            local seen = {}
            for row in barch.space[""] do
                if row.type == "key" and type(row.key) == "string" then
                    local v = barch.store.get(row.key)
                    if v ~= nil then seen[#seen+1] = row.key .. "=" .. v end
                end
            end
            return seen
        end
    """) == b"OK"
    seen = sorted(x.decode() for x in r.execute_command("keytypes"))
    for want in ("1.5=double key", "4242=int key", "plainstr=string key",
                 "two part=composite value"):
        assert want in seen, f"{want} missing from {seen}"

    # a number written by a client is addressed by its text, which is the same text
    # the client sent, so the script and the client name one key rather than two
    assert r.execute_command("SETF", "keyroundtrip", """
        function call()
            return { barch.store.get("4242"), barch.space[""]["two part"] }
        end
    """) == b"OK"
    assert r.execute_command("keyroundtrip") == [b"int key", b"composite value"]
    for k in ("4242", "1.5", "plainstr", "two part"):
        r.execute_command("DEL", k)

    # --- a cached source miss is not the same as an absent key, TODO 148 ------------
    # a foreign space has three states and a script could see only two of them. With a
    # fake source: one key the source has, one it does not (which GET turns into a
    # tomb), and one nothing has ever asked for
    conf = barch.KeyValue("configuration")
    conf.set("fn_fake.foreign", "fake")
    conf.set("fn_fake.missing_ttl", "30")
    conf.save()
    r.execute_command("USE", "fn_fake")
    r.execute_command("FOREIGN", "FAKE", "SET", "there", "from-source")
    assert r.get("there") == b"from-source"      # fills, and caches the value
    assert r.get("gone") is None                 # asks, gets nothing, and tombs it
    assert r.execute_command("SETF", "tombtell", """
        function call(k)
            local v = barch.store.get(k)
            if v == nil then return "unknown" end
            if v == barch.tomb then return "tombed" end
            return "value:" .. v
        end
    """) == b"OK"
    assert r.execute_command("tombtell", "there") == b"value:from-source"
    assert r.execute_command("tombtell", "gone") == b"tombed", \
        "a cached source miss must be distinguishable"
    assert r.execute_command("tombtell", "never-asked") == b"unknown"

    # the same three answers through the space value, which is the other way in
    assert r.execute_command("SETF", "tombspace", """
        function call(k)
            local v = barch.space["fn_fake"][k]
            if v == nil then return "unknown" end
            if v == barch.tomb then return "tombed" end
            return "value:" .. v
        end
    """) == b"OK"
    assert r.execute_command("tombspace", "gone") == b"tombed"
    assert r.execute_command("tombspace", "never-asked") == b"unknown"

    # the token has an identity of its own rather than being a bool a script can
    # confuse with a value, and it cannot be rewritten from inside a script
    assert r.execute_command("SETF", "tombkind", """
        function call()
            return { tostring(barch.tomb), tostring(barch.tomb == false) }
        end
    """) == b"OK"
    assert r.execute_command("tombkind") == [b"barch.tomb", b"false"]
    r.execute_command("USE", "")

    # --- the Luau states show up in INFO MEMORY, TODO 151 ---------------------------
    # a state belongs to the session using it, so the bytes are counted by the
    # allocator every state is built with rather than read out of someone else's VM
    before = r.info("memory")
    assert "used_memory_luau" in before, "INFO MEMORY should report the Luau states"
    assert r.execute_command("SETF", "statfn",
                             'function call() return 1 end') == b"OK"
    r.execute_command("statfn")
    after = r.info("memory")
    assert after["luau_states"] >= 1, after["luau_states"]
    assert after["luau_functions_compiled"] >= 1
    assert after["used_memory_luau"] > 0
    r.execute_command("REMF", "statfn")

    # --- one state, several spaces: names must not collide, TODO 150 ----------------
    # a session keeps one Luau state for every space it touches, so the compiled
    # functions of two spaces live in one map. If the key were the bare name, the
    # second space would get the first space's compiled function
    r.execute_command("USE", "")
    assert r.execute_command("SETF", "shared",
                             'function call() return "from-default" end') == b"OK"
    assert r.execute_command("SETF", "asks",
                             'local s = require("shared")\n'
                             'function call() return s.call() end') == b"OK"
    r.execute_command("USE", "collb")
    assert r.execute_command("SETF", "shared",
                             'function call() return "from-collb" end') == b"OK"
    assert r.execute_command("SETF", "asks",
                             'local s = require("shared")\n'
                             'function call() return s.call() end') == b"OK"
    r.execute_command("USE", "")
    assert r.execute_command("shared") == b"from-default"
    assert r.execute_command("asks") == b"from-default", "require must stay in its space"
    r.execute_command("USE", "collb")
    assert r.execute_command("shared") == b"from-collb"
    assert r.execute_command("asks") == b"from-collb", "require must stay in its space"
    for n in ("shared", "asks"):
        r.execute_command("REMF", n)
    r.execute_command("USE", "")
    for n in ("shared", "asks"):
        r.execute_command("REMF", n)

    # --- nested calls are bounded, TODO 98 E ----------------------------------------
    # CALLF is an ordinary built-in, so a script reaches a script, and each level used
    # to start with a deadline of its own - the deadline bounded one call and not a
    # tree. A function that calls itself now hits the depth limit and says so, and the
    # server is still there afterwards, which is the half that matters
    assert r.execute_command("SETF", "recurse", """
        function call(n)
            return barch.call("CALLF", "recurse", n)
        end
    """) == b"OK"
    try:
        r.execute_command("recurse", "1")
        raise AssertionError("unbounded recursion should have been stopped")
    except redis.ResponseError as e:
        assert "too deep" in str(e), e
    assert r.ping() is True

    # a chain shorter than the limit still works. The limit itself is a setting, and
    # configtest is where it is round tripped - asserting its value here would couple
    # this test to whatever another test last saved
    assert r.execute_command("SETF", "leaf", """
        function call() return "bottom" end
    """) == b"OK"
    assert r.execute_command("SETF", "viacall", """
        function call() return barch.call("CALLF", "leaf") end
    """) == b"OK"
    assert r.execute_command("viacall") == b"bottom"

    # and the depth does not accumulate across commands on one connection: the sub
    # caller is reused, so a connection would otherwise climb to the limit one call
    # at a time
    for _ in range(5):
        assert r.execute_command("viacall") == b"bottom"

    # --- the locked region, TODO 98 F6 ---------------------------------------------
    # a read and a write with nothing able to land between them. Without the lock this
    # is the classic lost update; with it, concurrent callers each see their own
    # increment
    assert r.execute_command("SETF", "lockedincr", """
        function call(k)
            local out
            barch.store.locked(k, function()
                local n = tonumber(barch.store.get(k)) or 0
                n = n + 1
                barch.store.set(k, tostring(n))
                out = n
            end)
            return out
        end
    """) == b"OK"
    r.delete("lk")
    assert r.execute_command("lockedincr", "lk") == 1
    assert r.execute_command("lockedincr", "lk") == 2
    assert r.get("lk") == b"2"

    # what the region returns is what the call returns
    assert r.execute_command("SETF", "lockedret", """
        function call(k)
            return barch.store.locked(k, function()
                return "from inside"
            end)
        end
    """) == b"OK"
    assert r.execute_command("lockedret", "lk") == b"from inside"

    # the three refusals that make holding a shard lock safe. Each one would otherwise
    # deadlock or stall every other connection on that shard
    assert r.execute_command("SETF", "lockedcall", """
        function call(k)
            return barch.store.locked(k, function()
                return barch.call("GET", k)
            end)
        end
    """) == b"OK"
    try:
        r.execute_command("lockedcall", "lk")
        raise AssertionError("barch.call inside a locked region must be refused")
    except redis.ResponseError as e:
        assert "locked region" in str(e), e

    # a region inside a region: the inner one is already covered, and letting it
    # through would drop the outer hold when it left
    assert r.execute_command("SETF", "locknest", """
        function call(k)
            return barch.store.locked(k, function()
                return barch.store.locked(k, function() return 1 end)
            end)
        end
    """) == b"OK"
    try:
        r.execute_command("locknest", "lk")
        raise AssertionError("a nested locked region must be refused")
    except redis.ResponseError as e:
        assert "locked region" in str(e), e

    # an error inside the region comes back as an error, and - the point of the test -
    # does not leave the shard locked. The call after it is what says so
    assert r.execute_command("SETF", "lockedboom", """
        function call(k)
            return barch.store.locked(k, function()
                error("boom")
            end)
        end
    """) == b"OK"
    try:
        r.execute_command("lockedboom", "lk")
        raise AssertionError("expected the error to come back out")
    except redis.ResponseError:
        pass
    assert r.execute_command("lockedincr", "lk") == 3, "the lock must have gone back"
    assert r.ping() is True

    # and the proof, since a single threaded increment would pass without any lock at
    # all: eight connections racing on one key. Measured with the lock taken out, this
    # same loop lost six of 2000 updates, so the test can see the thing it is testing
    import threading
    r.delete("race")
    def hammer():
        c = redis.Redis(port=PORT)
        for _ in range(250):
            c.execute_command("lockedincr", "race")
        c.close()
    crowd = [threading.Thread(target=hammer) for _ in range(8)]
    for th in crowd: th.start()
    for th in crowd: th.join()
    assert int(r.get("race")) == 2000, f"lost updates: {r.get('race')}"

    # the whole space, when no key is named. Every shard, so a read of any key is
    # covered rather than being a second shard
    assert r.execute_command("SETF", "lockedall", """
        function call(a, b)
            return barch.store.locked(function()
                barch.store.set(a, "one")
                barch.store.set(b, "two")
                return barch.store.get(a) .. "/" .. barch.store.get(b)
            end)
        end
    """) == b"OK"
    assert r.execute_command("lockedall", "spanA", "spanB") == b"one/two"

    # ...and the same two keys under a single key's lock must abort rather than
    # quietly take a second shard lock. Which of 347 shards a key lands on is not
    # something the test can predict, so it looks for the first key that is elsewhere
    assert r.execute_command("SETF", "lockedcross", """
        function call(k)
            return barch.store.locked(k, function()
                for i = 1, 200 do
                    local other = "cross_" .. i
                    local ok, e = pcall(function() return barch.store.get(other) end)
                    if not ok then return tostring(e) end
                end
                return "no other shard found"
            end)
        end
    """) == b"OK"
    crossed = r.execute_command("lockedcross", "lk").decode()
    assert "second shard" in crossed, f"expected the cross shard abort, got {crossed!r}"

    # shardNumber and hasLock, which exist so a script can work out what the locked
    # region will allow before it runs into the abort
    assert r.execute_command("SETF", "shardof", """
        function call(k)
            return barch.store.shardNumber(k)
        end
    """) == b"OK"
    n = r.execute_command("shardof", "lk")
    assert isinstance(n, int) and n >= 0, n
    assert r.execute_command("shardof", "lk") == n, "the same key must route the same way"

    # the pair the region would refuse can be found without asking for it: scan for a
    # key on another shard and check the two answers really differ
    assert r.execute_command("SETF", "findother", """
        function call(k)
            local mine = barch.store.shardNumber(k)
            for i = 1, 400 do
                local other = "cross_" .. i
                if barch.store.shardNumber(other) ~= mine then
                    return { other, barch.store.shardNumber(other) }
                end
            end
            return nil
        end
    """) == b"OK"
    other = r.execute_command("findother", "lk")
    assert other is not None, "expected some key on another shard"
    assert int(other[1]) != n

    # hasLock answers for where it is asked: false outside a region, true inside one
    # for the key that region locked
    assert r.execute_command("SETF", "lockstate", """
        function call(k)
            local before = barch.store.hasLock(k)
            local inside = barch.store.locked(k, function()
                return barch.store.hasLock(k)
            end)
            local after = barch.store.hasLock(k)
            return { tostring(before), tostring(inside), tostring(after) }
        end
    """) == b"OK"
    assert r.execute_command("lockstate", "lk") == [b"false", b"true", b"false"]

    # --- a function that returns nothing answers null, and does not take the server
    # down doing it. `lua_gettop` is 0 on an empty stack and 0 is not a valid Lua
    # index, so this used to be a SIGILL - see TODO 143. The PING is the point of the
    # test: a wrong answer here is a dead server, not a wrong reply
    assert r.execute_command("SETF", "returnsnothing", """
        function call(k)
        end
    """) == b"OK"
    assert r.execute_command("returnsnothing", "x") is None
    assert r.ping() is True

    # --- barch.store writes, so both halves of the interface agree -------------------
    assert r.execute_command("SETF", "storewrite", """
        function call(k, v)
            barch.store.set(k, v)
            local back = barch.store.get(k)
            barch.store.remove(k)
            -- a trailing nil is not a hole in a Lua table, it is just a shorter
            -- table, so the "it went" half has to be said rather than returned as nil
            local gone = barch.store.get(k) == nil
            return { back, gone and "gone" or "still there" }
        end
    """) == b"OK"
    assert r.execute_command("storewrite", "sw_k", "sw_v") == [b"sw_v", b"gone"]
    assert r.execute_command("EXISTS", "sw_k") == 0, "remove must actually remove"

    # a written key is visible to the ordinary commands, not just to the script
    assert r.execute_command("SETF", "storeput", """
        function call(k, v)
            barch.store.set(k, v)
        end
    """) == b"OK"
    r.execute_command("storeput", "sw_seen", "here")
    assert r.get("sw_seen") == b"here"

    # setting nil removes, the same as `sp[k] = nil` does
    assert r.execute_command("SETF", "storenil", """
        function call(k)
            barch.store.set(k, nil)
        end
    """) == b"OK"
    r.execute_command("storenil", "sw_seen")
    assert r.execute_command("EXISTS", "sw_seen") == 0

    for n in ("ch", "cz", "cl"):
        r.execute_command("DEL", n)

    # --- setBufferAt / getBufferAt, and the int helpers ---------------------------
    # missing is nil; a write creates; integers are little-endian so they match
    # buffer.readi32; an offset past the end is nil; a gap is zero-filled
    r.delete("bufk")
    assert r.execute_command("SETF", "bufmiss", """
        function call(k)
            return barch.store.getBufferAt(k) == nil
                and barch.store.getInt32At(k) == nil
                and barch.store.getInt64At(k) == nil
        end
    """) == b"OK"
    assert r.execute_command("bufmiss", "bufk") == 1

    assert r.execute_command("SETF", "bufi32", """
        function call(k, n)
            barch.store.setInt32At(k, n)
            return barch.store.getInt32At(k)
        end
    """) == b"OK"
    assert r.execute_command("bufi32", "bufk", 42) == 42
    raw = r.get("bufk")
    assert raw == b"\x2a\x00\x00\x00", raw
    assert r.execute_command("bufi32", "bufk", -7) == -7

    assert r.execute_command("SETF", "bufi64", """
        function call(k, n)
            barch.store.setInt64At(k, n)
            return barch.store.getInt64At(k)
        end
    """) == b"OK"
    big = 2 ** 62
    assert r.execute_command("bufi64", "bufk64", big) == big
    assert r.get("bufk64") == (big).to_bytes(8, "little", signed=True)

    assert r.execute_command("SETF", "bufraw", """
        function call(k, n)
            local b = buffer.create(4)
            buffer.writei32(b, 0, n)
            barch.store.setBufferAt(k, b)
            local g = barch.store.getBufferAt(k)
            if type(g) ~= "buffer" or buffer.len(g) ~= 4 then
                return "type"
            end
            return buffer.readi32(g, 0)
        end
    """) == b"OK"
    assert r.execute_command("bufraw", "bufrawk", 99) == 99

    # grow: write at offset 4 on an empty key, then the first four bytes are zero
    assert r.execute_command("SETF", "bufgrow", """
        function call(k)
            barch.store.setInt32At(k, 7, 4)
            return { barch.store.getInt32At(k, 0), barch.store.getInt32At(k, 4),
                     barch.store.getInt32At(k, 8) == nil }
        end
    """) == b"OK"
    r.delete("bufgrowk")
    assert r.execute_command("bufgrow", "bufgrowk") == [0, 7, 1]
    assert r.get("bufgrowk") == b"\x00\x00\x00\x00\x07\x00\x00\x00"

    # in-place overwrite keeps the rest of the value
    assert r.execute_command("SETF", "bufpatch", """
        function call(k)
            barch.store.setInt32At(k, 1, 0)
            return barch.store.getInt32At(k, 4)
        end
    """) == b"OK"
    assert r.execute_command("bufpatch", "bufgrowk") == 7

    # barch.art() is the same interface
    assert r.execute_command("SETF", "bufart", """
        function call()
            local q = barch.art()
            if q:getInt32At("n") ~= nil then return "empty" end
            q:setInt32At("n", 3)
            local b = buffer.create(2)
            buffer.writeu8(b, 0, 9)
            buffer.writeu8(b, 1, 8)
            q:setBufferAt("b", b)
            local g = q:getBufferAt("b")
            return { q:getInt32At("n"), buffer.len(g), buffer.readu8(g, 0), buffer.readu8(g, 1) }
        end
    """) == b"OK"
    assert r.execute_command("bufart") == [3, 2, 9, 8]

    # a string is refused; this wants a buffer
    assert r.execute_command("SETF", "bufneedbuf", """
        function call(k)
            local ok, err = pcall(function() barch.store.setBufferAt(k, "nope") end)
            return ok and "accepted" or "refused"
        end
    """) == b"OK"
    assert r.execute_command("bufneedbuf", "bufk") == b"refused"

    # increment: int32 in place vs tonumber/tostring. Timed from Python because
    # the sandbox has no clock. One CALLF, a hundred thousand loops on one key,
    # unordered. The lock is per increment, not around the whole loop. Slice and
    # deadline are function_slice_insns and function_deadline_ms.
    #
    # This was a million, which is fine on a release build - about a second - but
    # the ubuntu24 CI builds with COVERAGE=ON, and -fprofile-update=atomic turns
    # every basic block counter in the loop into an atomic increment. That is
    # roughly 4.5x, so the call took ~4.8s there and redis-py 8 hands a client a
    # 5 second socket_timeout unless you ask for something else. On a runner
    # slower than the machine it was written on it went over and CI failed on a
    # read timeout, not on anything the server did wrong. The ratio is what this
    # bench is for and a hundred thousand shows it just as well. See TODO 193.
    N = 100000
    r.execute_command("CONFIG", "SET", "function_deadline_ms", "60000")
    r.execute_command("CONFIG", "SET", "function_slice_insns", "10000000")
    # colon so USE does not have to stick across redis-py's pool
    r.execute_command("bufbench:KSPACE", "OPTION", "SET", "ORDERED", "OFF")
    ordered = r.execute_command("bufbench:KSPACE", "OPTION", "GET", "ORDERED")
    assert not ordered, ordered
    assert r.execute_command("bufbench:SETF", "incrstr", """
        function call(k, n)
            n = tonumber(n)
            for i = 1, n do
                barch.store.locked(k, function()
                    local v = tonumber(barch.store.get(k)) or 0
                    barch.store.set(k, tostring(v + 1))
                end)
            end
            return tonumber(barch.store.get(k))
        end
    """) == b"OK"
    assert r.execute_command("bufbench:SETF", "incri32", """
        function call(k, n)
            n = tonumber(n)
            for i = 1, n do
                barch.store.locked(k, function()
                    local v = barch.store.getInt32At(k) or 0
                    barch.store.setInt32At(k, v + 1)
                end)
            end
            return barch.store.getInt32At(k)
        end
    """) == b"OK"
    r.execute_command("bufbench:DEL", "incr_str", "incr_i32")
    t0 = time.perf_counter()
    assert r.execute_command("bufbench:CALLF", "incrstr", "incr_str", N) == N
    str_s = time.perf_counter() - t0
    t0 = time.perf_counter()
    assert r.execute_command("bufbench:CALLF", "incri32", "incr_i32", N) == N
    i32_s = time.perf_counter() - t0
    print(f"increment bench n={N} ordered=off: string {str_s:.4f}s "
          f"({1e6*str_s/N:.1f} us/op), int32 {i32_s:.4f}s "
          f"({1e6*i32_s/N:.1f} us/op), ratio {str_s/i32_s:.2f}x", flush=True)
    r.execute_command("CONFIG", "SET", "function_deadline_ms", "1000")
    r.execute_command("CONFIG", "SET", "function_slice_insns", "1000000")

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
    # --- FLUSHALL leaves configuration alone, TODO 149 ------------------------------
    # configuration is not data: it holds every space's settings and the global
    # functions, so clearing the caches must not unconfigure the server.
    #
    # Last in the file on purpose - FLUSHALL reaches every space, so anything after it
    # would be running against a suite whose functions had just been cleared
    conf = barch.KeyValue("configuration")
    conf.set("fa_probe.key_split", ".")
    conf.save()
    r.execute_command("USE", "configuration")
    assert r.execute_command("SETF", "globalfn", """
        function call() return "still here" end
    """) == b"OK"
    r.execute_command("USE", "")
    r.execute_command("SET", "flushme", "data")
    assert r.get("flushme") == b"data"
    r.execute_command("FLUSHALL")
    assert r.get("flushme") is None, "ordinary keys should be gone"
    r.execute_command("USE", "configuration")
    assert r.execute_command("GETF", "globalfn") is not None, \
        "a global function must survive FLUSHALL"
    assert b"GLOBALFN" in r.execute_command("KEYSF")
    assert conf.get("fa_probe.key_split") == ".", "settings must survive FLUSHALL"
    r.execute_command("REMF", "globalfn")
    r.execute_command("USE", "")

    for n in ("greet", "counter", "broken", "noentry", "spin", "boom", "r_err",
              "r_nil", "r_int", "r_float", "r_true", "r_false", "r_str", "r_array",
              "r_ok", "r_nested", "laterfn", "dotted", "exactly2", "atleast1",
              "setsglobal", "readsglobal", "version", "helpers", "usesHelpers",
              "usesUpper", "usesMissing", "cyclea", "cycleb", "selfref",
              "requses", "reqcase", "reqghost", "reqnosuch", "reqglob", "ghost",
              "roundtrip", "counterup", "readsmany", "survives",
              "ref_block", "ref_async", "ref_multi", "ref_unknown",
              "myGetrange", "myCount", "myKeys", "myBounds", "mySpace", "myHuge",
              "myComposite", "aclreads", "walker", "slowish",
              "bigint", "packcell", "bufwork", "spaceval", "spacedot",
              "spacebad", "spacewalk", "ckind", "cfield", "myHrandfield",
              "myZrangebyscore", "cbad", "storewrite", "storeput", "storenil",
              "returnsnothing", "lockedincr", "lockedret", "lockedcall",
              "locknest", "lockedboom", "lockedall", "lockedcross", "shardof", "findother",
              "lockstate", "keytypes", "keyroundtrip", "recurse", "leaf",
              "viacall", "tombtell", "tombspace", "tombkind",
              "bufmiss", "bufi32", "bufi64", "bufraw", "bufgrow", "bufpatch",
              "bufart", "bufneedbuf", "incrstr", "incri32"):
        try:
            r.execute_command("REMF", n)
            r.execute_command("fspace:REMF", n)
            r.execute_command("bufbench:REMF", n)
        except redis.exceptions.ResponseError:
            pass
    r.close()
    barch.stop()

print("complete function test")
