# simdjson in stored Luau: parse, open/atPointer, encode.
import scale
import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

PORT = scale.port(default=14087)

print("start simdjson luau test")
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


try:
    assert r.execute_command("SETF", "sjparse", '''
        function call()
            local j = simdjson.parse('{"a":1,"b":[true,null,"x"],"n":2.5}')
            local frombuf = simdjson.parse(buffer.fromstring('{"k":7}'))
            local back = simdjson.encode({a = 1, b = {2, 3}})
            local doc = simdjson.open('{"Image":{"Width":800,"IDs":[116,943]}}')
            return {
                j.a,
                j.b[1] and 1 or 0,
                j.b[2] == simdjson.null and 1 or 0,
                j.b[3],
                j.n,
                back,
                doc:atPointer("/Image/Width"),
                doc:at("/Image/IDs/1"),
                frombuf.k,
            }
        end
    ''') == b"OK"
    got = r.execute_command("sjparse")
    assert got[0] == 1, got
    assert got[1] == 1, got
    assert got[2] == 1, got
    assert got[3] == b"x", got
    assert float(got[4]) == 2.5, got
    assert b'"a"' in got[5] and b'"b"' in got[5], got
    assert got[6] == 800, got
    assert got[7] == 943, got
    assert got[8] == 7, got

    # the input side already took a buffer - see frombuf above. This is the
    # output side, and the round trip through both. See TODO 215.
    assert r.execute_command("SETF", "sjbuf", '''
        function call()
            local b = simdjson.encodeBuffer({a = 1, b = {2, 3}, s = "x"})
            local viastring = simdjson.encode({a = 1, b = {2, 3}, s = "x"})
            -- straight back in, without ever being a lua string
            local round = simdjson.parse(b)
            -- and open() takes it too
            local doc = simdjson.open(b)
            return {
                buffer.len(b),
                #viastring,
                buffer.tostring(b) == viastring and 1 or 0,
                round.a,
                round.b[2],
                round.s,
                doc:atPointer("/b/1"),
            }
        end
    ''') == b"OK"
    got = r.execute_command("sjbuf")
    assert got[0] == got[1], ("buffer and string encodings differ in length", got)
    assert got[2] == 1, ("buffer and string encodings differ", got)
    assert got[3] == 1, got
    assert got[4] == 3, got
    assert got[5] == b"x", got
    assert got[6] == 3, got

    # an empty encode still produces a usable buffer rather than a nil
    assert r.execute_command("SETF", "sjbufempty", '''
        function call()
            local b = simdjson.encodeBuffer({})
            return {buffer.len(b), buffer.tostring(b)}
        end
    ''') == b"OK"
    got = r.execute_command("sjbufempty")
    assert got[0] > 0 and got[1] in (b"{}", b"[]"), got

    e = refused("SETF", "sjbad", '''
        function call()
            return simdjson.parse("{")
        end
    ''')
    if e is None:
        e = refused("sjbad")
    assert e, e

    e = refused("SETF", "sjopenbad", '''
        function call()
            return simdjson.open("{")
        end
    ''')
    if e is None:
        e = refused("sjopenbad")
    assert e, e

    # parse took a value's type from its first byte and never looked past the
    # document, so these four came back as null or as [1,2] (TODO 408)
    assert r.execute_command("SETF", "sjstrict", '''
        function call(s)
            local ok = pcall(simdjson.parse, s)
            return ok and "parsed" or "refused"
        end
    ''') == b"OK"
    for bad in ("not json {", "nul", "null x", "[1,2] ]"):
        assert r.execute_command("sjstrict", bad) == b"refused", bad
    for good in ("null", "[1,2]", " {} ", "12"):
        assert r.execute_command("sjstrict", good) == b"parsed", good

    # getJson/setJson - simdjson.parse(get(k)) and set(k, simdjson.encode(t)) in
    # one call, on barch.store and on every space handle. See TODO 408.
    print("getJson and setJson", flush=True)
    other = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
    other.execute_command("USE", "jsonspace")
    other.execute_command("SET", "made", "1")
    checks = '''
        local out = {}
        local function want(ok, what) if not ok then out[#out + 1] = what end end
        sj("j1", {a = 1, b = {true, "x"}, s = "q\\"uote"})
        local t = gj("j1")
        want(t ~= nil and t.a == 1 and t.b[1] == true and t.b[2] == "x" and t.s == 'q"uote',
             "round trip")
        want(simdjson.parse(get("j1")).a == 1, "stored as plain JSON")
        sj("s", "hello")
        want(gj("s") == "hello", "a scalar round trips")
        want(gj("nope") == nil and select("#", gj("nope")) == 1, "missing is a bare nil")
        set("bad", "not json {")
        local v, why = gj("bad")
        want(v == nil and type(why) == "string" and #why > 0, "bad JSON is nil and a reason")
        set("n", "null")
        want(gj("n") == simdjson.null, "JSON null stays simdjson.null")
        sj("j1", nil)
        want(get("j1") == nil, "setJson nil removes")
        local cyc = {}
        cyc.me = cyc
        local ok, err = pcall(sj, "c", cyc)
        want(not ok and tostring(err):find("cyclic") ~= nil, "a cyclic table raises")
        want(get("c") == nil, "and writes nothing")
        ok, err = pcall(sj, "f", {f = function() end})
        want(not ok, "a function raises")
        return #out == 0 and "ok" or table.concat(out, ", ")
    '''
    surfaces = {
        "store": ("", "barch.store.getJson(k)", "barch.store.setJson(k, v)",
                  "barch.store.get(k)", "barch.store.set(k, v)"),
        "current": ("local h = barch.current()",) + ("h:getJson(k)", "h:setJson(k, v)",
                                                     "h:get(k)", "h:set(k, v)"),
        "named": ("local h = barch.space.jsonspace",) + ("h:getJson(k)", "h:setJson(k, v)",
                                                         "h:get(k)", "h:set(k, v)"),
        "art": ("local h = barch.art()",) + ("h:getJson(k)", "h:setJson(k, v)",
                                             "h:get(k)", "h:set(k, v)"),
    }
    for name, (prelude, gj, sj, get, put) in surfaces.items():
        body = '''function call()
        %s
        local function gj(k) return %s end
        local function sj(k, v) %s end
        local function get(k) return %s end
        local function set(k, v) %s end
        %s
        end''' % (prelude, gj, sj, get, put, checks)
        assert r.execute_command("SETF", "sjjson_" + name, body) == b"OK"
        got = r.execute_command("sjjson_" + name)
        assert got == b"ok", (name, got)
    # the stored bytes land where the handle points
    assert other.execute_command("GET", "s") == b'"hello"'

    # parseJson(k, names...) - just the named fields, one value per name, and
    # an extra value (the reason) only when something went wrong. TODO 409.
    print("parseJson", flush=True)
    fields = '''
        local out = {}
        local function want(ok, what) if not ok then out[#out + 1] = what end end
        set("doc", '{"a":1,"b":{"c":[1,2]},"n":null,"a":99,"s":"x"}')
        local a, b, zz, n, a2 = pj("doc", "a", "b", "zz", "n", "a")
        want(a == 1 and b.c[2] == 2 and zz == nil and n == simdjson.null and a2 == 1,
             "values by position, the first of a repeated key")
        want(select("#", pj("doc", "a", "zz")) == 2, "one value per name when all is well")
        local m1, m2 = pj("missing", "a", "b")
        want(m1 == nil and m2 == nil and select("#", pj("missing", "a", "b")) == 2,
             "a missing key is a nil per name")
        set("bad", "not json {")
        local x, y, why = pj("bad", "a", "b")
        want(x == nil and y == nil and type(why) == "string" and #why > 0,
             "bad JSON is a nil per name, then the reason")
        set("arr", "[1,2]")
        local p, why2 = pj("arr", "a")
        want(p == nil and tostring(why2):find("object") ~= nil, "an array isn't an object")
        set("tail", '{"a":1} ]')
        local t, why3 = pj("tail", "a")
        want(t == nil and why3 ~= nil, "a broken tail is refused, as parse refuses it")
        want(not pcall(pj, "doc"), "at least one name")
        want(not pcall(pj, "doc", 5), "names are strings")
        return #out == 0 and "ok" or table.concat(out, ", ")
    '''
    for name, (prelude, _, _, get, put) in surfaces.items():
        pj = ("barch.store.parseJson(k, ...)" if name == "store" else "h:parseJson(k, ...)")
        body = '''function call()
        %s
        local function pj(k, ...) return %s end
        local function set(k, v) %s end
        %s
        end''' % (prelude, pj, put, fields)
        assert r.execute_command("SETF", "sjfields_" + name, body) == b"OK"
        got = r.execute_command("sjfields_" + name)
        assert got == b"ok", (name, got)

    # rights still raise: nil for a refused read would pass for an empty key
    r.execute_command("SET", "rokey", '{"v":1}')
    assert r.execute_command("SETF", "sjro", '''
        function call()
            local t = barch.store.getJson("rokey")
            local ok, err = pcall(barch.store.setJson, "rokey", {v = 2})
            return {t.v, ok and "wrote" or tostring(err)}
        end
    ''') == b"OK"
    r.execute_command("ACL", "SETUSER", "jsonro", "on", ">pw", "+read", "+keys", "+data",
                      "+function", "+connection")
    ro = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2,
                     username="jsonro", password="pw")
    got = ro.execute_command("sjro")
    assert got[0] == 1 and b"not authorized to write" in got[1], got
    assert r.execute_command("GET", "rokey") == b'{"v":1}'

    print("complete simdjson luau test")
finally:
    try:
        barch.stop()
    except Exception:
        pass
