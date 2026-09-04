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

    print("complete simdjson luau test")
finally:
    try:
        barch.stop()
    except Exception:
        pass
