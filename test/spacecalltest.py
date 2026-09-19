# sp:call on a barch.space handle: any command, in that space - TODO 378.
#
# barch.call only runs in the function's own space, and from a script there was no
# way round it: `barch.call("imgs:GET", ...)` is an unknown command and USE doesn't
# move anything. `barch.space.imgs:call("GET", "k")` runs the command in imgs with
# the caller's rights there. All three runners are here - a RESP caller, an HTTP
# route and a space's file source - because a handle can be used under any of them.
import http.client

import scale

import redis
import barch

scale.workdir()
PORT = scale.port(default=14240)
HTTP_PORT = scale.port(1, default=18240)

print("start space call test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)

conf = barch.KeyValue("configuration")
conf.set("imgs.fs_source", "fetcher")

img = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
img.execute_command("USE", "imgs")
img.execute_command("FLUSHDB")
front = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
front.execute_command("USE", "front")
front.execute_command("FLUSHDB")

snips = [0]


def lua(body, conn=None):
    """run a snippet as a function stored in front. Each gets its own name: a session
    keeps what it compiled, so reusing a name would run the old snippet"""
    snips[0] += 1
    name = "snip%d" % snips[0]
    front.execute_command("SETF", name, "function call()\n%s\nend" % body)
    return (conn or front).execute_command(name.upper())


def refused(body, want, conn=None):
    try:
        lua(body, conn)
    except redis.ResponseError as e:
        assert want in str(e), (want, str(e))
        return
    raise AssertionError("should have been refused: " + body)


try:
    print("a command runs in the handle's space", flush=True)
    assert lua('return barch.space.imgs:call("SET", "k", "in imgs")') == b"OK"
    assert img.get("k") == b"in imgs"
    assert front.get("k") is None
    assert lua('return barch.space.imgs:call("GET", "k")') == b"in imgs"
    # numbers go in the way barch.call writes them, and replies come back as barch.call's
    assert lua('return barch.space.imgs:call("INCRBY", "n", 5)') == 5
    assert lua('return barch.space.imgs:call("HSET", "h", "a", 1, "b", 2)') == 2
    assert img.hget("h", "b") == b"2"

    print("barch.call and the running space are unchanged", flush=True)
    assert lua('return barch.call("GET", "k")') is None
    assert lua('return barch.current():call("SET", "own", "front")') == b"OK"
    assert front.get("own") == b"front" and img.get("own") is None

    print("what it refuses", flush=True)
    refused('return barch.art():call("GET", "k")', "barch.art() space has no name")
    refused('''return barch.store.locked("x", function()
        return barch.space.imgs:call("GET", "k")
    end)''', "not allowed inside a locked region")
    refused('return barch.space.imgs:call("RANGE", "a", "z")', "asynchronous")
    refused('return barch.space.imgs:call("MULTI")', "cannot call MULTI")
    refused('return barch.space.imgs:call()', "needs a command name")
    refused('return barch.space.imgs:call("NOSUCHCMD")', "unknown command")

    print("the caller's rights in that space", flush=True)
    r.execute_command("ACL", "SETUSER", "limited", "on", ">pw", "+read", "+write", "+keys",
                      "+data", "+function", "+connection")
    assert r.execute_command("KSPACE", "ACL", "imgs", "SETUSER", "limited", "on",
                             "-write") == b"OK"
    lim = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2,
                      username="limited", password="pw")
    lim.execute_command("USE", "front")
    # reading in imgs is still fine, writing there isn't, and writing at home is
    assert lua('return barch.space.imgs:call("GET", "k")', lim) == b"in imgs"
    refused('return barch.space.imgs:call("SET", "k", "nope")', "not authorized", lim)
    assert img.get("k") == b"in imgs"
    assert lua('return barch.call("SET", "mine", "1")', lim) == b"OK"

    print("from an HTTP route", flush=True)
    r.execute_command("ACL", "SETUSER", "webby", "on", ">pw", "+read", "+write", "+keys",
                      "+data", "+function")
    assert r.execute_command("KSPACE", "ACL", "imgs", "SETUSER", "webby", "on",
                             "-write") == b"OK"
    assert front.execute_command("SETF", "route", """function call() return "r" end
function get(req, res)
    res.body = barch.space.imgs:call("GET", "k") or "nil"
    res.code = 200
end
function post(req, res)
    local ok, err = pcall(function() return barch.space.imgs:call("SET", "k", "web") end)
    res.body = ok and "written" or tostring(err)
    res.code = 200
end
function transport()
    return { kind = "resource", route = "/c", user = "webby",
             methods = { GET = get, POST = post } }
end""") == b"OK"
    assert front.execute_command("SETF", "conf", """function call() return "c" end
function transport()
    return { kind = "http", bind = "127.0.0.1", keys = {"ROUTE"} }
end""") == b"OK"
    front.execute_command("HTTP", "START", "CONF", str(HTTP_PORT), "127.0.0.1")
    try:
        def req(method):
            c = http.client.HTTPConnection("127.0.0.1", HTTP_PORT, timeout=10)
            try:
                c.request(method, "/c", body="" if method == "POST" else None)
                resp = c.getresponse()
                return resp.status, resp.read()
            finally:
                c.close()
        assert req("GET") == (200, b"in imgs"), req("GET")
        status, body = req("POST")
        assert status == 200 and b"not authorized" in body, (status, body)
        assert img.get("k") == b"in imgs"
    finally:
        front.execute_command("HTTP", "STOP")

    print("from a space's file source", flush=True)
    # imgs' source counts its fetches in front - the third runner
    assert img.execute_command("SETF", "fetcher", """function call(path)
    barch.space.front:call("INCR", "fetches")
    return "made " .. path
end""") == b"OK"
    assert img.execute_command("FS", "FETCH", "/gen/a.txt") == b"made /gen/a.txt"
    assert front.get("fetches") == b"1", front.get("fetches")
    assert img.get("fetches") is None

    print("space call test complete", flush=True)
finally:
    barch.stop()
