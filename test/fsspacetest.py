# barch.fs.space(name): another key space's file store from Luau - TODO 374.
#
# `barch.fs` works on the space the call is running in, and a script had no way to
# reach anyone else's files: `barch.call("imgs:FS", ...)` is an unknown command in a
# script, and space handles have no file methods. So functions stored in `front`
# work on the pictures in `imgs` here, through every barch.fs function, and `front`
# itself has to come out untouched.
import http.client

import scale

import redis
import barch

scale.workdir()
PORT = scale.port(default=14200)
HTTP_PORT = scale.port(1, default=18200)

print("start fs space test", flush=True)
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

assert img.execute_command("SETF", "fetcher", """function call(path)
    if string.sub(path, 1, 9) == "/img/gen/" then return "made " .. path end
    return nil
end""") == b"OK"
img.execute_command("FS", "PUT", "/img/a.jpg", "picture a")
img.execute_command("FS", "PUT", "/img/b.jpg", "picture b")
front.execute_command("FS", "PUT", "/own.txt", "front's own")


snips = [0]


def lua(body):
    """run a snippet as a function stored in front, and return what it returns.
    Each gets a name of its own: a session keeps what it compiled, so redefining
    one name on the same connection would run the old snippet again"""
    snips[0] += 1
    name = "snip%d" % snips[0]
    front.execute_command("SETF", name, "function call()\n%s\nend" % body)
    return front.execute_command(name.upper())


try:
    print("reading another space's files", flush=True)
    assert lua('return barch.fs.space("imgs").get("/img/a.jpg")') == b"picture a"
    assert lua('return barch.fs.space("imgs").stat("/img/a.jpg").size') == 9
    got = lua('''local out = {}
    for i, e in ipairs(barch.fs.space("imgs").list("/img")) do out[i] = e.name end
    return out''')
    assert got == [b"a.jpg", b"b.jpg"], got
    # the paging arguments work bound too
    got = lua('''local out = {}
    for i, e in ipairs(barch.fs.space("imgs").list("/img", nil, 1, false, 1)) do out[i] = e.name end
    return out''')
    assert got == [b"b.jpg"], got
    # and barch.fs on its own is still the running space
    assert lua('return barch.fs.get("/own.txt")') == b"front's own"
    assert lua('return barch.fs.get("/img/a.jpg")') is None

    print("fetching through the other space's source", flush=True)
    assert lua('return barch.fs.space("imgs").fetch("/img/gen/x.jpg")') == b"made /img/gen/x.jpg"
    assert img.execute_command("FS", "GET", "/img/gen/x.jpg") == b"made /img/gen/x.jpg"
    assert front.execute_command("FS", "GET", "/img/gen/x.jpg") is None

    print("writing, moving and removing there", flush=True)
    assert lua('return barch.fs.space("imgs").put("/img/c.jpg", "picture c", "image/jpeg")') == 1
    assert img.execute_command("FS", "GET", "/img/c.jpg") == b"picture c"
    assert "type=image/jpeg" in img.execute_command("FS", "STAT", "/img/c.jpg").decode()
    assert lua('return barch.fs.space("imgs").mkdir("/img/thumbs")') == 1
    assert lua('return barch.fs.space("imgs").copy("/img/c.jpg", "/img/thumbs/c.jpg")') == 1
    assert lua('return barch.fs.space("imgs").rename("/img/b.jpg", "/img/bb.jpg")') == 1
    assert img.execute_command("FS", "GET", "/img/bb.jpg") == b"picture b"
    assert img.execute_command("FS", "GET", "/img/b.jpg") is None
    assert lua('return barch.fs.space("imgs").remove("/img/a.jpg")') == 1
    assert img.execute_command("FS", "GET", "/img/a.jpg") is None
    assert lua('return barch.fs.space("imgs").rmdir("/img/thumbs", true)') == 1
    assert img.execute_command("FS", "GET", "/img/thumbs/c.jpg") is None
    assert lua('return barch.fs.space("imgs").publish("/img/c.jpg")') == 1
    # none of it landed in front
    listed = [x.decode() for x in front.execute_command("FS", "LS", "/")]
    assert listed == ["file 11 1 own.txt"], listed

    print("one handle, kept and reused", flush=True)
    got = lua('''local imgs = barch.fs.space("imgs")
    imgs.put("/img/d.jpg", "d")
    return imgs.get("/img/d.jpg") .. "|" .. tostring(imgs.space)''')
    assert got == b"d|nil", got   # a bound table can't be bound again

    print("an unknown space is refused", flush=True)
    try:
        lua('return barch.fs.space("nosuchspace").get("/x")')
        raise AssertionError("an unknown space should be refused")
    except redis.ResponseError as e:
        assert "no key space called nosuchspace" in str(e), str(e)
    try:
        lua('return barch.fs.space("nosuchspace")')
        raise AssertionError("an unknown space should be refused at binding")
    except redis.ResponseError as e:
        assert "no key space called" in str(e), str(e)

    print("a route user without write in the other space is refused there", flush=True)
    r.execute_command("ACL", "SETUSER", "nowrite", "on", ">pw", "+read", "+write", "+keys",
                      "+data", "+function")
    assert r.execute_command("KSPACE", "ACL", "imgs", "SETUSER", "nowrite", "on",
                             "-write") == b"OK"
    assert front.execute_command("SETF", "writer", """function call() return "w" end
function put(req, res)
    local ok, err = pcall(barch.fs.space("imgs").put, "/img/route.jpg", "x")
    res.body = ok and "written" or tostring(err)
    res.code = 200
end
function peek(req, res)
    res.body = barch.fs.space("imgs").get("/img/c.jpg") or "none"
    res.code = 200
end
function transport()
    return { kind = "resource", route = "/w", user = "nowrite",
             methods = { POST = put, GET = peek } }
end""") == b"OK"
    assert front.execute_command("SETF", "conf", """function call() return "c" end
function transport()
    return { kind = "http", bind = "127.0.0.1", keys = {"WRITER"} }
end""") == b"OK"
    front.execute_command("HTTP", "START", "CONF", str(HTTP_PORT), "127.0.0.1")
    try:
        def req(method):
            c = http.client.HTTPConnection("127.0.0.1", HTTP_PORT, timeout=10)
            try:
                c.request(method, "/w", body="" if method == "POST" else None)
                resp = c.getresponse()
                return resp.status, resp.read()
            finally:
                c.close()
        status, body = req("POST")
        assert status == 200 and b"not authorized" in body, (status, body)
        assert img.execute_command("FS", "GET", "/img/route.jpg") is None
        # reading is still allowed
        status, body = req("GET")
        assert status == 200 and body == b"picture c", (status, body)
    finally:
        front.execute_command("HTTP", "STOP")

    print("fs space test complete", flush=True)
finally:
    barch.stop()
