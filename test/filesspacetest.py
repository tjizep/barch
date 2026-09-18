# A files route that serves another key space's file store - TODO 371.
#
# The HTTP server runs in `front`, and /images/img/* serves files out of `imgs`,
# fetching what `imgs` doesn't have through imgs' own fs_source. That's the shape
# the shop wants once its pictures live in a space of their own: still answered in
# C++ with no luau per request, still ETag and ranges, and read with the request
# user's rights in `imgs`, not in the space the server happens to run in.
import http.client

import scale

import redis
import barch

scale.workdir()
PORT = scale.port(default=14160)
HTTP_PORT = scale.port(1, default=18160)

print("start files space test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)

conf = barch.KeyValue("configuration")
conf.set("imgs.fs_source", "fetcher")
conf.set("imgs.missing_ttl", "60000")

img = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
img.execute_command("USE", "imgs")
img.execute_command("FLUSHDB")
front = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
front.execute_command("USE", "front")
front.execute_command("FLUSHDB")

# the source lives with the files it produces, in imgs
assert img.execute_command("SETF", "fetcher", """function call(path)
    barch.call("INCRBY", "asked", 1)
    if string.sub(path, 1, 9) == "/img/gen/" then
        return "generated for " .. path
    end
    return nil
end""") == b"OK"

png = b"\x89PNG\r\n\x1a\n" + bytes((i * 31 + 7) % 256 for i in range(150000))
img.execute_command("FS", "PUT", "/img/logo.png", png)
front.execute_command("FS", "PUT", "/own/hello.txt", "from front")

ROUTES = {
    # the new thing: another space, under a route of its own choosing
    "imgroute": '{ kind = "files", route = "/images/img/*", root = "/img", '
                'space = "imgs", source = true, cors = "*" }',
    # the same space again, as a user who may read front but not imgs
    "lockedroute": '{ kind = "files", route = "/locked/img/*", root = "/img", '
                   'space = "imgs", user = "noimg" }',
    # and front's own files, which must not change
    "ownroute": '{ kind = "files", route = "/own/*", root = "/own" }',
    "ownlocked": '{ kind = "files", route = "/ownlocked/*", root = "/own", user = "noimg" }',
}
for name, spec in ROUTES.items():
    assert front.execute_command("SETF", name, 'function call() return "%s" end\n'
                                 'function transport() return %s end' % (name, spec)) == b"OK"
assert front.execute_command("SETF", "conf", """function call() return "conf" end
function transport()
    return { kind = "http", bind = "127.0.0.1",
             keys = {"IMGROUTE", "LOCKEDROUTE", "OWNROUTE", "OWNLOCKED"} }
end""") == b"OK"

# noimg may read everywhere, except in imgs
r.execute_command("ACL", "SETUSER", "noimg", "on", ">pw", "+read", "+keys", "+data")
assert r.execute_command("KSPACE", "ACL", "imgs", "SETUSER", "noimg", "on", "-read") == b"OK"

started = [x.decode() for x in
           front.execute_command("HTTP", "START", "CONF", str(HTTP_PORT), "127.0.0.1")]
print("  " + " | ".join(started), flush=True)
assert any(x.startswith("IMGROUTE /images/img/*") and x.endswith("space=imgs")
           for x in started), started
assert any(x.startswith("OWNROUTE /own/*") and "space=" not in x for x in started), started


def get(path, headers=None, method="GET"):
    c = http.client.HTTPConnection("127.0.0.1", HTTP_PORT, timeout=10)
    try:
        c.request(method, path, headers=headers or {})
        resp = c.getresponse()
        return resp.status, resp.read(), dict((k.lower(), v) for k, v in resp.getheaders())
    finally:
        c.close()


try:
    print("a file from the other space, byte for byte", flush=True)
    st, body, h = get("/images/img/logo.png")
    assert st == 200, (st, body[:200])
    assert body == png, ("content changed", len(body), len(png))
    assert h.get("content-type") == "image/png", h
    assert h.get("access-control-allow-origin") == "*", h

    print("ranges and ETags work there too", flush=True)
    st, body, h = get("/images/img/logo.png", {"Range": "bytes=100-199"})
    assert st == 206 and body == png[100:200], (st, len(body))
    st, _, h = get("/images/img/logo.png", method="HEAD")
    etag = h.get("etag")
    assert st == 200 and etag, h
    st, body, _ = get("/images/img/logo.png", {"If-None-Match": etag})
    assert st == 304 and body == b"", (st, body)

    print("a missing file is fetched into the other space, once", flush=True)
    before = int(img.execute_command("GET", "asked") or 0)
    st, body, h = get("/images/img/gen/a.txt")
    assert st == 200 and body == b"generated for /img/gen/a.txt", (st, body)
    assert int(img.execute_command("GET", "asked")) == before + 1
    assert get("/images/img/gen/a.txt")[0] == 200
    assert int(img.execute_command("GET", "asked")) == before + 1, "fetched twice"
    # it landed in imgs, not in the space the server runs in
    assert img.execute_command("FS", "GET", "/img/gen/a.txt") == b"generated for /img/gen/a.txt"
    assert front.execute_command("FS", "GET", "/img/gen/a.txt") is None
    # what the source won't make is a 404
    assert get("/images/img/nothing.png")[0] == 404
    assert get("/images/img/../../own/hello.txt")[0] in (400, 404)

    print("the user's rights are the ones in the other space", flush=True)
    st, _, _ = get("/locked/img/logo.png")
    assert st == 403, st
    # while the same user can still read front's own files
    st, body, _ = get("/ownlocked/hello.txt")
    assert st == 200 and body == b"from front", (st, body)

    print("the server's own files are untouched", flush=True)
    st, body, _ = get("/own/hello.txt")
    assert st == 200 and body == b"from front", (st, body)
    assert get("/own/logo.png")[0] == 404
finally:
    front.execute_command("HTTP", "STOP")

print("space is refused where it means nothing", flush=True)
for name, spec, want in [
    ("badres", '{ kind = "resource", route = "/x", space = "imgs", '
               'methods = { GET = function(req, res) res.body = "x" end } }',
     "only for kind=files"),
    ("badconf", '{ kind = "files", route = "/c/*", space = "configuration" }',
     "configuration"),
]:
    assert front.execute_command("SETF", name, 'function call() return "x" end\n'
                                 'function transport() return %s end' % spec) == b"OK"
    front.execute_command("SETF", "conf2", 'function call() return "c" end\n'
                          'function transport() return { kind = "http", bind = "127.0.0.1", '
                          'keys = {"%s"} } end' % name.upper())
    try:
        front.execute_command("HTTP", "START", "CONF2", str(HTTP_PORT), "127.0.0.1")
        front.execute_command("HTTP", "STOP")
        raise AssertionError("%s should have been refused" % name)
    except redis.ResponseError as e:
        assert want in str(e), (name, str(e))

print("files space test complete", flush=True)
barch.stop()
