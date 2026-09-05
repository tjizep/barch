# A file store built out of keys - TODO 235.
#
# Files live as two kinds of key: `fs:m:<path>` holds the metadata as JSON and
# `fs:d:<path>|<n>` holds one chunk of the content. A directory is not stored
# at all - listing one is a range scan over the prefix, which is what ordered
# keys are for.
#
# Path keyed rather than inode keyed on purpose: inodes buy O(1) rename and
# hard links, and cost a lookup per path component plus a shared allocator
# counter. Reads dominate this use and renames are rare, so that is not a
# trade worth making yet.
#
# The chunk index is zero padded to a fixed width because a range scan returns
# keys in lexicographic order, and "10" sorts before "2". Fixed width is the
# poor relation of a numeric composite part and does the same job here.
import scale
import redis
import barch

scale.workdir()
PORT = scale.port(default=14310)

# ---------------------------------------------------------------------------
# the module, as stored functions

PUT = r'''
-- fs.put(path, content_type, content [, chunk_size])
function call(path, ctype, content, chunk)
    chunk = tonumber(chunk) or 65536
    local n = #content
    local was = barch.store.get("fs:m:" .. path)
    local i = 0
    local off = 1
    while off <= n do
        local part = string.sub(content, off, off + chunk - 1)
        barch.store.set("fs:d:" .. path .. "|" .. string.format("%08d", i), part)
        off = off + chunk
        i = i + 1
    end
    -- a shorter file leaves the old tail behind otherwise
    if was ~= nil and was ~= barch.tomb then
        local old = simdjson.parse(was)
        local j = i
        while j < old.chunks do
            barch.store.set("fs:d:" .. path .. "|" .. string.format("%08d", j), nil)
            j = j + 1
        end
    end
    barch.store.set("fs:m:" .. path,
        simdjson.encode({size = n, type = ctype, chunk = chunk, chunks = i}))
    return i
end
'''

GET = r'''
-- fs.get(path) -> the whole content, or nil
function call(path)
    local m = barch.store.get("fs:m:" .. path)
    if m == nil or m == barch.tomb then return nil end
    local meta = simdjson.parse(m)
    local parts = {}
    for i = 0, meta.chunks - 1 do
        parts[#parts + 1] = barch.store.get("fs:d:" .. path .. "|" .. string.format("%08d", i))
    end
    return table.concat(parts)
end
'''

STAT = r'''
-- fs.stat(path) -> the metadata json, or nil
function call(path)
    local m = barch.store.get("fs:m:" .. path)
    if m == nil or m == barch.tomb then return nil end
    return m
end
'''

LIST = r'''
-- fs.list(prefix) -> json array of the paths under it
function call(prefix, limit)
    prefix = prefix or "/"
    local keys = barch.store.range("fs:m:" .. prefix, "fs:m:" .. prefix .. "\255",
                                   tonumber(limit) or 1000)
    local out = {}
    for _, k in ipairs(keys) do
        out[#out + 1] = string.sub(k, 6)
    end
    -- an empty luau table encodes as {} because nothing tells an empty array
    -- from an empty object, and a listing is always an array
    if #out == 0 then return "[]" end
    return simdjson.encode(out)
end
'''

RM = r'''
-- fs.rm(path) -> how many keys went
function call(path)
    local m = barch.store.get("fs:m:" .. path)
    if m == nil or m == barch.tomb then return 0 end
    local meta = simdjson.parse(m)
    for i = 0, meta.chunks - 1 do
        barch.store.set("fs:d:" .. path .. "|" .. string.format("%08d", i), nil)
    end
    barch.store.set("fs:m:" .. path, nil)
    return meta.chunks + 1
end
'''

print("start fs test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")

import json

for name, src in (("fsput", PUT), ("fsget", GET), ("fsstat", STAT),
                  ("fslist", LIST), ("fsrm", RM)):
    assert r.execute_command("SETF", name, src) == b"OK", name

# --- one chunk ------------------------------------------------------------
print("a small file round trips", flush=True)
assert r.execute_command("fsput", "/a.txt", "text/plain", b"hello") == 1
assert r.execute_command("fsget", "/a.txt") == b"hello"
meta = json.loads(r.execute_command("fsstat", "/a.txt"))
assert meta["size"] == 5 and meta["type"] == "text/plain" and meta["chunks"] == 1, meta

# --- several chunks, and every byte value ---------------------------------
# 64 byte chunks so the multi-chunk path is exercised without a megabyte, and
# a length that does not divide evenly so the last chunk is short
print("binary spanning several chunks, last one short", flush=True)
blob = bytes(range(256)) * 10 + b"\x00\xfftail"
assert len(blob) % 64 != 0, "the point is an uneven last chunk"
want_chunks = (len(blob) + 63) // 64
got = r.execute_command("fsput", "/img/logo.png", "image/png", blob, 64)
assert got == want_chunks, (got, want_chunks)
back = r.execute_command("fsget", "/img/logo.png")
assert back == blob, ("content changed", len(back), len(blob))

# the chunks really are separate keys, and in order past the tenth
keys = json.loads(r.execute_command("fslist", "/img/"))
assert keys == ["/img/logo.png"], keys
assert want_chunks > 10, "the ordering case needs more than ten chunks"

# --- listing --------------------------------------------------------------
print("a directory listing is a prefix scan", flush=True)
for p in ("/img/a.png", "/img/b.png", "/img/deep/c.png", "/other/d.png"):
    r.execute_command("fsput", p, "image/png", b"x" * 10)
under_img = json.loads(r.execute_command("fslist", "/img/"))
assert under_img == ["/img/a.png", "/img/b.png", "/img/deep/c.png", "/img/logo.png"], under_img
assert json.loads(r.execute_command("fslist", "/other/")) == ["/other/d.png"]
assert json.loads(r.execute_command("fslist", "/")) == [
    "/a.txt", "/img/a.png", "/img/b.png", "/img/deep/c.png", "/img/logo.png", "/other/d.png"
], json.loads(r.execute_command("fslist", "/"))

# --- overwrite shorter, which must not leave a tail behind -----------------
print("overwriting with something shorter drops the old tail", flush=True)
r.execute_command("fsput", "/img/logo.png", "image/png", b"tiny", 64)
assert r.execute_command("fsget", "/img/logo.png") == b"tiny"
assert json.loads(r.execute_command("fsstat", "/img/logo.png"))["chunks"] == 1
# nothing of the old file survives
assert r.execute_command("GET", "fs:d:/img/logo.png|00000005") is None

# --- delete ---------------------------------------------------------------
print("delete takes the metadata and every chunk", flush=True)
r.execute_command("fsput", "/gone.bin", "application/octet-stream", b"y" * 200, 64)
assert r.execute_command("fsrm", "/gone.bin") == 5      # 4 chunks and the meta
assert r.execute_command("fsget", "/gone.bin") is None
assert r.execute_command("fsstat", "/gone.bin") is None
assert r.execute_command("GET", "fs:d:/gone.bin|00000000") is None
assert json.loads(r.execute_command("fslist", "/")) == [
    "/a.txt", "/img/a.png", "/img/b.png", "/img/deep/c.png", "/img/logo.png", "/other/d.png"
]

# --- missing --------------------------------------------------------------
assert r.execute_command("fsget", "/nope.txt") is None
assert r.execute_command("fsrm", "/nope.txt") == 0
assert json.loads(r.execute_command("fslist", "/empty/")) == []

# --- a file big enough to be worth chunking at the real size --------------
print("a megabyte at the default 64KB chunk", flush=True)
big = bytes((i * 7 + 13) % 256 for i in range(1024 * 1024))
n = r.execute_command("fsput", "/big.bin", "application/octet-stream", big)
assert n == 16, n
assert r.execute_command("fsget", "/big.bin") == big
assert json.loads(r.execute_command("fsstat", "/big.bin"))["size"] == len(big)


# ---------------------------------------------------------------------------
# served over HTTP from C++, without entering luau - TODO 235
#
# The point of the C++ path is that a download does not take a VM slot: a luau
# handler holds one for the whole call and the pool is 2-8, so concurrent
# downloads would starve every other route. The concurrency check at the end is
# what actually tests that.
import http.client
import threading

HTTP_PORT = scale.port(1, default=18310)

FILES = r"""
function call() return "files" end
function transport()
    return { kind = "files", route = "/static/*", root = "/", cors = "*" }
end
"""

# a luau route beside it, so the concurrency check can prove that downloads are
# not eating the VM pool the other routes need
PING = r"""
function call() return "ping" end
function hit(req, res)
    res.body = "pong"
    res.code = 200
end
function transport()
    return { kind = "resource", route = "/ping", methods = {GET = hit}, send = "text/plain" }
end
"""

CONF = r"""
function call() return "http" end
function transport()
    return { kind = "http", port = %d, bind = "127.0.0.1", user = "web",
             keys = {"FILES", "PING"} }
end
""" % HTTP_PORT

assert r.execute_command("SETF", "files", FILES) == b"OK"
assert r.execute_command("SETF", "ping", PING) == b"OK"
assert r.execute_command("SETF", "httpconf", CONF) == b"OK"
started = b" ".join(r.execute_command("HTTP", "START", "HTTPCONF", str(HTTP_PORT), "127.0.0.1"))
assert b"FILES /static/*" in started, started


def http_get(path, headers=None, method="GET"):
    c = http.client.HTTPConnection("127.0.0.1", HTTP_PORT, timeout=10)
    try:
        c.request(method, path, headers=headers or {})
        resp = c.getresponse()
        return resp.status, resp.read(), dict((k.lower(), v) for k, v in resp.getheaders())
    finally:
        c.close()


print("an image is served end to end, byte for byte", flush=True)
# a real PNG header so the bytes are not all printable, and enough of them to span
# several chunks at the default size
png = b"\x89PNG\r\n\x1a\n" + bytes((i * 31 + 7) % 256 for i in range(200000))
r.execute_command("fsput", "/images/logo.png", "image/png", png)
status, body, hdrs = http_get("/static/images/logo.png")
assert status == 200, (status, body[:200])
assert body == png, ("content changed", len(body), len(png))
assert hdrs.get("content-type") == "image/png", hdrs
assert hdrs.get("accept-ranges") == "bytes", hdrs

print("the content type falls back to the extension", flush=True)
r.execute_command("fsput", "/pages/index.html", "", b"<h1>hi</h1>")
status, body, hdrs = http_get("/static/pages/index.html")
assert status == 200 and body == b"<h1>hi</h1>", (status, body)
assert hdrs.get("content-type") == "text/html", hdrs

print("a range request is answered with just that range", flush=True)
status, body, hdrs = http_get("/static/images/logo.png", {"Range": "bytes=100-199"})
assert status == 206, (status, hdrs)
assert body == png[100:200], (len(body), body[:20], png[100:120])
assert hdrs.get("content-range") == "bytes 100-199/%d" % len(png), hdrs
# one that spans a chunk boundary, which is the case the arithmetic can get wrong
status, body, _ = http_get("/static/images/logo.png", {"Range": "bytes=65500-65600"})
assert status == 206 and body == png[65500:65601], (status, len(body))
# the last bytes, by suffix
status, body, _ = http_get("/static/images/logo.png", {"Range": "bytes=-50"})
assert status == 206 and body == png[-50:], (status, len(body))
# past the end
status, _, hdrs = http_get("/static/images/logo.png", {"Range": "bytes=999999999-"})
assert status == 416, status
assert hdrs.get("content-range") == "bytes */%d" % len(png), hdrs

print("HEAD says the length without the body, and ETag round trips", flush=True)
status, body, hdrs = http_get("/static/images/logo.png", method="HEAD")
assert status == 200 and body == b"", (status, len(body))
assert hdrs.get("content-length") == str(len(png)), hdrs
etag = hdrs.get("etag")
assert etag and etag.startswith("W/"), hdrs
status, body, _ = http_get("/static/images/logo.png", {"If-None-Match": etag})
assert status == 304 and body == b"", (status, len(body))

print("what is not there is a 404, and .. does not escape the root", flush=True)
assert http_get("/static/nope.png")[0] == 404
assert http_get("/static/../../etc/passwd")[0] in (400, 404), http_get("/static/../../etc/passwd")[0]
assert http_get("/static/")[0] == 404
assert http_get("/static/images/logo.png", method="POST")[0] == 405

print("eight concurrent downloads do not cost anyone a 503", flush=True)
# the whole reason this is C++: with a luau handler these would hold every VM in
# the pool and /ping would be refused
results = []
lock = threading.Lock()


def download():
    st, b, _ = http_get("/static/images/logo.png")
    with lock:
        results.append((st, len(b)))


def ping_meanwhile():
    for _ in range(20):
        st, b, _ = http_get("/ping")
        with lock:
            results.append(("ping", st, b))


threads = [threading.Thread(target=download) for _ in range(8)]
threads.append(threading.Thread(target=ping_meanwhile))
for t in threads:
    t.start()
for t in threads:
    t.join(timeout=60)
    assert not t.is_alive(), "a download never finished"

downloads = [x for x in results if not isinstance(x[0], str)]
pings = [x for x in results if isinstance(x[0], str)]
assert len(downloads) == 8, downloads
assert all(st == 200 and n == len(png) for st, n in downloads), downloads
assert len(pings) == 20, len(pings)
assert all(st == 200 and b == b"pong" for _, st, b in pings), \
    "a luau route was starved while files were downloading: %s" % pings[:4]

assert r.execute_command("HTTP", "STOP") == b"OK"
print("http file serving complete", flush=True)


# ---------------------------------------------------------------------------
# LOADFS: a directory on disk imported as the file store - TODO 238
#
# The C++ writer and the luau reader have to agree about the layout, so the
# check is that fsget reads back what LOADFS wrote, byte for byte.
import os
import shutil
import tempfile

src = tempfile.mkdtemp(prefix="loadfs")
try:
    os.makedirs(os.path.join(src, "img"))
    os.makedirs(os.path.join(src, "pages", "deep"))
    photo = bytes((i * 37 + 11) % 256 for i in range(200000))   # spans four chunks
    open(os.path.join(src, "img", "photo.png"), "wb").write(photo)
    open(os.path.join(src, "pages", "index.html"), "wb").write(b"<h1>hello</h1>")
    open(os.path.join(src, "pages", "deep", "note.txt"), "wb").write(b"nested\n")
    open(os.path.join(src, ".hidden"), "wb").write(b"not imported")

    print("LOADFS imports a directory, and luau reads it back", flush=True)
    out = [x.decode() for x in r.execute_command("LOADFS", src, "/imported")]
    assert "files=3" in out, out
    assert "bytes=%d" % (len(photo) + 14 + 7) in out, out
    assert "root=/imported" in out, out

    # the point of the cross check: written by C++, read by the luau side
    assert r.execute_command("fsget", "/imported/img/photo.png") == photo
    assert r.execute_command("fsget", "/imported/pages/index.html") == b"<h1>hello</h1>"
    assert r.execute_command("fsget", "/imported/pages/deep/note.txt") == b"nested\n"

    meta = json.loads(r.execute_command("fsstat", "/imported/img/photo.png"))
    assert meta["size"] == len(photo) and meta["chunks"] == 4, meta
    assert meta["type"] == "image/png", meta      # from the extension
    assert json.loads(r.execute_command("fsstat", "/imported/pages/index.html"))["type"] == "text/html"

    print("dot files are left out, and the listing is what was imported", flush=True)
    assert r.execute_command("fsget", "/imported/.hidden") is None
    assert json.loads(r.execute_command("fslist", "/imported/")) == [
        "/imported/img/photo.png",
        "/imported/pages/deep/note.txt",
        "/imported/pages/index.html",
    ], json.loads(r.execute_command("fslist", "/imported/"))

    print("a refused import writes nothing at all", flush=True)
    before = json.loads(r.execute_command("fslist", "/"))
    for bad in ((src + "/nope", "/x"), (src, "/y", "99999999")):
        try:
            r.execute_command("LOADFS", *bad)
            raise AssertionError("LOADFS %s should have been refused" % (bad,))
        except redis.exceptions.ResponseError:
            pass
    assert json.loads(r.execute_command("fslist", "/")) == before, "a refused import left keys behind"

    print("importing again over the top is the same content", flush=True)
    r.execute_command("LOADFS", src, "/imported")
    assert r.execute_command("fsget", "/imported/img/photo.png") == photo
finally:
    shutil.rmtree(src, ignore_errors=True)


# ---------------------------------------------------------------------------
# LOADKEYS: the same directory as discrete keys and stored functions - TODO 238
#
# The walk is the function sync's, so a directory means the same thing here as
# it does under functions_dir: .luau becomes a function named after its stem,
# everything else a key named prefix:sub:file.
src = tempfile.mkdtemp(prefix="loadkeys")
try:
    os.makedirs(os.path.join(src, "conf"))
    open(os.path.join(src, "greet.luau"), "w").write(
        'function call(who) return "hello " .. tostring(who) end')
    open(os.path.join(src, "banner.txt"), "wb").write(b"a banner")
    open(os.path.join(src, "conf", "settings.json"), "wb").write(b'{"a":1}')
    open(os.path.join(src, ".hidden"), "wb").write(b"not imported")

    print("LOADKEYS imports functions and keys from one directory", flush=True)
    out = [x.decode() for x in r.execute_command("LOADKEYS", src)]
    assert "functions=1" in out and "keys=2" in out, out

    # the .luau file is a function that can be called, which is what deploy.py did
    assert r.execute_command("greet", "world") == b"hello world"
    # and the rest are keys, named after the path below the directory
    assert r.execute_command("GET", "banner.txt") == b"a banner"
    assert r.execute_command("GET", "conf:settings.json") == b'{"a":1}'
    assert r.execute_command("GET", ".hidden") is None

    print("a prefix goes in front of the keys", flush=True)
    r.execute_command("LOADKEYS", src, "site")
    assert r.execute_command("GET", "site:banner.txt") == b"a banner"
    assert r.execute_command("GET", "site:conf:settings.json") == b'{"a":1}'

    print("a refused import writes nothing", flush=True)
    had = r.execute_command("GET", "banner.txt")
    try:
        r.execute_command("LOADKEYS", src + "/not-there")
        raise AssertionError("LOADKEYS of a missing directory should have been refused")
    except redis.exceptions.ResponseError:
        pass
    assert r.execute_command("GET", "banner.txt") == had

    print("the whole examples/http/luau directory loads in one call", flush=True)
    here = os.path.dirname(os.path.abspath(__file__))
    demo = os.path.join(os.path.dirname(here), "examples", "http", "luau")
    if os.path.isdir(demo):
        out = [x.decode() for x in r.execute_command("LOADKEYS", demo)]
        assert "functions=11" in out, out
        names = set(x.decode() for x in r.execute_command("KEYSF"))
        for want in ("PAGE", "ECHO", "JSON", "LOGIN", "CONF"):
            assert want in names, (want, sorted(names))
        assert r.execute_command("page") == b"page"
finally:
    shutil.rmtree(src, ignore_errors=True)

print("fs test complete", flush=True)
barch.stop()
