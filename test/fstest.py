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
import os

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
             keys = {"FILES", "PING", "VER"} }
end
""" % HTTP_PORT

assert r.execute_command("SETF", "files", FILES) == b"OK"
VER = """
function call() return "ver" end
function hit(req, res)
    local m = require(":/mod/http.luau")
    res.body = m.version()
    res.code = 200
end
function transport()
    return { kind = "resource", route = "/ver", methods = {GET = hit}, send = "text/plain" }
end
"""

r.execute_command("fsput", "/mod/http.luau", "text/plain",
                  "function version() return 'one' end\n")
assert r.execute_command("SETF", "ping", PING) == b"OK"
assert r.execute_command("SETF", "ver", VER) == b"OK"
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

print("a rewritten handler is picked up without a restart", flush=True)
# Two different mechanisms, and it is worth keeping them apart. A module the
# handler requires is looked up per request, so the epoch of DONE 234 covers it
# on its own. The handler's *own* source is compiled into the slot at HTTP START
# and called by reference, so only the slot reload of TODO 244 reaches it - that
# is what the /ping rewrite below tests, and it is the one that fails without it.
assert http_get("/ver")[1] == b"one", http_get("/ver")
import shutil as _shutil
import tempfile as _tempfile

# a script write is quiet, and publishing the *handler* does not publish the module
# it requires - the two are separate names. See TODO 246
r.execute_command("fsput", "/mod/http.luau", "text/plain",
                  "function version() return 'two' end\n")
assert http_get("/ver")[1] == b"one", "a script write published itself to the handler"
r.execute_command("SETF", "ver", VER, "RELOAD")
assert http_get("/ver")[1] == b"one", "publishing the handler published its module too"

# LOADFS ... RELOAD is how a module goes live
_moddir = _tempfile.mkdtemp(prefix="httpmod")
try:
    open(os.path.join(_moddir, "http.luau"), "w").write("function version() return 'two' end\n")
    r.execute_command("LOADFS", _moddir, "/mod", "RELOAD")
    assert http_get("/ver")[1] == b"two", "the module change did not reach the handler"
finally:
    _shutil.rmtree(_moddir, ignore_errors=True)

# and the handler's own source, not only what it requires
r.execute_command("SETF", "ping", PING.replace('"pong"', '"pong2"'))
assert http_get("/ping")[1] == b"pong", "a quiet write reached a live route"
r.execute_command("SETF", "ping", PING.replace('"pong"', '"pong2"'), "RELOAD")
assert http_get("/ping")[1] == b"pong2", "the handler rewrite did not reach the route"

# every slot in the pool, not just the one that happened to serve the last request
_moddir = _tempfile.mkdtemp(prefix="httpmod3")
try:
    open(os.path.join(_moddir, "http.luau"), "w").write("function version() return 'three' end\n")
    r.execute_command("LOADFS", _moddir, "/mod", "RELOAD")
finally:
    _shutil.rmtree(_moddir, ignore_errors=True)
_seen = set()
for _ in range(24):
    _seen.add(http_get("/ver")[1])
assert _seen == {b"three"}, ("a slot was still serving old code", _seen)

print("a versioned file gets a strong ETag that moves on a same-size rewrite", flush=True)
# the weak form could not tell these apart: same length, same type, same chunk count
_etagdir = _tempfile.mkdtemp(prefix="etag")
try:
    open(os.path.join(_etagdir, "f.txt"), "w").write("aaaaaaaa")
    r.execute_command("LOADFS", _etagdir, "/etag")
    _st, _b, _h = http_get("/static/etag/f.txt")
    _first = _h.get("etag")
    assert _st == 200 and _b == b"aaaaaaaa", (_st, _b)
    assert _first and not _first.startswith("W/"), ("expected a strong etag", _first)

    open(os.path.join(_etagdir, "f.txt"), "w").write("bbbbbbbb")   # same length
    r.execute_command("LOADFS", _etagdir, "/etag")
    _st, _b, _h = http_get("/static/etag/f.txt")
    assert _b == b"bbbbbbbb", _b
    assert _h.get("etag") != _first, "a same-size rewrite kept the same ETag"

    # and the conditional still works with it
    _st, _b, _ = http_get("/static/etag/f.txt", {"If-None-Match": _h.get("etag")})
    assert _st == 304, _st

    # a file written without a version still gets the weak form, and says so
    r.execute_command("fsput", "/etag/nover.txt", "text/plain", b"zzzz")
    _st, _b, _h = http_get("/static/etag/nover.txt")
    assert _st == 200 and _h.get("etag", "").startswith("W/"), _h.get("etag")
finally:
    _shutil.rmtree(_etagdir, ignore_errors=True)

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

# ---------------------------------------------------------------------------
# require() out of the file store - TODO 242
#
# `space:path` and `:path` load a module from fs: instead of a function key.
# require already returns the module's environment table, so a file that
# defines add() and closest() is used as hnsw.add(...) with nothing new
# invented - the only thing the fs form had to change is that a module is not
# a command and so does not need call().
print("require reads a module out of the file store", flush=True)
HNSW_MODULE = (
    "local counted = 0\n"
    "function add(a, b) counted = counted + 1 return a + b end\n"
    "function closest(x) return 'closest of ' .. tostring(x) end\n"
    "function calls() return counted end\n"
)
r.execute_command("fsput", "/extensions/hnsw.luau", "text/plain", HNSW_MODULE)
assert r.execute_command("SETF", "usehnsw",
    "function call()\n"
    "    local hnsw = require(':extensions/hnsw.luau')\n"
    "    return tostring(hnsw.add(2, 3)) .. ' / ' .. hnsw.closest('q')\n"
    "end") == b"OK"
assert r.execute_command("usehnsw") == b"5 / closest of q"

print("a module keeps its state between requires, like a lua module", flush=True)
assert r.execute_command("SETF", "twice",
    "function call()\n"
    "    local a = require(':extensions/hnsw.luau')\n"
    "    local b = require(':extensions/hnsw.luau')\n"
    "    a.add(1, 1)\n"
    "    b.add(1, 1)\n"
    "    return tostring(a.calls()) .. ':' .. tostring(b.calls())\n"
    "end") == b"OK"
_out = r.execute_command("twice").decode()
_lhs, _rhs = _out.split(":")
assert _lhs == _rhs, ("two requires gave two different modules", _out)


def _refused(*args):
    try:
        r.execute_command(*args)
        return None
    except redis.exceptions.ResponseError as e:
        return str(e)


print("a missing file and a cycle are both refused", flush=True)
r.execute_command("SETF", "nofile", "function call() require(':no/such.luau') return 'x' end")
_e = _refused("nofile")
assert _e and "has no file" in _e, _e
r.execute_command("fsput", "/extensions/loop.luau", "text/plain",
                  "local me = require(':extensions/loop.luau')\n")
r.execute_command("SETF", "cyc", "function call() require(':extensions/loop.luau') return 'x' end")
_e = _refused("cyc")
assert _e and "cycle" in _e, _e

print("a path may not climb out with ..", flush=True)
r.execute_command("SETF", "climb", "function call() require(':../etc/passwd') return 'x' end")
_e = _refused("climb")
assert _e and ".." in _e, _e

print("space:path reads from that space, not this one", flush=True)
r.execute_command("USE", "modules")
r.execute_command("SETF", "fsput", PUT)          # the writer, in the other space too
r.execute_command("fsput", "/shared/math.luau", "text/plain",
                  "function double(x) return x * 2 end\n")
r.execute_command("USE", "")
assert r.execute_command("SETF", "usemodules",
    "function call()\n"
    "    local m = require('modules:/shared/math.luau')\n"
    "    return tostring(m.double(21))\n"
    "end") == b"OK"
assert r.execute_command("usemodules") == b"42"
r.execute_command("SETF", "usehere", "function call() require(':/shared/math.luau') return 'x' end")
_e = _refused("usehere")
assert _e and "has no file" in _e, _e

# ---------------------------------------------------------------------------
# a rewrite is picked up by a live connection - TODO 243
#
# Nothing used to invalidate compiled luau: SETF followed by a call on the same
# connection ran the old source, because the compiled copy is cached for the
# life of the session. Both kinds of source are covered now, through an epoch
# bumped on any write that can change one.
print("a quiet write does not change what this connection runs", flush=True)
assert r.execute_command("SETF", "vers", "function call() return 'one' end") == b"OK"
assert r.execute_command("vers") == b"one"
assert r.execute_command("SETF", "vers", "function call() return 'two' end") == b"OK"
assert r.execute_command("vers") == b"one", "a quiet write published itself"

print("RELOAD on the write publishes it", flush=True)
assert r.execute_command("SETF", "vers", "function call() return 'two' end", "RELOAD") == b"OK"
assert r.execute_command("vers") == b"two", "SETF ... RELOAD did not publish"

print("a module in the file store follows the same rule", flush=True)
r.execute_command("fsput", "/mod/v.luau", "text/plain", "function v() return 'v1' end\n")
assert r.execute_command("SETF", "usev",
    "function call() local m = require(':/mod/v.luau') return m.v() end") == b"OK"
assert r.execute_command("usev") == b"v1"
# a script writing the file is quiet - there is no RELOAD to put on a store.set
r.execute_command("fsput", "/mod/v.luau", "text/plain", "function v() return 'v2' end\n")
assert r.execute_command("usev") == b"v1", "a script write published itself"
# and LOADFS with the word publishes what is on disk now
_pub = tempfile.mkdtemp(prefix="pubfs")
try:
    open(os.path.join(_pub, "v.luau"), "w").write("function v() return 'v2' end\n")
    r.execute_command("LOADFS", _pub, "/mod", "RELOAD")
    assert r.execute_command("usev") == b"v2", "LOADFS ... RELOAD did not publish"
finally:
    shutil.rmtree(_pub, ignore_errors=True)

print("LOADFS and LOADKEYS publish only when asked", flush=True)
# two directories, because LOADKEYS installs a .luau as a *function key* and
# refuses one with no call() - which is right, and means a module belongs in the
# file store and not in a LOADKEYS directory
_files = tempfile.mkdtemp(prefix="epochfs")
_fns = tempfile.mkdtemp(prefix="epochfn")
try:
    open(os.path.join(_files, "v.luau"), "w").write("function v() return 'v3' end\n")
    r.execute_command("LOADFS", _files, "/mod")
    assert r.execute_command("usev") == b"v2", "LOADFS published without being asked"
    r.execute_command("LOADFS", _files, "/mod", "RELOAD")
    assert r.execute_command("usev") == b"v3", "LOADFS ... RELOAD did not publish"

    open(os.path.join(_fns, "vers.luau"), "w").write("function call() return 'three' end\n")
    r.execute_command("LOADKEYS", _fns)
    assert r.execute_command("vers") == b"two", "LOADKEYS published without being asked"
    r.execute_command("LOADKEYS", _fns, "RELOAD")
    assert r.execute_command("vers") == b"three", "LOADKEYS ... RELOAD did not publish"
finally:
    shutil.rmtree(_files, ignore_errors=True)
    shutil.rmtree(_fns, ignore_errors=True)

# ---------------------------------------------------------------------------
# require(path, true) - the session asks for current code - TODO 247
#
# RELOAD on a write publishes to everything; this changes one VM. That is what
# makes it safe to give a script: a loader can ask for the current files without
# altering what any other connection is running.
print("a forced require reads the file again", flush=True)
r.execute_command("fsput", "/mod/f.luau", "text/plain", "function v() return 'f1' end\n")
assert r.execute_command("SETF", "fplain",
    "function call() return require(':/mod/f.luau').v() end") == b"OK"
assert r.execute_command("SETF", "fforce",
    "function call() return require(':/mod/f.luau', true).v() end") == b"OK"
assert r.execute_command("fplain") == b"f1"

# a second connection that has also compiled it, so the isolation check means
# something: it holds f1 from before the change
other = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
assert other.execute_command("fplain") == b"f1"

r.execute_command("fsput", "/mod/f.luau", "text/plain", "function v() return 'f2' end\n")
assert r.execute_command("fplain") == b"f1", "a quiet write reached a compiled module"
assert r.execute_command("fforce") == b"f2", "a forced require did not read the file again"

print("and only this session is affected by it", flush=True)
assert other.execute_command("fplain") == b"f1", \
    "one session forcing a reload changed another session's module"
# within the session it is a replacement, not a one-off: everything here sees it now
assert r.execute_command("fplain") == b"f2"
other.close()

print("the flag works on the function form too", flush=True)
r.execute_command("SETF", "helper", "function h() return 'h1' end\nfunction call() return 'helper' end")
r.execute_command("SETF", "usesh", "function call() return require('helper', true).h() end")
assert r.execute_command("usesh") == b"h1"
r.execute_command("SETF", "helper", "function h() return 'h2' end\nfunction call() return 'helper' end")
assert r.execute_command("usesh") == b"h2", "a forced require of a function key did not reload"

# ---------------------------------------------------------------------------
# a version in the metadata - TODO 248
#
# It buys two things: a forced require can tell "unchanged" from "rewritten"
# and skip the compile, and the HTTP ETag becomes strong, so a rewrite that
# lands on the same length is no longer indistinguishable from no rewrite.
_vdir = tempfile.mkdtemp(prefix="vers")
try:
    print("LOADFS versions what it writes, and the version moves per write", flush=True)
    open(os.path.join(_vdir, "m.luau"), "w").write("function v() return 'V1' end\n")
    r.execute_command("LOADFS", _vdir, "/vers")
    _m1 = json.loads(r.execute_command("GET", "fs:m:/vers/m.luau"))
    assert _m1["version"] == 1, _m1
    r.execute_command("LOADFS", _vdir, "/vers")
    _m2 = json.loads(r.execute_command("GET", "fs:m:/vers/m.luau"))
    assert _m2["version"] == 2, _m2

    print("a forced require of an unchanged file keeps what it compiled", flush=True)
    # the module holds a counter in its own state: if it is recompiled the counter
    # goes back to zero, which is how the test can see a compile happen at all
    open(os.path.join(_vdir, "m.luau"), "w").write(
        "local seen = 0\nfunction bump() seen = seen + 1 return seen end\n")
    r.execute_command("LOADFS", _vdir, "/vers")
    assert r.execute_command("SETF", "vbump",
        "function call() return require(':/vers/m.luau', true).bump() end") == b"OK"
    assert r.execute_command("vbump") == 1
    # no write in between, so nothing should be recompiled and the counter carries on
    assert r.execute_command("vbump") == 2, "a forced require recompiled an unchanged file"
    assert r.execute_command("vbump") == 3

    print("and rebuilds when the version has moved", flush=True)
    open(os.path.join(_vdir, "m.luau"), "w").write(
        "local seen = 100\nfunction bump() seen = seen + 1 return seen end\n")
    r.execute_command("LOADFS", _vdir, "/vers")
    assert r.execute_command("vbump") == 101, "a forced require missed a new version"

    print("a file with no version is rebuilt every time", flush=True)
    # fsput is the luau writer and keeps no version, so absent must mean "assume
    # changed" - the counter restarting is the compile happening
    r.execute_command("fsput", "/vers/nover.luau", "text/plain",
                      "local seen = 0\nfunction bump() seen = seen + 1 return seen end\n")
    assert r.execute_command("SETF", "nbump",
        "function call() return require(':/vers/nover.luau', true).bump() end") == b"OK"
    assert r.execute_command("nbump") == 1
    assert r.execute_command("nbump") == 1, "a file with no version was treated as unchanged"
finally:
    shutil.rmtree(_vdir, ignore_errors=True)

print("fs test complete", flush=True)
barch.stop()
