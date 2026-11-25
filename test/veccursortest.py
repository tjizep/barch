"""barch.vec on a store cursor - TODO 969.

vec.mask, filter, filterIn, fold, compare and foldBits take a cursor for their data
argument and read the value the cursor's last read saw, where it lies. What has to hold:

  - each kernel on a cursor answers what it answers on a copy of the value, for every
    kind, packed and strided, with sel, gids and bits, for every instruction set;
  - after any form of read, a partial one too, the kernel sees the whole value;
  - inside and outside a transaction, and in a transaction the snapshot's value
    even after another client changed the key;
  - a compressed value, and a range sharded space;
  - "the cursor has no value" before a read, after nil or a failed read;
    "the cursor has ended" when it has; "the cursor's value is gone" when the value
    changed size; a value that kept its size is read as it is now;
  - a cursor in a buffer only argument gets that argument's own error;
  - barch.vec.cursors is true.
"""
import math
import os
import random
import shutil
import struct
import subprocess
import sys

import scale
import redis

scale.workdir()
PORT = scale.port(default=15116)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

LUAU = r"""
local vec = barch.vec

local function why(fn, ...)
    local ok, err = pcall(fn, ...)
    if ok then return "no error" end
    return tostring(err)
end

local function records(out)
    local list = {}
    for i = 0, buffer.len(out) // 8 - 1 do
        list[#list + 1] = string.format("%.17g", buffer.readf64(out, i * 8))
    end
    return table.concat(list, ",")
end

local function fresh(groups)
    local out = buffer.create(groups * 32)
    for g = 0, groups - 1 do
        buffer.writef64(out, g * 32 + 16, math.huge)
        buffer.writef64(out, g * 32 + 24, -math.huge)
    end
    return out
end

-- every kernel on b, a buffer or a cursor, as one line each
local function battery(b, size, kind, off, stride, n, value, none)
    local r = {}
    local function add(...)
        local parts = {}
        for i, v in { ... } do
            parts[i] = if type(v) == "buffer" then buffer.tostring(v) else tostring(v)
        end
        r[#r + 1] = table.concat(parts, " ")
    end
    local sel = buffer.create(4 * (n // 3 + 1))
    local m = 0
    for i = n - 1, 0, -3 do
        buffer.writeu32(sel, 4 * m, i)
        m += 1
    end
    local gids = buffer.create(4 * math.max(n, 1))
    for i = 0, n - 1 do
        buffer.writeu32(gids, 4 * i, (i * 7) % 5)
    end
    for _, op in { "=", "~=", "<", ">=" } do
        local out = buffer.create(4 * n + 4)
        add("filter", op, vec.filter(kind, b, off, stride, op, value, none, nil, n, out), out)
        out = buffer.create(4 * m + 4)
        add("filter sel", op, vec.filter(kind, b, off, stride, op, value, none, sel, m, out), out)
        add("compare", op, vec.compare(kind, b, off, stride, op, value, none, n, nil, 3))
    end
    local out = fresh(1)
    vec.fold(kind, b, off, stride, none, nil, n, nil, out)
    add("fold", records(out))
    out = fresh(1)
    vec.fold(kind, b, off, stride, none, sel, m, nil, out)
    add("fold sel", records(out))
    out = fresh(5)
    vec.fold(kind, b, off, stride, none, nil, n, gids, out)
    add("fold gids", records(out))
    local _, bits = vec.compare(kind, b, off, stride, ">", value, none, n, nil, 5)
    out = fresh(1)
    vec.foldBits(kind, b, off, stride, none, bits, 5, n, nil, out)
    add("foldBits", records(out))
    out = fresh(5)
    vec.foldBits(kind, b, off, stride, none, bits, 5, n, gids, out)
    add("foldBits gids", records(out))
    if kind == "i8" or kind == "i16" or kind == "i32" then
        for _, len in { 16, 200 } do
            local set = buffer.create(len)
            for i = 0, len - 1, 3 do
                buffer.writeu8(set, i, 1)
            end
            out = buffer.create(4 * n + 4)
            add("filterIn", len, vec.filterIn(kind, b, off, stride, set, nil, n, out), out)
        end
    end
    out = buffer.create(4 * size + 4)
    add("mask", vec.mask(b, 1, size - 1, out), out)
    return r
end

local function opt(text)
    if text == "-" then return nil end
    return tonumber(text)
end

-- the data the last read saw: "size", "full" or "part" says how it was read
local function read(cur, key, form)
    if form == "full" then
        local size = cur:read(key)
        return cur:read(key, buffer.create(size))
    elseif form == "part" then
        return cur:read(key, buffer.create(4), 0, 2, 4)
    end
    return cur:read(key)
end

local kept

function call(step, ...)
    local a = { ... }
    if step == "cursors" then
        return tostring(vec.cursors)
    end
    if step == "same" then
        -- key, lo, hi, kind, off, stride, n, value, none, form
        local key, kind = a[1], a[4]
        local off, stride, n = tonumber(a[5]), tonumber(a[6]), tonumber(a[7])
        local value = if kind == "str" then a[8] else tonumber(a[8])
        local none = opt(a[9])
        local copy = barch.store.getBufferAt(key, 0)
        local size = buffer.len(copy)
        local cur = barch.store.cursor(a[2], a[3])
        local got = read(cur, key, a[10])
        assert(got == size, "the read's size")
        local want = battery(copy, size, kind, off, stride, n, value, none)
        local have = battery(cur, size, kind, off, stride, n, value, none)
        return { table.concat(want, "\n"), table.concat(have, "\n") }
    end
    if step == "sum" then
        -- sum(key, n [, lo, hi]): the first n i32s of key's value through cursor kept,
        -- which a read opens when lo and hi are given
        if a[3] then
            kept = barch.store.cursor(a[3], a[4])
            kept:read(a[1])
        end
        local out = fresh(1)
        local ok, err = pcall(vec.fold, "i32", kept, 0, 4, nil, nil, tonumber(a[2]), nil, out)
        if not ok then return tostring(err) end
        return string.format("%.17g", buffer.readf64(out, 8))
    end
    if step == "errors" then
        -- key holds 16 bytes; missing doesn't exist; both are in [lo, hi)
        local key, missing, lo, hi = a[1], a[2], a[3], a[4]
        local out = fresh(1)
        local cur = barch.store.cursor(lo, hi)
        local function fold(n)
            return why(vec.fold, "i8", cur, 0, 1, nil, nil, n or 16, nil, out)
        end
        local msgs = {}
        local function add(v) msgs[#msgs + 1] = v end
        add(fold())                                         -- no read yet
        cur:read(key)
        add(fold())                                         -- a value
        add(fold(17))                                       -- past its end
        cur:read(missing)
        add(fold())                                         -- after nil
        cur:read(key)
        pcall(function() return cur:read("zz") end)
        add(fold())                                         -- after a failed read
        cur:read(key)
        -- a cursor where only a buffer goes gets what any other value there gets
        local b = buffer.create(16)
        local cases = {
            function(x) return vec.fold("i8", b, 0, 1, nil, x, 4, nil, out) end,
            function(x) return vec.fold("i8", cur, 0, 1, nil, nil, 4, x, out) end,
            function(x) return vec.fold("i8", cur, 0, 1, nil, nil, 4, nil, x) end,
            function(x) return vec.foldBits("i8", cur, 0, 1, nil, x, 0, 4, nil, out) end,
            function(x) return vec.filterIn("i8", cur, 0, 1, x, nil, 4, buffer.create(16)) end,
            function(x) return vec.filter("i8", cur, 0, 1, "=", 1, nil, x, 4, buffer.create(16)) end,
            function(x) return vec.filter("i8", cur, 0, 1, "=", 1, nil, nil, 4, x) end,
            function(x) return vec.compare("i8", cur, 0, 1, "=", 1, nil, 4, x, 0) end,
            function(x) return vec.mask(b, 0, 4, x) end,
            function(x) return vec.maskBits(x, 0, 4, buffer.create(16)) end,
            function(x) return vec.bits("and", x, 0, b, 0, 4, nil, 0) end,
            function(x) return vec.offsets(x, 0, 2, 0, buffer.create(16)) end,
            function(x) return vec.grouper("i8"):ids(x, 0, 1, nil, nil, 4, buffer.create(16)) end,
        }
        for _, f in cases do
            local mine, other = why(f, cur), why(f, true)
            add(if mine == other then "same" else mine .. " / " .. other)
        end
        -- one the same size reads as it is now, one of another size is gone
        barch.store.set(key, string.rep("\2", 16))
        out = fresh(1)
        add(fold())
        add(tostring(buffer.readf64(out, 8)))
        barch.store.set(key, string.rep("\2", 15))
        add(fold())
        cur:read(key)
        barch.store.remove(key)
        add(fold(15))
        barch.store.set(key, string.rep("\1", 16))
        return msgs
    end
    if step == "tomb" then
        -- in a transaction: a key it removed reads as nil, which clears the value
        local cur = barch.store.cursor(a[2], a[3])
        cur:read(a[1])
        barch.store.remove(a[1])
        local t = cur:read(a[1])
        local out = fresh(1)
        return { tostring(t), why(vec.fold, "i8", cur, 0, 1, nil, nil, 1, nil, out) }
    end
    error("unknown step " .. tostring(step))
end
"""

KINDS = ["i8", "i16", "i32", "i64", "f64", "str"]
FMT = {"i8": "b", "i16": "h", "i32": "i", "i64": "q", "f64": "d"}
WIDTH = {"i8": 1, "i16": 2, "i32": 4, "i64": 8, "f64": 8, "str": 8}
NONE = {"i8": -128, "i16": -32768, "i32": -2**31, "i64": -2**63}
N = 64 * 9 + 13

failures = []


def check(ok, what):
    print("  %-72s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures.append(what)
    return ok


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def short(v):
    return str(v)[:120]


def block(kind, rng, shape, n=N, repeat=False):
    """n elements of kind as a column (packed) or as rows of an odd width; a tenth nil"""
    vals = []
    for i in range(n):
        if repeat:
            vals.append(None if i % 10 == 0 else (i % 7) if kind != "str" else "ab")
        elif rng.random() < 0.1:
            vals.append(None)
        elif kind == "str":
            vals.append("".join(rng.choice("abcz") for _ in range(rng.randrange(0, 12))))
        elif kind == "f64":
            vals.append(rng.uniform(-20, 40))
        else:
            vals.append(rng.randint(-20, 40))
    w = WIDTH[kind]
    off, stride = (0, w) if shape == "column" else (3, w + 5)
    data = bytearray(b"\x5a" * (off + n * stride))
    heap = bytearray()
    for i, v in enumerate(vals):
        at = off + i * stride
        if kind == "str":
            if v is None:
                struct.pack_into("<II", data, at, 0, 0)
            else:
                s = v.encode()
                struct.pack_into("<II", data, at, len(data) + len(heap), len(s) + 1)
                heap += s
        elif kind == "f64":
            struct.pack_into("<d", data, at, math.nan if v is None else v)
        else:
            struct.pack_into("<" + FMT[kind], data, at, NONE[kind] if v is None else v)
    return bytes(data + heap), off, stride


def same(c, where, key, kind, off, stride, form):
    none = str(NONE[kind]) if kind in NONE else "-"
    value = "abc" if kind == "str" else "10"
    got = answer(c, "CALLF", "vecc", "same", key, "a", "b", kind, off, stride, N, value, none, form)
    ok = isinstance(got, list) and len(got) == 2 and got[0] == got[1] and len(got[0]) > 0
    if not ok and isinstance(got, list) and len(got) == 2:
        for w, h in zip(got[0].split(b"\n"), got[1].split(b"\n")):
            if w != h:
                print("    copy   %s\n    cursor %s" % (short(w), short(h)))
                break
    return check(ok, "%s: %s" % (where, short(got) if not isinstance(got, list) else "same as a copy"))


def run_checks(isa, c, other, space):
    rng = random.Random(969)
    check(answer(c, "CALLF", "vecc", "cursors") == b"true", "%s %s: barch.vec.cursors" % (isa, space))
    for kind in KINDS:
        for shape in ("column", "rows"):
            key = "a:%s:%s" % (kind, shape)
            data, off, stride = block(kind, rng, shape)
            c.set(key, data)
            ok = True
            for form in ("size", "full", "part"):
                ok &= same(c, "%s %s %s %s after a %s read" % (isa, space, kind, shape, form),
                           key, kind, off, stride, form)
            for mode in ((), ("PESSIMISTIC",)):
                c.execute_command("BEGINTX", *mode)
                same(c, "%s %s %s %s in a transaction %s" % (isa, space, kind, shape, " ".join(mode).lower()),
                     key, kind, off, stride, "size")
                answer(c, "COMMITTX")
    # a value the store keeps compressed
    data, off, stride = block("i32", rng, "column", n=8192, repeat=True)
    c.set("a:packed", data)
    same(c, "%s %s a compressible value" % (isa, space), "a:packed", "i32", off, stride, "size")

    # in a transaction the snapshot's value, though another client changed it
    vals = list(range(1, 65))
    c.set("a:snap", struct.pack("<64i", *vals))
    c.execute_command("BEGINTX")
    first = answer(c, "CALLF", "vecc", "sum", "a:snap", 64, "a", "b")
    other.set("a:snap", struct.pack("<64i", *[2 * v for v in vals]))
    later = answer(c, "CALLF", "vecc", "sum", "a:snap", 64)
    other.set("a:snap", struct.pack("<32i", *vals[:32]))
    shorter = answer(c, "CALLF", "vecc", "sum", "a:snap", 64)
    answer(c, "ABORTTX")
    check(first == later == shorter == b"2080",
          "%s %s: a transaction's kernel reads its snapshot (%s %s %s)" % (isa, space, first, later, shorter))
    ended = answer(c, "CALLF", "vecc", "sum", "a:snap", 32)
    check(ended == b"vec.fold: the cursor has ended", "%s %s: and the cursor ends with it (%s)" % (isa, space, short(ended)))
    answer(c, "CALLF", "vecc", "sum", "a:snap", 32, "a", "b")
    ended = answer(c, "CALLF", "vecc", "sum", "a:snap", 32)
    check(ended == b"vec.fold: the cursor has ended", "%s %s: outside one it ends with the call (%s)" % (isa, space, short(ended)))

    c.set("a:err", b"\x01" * 16)
    got = answer(c, "CALLF", "vecc", "errors", "a:err", "a:none", "a", "b")
    NOVALUE = b"vec.fold: the cursor has no value"
    want = [NOVALUE, b"no error", b"vec.fold: reads past the end of a buffer", NOVALUE, NOVALUE] + \
           [b"same"] * 13 + [b"no error", b"32", b"vec.fold: the cursor's value is gone",
                             b"vec.fold: the cursor's value is gone"]
    check(isinstance(got, list) and len(got) == len(want), "%s %s: %d error cases (%s)" % (isa, space, len(want), short(got)))
    if isinstance(got, list):
        for i, (g, w) in enumerate(zip(got, want)):
            check(g == w, "%s %s: error case %d is %r" % (isa, space, i + 1, w) + ("" if g == w else " got %r" % g))
    c.execute_command("BEGINTX")
    got = answer(c, "CALLF", "vecc", "tomb", "a:err", "a", "b")
    answer(c, "ABORTTX")
    check(got == [b"nil", NOVALUE], "%s %s: a key the transaction removed clears it (%s)" % (isa, space, short(got)))


for want_isa in ["scalar", "avx2", "avx512"]:
    data = os.path.join(os.getcwd(), "veccursor_data_" + want_isa)
    shutil.rmtree(data, ignore_errors=True)
    os.makedirs(data)
    env = dict(os.environ)
    if want_isa == "avx512":
        env.pop("BARCH_VEC_ISA", None)
    else:
        env["BARCH_VEC_ISA"] = want_isa
    log = open(os.path.join(data, "barchd.log"), "wb")
    proc = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", data,
                             "--no-save-on-exit", "-c", "compression=zstd"],
                            stdout=log, stderr=subprocess.STDOUT, env=env)
    try:
        scale.wait_for_port(PORT, proc=proc, what="barchd")
        setup = redis.Redis(host="127.0.0.1", port=PORT, single_connection_client=True,
                            socket_timeout=None, retry=None)
        setup.execute_command("USE", "vcsetup")
        setup.execute_command("configuration:SET", "vcr.ordered", "1")
        setup.execute_command("configuration:SET", "vcr.shards", "7")
        setup.execute_command("configuration:SET", "vcr.range_sharded", "1")
        for space in ("vch", "vcr"):
            c, other = [redis.Redis(host="127.0.0.1", port=PORT, single_connection_client=True,
                                    socket_timeout=None, retry=None) for _ in range(2)]
            c.execute_command("USE", space)
            other.execute_command("USE", space)
            c.execute_command("SETF", "vecc", LUAU)
            run_checks(want_isa, c, other, space)
            c.close()
            other.close()
    finally:
        proc.terminate()
        proc.wait(60)
        log.close()

print("FAILED: %d" % len(failures) if failures else "ok")
sys.exit(1 if failures else 0)
