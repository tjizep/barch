# barch.vec bitmaps - TODO 966, bit offsets TODO 967.
#
# A query like "sum x where x > 5 and x < 10" runs as two compares that each
# write a bitmap, an AND of the two, and one fold that takes the bitmap:
#
#   vec.compare(kind, b, off, stride, op, value, none, n, bits?, bitsoff) -> count, bits
#   vec.bits(op, a, aoff, b, boff, n, out?, outoff) -> count, out
#                                                  and, or, xor, andnot, not
#   vec.foldBits(kind, b, off, stride, none, bits, bitsoff, n, gids, out)
#   vec.maskBits(bits, bitsoff, n, out) -> count   the set bits as u32 indexes
#
# Bit i is bit i % 8 of byte i // 8, as in buffer.readbits and writebits. A
# bitmap's n bits start at a bit offset; a buffer written gets only those n
# bits changed. A byte mask becomes bits through compare("i8", m, off, 1,
# "~=", 0, nil, n, bits, bitsoff).
#
# Every answer here is computed in Python. barchd runs once per instruction set
# the machine has (BARCH_VEC_ISA), since each has its own copy of the kernels.
import math
import os
import random
import struct
import subprocess
import sys

import scale
import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

scale.workdir()
PORT = scale.port(default=15114)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

LUAU = r"""
local vec = barch.vec

local function opt(text)
    if text == "-" then
        return nil
    end
    return tonumber(text)
end

local function g17(x)
    return string.format("%.17g", x)
end

local function indexes(b, n)
    local out = {}
    for k = 0, n - 1 do
        out[k + 1] = tostring(buffer.readu32(b, k * 4))
    end
    return out
end

local function records(out, groups)
    local list = {}
    for i = 0, groups * 4 - 1 do
        list[#list + 1] = g17(buffer.readf64(out, i * 8))
    end
    return list
end

local function fresh(groups)
    local out = buffer.create(groups * 32)
    for g = 0, groups - 1 do
        buffer.writef64(out, g * 32 + 16, math.huge)
        buffer.writef64(out, g * 32 + 24, -math.huge)
    end
    return out
end

function call(step, ...)
    local a = { ... }
    if step == "isa" then
        return vec.isa
    end
    if step == "compare" then
        local kind, b = a[1], buffer.fromstring(a[2])
        local value = if kind == "str" then a[6] else tonumber(a[6])
        local n = tonumber(a[8])
        local given = if a[9] == "given" then buffer.fromstring(a[11]) else nil
        local count, bits = vec.compare(kind, b, tonumber(a[3]), tonumber(a[4]), a[5], value,
                                        opt(a[7]), n, given, tonumber(a[10]))
        assert(given == nil or bits == given, "compare answers the bits it was given")
        return { tostring(count), buffer.tostring(bits) }
    end
    if step == "bits" then
        local op, n = a[1], tonumber(a[6])
        local x = buffer.fromstring(a[2])
        local y = if a[4] == "-" then nil else buffer.fromstring(a[4])
        local yoff = if a[4] == "-" then nil else tonumber(a[5])
        local out = if a[7] == "a" then x elseif a[7] == "given" then buffer.fromstring(a[9]) else nil
        local count, got = vec.bits(op, x, tonumber(a[3]), y, yoff, n, out, tonumber(a[8]))
        assert(out == nil or got == out, "bits answers the out it was given")
        return { tostring(count), buffer.tostring(got) }
    end
    if step == "foldBits" then
        local kind, b = a[1], buffer.fromstring(a[2])
        local gids = if a[9] == "-" then nil else buffer.fromstring(a[9])
        local groups = tonumber(a[10])
        local out = fresh(groups)
        vec.foldBits(kind, b, tonumber(a[3]), tonumber(a[4]), opt(a[5]), buffer.fromstring(a[6]),
                     tonumber(a[7]), tonumber(a[8]), gids, out)
        return records(out, groups)
    end
    if step == "maskBits" then
        local n = tonumber(a[3])
        local out = buffer.create(math.max(n, 1) * 4)
        return indexes(out, vec.maskBits(buffer.fromstring(a[1]), tonumber(a[2]), n, out))
    end
    if step == "luau" then
        -- bitmaps made and read by buffer.writebits and readbits: flags is a
        -- string of 0s and 1s set at bit off, the i32 column's compare goes
        -- to bit off2 of another buffer
        local col, flags, off, off2 = buffer.fromstring(a[1]), a[2], tonumber(a[3]), tonumber(a[4])
        local n = #flags
        local bm = buffer.create((off + n + 7) // 8)
        for i = 1, n do
            buffer.writebits(bm, off + i - 1, 1, if string.sub(flags, i, i) == "1" then 1 else 0)
        end
        local out = fresh(1)
        vec.foldBits("i32", col, 0, 4, nil, bm, off, n, nil, out)
        local gt = buffer.create((off2 + n + 7) // 8)
        vec.compare("i32", col, 0, 4, ">", tonumber(a[5]), nil, n, gt, off2)
        local seen = {}
        for i = 0, n - 1 do
            seen[i + 1] = tostring(buffer.readbits(gt, off2 + i, 1))
        end
        return { table.concat(seen), table.unpack(records(out, 1)) }
    end
    if step == "query" then
        -- sum x where x > lo and x < hi, over an i32 column
        local b, n = buffer.fromstring(a[1]), tonumber(a[2])
        local _, above = vec.compare("i32", b, 0, 4, ">", tonumber(a[3]), nil, n, nil, 0)
        local _, below = vec.compare("i32", b, 0, 4, "<", tonumber(a[4]), nil, n, nil, 0)
        local kept = vec.bits("and", above, 0, below, 0, n, above, 0)
        local out = fresh(1)
        vec.foldBits("i32", b, 0, 4, nil, above, 0, n, nil, out)
        return { tostring(kept), table.unpack(records(out, 1)) }
    end
    if step == "errors" then
        local b = buffer.create(16)
        local bits = buffer.create(1)
        local out = buffer.create(64)
        local cases = {
            function() vec.compare("u8", b, 0, 1, "=", 1, nil, 2, nil, 0) end,
            function() vec.compare("i8", b, 0, 1, "!=", 1, nil, 2, nil, 0) end,
            function() vec.compare("i8", b, 0, 1, "=", 1, nil, 17, nil, 0) end,
            function() vec.compare("i8", b, 0, 1, "=", 1, nil, 9, bits, 0) end,
            function() vec.compare("i32", b, 0, 4, "=", "x", nil, 2, nil, 0) end,
            function() vec.compare("str", b, 0, 8, "=", 1, nil, 2, nil, 0) end,
            function() vec.compare("i8", b, 0, 1, "=", 1, nil, 2, 7, 0) end,
            function() vec.bits("nand", bits, 0, bits, 0, 8, nil, 0) end,
            function() vec.bits("and", bits, 0, bits, 0, 9, nil, 0) end,
            function() vec.bits("and", bits, 0, nil, 0, 8, nil, 0) end,
            function() vec.bits("not", bits, 0, bits, 0, 8, nil, 0) end,
            function() vec.bits("or", bits, 0, bits, 0, 8, buffer.create(0), 0) end,
            function() vec.foldBits("i8", b, 0, 1, nil, bits, 0, 9, nil, out) end,
            function() vec.foldBits("i8", b, 0, 1, nil, bits, 0, 8, buffer.create(4), out) end,
            function() vec.foldBits("i8", b, 0, 1, nil, bits, 0, 8, nil, buffer.create(8)) end,
            function() vec.foldBits("i8", b, 0, 1, nil, nil, 0, 8, nil, out) end,
            function() vec.maskBits(bits, 0, 9, out) end,
            function() vec.maskBits(buffer.fromstring("\xff"), 0, 8, buffer.create(4)) end,
            -- bit offsets
            function() vec.compare("i8", b, 0, 1, "=", 1, nil, 2, bits, 7) end,
            function() vec.compare("i8", b, 0, 1, "=", 1, nil, 2, nil, -1) end,
            function() vec.compare("i8", b, 0, 1, "=", 1, nil, 2, nil) end,
            function() vec.bits("and", bits, 1, bits, 0, 8, nil, 0) end,
            function() vec.bits("and", bits, 0, bits, 1, 8, nil, 0) end,
            function() vec.bits("or", bits, 0, bits, 0, 8, buffer.create(1), 1) end,
            function() vec.bits("not", bits, 0, nil, nil, 4, bits, 2) end,
            function() vec.bits("and", bits, 0, bits, 0.5, 4, nil, 0) end,
            function() vec.foldBits("i8", b, 0, 1, nil, bits, 1, 8, nil, out) end,
            function() vec.foldBits("i8", b, 0, 1, nil, bits, nil, 8, nil, out) end,
            function() vec.maskBits(bits, 5, 4, out) end,
        }
        local msgs = {}
        for i, f in cases do
            local ok, err = pcall(f)
            msgs[i] = if ok then "no error" else tostring(err)
        end
        return msgs
    end
    error("unknown step " .. tostring(step))
end
"""

KINDS = ["i8", "i16", "i32", "i64", "f64", "str"]
FMT = {"i8": "b", "i16": "h", "i32": "i", "i64": "q", "f64": "d"}
WIDTH = {"i8": 1, "i16": 2, "i32": 4, "i64": 8, "f64": 8, "str": 8}
NONE = {"i8": -128, "i16": -32768, "i32": -2**31, "i64": -2**63}
OPS = ["=", "~=", "<", "<=", ">", ">="]
# not a multiple of 64 or of 8: a short last word and a short last byte
N = 64 * 21 + 13

failures = []


def check(ok, what):
    print("  %-72s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures.append(what)


def values(kind, rng, n=N):
    """n elements, about a tenth nil, mostly in -20..40 so 5 and 10 matter."""
    out = []
    for _ in range(n):
        if rng.random() < 0.1:
            out.append(None)
        elif kind == "str":
            out.append("".join(rng.choice("abcz") for _ in range(rng.randrange(0, 12))))
        elif kind == "f64":
            out.append(rng.choice([rng.uniform(-20, 40), float(rng.randint(-20, 40))]))
        elif rng.random() < 0.05:
            # i64's ends don't survive the %.17g a value is sent as
            lo = 2**40 + 1 if kind == "i64" else -(NONE[kind] + 1)
            out.append(rng.choice([lo, -lo]))
        else:
            out.append(rng.randint(-20, 40))
    return out


def layout(kind, vals, shape):
    """The elements as a column block (packed) or as rows of an odd width."""
    w = WIDTH[kind]
    off, stride = (0, w) if shape == "column" else (3, w + 5)
    data = bytearray(off + len(vals) * stride)
    for i in range(len(data)):
        data[i] = 0x5A
    heap = bytearray()
    for i, v in enumerate(vals):
        at = off + i * stride
        if kind == "str":
            if v is None:
                struct.pack_into("<II", data, at, 0, 0)
            else:
                b = v.encode()
                struct.pack_into("<II", data, at, len(data) + len(heap), len(b) + 1)
                heap += b
        elif kind == "f64":
            struct.pack_into("<d", data, at, math.nan if v is None else v)
        else:
            struct.pack_into("<" + FMT[kind], data, at, NONE[kind] if v is None else v)
    return bytes(data + heap), off, stride


def passes(op, v, w):
    if v is None:
        return op == "~="
    if isinstance(v, str):
        v, w = v.encode(), w.encode()
    return {"=": v == w, "~=": v != w, "<": v < w, "<=": v <= w, ">": v > w, ">=": v >= w}[op]


def pack_bits(flags):
    out = bytearray((len(flags) + 7) // 8)
    for i, f in enumerate(flags):
        if f:
            out[i >> 3] |= 1 << (i & 7)
    return bytes(out)


def nbytes(bits):
    return (bits + 7) // 8


def place(flags, off, base):
    """base with bits off to off + len(flags) - 1 set to flags"""
    out = bytearray(base)
    for i, f in enumerate(flags):
        j = off + i
        out[j >> 3] = out[j >> 3] & ~(1 << (j & 7)) | (int(f) << (j & 7))
    return bytes(out)


def noise(rng, size):
    return bytes(rng.randrange(256) for _ in range(size))


# aligned and not, within a byte, a word and past one
OFFS = [0, 1, 7, 8, 13, 64, 70]


def unpack_bits(b, n):
    return [bool(b[i >> 3] >> (i & 7) & 1) for i in range(n)]


def records(kind, vals, flags, gids, groups):
    out = []
    for g in range(groups):
        mine = [v for v, f, h in zip(vals, flags, gids) if f and h == g and v is not None]
        if kind == "str":
            out.append([len(mine), 0, math.inf, -math.inf])
        else:
            out.append([len(mine), sum(mine), min(mine, default=math.inf), max(mine, default=-math.inf)])
    return out


def same_records(got, want, kind):
    got = [float(x) for x in got]
    for g, rec in enumerate(want):
        count, total, least, most = got[g * 4:g * 4 + 4]
        if count != rec[0] or least != rec[2] or most != rec[3]:
            return False
        if kind == "f64":
            # a native sum may add in another order
            scale_ = sum(abs(x) for x in [rec[1], least, most] if math.isfinite(x)) or 1
            if abs(total - rec[1]) > 1e-9 * scale_ * max(rec[0], 1):
                return False
        elif total != rec[1]:
            return False
    return True


def bit_patterns(rng, n):
    """Bitmaps that hit every case a fold over words has: random, sparse,
    and runs of whole words all set or all clear."""
    words = (n + 63) // 64
    run = []
    for w in range(words):
        pick = rng.choice(["all", "none", "mixed"])
        for j in range(64):
            run.append(pick == "all" or (pick == "mixed" and rng.random() < 0.5))
    return {
        "random": [rng.random() < 0.5 for _ in range(n)],
        "sparse": [rng.random() < 0.03 for _ in range(n)],
        "words": run[:n],
        "all": [True] * n,
        "none": [False] * n,
    }


def run_checks(c, isa):
    call = lambda step, *a: c.execute_command("CALLF", "vecbits", step, *a)

    for kind in KINDS:
        for shape in ["column", "row"]:
            rng = random.Random("%s%s" % (kind, shape))
            vals = values(kind, rng)
            data, off, stride = layout(kind, vals, shape)
            none = "-" if kind in ("f64", "str") else str(NONE[kind])
            present = [v for v in vals if v is not None]
            bad = []
            case = 0
            for op in OPS:
                ws = [rng.choice(present), rng.choice(present)]
                if kind not in ("str", "f64"):
                    # numbers compare as exact reals; one no value equals
                    ws += [5.5] if kind == "i64" else [5.5, NONE[kind] - 1]
                for w in ws:
                    arg = w if kind == "str" else "%.17g" % w
                    want = [passes(op, v, w) for v in vals]
                    for n, given in [(N, "new"), (N, "given"), (N - 13, "given"), (0, "new")]:
                        boff = OFFS[case % len(OFFS)]
                        case += 1
                        # a given buffer runs a byte past the bits, all noise
                        base = noise(rng, nbytes(boff + n) + 1) if given == "given" else bytes(nbytes(boff + n))
                        got = call("compare", kind, data, off, stride, op, arg, none, n, given, boff, base)
                        if got != [str(sum(want[:n])).encode(), place(want[:n], boff, base)]:
                            bad.append((op, w, n, given, boff))
            check(not bad, "%s %s %s: compare writes each element's bit %s" % (isa, kind, shape, bad[:2]))

    rng = random.Random("bits")
    case = 0
    for n in [N, 64, 8, 1, 0]:
        x = [rng.random() < 0.5 for _ in range(n)]
        y = [rng.random() < 0.5 for _ in range(n)]
        ops = {"and": lambda p, q: p and q, "or": lambda p, q: p or q, "xor": lambda p, q: p != q,
               "andnot": lambda p, q: p and not q, "not": lambda p, q: not p}
        bad = []
        for op, f in ops.items():
            want = [f(p, q) for p, q in zip(x, y)]
            for mode in ["new", "a", "given"]:
                xoff, yoff, ooff = (OFFS[(case + k) % len(OFFS)] for k in range(3))
                case += 1
                # x and y sit in noise, which a result must not take in
                xb = place(x, xoff, noise(rng, nbytes(xoff + n) + 1))
                yb = place(y, yoff, noise(rng, nbytes(yoff + n) + 1))
                if mode == "a":
                    ooff, base = xoff, xb
                elif mode == "given":
                    base = noise(rng, nbytes(ooff + n) + 1)
                else:
                    base = bytes(nbytes(ooff + n))
                got = call("bits", op, xb, xoff, "-" if op == "not" else yb, yoff, n, mode, ooff, base)
                if got != [str(sum(want)).encode(), place(want, ooff, base)]:
                    bad.append((op, mode, xoff, yoff, ooff))
        check(not bad, "%s n=%d: bits and, or, xor, andnot, not %s" % (isa, n, bad[:3]))

    for kind in KINDS:
        for shape in ["column", "row"]:
            rng = random.Random("fold%s%s" % (kind, shape))
            vals = values(kind, rng)
            data, off, stride = layout(kind, vals, shape)
            none = "-" if kind in ("f64", "str") else str(NONE[kind])
            bad = []
            for at, (name, flags) in enumerate(bit_patterns(rng, N).items()):
                boff = OFFS[at % len(OFFS)]
                bits = place(flags, boff, noise(rng, nbytes(boff + N)))
                got = call("foldBits", kind, data, off, stride, none, bits, boff, N, "-", 1)
                if not same_records(got, records(kind, vals, flags, [0] * N, 1), kind):
                    bad.append((name, boff))
                gids = [rng.randrange(5) for _ in range(N)]
                boff = OFFS[(at + 3) % len(OFFS)]
                bits = place(flags, boff, noise(rng, nbytes(boff + N)))
                got = call("foldBits", kind, data, off, stride, none, bits, boff, N,
                           struct.pack("<%dI" % N, *gids), 5)
                if not same_records(got, records(kind, vals, flags, gids, 5), kind):
                    bad.append((name + " gids", boff))
                # bits past n are ignored
                got = call("foldBits", kind, data, off, stride, none, pack_bits([True] * N), 0, N - 5, "-", 1)
                if not same_records(got, records(kind, vals[:N - 5], [True] * (N - 5), [0] * N, 1), kind):
                    bad.append("n - 5")
            check(not bad, "%s %s %s: foldBits adds the elements whose bit is set %s" % (isa, kind, shape, bad))
            if kind == "i32" and shape == "column":
                # without none every integer is a value
                got = call("foldBits", kind, data, off, stride, "-", pack_bits([True] * N), 0, N, "-", 1)
                ints = [NONE[kind] if v is None else v for v in vals]
                check(same_records(got, records(kind, ints, [True] * N, [0] * N, 1), kind),
                      "%s i32: foldBits without none reads every integer" % isa)

    rng = random.Random("mask")
    flags = [rng.random() < 0.3 for _ in range(N)]
    for boff in OFFS:
        bits = place(flags, boff, noise(rng, nbytes(boff + N)))
        got = [int(x) for x in call("maskBits", bits, boff, N)]
        check(got == [i for i, f in enumerate(flags) if f], "%s: maskBits at bit %d lists the set bits" % (isa, boff))
    got = call("maskBits", b"", 0, 0)
    check(got == [], "%s: maskBits of no bits lists none" % isa)

    # a byte mask (lent's presence bytes) becomes bits through compare
    rng = random.Random("bytes")
    for stride, moff, boff in [(1, 0, 0), (1, 5, 13), (3, 2, 70)]:
        flags = [rng.random() < 0.5 for _ in range(N)]
        m = bytearray(noise(rng, moff + N * stride))
        for i, f in enumerate(flags):
            m[moff + i * stride] = int(f)
        base = noise(rng, nbytes(boff + N) + 1)
        got = call("compare", "i8", bytes(m), moff, stride, "~=", "0", "-", N, "given", boff, base)
        check(got == [str(sum(flags)).encode(), place(flags, boff, base)],
              "%s: compare turns a byte mask at stride %d into bits at %d" % (isa, stride, boff))

    # Luau's own buffer.writebits and readbits agree on the layout
    rng = random.Random("luau")
    xs = [rng.randint(-5, 20) for _ in range(300)]
    flags = [rng.random() < 0.4 for _ in range(300)]
    for off, off2 in [(0, 0), (5, 70), (64, 3)]:
        got = call("luau", struct.pack("<300i", *xs), "".join("1" if f else "0" for f in flags), off, off2, 7)
        mine = [x for x, f in zip(xs, flags) if f]
        check(got[0].decode() == "".join("1" if x > 7 else "0" for x in xs)
              and [float(v) for v in got[1:]] == [len(mine), sum(mine), min(mine), max(mine)],
              "%s: writebits at %d into foldBits, compare at %d out to readbits" % (isa, off, off2))

    rng = random.Random("query")
    xs = [rng.randint(-5, 20) for _ in range(100000)]
    got = call("query", struct.pack("<%di" % len(xs), *xs), len(xs), 5, 10)
    mine = [x for x in xs if 5 < x < 10]
    check([float(v) for v in got] == [len(mine), len(mine), sum(mine), min(mine), max(mine)],
          "%s: sum x where x > 5 and x < 10 is %d over %d" % (isa, sum(mine), len(mine)))

    got = [m.decode() for m in call("errors")]
    want = [
        "vec.compare: unknown kind u8",
        "vec.compare: unknown op !=",
        "vec.compare: reads past the end of a buffer",
        "vec.compare: writes past the end of a buffer",
        "vec.compare: kind i32 compares with a number",
        "vec.compare: kind str compares with a string",
        "vec.compare: takes a buffer or nil for bits",
        "vec.bits: unknown op nand",
        "vec.bits: reads past the end of a buffer",
        "vec.bits: and takes two buffers of bits",
        "vec.bits: not takes nil for its second bits",
        "vec.bits: writes past the end of a buffer",
        "vec.foldBits: reads past the end of a buffer",
        "vec.foldBits: takes a buffer of n u32 group ids or nil for gids",
        "vec.foldBits: writes past the end of a buffer",
        "vec.foldBits: takes a buffer of bits",
        "vec.maskBits: reads past the end of a buffer",
        "vec.maskBits: writes past the end of a buffer",
        "vec.compare: writes past the end of a buffer",
        "vec.compare: bitsoff must be a whole number from 0",
        "vec.compare: bitsoff must be a whole number from 0",
        "vec.bits: reads past the end of a buffer",
        "vec.bits: reads past the end of a buffer",
        "vec.bits: writes past the end of a buffer",
        "vec.bits: out overlaps an input at another offset",
        "vec.bits: boff must be a whole number from 0",
        "vec.foldBits: reads past the end of a buffer",
        "vec.foldBits: bitsoff must be a whole number from 0",
        "vec.maskBits: reads past the end of a buffer",
    ]
    check(len(got) == len(want), "%s: %d error cases" % (isa, len(want)))
    for g, w in zip(got, want):
        check(g == w, "%s: error %r" % (isa, w) + ("" if g == w else " got %r" % g))


seen = set()
for want_isa in ["scalar", "avx2", "avx512"]:
    data = os.path.join(os.getcwd(), "vecbits_data_" + want_isa)
    os.makedirs(data, exist_ok=True)
    for f in os.listdir(data):
        if f.endswith(".dat"):
            os.remove(os.path.join(data, f))
    env = dict(os.environ)
    if want_isa == "avx512":
        env.pop("BARCH_VEC_ISA", None)
    else:
        env["BARCH_VEC_ISA"] = want_isa
    log = open(os.path.join(data, "barchd.log"), "wb")
    proc = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", data],
                            stdout=log, stderr=subprocess.STDOUT, env=env)
    try:
        scale.wait_for_port(PORT, proc=proc, what="barchd")
        # no retry: redis-py's own sends a timed out command again on a new
        # connection, which has no USE
        c = redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=None,
                        retry=Retry(NoBackoff(), 0))
        c.execute_command("USE", "vecbits")
        c.execute_command("SETF", "vecbits", LUAU)
        isa = c.execute_command("CALLF", "vecbits", "isa").decode()
        if isa in seen:
            print("  %s: this machine has no %s, already checked" % (isa, want_isa))
        else:
            seen.add(isa)
            run_checks(c, isa)
        c.close()
    finally:
        proc.terminate()
        proc.wait(60)
        log.close()

print("FAILED: %d" % len(failures) if failures else "ok")
sys.exit(1 if failures else 0)
