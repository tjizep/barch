#!/usr/bin/env python3
"""
barch.uuid() hands out UUIDv7s that sort by time as keys - TODO 761, and barch.time()
answers the time in ms - TODO 763.

One barchd: the format, the version and variant bits, the time in the first 48 bits,
strict order across many calls in one CALLF and across connections calling at once,
and uuids used as keys coming back from RANGE in the order they were made. Then
barch.time() is the clock and agrees with the time in a uuid. clustertimetest.py has
both in a Raft space, where every member has to answer the same.
"""
import os
import re
import socket
import subprocess
import sys
import threading
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14825)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-74s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")

DATA = os.path.join(os.getcwd(), "uuid_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "uuid.log"), "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "uuid_" + str(int(time.time()))

try:
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, single_connection_client=True,
                    decode_responses=True)
    c.execute_command("USE", SPACE)

    print("one uuid")
    check(c.execute_command("SETF", "one", "function call() return barch.uuid() end") == "OK", "SETF one")
    before = int(time.time() * 1000)
    try:
        u = c.execute_command("CALLF", "one")
    except redis.ResponseError as e:
        u = "error: %s" % e
    after = int(time.time() * 1000)
    check(isinstance(u, str) and UUID.match(u) is not None, "looks like a UUIDv7 (%s)" % u)
    ms = int(u.replace("-", "")[:12], 16) if UUID.match(u or "") else 0
    check(before - 1000 <= ms <= after + 1000, "its first 48 bits are the time in ms (%d, now %d)" % (ms, after))

    print("many in one call")
    check(c.execute_command("SETF", "many", """
function call(n)
    local out = {}
    for i = 1, tonumber(n) do out[i] = barch.uuid() end
    return out
end""") == "OK", "SETF many")
    try:
        got = c.execute_command("CALLF", "many", 20000)
    except redis.ResponseError as e:
        got = []
    check(len(got) == 20000 and all(UUID.match(x) for x in got), "20000 of them, every one well formed")
    check(all(got[i] < got[i + 1] for i in range(len(got) - 1)), "each sorts after the one before")

    print("connections calling at once")
    results = {}

    def worker(n):
        w = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, single_connection_client=True,
                        decode_responses=True)
        w.execute_command("USE", SPACE)
        out = []
        try:
            for _ in range(20):
                out += w.execute_command("CALLF", "many", 500)
        except redis.ResponseError:
            pass
        results[n] = out

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    every = [x for n in results for x in results[n]]
    check(len(every) == 8 * 20 * 500, "8 connections got 10000 each (%d)" % len(every))
    check(all(all(r[i] < r[i + 1] for i in range(len(r) - 1)) for r in results.values()),
          "each connection's come out in order")
    check(len(set(every) | set(got)) == len(every) + len(got), "and no uuid was handed out twice")

    print("as keys")
    check(c.execute_command("SETF", "put", """
function call(n)
    for i = 1, tonumber(n) do barch.store.set(barch.uuid(), tostring(i)) end
    return "OK"
end""") == "OK", "SETF put")
    try:
        c.execute_command("CALLF", "put", 1000)
    except redis.ResponseError as e:
        print("  put: %s" % e)
    keys = c.execute_command("RANGE", "0", "g")
    check([c.get(k) for k in keys] == [str(i) for i in range(1, 1001)],
          "RANGE answers them in the order they were made (%d keys)" % len(keys))

    print("barch.time()")
    check(c.execute_command("SETF", "now", """
function call()
    local t1 = barch.time()
    local u = barch.uuid()
    local t2 = barch.time()
    return {string.format('%d', t1), u, string.format('%d', t2), type(t1), tostring(t1 == math.floor(t1))}
end""") == "OK", "SETF now")
    before = int(time.time() * 1000)
    try:
        t1, u, t2, kind, whole = c.execute_command("CALLF", "now")
        t1, t2 = int(t1), int(t2)
    except (redis.ResponseError, ValueError) as e:
        t1, u, t2, kind, whole = 0, "", 0, str(e), ""
    after = int(time.time() * 1000)
    check(kind == "number" and whole == "true", "a whole number of ms (%s %s)" % (kind, whole))
    check(before - 1000 <= t1 <= t2 <= after + 1000, "it's the clock (%d..%d, now %d)" % (t1, t2, after))
    check(UUID.match(u or "") is not None and t1 <= int(u.replace("-", "")[:12], 16) <= t2,
          "and a uuid made between two reads has a time between them")

    print("barch.uuidAsBuffer()")
    raw_c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, single_connection_client=True,
                        decode_responses=False)
    raw_c.execute_command("USE", SPACE)

    check(c.execute_command("SETF", "buf_new", """
function call()
    local b = barch.uuidAsBuffer()
    local b2 = barch.uuidAsBuffer(nil)
    return {type(b), tostring(buffer.len(b)), barch.uuidToString(b), barch.uuidToString(b2)}
end""") == "OK", "SETF buf_new")
    before = int(time.time() * 1000)
    btype, blen, bs1, bs2 = c.execute_command("CALLF", "buf_new")
    after = int(time.time() * 1000)
    check(btype == "buffer" and blen == "16", "uuidAsBuffer() returns a 16-byte buffer (%s, %s)" % (btype, blen))
    check(UUID.match(bs1) is not None and UUID.match(bs2) is not None and bs1 < bs2,
          "uuidAsBuffer() and uuidAsBuffer(nil) make ordered UUIDv7s")
    bms = int(bs1.replace("-", "")[:12], 16)
    check(before - 1000 <= bms <= after + 1000, "uuidAsBuffer() timestamp matches clock")

    check(c.execute_command("SETF", "buf_raw", """
function call(s)
    if s then return barch.uuidAsBuffer(s) end
    return barch.uuidAsBuffer()
end""") == "OK", "SETF buf_raw")
    known = "018f3a6c-1b2e-7c4d-9a8b-2c3d4e5f6071"
    known_bytes = bytes.fromhex(known.replace("-", ""))
    got_raw = raw_c.execute_command("CALLF", "buf_raw", known)
    check(got_raw == known_bytes, "uuidAsBuffer(str) parses exact 16 bytes")
    check(raw_c.execute_command("CALLF", "buf_raw", known.upper()) == known_bytes,
          "uuidAsBuffer(str) parses uppercase hex")
    check(raw_c.execute_command("CALLF", "buf_raw", known.replace("-", "")) == known_bytes,
          "uuidAsBuffer(str) parses 32-char compact hex")
    check(raw_c.execute_command("CALLF", "buf_raw", "550e8400-e29b-41d4-a716-446655440000")
          == bytes.fromhex("550e8400e29b41d4a716446655440000"),
          "uuidAsBuffer(str) parses UUIDv4")
    check(raw_c.execute_command("CALLF", "buf_raw", "00000000-0000-0000-0000-000000000000") == b"\x00" * 16,
          "uuidAsBuffer(str) parses nil uuid")
    check(raw_c.execute_command("CALLF", "buf_raw", "ffffffff-ffff-ffff-ffff-ffffffffffff") == b"\xff" * 16,
          "uuidAsBuffer(str) parses max uuid")
    gen_raw = raw_c.execute_command("CALLF", "buf_raw")
    check(isinstance(gen_raw, bytes) and len(gen_raw) == 16 and (gen_raw[6] >> 4) == 7 and (gen_raw[8] & 0xc0) == 0x80,
          "uuidAsBuffer() raw bytes have v7 version and RFC 9562 variant")

    print("shared monotonic order across uuid(), uuidAsBuffer(), uuidWrite()")
    check(c.execute_command("SETF", "interleaved", """
function call(n)
    local out = {}
    local scratch = buffer.create(32)
    for i = 1, tonumber(n) do
        local a = barch.uuid()
        local b = barch.uuidToString(barch.uuidAsBuffer())
        barch.uuidWrite(scratch, 8)
        local c = barch.uuidToString(scratch, 8)
        table.insert(out, a)
        table.insert(out, b)
        table.insert(out, c)
    end
    return out
end""") == "OK", "SETF interleaved")
    seq = c.execute_command("CALLF", "interleaved", 1000)
    check(len(seq) == 3000 and all(UUID.match(x) for x in seq), "3000 interleaved uuids all valid")
    check(all(seq[i] < seq[i + 1] for i in range(len(seq) - 1)),
          "uuid(), uuidAsBuffer(), and uuidWrite() share one strictly increasing order")

    print("uuidToString, uuidWrite, uuidTime, uuidVersion, uuidCompare")
    check(c.execute_command("SETF", "helpers", """
function call()
    local s = "018f3a6c-1b2e-7c4d-9a8b-2c3d4e5f6071"
    local b = barch.uuidAsBuffer(s)
    assert(barch.uuidToString(b) == s)

    -- write into a larger buffer at an offset and check surrounding bytes stay intact
    local big = buffer.create(40)
    buffer.fill(big, 0, 0x55, 40)
    barch.uuidWrite(big, 12, s)
    assert(buffer.readu8(big, 11) == 0x55)
    assert(buffer.readu8(big, 28) == 0x55)
    assert(barch.uuidToString(big, 12) == s)
    assert(barch.uuidToString(big, integer.create(12)) == s)

    -- failed uuidWrite leaves destination untouched
    local ok = pcall(barch.uuidWrite, big, 12, "not-a-uuid")
    assert(not ok)
    assert(barch.uuidToString(big, 12) == s)

    -- uuidTime and uuidVersion on string, buffer, and buffer+offset
    local expected_ms = 0x018f3a6c1b2e
    assert(barch.uuidTime(s) == expected_ms)
    assert(barch.uuidTime(b) == expected_ms)
    assert(barch.uuidTime(big, 12) == expected_ms)
    assert(barch.uuidVersion(s) == 7)
    assert(barch.uuidVersion(b) == 7)
    assert(barch.uuidVersion(big, 12) == 7)

    local v4 = "550e8400-e29b-41d4-a716-446655440000"
    assert(barch.uuidVersion(v4) == 4)
    assert(barch.uuidVersion(barch.uuidAsBuffer(v4)) == 4)
    assert(barch.uuidVersion("00000000-0000-0000-0000-000000000000") == 0)
    assert(barch.uuidVersion("ffffffff-ffff-ffff-ffff-ffffffffffff") == 15)

    -- uuidCompare: string/buffer/mixed and unsigned byte ordering
    local lo = barch.uuidAsBuffer("7fffffff-ffff-7fff-8000-000000000000")
    local hi = barch.uuidAsBuffer("80000000-0000-7000-8000-000000000000")
    assert(barch.uuidCompare(lo, hi) == -1)
    assert(barch.uuidCompare(hi, lo) == 1)
    assert(barch.uuidCompare(lo, lo) == 0)
    assert(barch.uuidCompare(b, s) == 0)
    assert(barch.uuidCompare(s:upper(), b) == 0)
    assert(barch.uuidCompare(lo, "80000000000070008000000000000000") == -1)

    -- works inside a locked region
    local locked_u = barch.store.locked("k", function()
        local lb = barch.uuidAsBuffer()
        return barch.uuidToString(lb)
    end)
    assert(#locked_u == 36)
    return "OK"
end""") == "OK", "SETF helpers")
    check(c.execute_command("CALLF", "helpers") == "OK", "uuidToString/Write/Time/Version/Compare all match")

    print("wrong types and invalid arguments")
    check(c.execute_command("SETF", "badargs", """
function call()
    local function mustFail(label, fn, ...)
        local ok = pcall(fn, ...)
        if ok then error("expected failure: " .. label) end
    end

    local b16 = barch.uuidAsBuffer()
    local b15 = buffer.create(15)
    local b17 = buffer.create(17)
    local b32 = buffer.create(32)
    local s = "018f3a6c-1b2e-7c4d-9a8b-2c3d4e5f6071"
    local v4 = "550e8400-e29b-41d4-a716-446655440000"

    -- uuidAsBuffer wrong types, extra args, malformed strings
    mustFail("uuidAsBuffer number", barch.uuidAsBuffer, 123)
    mustFail("uuidAsBuffer bool", barch.uuidAsBuffer, true)
    mustFail("uuidAsBuffer table", barch.uuidAsBuffer, {})
    mustFail("uuidAsBuffer buffer", barch.uuidAsBuffer, b16)
    mustFail("uuidAsBuffer function", barch.uuidAsBuffer, function() end)
    mustFail("uuidAsBuffer 2 args", barch.uuidAsBuffer, s, s)
    mustFail("uuidAsBuffer empty", barch.uuidAsBuffer, "")
    mustFail("uuidAsBuffer short", barch.uuidAsBuffer, "018f3a6c-1b2e-7c4d-9a8b-2c3d4e5f607")
    mustFail("uuidAsBuffer bad dash", barch.uuidAsBuffer, "018f3a6c_1b2e_7c4d_9a8b_2c3d4e5f6071")
    mustFail("uuidAsBuffer non-hex", barch.uuidAsBuffer, "018f3a6g-1b2e-7c4d-9a8b-2c3d4e5f6071")
    mustFail("uuidAsBuffer space", barch.uuidAsBuffer, " " .. s)
    mustFail("uuidAsBuffer nul", barch.uuidAsBuffer, "018f3a6c-1b2e-7c4d-9a8b-2c3d4e5f60" .. string.char(0) .. "1")

    -- uuidToString errors
    mustFail("uuidToString 0 args", barch.uuidToString)
    mustFail("uuidToString 3 args", barch.uuidToString, b16, 0, 1)
    mustFail("uuidToString string", barch.uuidToString, s)
    mustFail("uuidToString number", barch.uuidToString, 42)
    mustFail("uuidToString nil", barch.uuidToString, nil)
    mustFail("uuidToString 15 bytes", barch.uuidToString, b15)
    mustFail("uuidToString 17 bytes no off", barch.uuidToString, b17)
    mustFail("uuidToString neg off", barch.uuidToString, b32, -1)
    mustFail("uuidToString float off", barch.uuidToString, b32, 1.5)
    mustFail("uuidToString nan off", barch.uuidToString, b32, 0/0)
    mustFail("uuidToString str off", barch.uuidToString, b32, "0")
    mustFail("uuidToString oob off", barch.uuidToString, b32, 17)
    mustFail("uuidToString neg int off", barch.uuidToString, b32, integer.create(-1))

    -- uuidWrite errors
    mustFail("uuidWrite 0 args", barch.uuidWrite)
    mustFail("uuidWrite 1 arg", barch.uuidWrite, b32)
    mustFail("uuidWrite 4 args", barch.uuidWrite, b32, 0, s, 1)
    mustFail("uuidWrite non-buf", barch.uuidWrite, s, 0)
    mustFail("uuidWrite oob", barch.uuidWrite, b32, 17)
    mustFail("uuidWrite neg off", barch.uuidWrite, b32, -1)
    mustFail("uuidWrite float off", barch.uuidWrite, b32, 0.5)
    mustFail("uuidWrite num str", barch.uuidWrite, b32, 0, 123)
    mustFail("uuidWrite buf str", barch.uuidWrite, b32, 0, b16)
    mustFail("uuidWrite bad str", barch.uuidWrite, b32, 0, "bad")

    -- uuidTime errors
    mustFail("uuidTime 0 args", barch.uuidTime)
    mustFail("uuidTime 3 args", barch.uuidTime, b16, 0, 1)
    mustFail("uuidTime number", barch.uuidTime, 123)
    mustFail("uuidTime bad str", barch.uuidTime, "bad")
    mustFail("uuidTime str with off", barch.uuidTime, s, 0)
    mustFail("uuidTime 17 bytes no off", barch.uuidTime, b17)
    mustFail("uuidTime oob off", barch.uuidTime, b32, 17)
    mustFail("uuidTime v4 str", barch.uuidTime, v4)
    mustFail("uuidTime v4 buf", barch.uuidTime, barch.uuidAsBuffer(v4))
    mustFail("uuidTime bad variant", barch.uuidTime, "018f3a6c-1b2e-7c4d-0a8b-2c3d4e5f6071")

    -- uuidVersion errors
    mustFail("uuidVersion 0 args", barch.uuidVersion)
    mustFail("uuidVersion number", barch.uuidVersion, 123)
    mustFail("uuidVersion str with off", barch.uuidVersion, s, 0)
    mustFail("uuidVersion 15 bytes", barch.uuidVersion, b15)
    mustFail("uuidVersion bad str", barch.uuidVersion, "bad")

    -- uuidCompare errors
    mustFail("uuidCompare 0 args", barch.uuidCompare)
    mustFail("uuidCompare 1 arg", barch.uuidCompare, s)
    mustFail("uuidCompare 3 args", barch.uuidCompare, s, s, s)
    mustFail("uuidCompare num", barch.uuidCompare, 123, s)
    mustFail("uuidCompare bad str", barch.uuidCompare, s, "bad")
    mustFail("uuidCompare 15 bytes", barch.uuidCompare, b15, b16)
    mustFail("uuidCompare 17 bytes", barch.uuidCompare, b16, b17)
    return "OK"
end""") == "OK", "SETF badargs")
    check(c.execute_command("CALLF", "badargs") == "OK", "all wrong types, bad offsets, and malformed uuids raise errors")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
