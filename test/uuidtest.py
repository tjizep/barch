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
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
