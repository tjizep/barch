#!/usr/bin/env python3
"""
A \\1 byte in a key comes back whole - TODO 760.

A key's string ends at its last byte, a 0. Inside a key made of parts (one with a space in
it, or a container's name and field) a 1 ends each part, and the decoder used to stop every
string at the first 0 or 1. So `SET "a\\1b" v` stored the key and GET found it, but KEYS and
RANGE answered `a`, and anything written back with that name went to a different key.

One barchd: plain keys with a 1 and the other low bytes in them, read back through KEYS,
RANGE, SCAN and barch.store.range, and each name used again to read its value.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14824)
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


DATA = os.path.join(os.getcwd(), "keybytes_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "keybytes.log"), "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "keybytes_" + str(int(time.time()))
KEYS = [b"a\x01b", b"\x01lead", b"trail\x01", b"two\x01\x01ones", b"a\x01\x03b", b"c\x02d", b"plain"]

try:
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, single_connection_client=True)
    c.execute_command("USE", SPACE)
    for i, k in enumerate(KEYS):
        c.set(k, b"v%d" % i)

    print("a plain key with a 1 in it")
    check(sorted(c.keys("*")) == sorted(KEYS), "KEYS answers every key whole (%s)" % sorted(c.keys("*")))
    got = c.execute_command("RANGE", "\x01", "\x7f")
    check(sorted(got) == sorted(KEYS), "RANGE does too (%s)" % got)
    scanned = []
    cursor = 0
    while True:
        cursor, batch = c.scan(cursor, count=100)
        scanned += batch
        if int(cursor) == 0:
            break
    check(sorted(scanned) == sorted(KEYS), "and SCAN (%s)" % sorted(scanned))
    check(c.execute_command("SETF", "keys_in", """
function call()
    local out = {}
    for _, k in barch.store.range("\\1", "\\127", 100) do
        table.insert(out, k)
    end
    return out
end""") == b"OK", "SETF keys_in")
    got = c.execute_command("CALLF", "keys_in")
    check(sorted(got) == sorted(KEYS), "and barch.store.range (%s)" % got)
    want = {k: b"v%d" % i for i, k in enumerate(KEYS)}
    check(all(c.get(k) == want.get(k) for k in c.keys("*")),
          "each name KEYS answers reads its own value back")
    check(c.get(b"a") is None and c.get(b"two") is None, "and the cut short names hold nothing")

    # a field or a member is the last part of its container's key, so nothing follows
    # it either. A container's name is not: a part follows it, and there a 1 is
    # ambiguous with the end of the part - TODO 762
    print("the last part of a container's key")
    c.hset("hash", b"f\x01g", "1")
    check(c.hkeys("hash") == [b"f\x01g"], "HKEYS answers the field whole (%s)" % c.hkeys("hash"))
    c.zadd("zset", {b"m\x01n": 1})
    check(c.zrange("zset", 0, -1) == [b"m\x01n"], "ZRANGE answers the member whole (%s)" % c.zrange("zset", 0, -1))
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
