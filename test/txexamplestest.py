#!/usr/bin/env python3
"""
The transaction examples in examples/transactions still run - TODO 727, 730.

Each one is run against one barchd twice, as Python and with --luau as the stored
functions in examples/transactions/luau, and must exit 0 with every rule it checks
held each time.

Their numbers are int64s, the 8 bytes setInt64At writes, worked out with Luau's
`integer` type rather than doubles - TODO 736. So the Luau ledger is also run on
balances past 2^53, where a double can't hold every cent, and must stay exact, and a
transfer that would take a balance past 2^63 must be refused with nothing written.
"""
import os
import socket
import struct
import subprocess
import sys
import time

import redis
import scale

scale.workdir()
PORT = scale.port(default=14817)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)
EXAMPLES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "examples", "transactions")

failures = 0


def check(ok, what):
    global failures
    print("  %-74s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


DATA = os.path.join(os.getcwd(), "txexamples_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txexamples.log"), "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

try:
    for name in ("ledger", "auction", "cinema", "orders", "warehouse", "oncall"):
        for how in ([], ["--luau"]):
            t0 = time.time()
            r = subprocess.run([sys.executable, os.path.join(EXAMPLES, name + ".py"), "--port", str(PORT)] + how,
                               capture_output=True, text=True, timeout=300)
            out = r.stdout + r.stderr
            print(r.stdout, end="")
            ok = r.returncode == 0 and "Traceback" not in out
            check(ok, "%s%s ran and its rules held (%.1f s)" % (name, " in Luau" if how else "", time.time() - t0))
            if not ok:
                print(r.stderr[-2000:])

    print("the Luau ledger past 2^53")
    big = redis.Redis(host="127.0.0.1", port=PORT, single_connection_client=True)
    big.execute_command("USE", "example_big")
    for f in ("txretry", "ledger"):
        with open(os.path.join(EXAMPLES, "luau", f + ".luau")) as source:
            big.execute_command("SETF", f, source.read())

    def balance(a):
        v = big.get(a)
        return struct.unpack("<q", v)[0] if v is not None and len(v) == 8 else v

    def answer(*cmd):
        try:
            return big.execute_command(*cmd)
        except redis.ResponseError as e:
            return "ERR " + str(e)

    START = 2 ** 62
    big.set("acct:a", struct.pack("<q", START))
    big.set("acct:b", struct.pack("<q", 0))
    big.set("acct:c", struct.pack("<q", 2 ** 63 - 6))
    big.delete("journal:next")
    got = answer("CALLF", "ledger", "transfer", "acct:a", "acct:b", 1)
    check(got == 1 and balance("acct:a") == START - 1 and balance("acct:b") == 1,
          "one cent off 2^62 is exact (%s, %s)" % (got, balance("acct:a")))
    got = answer("CALLF", "ledger", "audit", "acct:a", "acct:b")
    check(got == START, "and the audit adds back up to 2^62 exactly (%s)" % got)
    got = answer("CALLF", "ledger", "transfer", "acct:a", "acct:c", 10)
    check("past 64 bits" in str(got) and balance("acct:a") == START - 1 and balance("acct:c") == 2 ** 63 - 6
          and big.get("journal:2") is None,
          "a transfer past 2^63 is refused, nothing written (%s)" % str(got)[:50])
    got = answer("CALLF", "ledger", "transfer", "acct:a", "acct:b", str(2 ** 63))
    check("not a whole number" in str(got) and balance("acct:b") == 1,
          "and so is an amount that doesn't fit in 64 bits (%s)" % str(got)[:50])
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
