#!/usr/bin/env python3
"""
The transaction examples in examples/transactions still run - TODO 727, 730.

Each one is run against one barchd twice, as Python and with --luau as the stored
functions in examples/transactions/luau, and must exit 0 with every rule it checks
held each time.
"""
import os
import socket
import subprocess
import sys
import time

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
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
