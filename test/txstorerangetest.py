#!/usr/bin/env python3
"""
A script's barch.store range reads inside a transaction - TODO 701.

barch.store.range, count, min and max, and a page walk (`for row in barch.space[""]`),
were refused inside a transaction, since nothing checked them at COMMITTX. Now they go
through the transaction's overlay as RANGE and COUNT do: they read the snapshot taken at
BEGINTX with the transaction's own writes on top, and each is logged for the phantom
check, so a key another client writes into what the script walked makes COMMITTX
answer TXCONFLICT.

Checked against one barchd, a space without Raft:
  - each read sees the snapshot: a key another client adds or deletes after BEGINTX
    isn't seen, and the transaction's own writes and deletes are;
  - min sees past the transaction's own deletes and past a key another client added
    below the smallest after BEGINTX, and max past a function added above the largest
    (a script's max is always a function, since functions sort above every key);
  - a key written into a range, count or page walk the script made, or below a min or
    above a max it read, makes COMMITTX answer TXCONFLICT and make nothing;
  - a key written outside what was walked lets it commit;
  - locked regions are still refused, saying why.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14780)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


DATA = os.path.join(os.getcwd(), "txstorerange_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txstorerange.log"), "a")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

TAG = str(int(time.time()))
SPACE = "txsr" + TAG


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=30, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", SPACE)
    return c


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


FN = "function call() return 1 end\n"
FUNCTIONS = {
    "srange": "function call(lo, hi) return table.concat(barch.store.range(lo, hi, 100), ',') end\n",
    "srangeat": "function call(lo, hi, at) return table.concat(barch.store.range(lo, hi, 100, tonumber(at)), ',') end\n",
    "scount": "function call(lo, hi) return barch.store.count(lo, hi) end\n",
    "smin": "function call() return barch.store.min() or 'nil' end\n",
    "smax": "function call() return barch.store.max() or 'nil' end\n",
    "swalk": "function call() local out = {} for row in barch.space['" + SPACE + "'] do"
             " if row.type == 'key' then out[#out + 1] = row.key end end"
             " return table.concat(out, ',') end\n",
    "sset": "function call(k, v) barch.store.set(k, v) return 'set' end\n",
    "slocked": "function call(k) return barch.store.locked(k, function() return 'in' end) end\n",
}


def reset(*keys):
    c = client()
    for k in c.execute_command("KEYS", "*"):
        c.delete(k)
    for k in keys:
        c.set(k, "v")


def conflicts(read, write, what):
    """read: the script's call inside a transaction; write: another client's command after it"""
    c = client()
    c.execute_command("BEGINTX")
    seen = answer(c, *read)
    answer(client(), *write)
    c.execute_command("SET", "zz-mark", "x")
    got = answer(c, "COMMITTX")
    ok = "TXCONFLICT" in str(got) and client().get("zz-mark") is None
    check(ok, "%s: TXCONFLICT (%s, %s)" % (what, str(seen)[:20], str(got)[:40]))


def commits(read, write, what):
    c = client()
    c.execute_command("BEGINTX")
    answer(c, *read)
    answer(client(), *write)
    c.execute_command("SET", "zz-mark", "x")
    got = answer(c, "COMMITTX")
    check(got == "OK" and client().get("zz-mark") == "x", "%s: commits (%s)" % (what, str(got)[:40]))
    client().delete("zz-mark")


try:
    c = client()
    for name, body in FUNCTIONS.items():
        check(answer(c, "SETF", name, body) == "OK", "SETF %s" % name)

    print("the reads see the snapshot and the transaction's own writes")
    reset("k2", "k3", "k4")
    c.execute_command("BEGINTX")
    other = client()
    other.set("k1", "v")
    other.set("k5", "v")
    other.delete("k3")
    other.execute_command("SETF", "zz0", FN)
    check(answer(c, "CALLF", "srange", "k", "l") == "k2,k3,k4", "range sees BEGINTX's keys")
    check(answer(c, "CALLF", "scount", "k", "l") == 3, "count too")
    # a script's max is the top function, since functions sort above every key
    check(answer(c, "CALLF", "smin") == "k2" and answer(c, "CALLF", "smax") == "SWALK",
          "min and max ignore keys and functions added since")
    check(answer(c, "CALLF", "swalk") == "k2,k3,k4", "the page walk too")
    c.execute_command("SET", "k35", "mine")
    c.execute_command("DEL", "k2")
    answer(c, "CALLF", "sset", "k45", "mine")
    check(answer(c, "CALLF", "srange", "k", "l") == "k3,k35,k4,k45", "range sees its own writes and deletes")
    check(answer(c, "CALLF", "srangeat", "k", "l", 1) == "k35,k4,k45", "range with an offset too")
    check(answer(c, "CALLF", "scount", "k", "l") == 4, "count too")
    check(answer(c, "CALLF", "smin") == "k3", "min steps past its own delete")
    c.execute_command("DEL", "k45", "k4")
    check(answer(c, "CALLF", "swalk") == "k3,k35", "the page walk sees them")
    c.execute_command("ABORTTX")

    print("phantoms make COMMITTX answer TXCONFLICT")
    reset("k2", "k3", "k4")
    conflicts(("CALLF", "srange", "k", "l"), ("SET", "k25", "v"), "a key added into a range")
    reset("k2", "k3", "k4")
    conflicts(("CALLF", "srange", "k", "l"), ("DEL", "k3"), "a key deleted from a range")
    reset("k2", "k3", "k4")
    conflicts(("CALLF", "scount", "k", "l"), ("SET", "k25", "v"), "a key added into a count")
    reset("k2", "k3", "k4")
    conflicts(("CALLF", "smin",), ("SET", "k1", "v"), "a key added below the min")
    reset("k2", "k3", "k4")
    conflicts(("CALLF", "smin",), ("DEL", "k2"), "the min deleted")
    reset("k2", "k3", "k4")
    conflicts(("CALLF", "smax",), ("SETF", "zz1", FN), "a function added above the max")
    reset("k2", "k3", "k4")
    conflicts(("CALLF", "swalk",), ("SET", "a1", "v"), "a key added anywhere in a page walk")

    print("a key outside what was walked doesn't")
    reset("k2", "k3", "k4")
    commits(("CALLF", "srange", "k", "l"), ("SET", "m1", "v"), "a key added past a range")
    reset("k2", "k3", "k4")
    commits(("CALLF", "smin",), ("SET", "k35", "v"), "a key added above the min")
    reset("k2", "k3", "k4")
    commits(("CALLF", "smax",), ("SET", "k15", "v"), "a key added below the max")

    print("still refused")
    reset("k2")
    c.execute_command("BEGINTX")
    got = answer(c, "CALLF", "slocked", "k2")
    c.execute_command("ABORTTX")
    check("transaction" in str(got), "locked says it's the transaction (%s)" % str(got)[:50])
    check(answer(c, "CALLF", "srange", "k", "l") == "k2", "range works outside one")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
