#!/usr/bin/env python3
"""
A transaction's limits - TODO 701.

tx_max_keys and tx_max_bytes bound what a transaction holds: the keys it wrote,
deleted and read, and the bytes they take, its overlay's and its read log's. A command
that takes it past either is answered with an error naming the setting, and from then
on the transaction can only be ended: every command but ABORTTX and COMMITTX answers
the same, and COMMITTX aborts it, making nothing. tx_max_age_ms bounds how long one may
stay open: the space's maintenance thread lets go of an older one's snapshot, so the
before-images it held go even while its session sits idle, and the session finds out
at its next command. 0 is no limit.

Checked against one barchd, a space without Raft:
  - the defaults are 100000 keys, 64 MB and 60 s;
  - past tx_max_keys by SETs, by one MSET, and by GETs, the command answers
    TXABORTED naming the setting (a GET, which writes its value straight to the reply,
    answers, and the command after it says so), so does each command after, COMMITTX
    aborts it and nothing of it is made, and ABORTTX ends one too;
  - past tx_max_bytes by one large value, the same;
  - a script's barch.store.set past tx_max_keys fails the script, naming the setting;
  - a transaction left open past tx_max_age_ms has its layers freed while idle
    (tx_layers back to 0, tx_expired counted), and its next command and COMMITTX
    answer TXABORTED naming the setting;
  - with the limits 0, a transaction past the old ones commits;
  - tx_over_limit counts the transactions that went past a size limit.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14790)
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


DATA = os.path.join(os.getcwd(), "txlimits_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txlimits.log"), "a")
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
SPACE = "txlim" + TAG


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


def config(name, value=None):
    c = client()
    if value is None:
        got = c.execute_command("CONFIG", "GET", name)
        return got[name] if isinstance(got, dict) else got[1]
    return c.execute_command("CONFIG", "SET", name, str(value))


def stats():
    text = client().execute_command("INFO", "transactions")
    if isinstance(text, dict):
        return {k: int(v) for k, v in text.items()}
    out = {}
    for line in str(text).splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            out[k.strip()] = int(v)
    return out


def ended_only(c, setting, what):
    """after a command took it past `setting`: every command refuses, COMMITTX aborts"""
    got = [answer(c, "GET", "anything"), answer(c, "SET", "more", "x"), answer(c, "COMMITTX")]
    check(all("TXABORTED" in str(g) and setting in str(g) for g in got),
          "%s: each command after, and COMMITTX, say so (%s)" % (what, str(got[2])[:50]))
    check(answer(c, "GET", "anything") is None, "%s: and it's ended" % what)


try:
    print("the defaults")
    check([config(n) for n in ("tx_max_keys", "tx_max_bytes", "tx_max_age_ms")] == ["100000", "67108864", "60000"],
          "100000 keys, 64 MB, 60 s")

    print("tx_max_keys")
    config("tx_max_keys", 10)
    before = stats()
    c = client()
    c.execute_command("BEGINTX")
    oks = [answer(c, "SET", "k%d" % i, "v") for i in range(10)]
    got = answer(c, "SET", "k10", "v")
    check(all(o is True or o == "OK" for o in oks) and "TXABORTED" in str(got) and "tx_max_keys (10)" in str(got),
          "ten SETs, then the eleventh answers TXABORTED (%s)" % str(got)[:60])
    ended_only(c, "tx_max_keys", "SETs")
    check(client().get("k0") is None, "nothing of it was made")

    c.execute_command("BEGINTX")
    got = answer(c, "MSET", *[x for i in range(20) for x in ("m%d" % i, "v")])
    check("tx_max_keys" in str(got), "one MSET of twenty answers it (%s)" % str(got)[:50])
    check(answer(c, "ABORTTX") in (True, "OK"), "ABORTTX ends it")
    check(client().get("m0") is None, "and nothing of it was made")

    other = client()
    for i in range(20):
        other.set("r%d" % i, "v")
    c.execute_command("BEGINTX")
    reads = [answer(c, "GET", "r%d" % i) for i in range(11)]
    # a GET writes its value straight to the reply, so the eleventh answers; the next says
    check(reads == ["v"] * 11, "eleven GETs answer")
    ended_only(c, "tx_max_keys", "GETs: reads count too")
    check(stats()["tx_over_limit"] - before.get("tx_over_limit", 0) == 3, "tx_over_limit counts the three")

    print("a script's barch.store")
    check(answer(c, "SETF", "fill", "function call(n) for i = 1, tonumber(n) do barch.store.set('f' .. i, 'v') end"
                 " return 'done' end\n") == "OK", "SETF fill")
    c.execute_command("BEGINTX")
    got = answer(c, "CALLF", "fill", "20")
    check("tx_max_keys" in str(got) and "TXABORTED" in str(got), "barch.store.set past it fails the script (%s)"
          % str(got)[:70])
    ended_only(c, "tx_max_keys", "a script")
    check(client().get("f1") is None, "and nothing of it was made")
    config("tx_max_keys", 100000)

    print("tx_max_bytes")
    config("tx_max_bytes", 10000)
    c.execute_command("BEGINTX")
    small = answer(c, "SET", "b1", "x" * 1000)
    got = answer(c, "SET", "b2", "x" * 20000)
    check(small in (True, "OK") and "tx_max_bytes (10000)" in str(got), "a 20000 byte value answers it (%s)"
          % str(got)[:60])
    ended_only(c, "tx_max_bytes", "bytes")
    check(client().get("b1") is None, "nothing of it was made")
    config("tx_max_bytes", 67108864)

    print("tx_max_age_ms")
    config("tx_max_age_ms", 500)
    before = stats()
    c.execute_command("BEGINTX")
    check(c.get("r1") == "v", "a read in it")
    for i in range(5):
        other.set("r%d" % i, "changed")
    layers = stats()["tx_layers"]
    deadline = time.time() + 10
    while time.time() < deadline and stats()["tx_layers"] > 0:
        time.sleep(0.1)
    after = stats()
    check(layers >= 1 and after["tx_layers"] == 0 and after["tx_expired"] - before.get("tx_expired", 0) == 1,
          "left idle, its layers go (%d, then %d) and tx_expired counts it" % (layers, after["tx_layers"]))
    got = answer(c, "GET", "r1")
    check("TXABORTED" in str(got) and "tx_max_age_ms (500" in str(got), "its next command says so (%s)"
          % str(got)[:60])
    got = answer(c, "COMMITTX")
    check("TXABORTED" in str(got) and "tx_max_age_ms" in str(got), "and COMMITTX (%s)" % str(got)[:60])
    c.execute_command("BEGINTX")
    c.execute_command("SET", "young", "x")
    check(answer(c, "COMMITTX") in (True, "OK") and client().get("young") == "x", "one ended in time commits")
    config("tx_max_age_ms", 60000)

    print("no limits")
    for n in ("tx_max_keys", "tx_max_bytes", "tx_max_age_ms"):
        config(n, 0)
    c.execute_command("BEGINTX")
    c.execute_command("MSET", *[x for i in range(200) for x in ("n%d" % i, "v" * 100)])
    check(answer(c, "COMMITTX") in (True, "OK") and client().get("n199") == "v" * 100, "200 keys commit with 0")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
