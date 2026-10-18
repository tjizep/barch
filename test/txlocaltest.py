#!/usr/bin/env python3
"""
Transactions in a space with no Raft - TODO 450. See docs/TRANSACTIONS.md.

The same BEGINTX / COMMITTX / ABORTTX as a replicated space, with the node as the only
member (choice 6): its writes are gathered, its reads see them first and are checked at
COMMITTX, which makes the writes under the space's read gate. In the change log each
write carries the transaction's tag and a commit marker follows; a replay applies
tagged writes only when it finds the marker.

Checked against one barchd, a space with four shards and its change log on
(aof_durability=each):
  - a transaction's writes are all there after COMMITTX, and nothing before;
  - a reader running MGET over the keys during 30 transactions never sees part of one;
  - ABORTTX, and a connection closed in a transaction, leave nothing;
  - two clients each running INCR in a transaction 50 times, again on TXCONFLICT: 100;
    and two that both read a key before either commits: the second gets TXCONFLICT;
  - kill -9 after a COMMITTX: a restart has all of it;
  - kill -9 halfway through a commit (BARCH_TEST_TX_HALF_PAUSE_MS): a restart has none
    of it, and what came before is intact;
  - a write that fails part way (BARCH_TEST_TX_FAIL_AT): COMMITTX answers an error and
    the keys are as they were, before and after a restart.
"""
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14650)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "txlocal_data")
LOGS = os.path.join(os.getcwd(), "txlocal_logs")
SPACE = "acct"
KEYS = ["k%d" % i for i in range(10)]
ARGS = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000",
        "-c", "aof_dir=" + LOGS, "-c", "aof_durability=each"]
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def start(env=None):
    log = open(os.path.join(os.getcwd(), "txlocal.log"), "a")
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA,
                          "--no-save-on-exit"] + ARGS, stdout=log, stderr=log,
                         env=dict(os.environ, **(env or {})))
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            raise AssertionError("barchd exited with %s" % p.returncode)
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % PORT)


def kill(p):
    p.send_signal(signal.SIGKILL)
    p.wait(timeout=30)


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", SPACE)
    return c


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def transaction(c, value):
    c.execute_command("BEGINTX")
    for k in KEYS:
        c.execute_command("SET", k, value)
    return answer(c, "COMMITTX")


def values():
    return client().execute_command("MGET", *KEYS)


for d in (DATA, LOGS):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
p = start()
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60)
    r.execute_command("configuration:SET", SPACE + ".shards", "4")
    r.execute_command("configuration:SET", SPACE + ".aof", "on")
    r.execute_command("configuration:SAVE")     # the settings themselves, across a kill

    print("a transaction")
    c = client()
    c.execute_command("BEGINTX")
    for k in KEYS:
        c.execute_command("SET", k, "v1")
    check(values() == [None] * 10, "nothing is there before COMMITTX")
    check(answer(c, "COMMITTX") == "OK" and values() == ["v1"] * 10, "and all of it after")

    print("a reader throughout 30 more")
    stop = threading.Event()
    seen, mixed = [], []

    def reader():
        rc = client()
        while not stop.is_set():
            got = rc.execute_command("MGET", *KEYS)
            seen.append(1)
            if len(set(got)) != 1:
                mixed.append(got)

    t = threading.Thread(target=reader)
    t.start()
    got = [transaction(c, "t%d" % i) for i in range(30)]
    stop.set()
    t.join()
    check(got == ["OK"] * 30, "each answers OK")
    check(not mixed and len(seen) > 50, "no read saw part of one (%d reads, %s)" % (len(seen), mixed[:2]))

    print("ABORTTX, and a connection closed in a transaction")
    c.execute_command("BEGINTX")
    c.execute_command("SET", "k0", "aborted")
    check(answer(c, "ABORTTX") == "OK", "ABORTTX answers OK")
    gone = client()
    gone.execute_command("BEGINTX")
    gone.execute_command("SET", "k1", "never")
    gone.close()
    time.sleep(0.2)
    check(values() == ["t29"] * 10, "and nothing of either is there")

    print("INCR from two clients")

    def incr_loop():
        ic = client()
        for _ in range(50):
            while True:
                ic.execute_command("BEGINTX")
                ic.execute_command("INCR", "counter")
                if answer(ic, "COMMITTX") == "OK":
                    break

    ts = [threading.Thread(target=incr_loop) for _ in range(2)]
    [t.start() for t in ts]
    [t.join(120) for t in ts]
    check(client().execute_command("GET", "counter") == "100", "the key ends at 100 (%s)"
          % client().execute_command("GET", "counter"))

    print("two that read the same key, then both commit")
    a, b = client(), client()
    for x in (a, b):
        x.execute_command("BEGINTX")
        x.execute_command("INCR", "counter")
    first, second = answer(a, "COMMITTX"), answer(b, "COMMITTX")
    check(first == "OK" and "TXCONFLICT" in str(second),
          "the second answers TXCONFLICT (%s, %s)" % (first, str(second)[:50]))
    check(client().execute_command("GET", "counter") == "101", "and the key is 101, not lost or doubled")

    print("kill -9 after a COMMITTX")
    check(transaction(c, "durable") == "OK", "COMMITTX answers OK")
    kill(p)
    p = start()
    check(values() == ["durable"] * 10, "a restart has all of it (%s)" % values()[:3])
    kill(p)

    print("kill -9 halfway through a commit")
    p = start({"BARCH_TEST_TX_HALF_PAUSE_MS": "3000"})
    hc = client()
    hc.execute_command("BEGINTX")
    for k in KEYS:
        hc.execute_command("SET", k, "half")
    th = threading.Thread(target=lambda: answer(hc, "COMMITTX"), daemon=True)
    th.start()
    time.sleep(1.0)
    kill(p)
    p = start()
    check(values() == ["durable"] * 10, "a restart has none of it (%s)" % values())
    check(client().execute_command("GET", "counter") == "101", "and what came before is there")
    check(transaction(client(), "after") == "OK" and values() == ["after"] * 10, "and a new one commits")
    kill(p)
    p = start()
    check(values() == ["after"] * 10, "and replays after the half one (%s)" % values()[:3])
    kill(p)

    print("a write that fails part way")
    p = start({"BARCH_TEST_TX_FAIL_AT": "5"})
    got = transaction(client(), "failed")
    check(str(got).startswith("ERR") and "nothing of it is left" in str(got),
          "COMMITTX answers an error (%s)" % str(got)[:70])
    check(values() == ["after"] * 10, "the keys are as they were (%s)" % values()[:6])
    kill(p)
    p = start()
    check(values() == ["after"] * 10, "and after a restart (%s)" % values()[:6])
finally:
    try:
        kill(p)
    except Exception:       # noqa: BLE001 - already gone
        pass

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
