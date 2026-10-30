#!/usr/bin/env python3
"""
SETINTX, a test command - TODO 778.

SETINTX key value [PESSIMISTIC] is BEGINTX, SET and COMMITTX in one call, so a benchmark
can put a SET in a transaction against a plain SET with the same round trips and parsing.
It's only there with BARCH_TEST_SETINTX=1 in barchd's environment. Its SET has to run the
way a client's does inside a transaction, so this checks that:
  - without the switch it's refused;
  - it sets the key, and counts one begin and one commit, both kinds;
  - wrong arity, or anything but PESSIMISTIC after the value, is refused;
  - inside an open transaction it's refused, and the key isn't touched;
  - a SET that fails leaves no transaction open;
  - the pessimistic kind waits for a key another transaction has locked, then sets it,
    and the optimistic kind is refused with TXCONFLICT;
  - what it set is in the change log: after kill -9 a restart has it.
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
PORT = scale.port(default=19340)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "txsetintx_data")
LOGS = os.path.join(os.getcwd(), "txsetintx_logs")
SPACE = "s"
ARGS = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000",
        "-c", "aof_dir=" + LOGS, "-c", "aof_durability=timer"]
ON = {"BARCH_TEST_SETINTX": "1"}
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def start(env=None):
    log = open(os.path.join(os.getcwd(), "txsetintx.log"), "a")
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


def answer(c, *args):
    # the answer, or the error's text
    try:
        return c.execute_command(*args)
    except redis.ResponseError as e:
        return "error: " + str(e)


def stats(c):
    got = c.execute_command("INFO", "transactions")
    return {k: v for k, v in got.items() if isinstance(v, int)}


for d in (DATA, LOGS):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)

print("without BARCH_TEST_SETINTX")
p = start()
try:
    c = client()
    got = answer(c, "SETINTX", "a", "1")
    check("test command" in got, "refused (%s)" % got)
    check(c.get("a") is None, "and nothing set")
finally:
    kill(p)

p = start(ON)
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60)
    r.execute_command("configuration:SET", SPACE + ".aof", "on")
    r.execute_command("configuration:SAVE")     # across the kill below
    c = client()

    print("with it")
    before = stats(c)
    check(answer(c, "SETINTX", "a", "1") == "OK", "SETINTX a 1 is OK")
    check(c.get("a") == "1", "and a is 1")
    check(answer(c, "SETINTX", "b", "2", "PESSIMISTIC") == "OK", "SETINTX b 2 PESSIMISTIC is OK")
    check(answer(c, "SETINTX", "b", "3", "pessimistic") == "OK", "in lower case too")
    check(c.get("b") == "3", "and b is 3")
    after = stats(c)
    check(after["tx_begins"] - before["tx_begins"] == 3, "three begins counted")
    check(after["tx_commits"] - before["tx_commits"] == 3, "three commits counted")
    check(after["tx_live"] == 0, "and none left open")

    print("what it refuses")
    for args in (["SETINTX", "a"], ["SETINTX", "a", "1", "PESSIMISTIC", "x"]):
        got = answer(c, *args)
        check(got.startswith("error"), "%d arguments (%s)" % (len(args) - 1, got))
    got = answer(c, "SETINTX", "a", "9", "OPTIMISTIC")
    check(got.startswith("error") and "PESSIMISTIC" in got, "OPTIMISTIC after the value (%s)" % got)
    check(c.get("a") == "1", "and a is still 1")
    check(c.execute_command("BEGINTX") == "OK", "BEGINTX")
    got = answer(c, "SETINTX", "a", "9")
    # the dispatcher refuses it first, as it does any command a transaction doesn't take
    check(got.startswith("error") and "transaction" in got, "SETINTX inside it is refused")
    check(c.execute_command("ABORTTX") == "OK", "the open one still aborts")
    check(c.get("a") == "1", "and a is still 1")

    print("a SET that fails")
    got = answer(c, "SETINTX", "c", "1")
    check(got == "OK", "a fresh key first (%s)" % got)
    # SET's options aren't SETINTX's, so the SET that fails is one of a key SET refuses
    got = answer(c, "SETINTX", "", "1")
    check(got.startswith("error") and "empty" in got, "an empty key is SET's error (%s)" % got)
    check(stats(c)["tx_live"] == 0, "no transaction left open")
    check(answer(c, "SETINTX", "c", "2") == "OK", "the next SETINTX works")
    check(answer(c, "BEGINTX") == "OK" and answer(c, "ABORTTX") == "OK", "and BEGINTX does")

    print("a key another transaction has locked")
    other = client()
    check(other.execute_command("BEGINTX", "PESSIMISTIC") == "OK", "the other client begins, pessimistic")
    check(other.set("x", "a"), "and sets x, locking it")
    got = answer(c, "SETINTX", "x", "o")
    check(got.startswith("error") and "TXCONFLICT" in got, "optimistic SETINTX x gets TXCONFLICT (%s)" % got)
    done = {}

    def pessimistic():
        t = time.time()
        done["answer"] = answer(client(), "SETINTX", "x", "b", "PESSIMISTIC")
        done["took"] = time.time() - t

    t = threading.Thread(target=pessimistic)
    t.start()
    time.sleep(0.5)
    check("answer" not in done, "SETINTX x PESSIMISTIC waits for the lock")
    check(other.execute_command("COMMITTX") == "OK", "the other commits")
    t.join(30)
    check(done.get("answer") == "OK", "then it's OK (%s)" % done.get("answer"))
    check(done.get("took", 0) >= 0.45, "after %.2f s" % done.get("took", 0))
    check(c.get("x") == "b", "and x is b")
    check(stats(c)["tx_live"] == 0, "no transaction left open")
    check(c.execute_command("DBSIZE") >= 4, "the keys are there before the kill")
finally:
    kill(p)

print("after kill -9")
p = start(ON)
try:
    c = client()
    check(c.mget("a", "b", "c", "x") == ["1", "3", "2", "b"], "the change log had them all")
    check(answer(c, "SETINTX", "a", "after") == "OK" and c.get("a") == "after", "and SETINTX still works")
finally:
    kill(p)

print("passed" if failures == 0 else "%d FAILED" % failures)
sys.exit(1 if failures else 0)
