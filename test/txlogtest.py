#!/usr/bin/env python3
"""
A transaction in a space with no Raft, against its change log - TODO 450.

TestTxLocal checks the transaction itself. This checks the log under it: what a replay
makes of a transaction's tagged writes and its commit marker when the log is cut, torn,
synced late, saved over or full. And two read anomalies a key value store can get wrong.
One barchd, a four-shard space with its change log on; no interval saves, so the log is
the only way back unless the test saves.

  - replay ordering: one key set, set again and deleted over three transactions, with
    plain writes in between, comes back exactly as it was;
  - buffered sync: under aof_durability=timer, 100 transactions answered OK and then
    kill -9: all 100 come back. A kill -9 keeps what the kernel holds, so this shows
    each was written before its OK, not that the marker's fsync happened (only a power
    loss would tell those apart);
  - a torn commit marker: the transaction comes back as none of it, and the one before
    it is whole;
  - a torn write in the middle of a transaction: none of it either;
  - a crash during saves: transactions committing in a loop while SAVE runs over and
    over, kill -9 at three points: every key from one transaction, the last one
    answered OK or the one after it;
  - a stale read: a transaction reads a key, another commits a new value, the first
    reads it again and sees what it read before, and its COMMITTX answers TXCONFLICT;
  - write skew: T1 reads A and sets B if A is 0, T2 reads B and sets A if B is 0, both
    reading before either commits: one commits, the other answers TXCONFLICT;
  - range reads (SCAN, KEYS, RANGE) are refused inside a transaction, so there's no
    phantom to see;
  - out of disk (RLIMIT_FSIZE on the change log): a transaction the log can't hold is
    refused before any of it is made - in memory and after a restart.
"""
import os
import random
import resource
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
PORT = scale.port(default=14660)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "txlog_data")
LOGS = os.path.join(os.getcwd(), "txlog_logs")
SPACE = "ledger"
KEYS = ["k%d" % i for i in range(10)]
QUIET = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000",
         "-c", "aof_dir=" + LOGS]
EACH = QUIET + ["-c", "aof_durability=each"]
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def start(args=EACH, file_limit=None):
    def limit():
        if file_limit is not None:
            signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
            resource.setrlimit(resource.RLIMIT_FSIZE, (file_limit, file_limit))

    log = open(os.path.join(os.getcwd(), "txlog.log"), "a")
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA,
                          "--no-save-on-exit"] + args, stdout=log, stderr=log, preexec_fn=limit)
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
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=120, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", SPACE)
    return c


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def transaction(c, writes):
    """BEGINTX, each (command, args...), COMMITTX's answer"""
    c.execute_command("BEGINTX")
    for w in writes:
        c.execute_command(*w)
    return answer(c, "COMMITTX")


def values(keys=KEYS):
    return client().execute_command("MGET", *keys)


def tear(value):
    """flip a byte inside the last place `value` is in the change log"""
    for root, _, files in os.walk(LOGS):
        for name in files:
            path = os.path.join(root, name)
            with open(path, "rb") as f:
                data = bytearray(f.read())
            at = data.rfind(value)
            if at < 0:
                continue
            data[at + len(value) // 2] ^= 0xFF
            with open(path, "wb") as f:
                f.write(data)
            return True
    return False


for d in (DATA, LOGS):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
p = start()
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60)
    r.execute_command("configuration:SET", SPACE + ".shards", "4")
    r.execute_command("configuration:SET", SPACE + ".aof", "on")
    r.execute_command("configuration:SAVE")

    print("replay ordering")
    c = client()
    got = [transaction(c, [("SET", "k1", "v1")]),
           c.execute_command("SET", "k2", "plain1"),
           transaction(c, [("SET", "k1", "v2"), ("SET", "k2", "t2")]),
           c.execute_command("SET", "k3", "x"),
           transaction(c, [("DEL", "k1"), ("SET", "k4", "a")]),
           c.execute_command("SET", "k4", "b")]
    before = values(["k1", "k2", "k3", "k4"])
    kill(p)
    p = start()
    after = values(["k1", "k2", "k3", "k4"])
    check(before == [None, "t2", "x", "b"] and after == before,
          "a restart holds exactly what was there (%s, %s)" % (before, after))
    kill(p)

    print("buffered sync: 100 transactions under aof_durability=timer, then kill -9")
    p = start(QUIET + ["-c", "aof_durability=timer"])
    c = client()
    oks = sum(1 for i in range(100) if transaction(c, [("SET", "s%d" % i, str(i)), ("INCR", "count")]) == "OK")
    kill(p)
    p = start()
    back = values(["s%d" % i for i in range(100)])
    check(oks == 100 and back == [str(i) for i in range(100)] and client().execute_command("GET", "count") == "100",
          "all 100 come back (%d answered OK, %d back, count %s)"
          % (oks, sum(1 for v in back if v is not None), client().execute_command("GET", "count")))

    print("a torn commit marker")
    c = client()
    check(transaction(c, [("SET", k, "whole") for k in KEYS]) == "OK", "a transaction that stays")
    check(transaction(c, [("SET", k, "marker-torn") for k in KEYS]) == "OK", "and one whose marker gets torn")
    kill(p)
    check(tear(b"\x01tx-commit"), "the last commit marker is torn")
    p = start()
    check(values() == ["whole"] * 10, "none of the torn one, all of the one before (%s)" % values()[:3])

    print("a torn write in the middle of a transaction")
    c = client()
    check(transaction(c, [("SET", k, "mid-%s" % k) for k in KEYS]) == "OK", "a transaction")
    kill(p)
    check(tear(b"mid-k5"), "its sixth write is torn")
    p = start()
    check(values() == ["whole"] * 10, "none of it comes back (%s)" % values()[:6])
    kill(p)

    print("a crash during saves")
    for round_ in range(3):
        p = start()
        stop = threading.Event()
        last_ok = [None]

        def committer():
            cc = client()
            i = 0
            while not stop.is_set():
                i += 1
                try:
                    if transaction(cc, [("SET", k, "r%d_%d" % (round_, i)) for k in KEYS]) == "OK":
                        last_ok[0] = i
                except redis.RedisError:
                    return

        def saver():
            sc = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60)
            while not stop.is_set():
                try:
                    sc.execute_command(SPACE + ":SAVE")
                except redis.RedisError:
                    return

        ts = [threading.Thread(target=committer, daemon=True), threading.Thread(target=saver, daemon=True)]
        [t.start() for t in ts]
        time.sleep(1.0 + random.random())
        seen_ok = last_ok[0]
        kill(p)
        stop.set()
        [t.join(10) for t in ts]
        p = start()
        got = values()
        n = int(got[0].split("_")[1]) if got[0] and got[0].startswith("r%d_" % round_) else None
        check(len(set(got)) == 1 and n is not None and seen_ok is not None and n >= seen_ok,
              "round %d: every key from one transaction, at or past the last OK (%s, last OK %s)"
              % (round_ + 1, got[0], seen_ok))
        kill(p)

    p = start()
    print("a stale read")
    t1, t2 = client(), client()
    t2.execute_command("SET", "a", "old")
    t1.execute_command("BEGINTX")
    first = t1.execute_command("GET", "a")
    check(transaction(t2, [("SET", "a", "new")]) == "OK", "another transaction commits a new value")
    again = t1.execute_command("GET", "a")
    t1.execute_command("SET", "b", "from-t1")
    got = answer(t1, "COMMITTX")
    check(first == "old" and again == "old", "the first reads what it read before (%s, %s)" % (first, again))
    check("TXCONFLICT" in str(got) and client().execute_command("GET", "b") is None,
          "and its COMMITTX answers TXCONFLICT, making nothing (%s)" % str(got)[:50])

    print("write skew")
    c = client()
    c.execute_command("MSET", "A", "0", "B", "0")
    t1, t2 = client(), client()
    t1.execute_command("BEGINTX")
    t2.execute_command("BEGINTX")
    if t1.execute_command("GET", "A") == "0":
        t1.execute_command("SET", "B", "1")
    if t2.execute_command("GET", "B") == "0":
        t2.execute_command("SET", "A", "1")
    got = [answer(t1, "COMMITTX"), answer(t2, "COMMITTX")]
    ab = c.execute_command("MGET", "A", "B")
    check(got.count("OK") == 1 and "TXCONFLICT" in str(got) and ab.count("1") == 1,
          "one commits, the other answers TXCONFLICT (%s, A B = %s)" % ([str(g)[:30] for g in got], ab))

    print("range reads in a transaction")
    # taken since TODO 701, and a key added to a range walked is a phantom COMMITTX sees
    c, d = client(), client()
    for walk in (("SCAN", "0"), ("KEYS", "*"), ("RANGE", "a", "z", "10")):
        c.execute_command("BEGINTX")
        answer(c, *walk)
        d.execute_command("SET", "phantom", walk[0])
        c.execute_command("SET", "after_" + walk[0], "1")
        got = answer(c, "COMMITTX")
        d.execute_command("DEL", "phantom")
        check("TXCONFLICT" in str(got), "%s, then a key added to what it walked: TXCONFLICT (%s)"
              % (walk[0], str(got)[:40]))
    for k in KEYS:
        c.execute_command("SET", k, "kept")
    kill(p)

    print("out of disk")
    limit = 1 << 20
    p = start(file_limit=limit)
    c = client()
    big = "b" * 4096
    i = 0
    while i < 10 * limit // len(big):
        try:
            c.execute_command("SET", "fill%06d" % i, big)
        except redis.ResponseError:
            break
        i += 1
    # small writes first, which would fit, then ones the log can't take
    c.execute_command("BEGINTX")
    for k in KEYS[:3]:
        c.execute_command("SET", k, "tiny")
    for k in KEYS[3:]:
        c.execute_command("SET", k, big)
    got = answer(c, "COMMITTX")
    check(str(got).startswith("ERR"), "COMMITTX is refused (%s)" % str(got)[:70])
    check(values() == ["kept"] * 10, "and nothing of it is in memory (%s)" % [str(v)[:6] for v in values()])
    kill(p)
    p = start()
    check(values() == ["kept"] * 10, "or after a restart (%s)" % [str(v)[:6] for v in values()])
finally:
    try:
        kill(p)
    except Exception:       # noqa: BLE001 - already gone
        pass

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
