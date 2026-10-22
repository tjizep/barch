#!/usr/bin/env python3
"""
A key lock waited for without holding up its I/O thread - TODO 721.

Connections share the server's I/O threads, round robin as they're accepted. A
pessimistic transaction (TODO 719) that waits for a lock used to wait on its thread,
so when the lock's holder was on the same thread its COMMITTX couldn't run until the
wait timed out. Now the waiting session parks, as BLPOP does, and is woken when the
lock goes.

Checked against one barchd with no Raft, tx_lock_wait_ms 1500, with the holder and the
waiter on the same I/O thread (shown first, with a plain SET inside EXEC, which still
waits on its thread):
  - the waiter parks; the holder's PING and COMMITTX are answered at once, and the
    waiter goes on with the holder's write;
  - a parked one with nobody letting go times out with TXLOCKTIMEOUT, while the
    holder's thread stays free;
  - a plain SET parks the same way, and goes on at the commit;
  - commands pipelined behind a parked one are answered after it, in order;
  - a parked connection closed: the next waiter still gets the lock, nothing is left
    listed and no client is counted as blocked;
  - INFO transactions waiting lists only the waiting ones (TODO 723);
  - one woken on a key that parks again on another still wakes the next on the first
    (TODO 728).
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
PORT = scale.port(default=14814)
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


WAIT_MS = 1500
DATA = os.path.join(os.getcwd(), "txlockpark_data")
os.makedirs(DATA, exist_ok=True)
LOG = os.path.join(os.getcwd(), "txlockpark.log")
log = open(LOG, "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000", "-c", "tx_lock_wait_ms=%d" % WAIT_MS],
                     stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "txpark_" + str(int(time.time()))


def pool_size():
    deadline = time.time() + 10
    while time.time() < deadline:
        log.flush()
        m = re.search(r"resp pool size\D+(\d+)", open(LOG).read())
        if m:
            return int(m.group(1))
        time.sleep(0.1)
    return None


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)
    c.response_callbacks.pop("INFO", None)
    c.execute_command("USE", SPACE)
    return c


def run(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERROR " + str(e)


def later(c, out, name, *cmd):
    def go():
        t0 = time.time()
        out[name] = run(c, *cmd)
        out[name + "_s"] = time.time() - t0
    t = threading.Thread(target=go)
    t.start()
    return t


def timed(c, *cmd):
    t0 = time.time()
    r = run(c, *cmd)
    return r, time.time() - t0


def send(c, *cmds):
    """commands written in one go on `c`'s own connection, not read: redis-py's pipeline()
    takes another connection from the pool"""
    c.connection.send_packed_command(c.connection.pack_commands([list(cmd) for cmd in cmds]))


def answers(c, n):
    out = []
    for _ in range(n):
        try:
            out.append(c.connection.read_response())
        except redis.ResponseError as e:
            out.append("ERROR " + str(e))
    return out


def same_thread_pair(n):
    """a holder and a waiter `n` connections apart: the same I/O thread, round robin"""
    h = client()
    between = [client() for _ in range(n - 1)]
    w = client()
    return h, w, between


def listed(watch):
    return [line for line in watch.execute_command("INFO", "transactions").splitlines()
            if line.startswith("tx") and line[2:3].isdigit()]


def blocked_clients(watch):
    m = re.search(r"blocked_clients:(\d+)", watch.execute_command("INFO", "clients"))
    return int(m.group(1)) if m else None


try:
    n = pool_size()
    check(n is not None and n > 0, "the I/O pool's size, from the log (%s)" % n)
    h, w, between = same_thread_pair(n)
    watch = client()

    print("the holder and the waiter share a thread")
    # a plain write inside EXEC still waits on its thread, so it holds up the holder too
    run(h, "BEGINTX", "PESSIMISTIC")
    run(h, "SET", "probe", "h")
    out = {}

    def run_exec():
        t0 = time.time()
        send(w, ("MULTI",), ("PING",), ("SET", "probe", "w"), ("EXEC",))
        out["exec"] = answers(w, 4)
        out["exec_s"] = time.time() - t0
    t = threading.Thread(target=run_exec)
    t.start()
    time.sleep(0.3)
    _, ping_s = timed(h, "PING")
    t.join()
    run(h, "ABORTTX")
    check(ping_s >= 0.8, "the holder's PING waited for the waiter's EXEC (%.2f s)" % ping_s)

    print("parked, then woken by the holder's commit")
    run(w, "SET", "k", "0")
    check(run(h, "BEGINTX", "PESSIMISTIC") == "OK", "the holder begins")
    check(run(h, "SET", "k", "1") is True, "and writes k")
    check(run(w, "BEGINTX", "PESSIMISTIC") == "OK", "the waiter begins")
    out = {}
    t = later(w, out, "get", "GET", "k")
    time.sleep(0.3)
    check("get" not in out, "the waiter waits")
    rows = [r for r in listed(watch) if "waiting=a lock on k," in r]
    check(len(rows) == 1 and "state=waiting" in rows[0] and "command=get," in rows[0].lower(),
          "listed as waiting, with its command (%s)" % (rows[0][rows[0].find("state="):] if rows else rows))
    # INFO transactions waiting - TODO 723: only the waiting one
    waiting = [line for line in watch.execute_command("INFO", "transactions", "waiting").splitlines()
               if line.startswith("tx") and line[2:3].isdigit()]
    check(len(listed(watch)) == 2, "both listed in INFO transactions (%d)" % len(listed(watch)))
    check(len(waiting) == 1 and waiting[0].startswith("tx0:") and "waiting=a lock on k," in waiting[0],
          "only the waiter in INFO transactions waiting (%d)" % len(waiting))
    check("tx_live:2" in watch.execute_command("INFO", "transactions", "WAITING"),
          "with the counters, any case")
    check(str(run(watch, "INFO", "transactions", "idle")).startswith("ERROR INFO transactions takes only WAITING"),
          "another word refused")
    pong, ping_s = timed(h, "PING")
    check(ping_s < 0.3, "the holder's PING isn't held up (%.3f s)" % ping_s)
    ok, commit_s = timed(h, "COMMITTX")
    check(ok == "OK" and commit_s < 0.3, "nor its COMMITTX (%s, %.3f s)" % (ok, commit_s))
    t.join()
    check(out.get("get") == "1", "the waiter reads the holder's write (%s)" % out.get("get"))
    check(out.get("get_s", 9) < 1.2, "as soon as it committed (%.2f s)" % out.get("get_s", 9))
    check(run(w, "COMMITTX") == "OK", "the waiter commits")

    print("parked with nobody letting go")
    run(h, "BEGINTX", "PESSIMISTIC")
    run(h, "SET", "k", "2")
    run(w, "BEGINTX", "PESSIMISTIC")
    out = {}
    t = later(w, out, "get", "GET", "k")
    time.sleep(0.3)
    pings = [timed(h, "PING")[1] for _ in range(5)]
    check(max(pings) < 0.3, "the holder is answered all along (%.3f s at most)" % max(pings))
    t.join()
    got = str(out.get("get"))
    check(got.startswith("ERROR TXLOCKTIMEOUT k is locked by transaction"), "TXLOCKTIMEOUT (%s)" % got[:70])
    check(WAIT_MS / 1000.0 - 0.1 <= out.get("get_s", 0) < WAIT_MS / 1000.0 + 1,
          "after tx_lock_wait_ms (%.2f s)" % out.get("get_s", 0))
    check(str(run(w, "GET", "x")).startswith("ERROR TXLOCKTIMEOUT"), "then it can only end")
    run(w, "ABORTTX")
    check(run(h, "COMMITTX") == "OK", "the holder commits")

    print("a plain SET")
    run(h, "BEGINTX", "PESSIMISTIC")
    run(h, "GET", "k")
    out = {}
    t = later(w, out, "set", "SET", "k", "plain")
    time.sleep(0.3)
    check("set" not in out, "parks on a key a transaction read")
    _, ping_s = timed(h, "PING")
    check(ping_s < 0.3, "the holder isn't held up (%.3f s)" % ping_s)
    check(run(h, "COMMITTX") == "OK", "the holder commits")
    t.join()
    check(out.get("set") is True and out.get("set_s", 9) < 1.2,
          "the SET goes on (%s, %.2f s)" % (out.get("set"), out.get("set_s", 9)))
    check(run(h, "GET", "k") == "plain", "and is made")

    print("pipelined behind a parked one")
    run(h, "BEGINTX", "PESSIMISTIC")
    run(h, "SET", "k", "3")
    run(w, "BEGINTX", "PESSIMISTIC")
    send(w, ("GET", "k"), ("SET", "q", "1"), ("GET", "q"))
    out = {}

    def run_pipe():
        out["pipe"] = answers(w, 3)
    t = threading.Thread(target=run_pipe)
    t.start()
    time.sleep(0.3)
    check("pipe" not in out, "nothing answered while the first is parked")
    check(run(h, "COMMITTX") == "OK", "the holder commits")
    t.join()
    check(out.get("pipe") == ["3", "OK", "1"], "each answered, in order (%s)" % out.get("pipe"))
    check(run(w, "COMMITTX") == "OK", "and the waiter commits")

    print("woken, then parked on another key")
    # W1 waits for x, gets it when x goes, and parks again on y; W2, behind it on x,
    # must be woken too: x is free for it then - TODO 728
    g, w1, w2 = client(), client(), client()
    run(h, "BEGINTX", "PESSIMISTIC")
    run(h, "SET", "x", "hx")
    run(g, "BEGINTX", "PESSIMISTIC")
    run(g, "SET", "y", "gy")
    run(w1, "BEGINTX", "PESSIMISTIC")
    run(w2, "BEGINTX", "PESSIMISTIC")
    out = {}
    done_at = {}

    def ask(c, name, *cmd):
        out[name] = run(c, *cmd)
        done_at[name] = time.time()
    t1 = threading.Thread(target=ask, args=(w1, "w1", "MGET", "x", "y"))
    t1.start()
    time.sleep(0.2)
    t2 = threading.Thread(target=ask, args=(w2, "w2", "GET", "x"))
    t2.start()
    time.sleep(0.2)
    check("w1" not in out and "w2" not in out, "both wait for x")
    committed = time.time()
    check(run(h, "COMMITTX") == "OK", "x's holder commits")
    t2.join(timeout=WAIT_MS / 1000.0 + 2)
    check(out.get("w2") == "hx" and done_at.get("w2", 99) - committed < 0.8,
          "the second goes on at once (%s, %.2f s)" % (out.get("w2"), done_at.get("w2", 99) - committed))
    check("w1" not in out, "the first still waits, now for y")
    check(run(g, "COMMITTX") == "OK", "y's holder commits")
    t1.join()
    check(out.get("w1") == ["hx", "gy"], "and the first goes on (%s)" % out.get("w1"))
    run(w1, "COMMITTX")
    run(w2, "COMMITTX")

    print("a parked connection closed")
    other = client()
    run(h, "BEGINTX", "PESSIMISTIC")
    run(h, "SET", "k", "4")
    run(w, "BEGINTX", "PESSIMISTIC")
    out = {}
    send(w, ("GET", "k"))
    time.sleep(0.2)
    run(other, "BEGINTX", "PESSIMISTIC")
    t2 = later(other, out, "other", "GET", "k")
    time.sleep(0.2)
    w.connection.disconnect()
    time.sleep(0.2)
    check(run(h, "COMMITTX") == "OK", "the holder commits")
    t2.join()
    check(out.get("other") == "4" and out.get("other_s", 9) < 1.2,
          "the next waiter gets the lock (%s, %.2f s)" % (out.get("other"), out.get("other_s", 9)))
    check(run(other, "COMMITTX") == "OK", "and commits")
    deadline = time.time() + 5
    while (listed(h) or blocked_clients(h)) and time.time() < deadline:
        time.sleep(0.05)
    check(listed(h) == [], "nothing listed")
    check(blocked_clients(h) == 0, "no client counted as blocked (%s)" % blocked_clients(h))
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
