#!/usr/bin/env python3
"""
Pessimistic transactions, BEGINTX PESSIMISTIC - TODO 719.

A pessimistic transaction locks each key as it uses it, shared for a read and exclusive
for a write, and holds the locks until it ends. A lock someone else holds is waited for
up to tx_lock_wait_ms, then the command fails and the transaction can only be ended.

Checked against one barchd with no Raft, tx_lock_wait_ms 1500:
  - BEGINTX takes PESSIMISTIC and nothing else;
  - two pessimistic INCRs of one key: the second waits for the first to commit, then
    both commit, and neither increment is lost;
  - INFO transactions shows the waiting one, on the holder's id;
  - shared locks don't hold each other up;
  - a lock not got in time: TXLOCKTIMEOUT naming the holder, and the transaction can
    only end;
  - two transactions waiting on each other: one answers TXDEADLOCK, the other goes on;
  - a plain SET waits for a key a transaction holds, and goes on when it ends, or times
    out with TXLOCKTIMEOUT;
  - a stored function's barch.store write to a held key is refused at the shard, TXLOCKED;
  - an optimistic transaction writing a key a pessimistic one holds gets TXCONFLICT;
  - closing the connection lets every lock go;
  - RANGE, KEYS, COUNT and SCAN are taken in a pessimistic transaction, their ranges
    locked (TODO 749, test/txrangelocktest.py has the rest), and so is a stored
    function (TODO 732, test/txluautest.py has the rest);
  - many clients incrementing one key in pessimistic transactions: every one commits
    and the count is right.
"""
import os
import socket
import subprocess
import sys
import threading
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14813)
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
DATA = os.path.join(os.getcwd(), "txpessimistic_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txpessimistic.log"), "a")
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

SPACE = "txpess_" + str(int(time.time()))


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)
    c.response_callbacks.pop("INFO", None)
    c.execute_command("USE", SPACE)
    return c


def run(c, *cmd):
    """the answer, or the error's text"""
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERROR " + str(e)


def later(c, out, name, *cmd):
    """`cmd` on its own thread, its answer and how long it took in out[name]"""
    def go():
        t0 = time.time()
        out[name] = run(c, *cmd)
        out[name + "_s"] = time.time() - t0
    t = threading.Thread(target=go)
    t.start()
    return t


def listed(watch):
    rows = []
    for line in watch.execute_command("INFO", "transactions").splitlines():
        if line.startswith("tx") and line[2:3].isdigit() and ":" in line:
            row = {}
            for part in line.split(":", 1)[1].split(","):
                k, _, v = part.partition("=")
                row[k] = v
            rows.append(row)
    return rows


def tx_id(watch, before):
    """the id of the one transaction listed now that wasn't in `before`"""
    new = [r["id"] for r in listed(watch) if r["id"] not in before]
    return new[0] if len(new) == 1 else None


def ids(watch):
    return {r["id"] for r in listed(watch)}


try:
    watch = client()

    print("the option")
    a = client()
    check(run(a, "BEGINTX", "PESSIMISTIC") == "OK", "BEGINTX PESSIMISTIC")
    check(run(a, "ABORTTX") == "OK", "ABORTTX")
    check(run(a, "BEGINTX", "pessimistic") == "OK", "in any case")
    check(run(a, "ABORTTX") == "OK", "ABORTTX")
    check(run(a, "BEGINTX", "OPTIMISTIC").startswith("ERROR"), "nothing else")

    print("two INCRs of one key")
    b = client()
    run(watch, "SET", "n", "0")
    seen = ids(watch)
    check(run(a, "BEGINTX", "PESSIMISTIC") == "OK", "the first begins")
    aid = tx_id(watch, seen)
    check(run(a, "INCR", "n") == 1, "the first increments")
    check(run(b, "BEGINTX", "PESSIMISTIC") == "OK", "the second begins")
    bid = tx_id(watch, seen | {aid})
    out = {}
    t = later(b, out, "incr", "INCR", "n")
    time.sleep(0.4)
    check("incr" not in out, "the second waits for the lock")
    rows = {r["id"]: r for r in listed(watch)}
    br = rows.get(bid, {})
    check(br.get("state") == "waiting" and br.get("waiting") == "a lock on n",
          "listed waiting on a lock on n (%s, %s)" % (br.get("state"), br.get("waiting")))
    check(br.get("waiting_on") == aid, "on the first one's id (%s)" % br.get("waiting_on"))
    ar = rows.get(aid, {})
    check(ar.get("held") == "n" and ar.get("holds") == "1",
          "the first holds n (%s, %s)" % (ar.get("held"), ar.get("holds")))
    check(run(a, "COMMITTX") == "OK", "the first commits")
    t.join()
    check(out.get("incr") == 2, "the second goes on, and reads the first's write (%s)" % out.get("incr"))
    check(run(b, "COMMITTX") == "OK", "the second commits, no TXCONFLICT")
    check(run(watch, "GET", "n") == "2", "no increment lost (%s)" % run(watch, "GET", "n"))

    print("shared locks")
    run(watch, "SET", "s", "v")
    run(a, "BEGINTX", "PESSIMISTIC")
    run(b, "BEGINTX", "PESSIMISTIC")
    check(run(a, "GET", "s") == "v", "the first reads")
    t0 = time.time()
    check(run(b, "GET", "s") == "v" and time.time() - t0 < 0.5, "the second reads too, without waiting")
    check(run(a, "COMMITTX") == "OK" and run(b, "COMMITTX") == "OK", "both commit")

    print("a lock not got in time")
    run(a, "BEGINTX", "PESSIMISTIC")
    run(a, "SET", "t", "1")
    aid = [r["id"] for r in listed(watch)][0]
    run(b, "BEGINTX", "PESSIMISTIC")
    t0 = time.time()
    got = run(b, "GET", "t")
    waited = time.time() - t0
    check(got.startswith("ERROR TXLOCKTIMEOUT") and aid in got, "TXLOCKTIMEOUT, naming the holder (%s)" % got)
    check(WAIT_MS / 1000.0 - 0.1 <= waited < WAIT_MS / 1000.0 + 2, "after tx_lock_wait_ms (%.2f s)" % waited)
    got = run(b, "GET", "s")
    check(got.startswith("ERROR TXLOCKTIMEOUT"), "then it can only end (%s)" % got)
    check(run(b, "COMMITTX").startswith("ERROR"), "COMMITTX ends it, refused")
    check(run(a, "COMMITTX") == "OK", "the holder commits")
    check(run(watch, "GET", "t") == "1", "its write is there")

    print("two waiting on each other")
    run(a, "BEGINTX", "PESSIMISTIC")
    run(b, "BEGINTX", "PESSIMISTIC")
    run(a, "SET", "x", "a")
    run(b, "SET", "y", "b")
    out = {}
    ta = later(a, out, "a", "SET", "y", "a")
    tb = later(b, out, "b", "SET", "x", "b")
    ta.join()
    tb.join()
    # one gives way with TXDEADLOCK at once, and the other goes on - TODO 724
    got = sorted([str(out.get("a")), str(out.get("b"))])
    check(got[0].startswith("ERROR TXDEADLOCK") and got[1] == "True",
          "one answers TXDEADLOCK, the other goes on (%s | %s)" % (out.get("a"), out.get("b")))
    check(max(out.get("a_s", 9), out.get("b_s", 9)) < 1.0, "without waiting out tx_lock_wait_ms")
    run(a, "ABORTTX")
    run(b, "ABORTTX")
    check(listed(watch) == [], "nothing listed after")

    print("a plain write")
    plain = client()
    run(a, "BEGINTX", "PESSIMISTIC")
    run(a, "GET", "w")
    out = {}
    t = later(plain, out, "set", "SET", "w", "plain")
    time.sleep(0.4)
    check("set" not in out, "a plain SET waits for a key read in a transaction")
    check(run(a, "COMMITTX") == "OK", "the transaction commits")
    t.join()
    check(out.get("set") is True or out.get("set") == "OK", "the SET goes on (%s)" % out.get("set"))
    check(out.get("set_s", 0) >= 0.35, "after the transaction ended (%.2f s)" % out.get("set_s", 0))
    run(a, "BEGINTX", "PESSIMISTIC")
    run(a, "SET", "w", "tx")
    t0 = time.time()
    got = run(plain, "SET", "w", "plain2")
    waited = time.time() - t0
    check(str(got).startswith("ERROR TXLOCKTIMEOUT"), "one waiting too long: TXLOCKTIMEOUT (%s)" % got)
    check(waited >= WAIT_MS / 1000.0 - 0.1, "after tx_lock_wait_ms (%.2f s)" % waited)
    got = run(plain, "DEL", "w")
    check(str(got).startswith("ERROR TXLOCKTIMEOUT"), "DEL too (%s)" % got)
    check(run(a, "COMMITTX") == "OK", "the transaction commits")
    check(run(watch, "GET", "w") == "tx", "its write stands (%s)" % run(watch, "GET", "w"))
    check(run(plain, "SET", "other", "1") is True, "a key nobody holds isn't held up")

    print("a stored function's write")
    run(watch, "SETF", "stamp", "function call() barch.store.set('f', 'fn') return 1 end")
    run(a, "BEGINTX", "PESSIMISTIC")
    run(a, "GET", "f")
    got = run(plain, "CALLF", "stamp")
    check(str(got).startswith("ERROR") and "TXLOCKED" in got and "locked by transaction" in got,
          "refused at the shard, a key a transaction holds (%s)" % got)
    check(run(a, "GET", "f") is None, "the transaction still reads nothing there")
    check(run(a, "COMMITTX") == "OK", "the transaction commits")
    check(run(plain, "CALLF", "stamp") == 1 and run(watch, "GET", "f") == "fn", "then the function writes")

    print("an optimistic transaction against a held key")
    o = client()
    run(a, "BEGINTX", "PESSIMISTIC")
    run(a, "GET", "w")
    run(o, "BEGINTX")
    run(o, "SET", "w", "opt")
    got = run(o, "COMMITTX")
    check(str(got).startswith("ERROR TXCONFLICT") and "locked" in got, "TXCONFLICT (%s)" % got)
    check(run(a, "GET", "w") == "tx", "the pessimistic one reads what it read")
    check(run(a, "COMMITTX") == "OK", "and commits")

    print("a closed connection")
    c = client()
    run(c, "BEGINTX", "PESSIMISTIC")
    run(c, "SET", "c", "1")
    run(c, "GET", "c2")
    c.close()
    deadline = time.time() + 10
    while listed(watch) and time.time() < deadline:
        time.sleep(0.05)
    t0 = time.time()
    check(run(plain, "SET", "c", "2") is True and run(plain, "SET", "c2", "2") is True
          and time.time() - t0 < 0.5, "its locks are let go")
    check(run(watch, "GET", "c") == "2", "and its write was never made")

    print("walks in one (TODO 749, test/txrangelocktest.py has the rest)")
    run(a, "BEGINTX", "PESSIMISTIC")
    for cmd in (["KEYS", "*"], ["RANGE", "a", "z", "10"], ["COUNT", "a", "z"], ["SCAN", "0"]):
        got = run(a, *cmd)
        check(not str(got).startswith("ERROR"), "%s is taken (%s)" % (cmd[0], str(got)[:60]))
    got = run(a, "CALLF", "nosuch")
    check("no such function" in str(got) and "pessimistic" not in got,
          "but a stored function is taken (%s)" % got[:60])
    run(a, "ABORTTX")

    print("many clients, one key")
    run(watch, "SET", "count", "0")
    CLIENTS, EACH = 4, 25
    results = {"ok": 0, "other": []}
    lock = threading.Lock()

    def worker():
        w = client()
        for _ in range(EACH):
            run(w, "BEGINTX", "PESSIMISTIC")
            run(w, "INCR", "count")
            got = run(w, "COMMITTX")
            with lock:
                if got == "OK":
                    results["ok"] += 1
                else:
                    results["other"].append(got)
        w.close()
    ts = [threading.Thread(target=worker) for _ in range(CLIENTS)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    check(results["ok"] == CLIENTS * EACH, "every one commits (%d, %s)" % (results["ok"], results["other"][:2]))
    check(run(watch, "GET", "count") == str(CLIENTS * EACH), "the count is right (%s)" % run(watch, "GET", "count"))
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
