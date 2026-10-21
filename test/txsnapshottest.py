#!/usr/bin/env python3
"""
A transaction reads its snapshot: before-images, an undo log - TODO 701.

The base stays current. While any transaction is open on a space, a write first saves
what its key held in the current version's layer; a transaction reads its overlay, then
the layers from its snapshot up, then the base. So it sees the store as it was when it
began, whatever is written since, and its COMMITTX still answers TXCONFLICT when what it
read has changed.

Checked against one barchd, a space without Raft:
  - after a transaction's BEGINTX, another client changes a key, deletes one and adds
    one: the transaction reads all three as they were at BEGINTX, through GET, MGET and
    EXISTS, and once it writes, its COMMITTX answers TXCONFLICT (checked against the
    layers, not a hash of each value);
  - a transaction that only reads commits, though what it read has changed;
  - an INCR in a transaction whose key another client INCRs first answers TXCONFLICT;
  - a transaction that begins after those writes reads them;
  - one key changed twice after a snapshot reads as it was before the first change;
  - INFO transactions shows a layer while a snapshot is open with writes after it, and
    none once every transaction has ended;
  - a reader running BEGINTX, GET x, GET y, ABORTTX while transactions commit
    SET x i, SET y i never sees x and y differ;
  - transactions that INCR a key (retrying on TXCONFLICT) and plain INCRs of it at
    once lose nothing.
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
PORT = scale.port(default=14760)
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


DATA = os.path.join(os.getcwd(), "txsnapshot_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txsnapshot.log"), "a")
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
SPACE = "txs" + TAG
ROUNDS = max(100, int(1500 * float(os.environ.get("BARCH_TEST_SCALE", "1"))))


def client(space=SPACE):
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", space)
    return c


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


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


try:
    tx, other = client(), client()
    other.execute_command("MSET", "a", "1", "b", "1", "c", "1")

    print("reads as of BEGINTX")
    tx.execute_command("BEGINTX")
    check(tx.execute_command("GET", "a") == "1", "the transaction reads a")
    other.execute_command("SET", "b", "2")
    other.execute_command("DEL", "c")
    other.execute_command("SET", "d", "new")
    other.execute_command("SET", "a", "2")
    got = [tx.execute_command("GET", "b"), tx.execute_command("GET", "c"), tx.execute_command("GET", "d")]
    check(got == ["1", "1", None], "a changed key, a deleted one and a new one read as they were (%s)" % got)
    got = tx.execute_command("MGET", "a", "b", "c", "d")
    check(list(got) == ["1", "1", "1", None], "and through MGET (%s)" % got)
    got = tx.execute_command("EXISTS", "c", "d")
    check(got == 1, "and EXISTS (%s)" % got)
    check(stats()["tx_layers"] >= 1, "a layer holds them (%s)" % stats()["tx_layers"])
    tx.execute_command("SET", "z", "from the transaction")
    got = answer(tx, "COMMITTX")
    check("TXCONFLICT" in str(got) and other.get("z") is None,
          "one that writes answers TXCONFLICT, since what it read changed (%s)" % str(got)[:60])
    check(stats()["tx_layers"] == 0, "and the layers go with the last snapshot (%s)" % stats()["tx_layers"])

    print("a transaction that only reads")
    tx.execute_command("BEGINTX")
    first = tx.execute_command("GET", "a")
    other.execute_command("SET", "a", "3")
    got = answer(tx, "COMMITTX")
    check(first == "2" and got in ("OK", True), "commits, since it read one moment's store (%r %r)" % (first, got))
    other.execute_command("SET", "a", "2")

    print("a write to a key that was read, between the check and the write")
    # a write a transaction makes to a key it read can't overwrite a plain write that
    # came after its check: the commit holds the space from the check to its last write
    tx.execute_command("BEGINTX")
    n = int(tx.execute_command("INCR", "counter"))
    other.execute_command("INCR", "counter")
    got = answer(tx, "COMMITTX")
    check("TXCONFLICT" in str(got) and other.get("counter") == "1",
          "an INCR whose key another client INCRs first answers TXCONFLICT, losing nothing (%r %s)"
          % (other.get("counter"), str(got)[:40]))

    print("a later snapshot")
    tx.execute_command("BEGINTX")
    got = tx.execute_command("MGET", "a", "b", "c", "d")
    tx.execute_command("ABORTTX")
    check(list(got) == ["2", "2", None, "new"], "a transaction begun after the writes reads them (%s)" % got)

    print("a key changed twice")
    other.execute_command("SET", "e", "first")
    tx.execute_command("BEGINTX")
    other.execute_command("SET", "e", "second")
    other.execute_command("SET", "e", "third")
    got = tx.execute_command("GET", "e")
    tx.execute_command("ABORTTX")
    check(got == "first", "reads as it was before the first change (%s)" % got)

    print("readers while transactions commit")
    other.execute_command("MSET", "x", "0", "y", "0")
    torn = []
    errors = []
    stop = threading.Event()

    def writer():
        w = client()
        i = 0
        while not stop.is_set():
            i += 1
            w.execute_command("BEGINTX")
            w.execute_command("SET", "x", str(i))
            w.execute_command("SET", "y", str(i))
            if answer(w, "COMMITTX") not in ("OK", True):
                errors.append(i)

    def reader():
        r = client()
        for _ in range(ROUNDS):
            r.execute_command("BEGINTX")
            x = r.execute_command("GET", "x")
            time.sleep(0.0005)
            y = r.execute_command("GET", "y")
            r.execute_command("ABORTTX")
            if x != y:
                torn.append((x, y))

    wt = threading.Thread(target=writer)
    wt.start()
    readers = [threading.Thread(target=reader) for _ in range(2)]
    for t in readers:
        t.start()
    for t in readers:
        t.join()
    stop.set()
    wt.join()
    check(not torn, "a reader never sees x and y differ (%d reads, %s)" % (2 * ROUNDS, torn[:3]))
    check(not errors, "every writer's commit went in (%s)" % errors[:3])
    check(stats()["tx_layers"] == 0 and stats()["tx_live"] == 0, "and nothing is left open (%s)" % stats())

    print("transactions and plain writes on one key")
    other.execute_command("SET", "hot", "0")
    each = max(50, ROUNDS // 3)
    gave_up = []

    def tx_incr():
        w = client()
        for _ in range(each):
            for attempt in range(200):
                w.execute_command("BEGINTX")
                w.execute_command("INCR", "hot")
                if answer(w, "COMMITTX") in ("OK", True):
                    break
            else:
                gave_up.append(1)

    def plain_incr():
        w = client()
        for _ in range(each):
            w.execute_command("INCR", "hot")

    ws = [threading.Thread(target=tx_incr) for _ in range(3)] + [threading.Thread(target=plain_incr) for _ in range(2)]
    for t in ws:
        t.start()
    for t in ws:
        t.join()
    check(not gave_up and other.get("hot") == str(5 * each),
          "no INCR is lost: %d expected, %s held" % (5 * each, other.get("hot")))
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
