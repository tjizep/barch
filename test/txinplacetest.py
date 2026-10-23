#!/usr/bin/env python3
"""
In-place writes inside a transaction - TODO 737.

setBufferAt and incrIntAt (so setInt32/64At, setF32/64At and incrInt32/64ByAt) were
refused in a transaction, so a stored function couldn't keep a balance as a binary
int64 and change it in barch.tx.run. Now they go through the overlay: a key that isn't
there yet is copied in whole from beneath first, its read logged like GET's, and then
changed in the copy.

Checked against one barchd, a space without Raft:
  - an increment in a transaction is seen inside it, not outside, until COMMITTX; the
    bytes it didn't touch and the key's TTL come through;
  - an aborted transaction leaves the old bytes exactly as they were, a value grown
    in it included;
  - a key changed beneath since the transaction read it makes COMMITTX answer
    TXCONFLICT, and the other client's write stands;
  - so does an in-place write (setInt64At) beneath a plain GET in a transaction: it
    used to go straight into the leaf, past the snapshot's versions;
  - a missing key starts at init, and so does one the transaction deleted;
  - transfers between int64 balances in barch.tx.run from several clients at once
    keep the total, and every increment lands;
  - an in-place write in a space with a change log is in the log, so it's back after
    a kill -9 (it went straight into the leaf, past the log).
"""
import os
import shutil
import socket
import struct
import subprocess
import sys
import threading
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14775)
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


DATA = os.path.join(os.getcwd(), "txinplace_data")
LOGS = os.path.join(os.getcwd(), "txinplace_logs")
for d in (DATA, LOGS):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
log = open(os.path.join(os.getcwd(), "txinplace.log"), "a")


def start():
    # nothing saved on its own, so after a kill -9 only the change log has the writes
    q = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                          "-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000",
                          "-c", "aof_durability=timer"], stdout=log, stderr=log)
    end = time.time() + 60
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            break
        except OSError:
            time.sleep(0.1)
    return q


p = start()

SPACE = "txip" + str(int(time.time()))
LOGGED = SPACE + "log"


def client(space=SPACE):
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=30, single_connection_client=True)
    c.execute_command("USE", space)
    return c


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def int_at(raw, off):
    return struct.unpack_from("<q", raw, off)[0] if raw is not None and len(raw) >= off + 8 else None


FUNCTIONS = {
    "incr": "function call(k, by, init, off) return barch.store.incrInt64ByAt(k, by, init, off) end\n",
    "put": "function call(k, n, off) barch.store.setInt64At(k, n, off) return 'ok' end\n",
    "at": "function call(k, off) return barch.store.getInt64At(k, off) end\n",
    "grow": "function call(k, off, s) barch.store.setBufferAt(k, buffer.fromstring(s), tonumber(off)) return 'ok' end\n",
    # a transfer between two binary balances, locked in key order, and their sum
    "move": """
        function call(a, b, by)
            return barch.tx.run(function()
                local first, second = a, b
                if second < first then first, second = second, first end
                barch.store.get(first, true)
                barch.store.get(second, true)
                barch.store.incrInt64ByAt(a, -tonumber(by), 0, 0)
                return barch.store.incrInt64ByAt(b, tonumber(by), 0, 0)
            end, true)
        end
    """,
    "bump": """
        function call(k)
            return barch.tx.run(function() return barch.store.incrInt64ByAt(k, 1, 1, 0) end, true)
        end
    """,
}

try:
    c = client()
    for name, body in FUNCTIONS.items():
        check(answer(c, "SETF", name, body) == b"OK", "SETF %s" % name)

    print("an increment in a transaction")
    answer(c, "CALLF", "put", "k", 100, 0)
    answer(c, "CALLF", "put", "k", 7, 8)
    c.execute_command("PEXPIRE", "k", 600000)
    before = c.get("k")
    c.execute_command("BEGINTX")
    got = answer(c, "CALLF", "incr", "k", 5, 0, 0)
    check(got == 105, "is taken, and answers the sum (%s)" % str(got)[:80])
    check(answer(c, "CALLF", "at", "k", 0) == 105, "a read in the transaction sees it")
    check(client().get("k") == before, "another client doesn't, before COMMITTX")
    check(answer(c, "COMMITTX") == b"OK", "COMMITTX")
    after = client().get("k")
    check(int_at(after, 0) == 105 and int_at(after, 8) == 7 and len(after) == 16,
          "it's made, and the bytes it didn't touch are kept (%s)" % after)
    check(client().pttl("k") > 0, "and so is the key's TTL")

    print("an aborted transaction")
    before = client().get("k")
    c.execute_command("BEGINTX")
    got = [answer(c, "CALLF", "put", "k", 999, 0), answer(c, "CALLF", "incr", "k", 1, 0, 8),
           answer(c, "CALLF", "grow", "k", 16, "tail")]
    inside = answer(c, "GET", "k")
    check(got == [b"ok", 8, b"ok"] and int_at(inside, 0) == 999 and inside.endswith(b"tail"),
          "set, incr and a grow are taken (%s)" % got)
    check(answer(c, "ABORTTX") == b"OK" and client().get("k") == before, "the old bytes come back")

    print("a key changed beneath")
    c.execute_command("BEGINTX")
    answer(c, "CALLF", "incr", "k", 1, 0, 0)
    answer(client(), "CALLF", "put", "k", 500, 0)
    got = answer(c, "COMMITTX")
    check("TXCONFLICT" in str(got), "COMMITTX answers TXCONFLICT (%s)" % str(got)[:60])
    check(int_at(client().get("k"), 0) == 500, "and the other client's write stands")

    print("an in-place write beneath a transaction's plain read")
    answer(c, "CALLF", "put", "p", 1, 0)
    c.execute_command("BEGINTX")
    seen = answer(c, "GET", "p")
    answer(client(), "CALLF", "put", "p", 2, 0)
    c.execute_command("SET", "p2", "x")
    got = answer(c, "COMMITTX")
    check(int_at(seen, 0) == 1 and "TXCONFLICT" in str(got),
          "COMMITTX answers TXCONFLICT (%s)" % str(got)[:60])

    print("a key that isn't there")
    c.execute_command("BEGINTX")
    got = answer(c, "CALLF", "incr", "fresh", 3, 42, 0)
    done = answer(c, "COMMITTX")
    check(got == 42 and done == b"OK" and int_at(client().get("fresh"), 0) == 42,
          "starts at init (%s)" % str(got)[:60])
    answer(c, "CALLF", "put", "gone", 10, 0)
    c.execute_command("BEGINTX")
    c.execute_command("DEL", "gone")
    got = answer(c, "CALLF", "incr", "gone", 1, 9, 0)
    c.execute_command("ABORTTX")
    check(got == 9, "and so does one the transaction deleted (%s)" % str(got)[:60])

    print("binary balances in barch.tx.run, several clients at once")
    accounts = ["acct:%d" % i for i in range(5)]
    for a in accounts:
        answer(c, "CALLF", "put", a, 10000, 0)
    errors = []

    def mover(n):
        r = client()
        for i in range(150):
            answer(r, "CALLF", "bump", "bumps")
            a, b = accounts[(n + i) % 5], accounts[(n * 3 + i + 1) % 5]
            if a == b:
                continue
            got = answer(r, "CALLF", "move", a, b, (i % 7) + 1)
            if isinstance(got, str):
                errors.append(got)

    threads = [threading.Thread(target=mover, args=(n,)) for n in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(120)
    total = sum(int_at(client().get(a), 0) for a in accounts)
    check(not errors, "every transfer answered (%d didn't%s)" % (len(errors), ", e.g. " + errors[0][:60] if errors else ""))
    check(total == 50000, "the total is kept (%d)" % total)
    check(int_at(client().get("bumps"), 0) == 6 * 150, "every increment landed (%s)" % int_at(client().get("bumps"), 0))

    print("an in-place write that waits for a lock")
    # eight callers after the holder, so one shares its I/O thread. A call that waited
    # there, rather than parking, held that thread up and COMMITTX with it until the
    # lock wait (5 s) ran out
    answer(c, "CALLF", "put", "w", 100, 0)
    holder = client()
    holder.execute_command("BEGINTX", "PESSIMISTIC")
    holder.execute_command("GETFORUPDATE", "w")
    waiters = [client() for _ in range(8)]
    got = [None] * len(waiters)

    def wait_on(n):
        got[n] = answer(waiters[n], "CALLF", "bump", "w")

    threads = [threading.Thread(target=wait_on, args=(n,)) for n in range(len(waiters))]
    for t in threads:
        t.start()
    time.sleep(0.3)
    t0 = time.time()
    holder.execute_command("SET", "w", struct.pack("<q", 1000))
    done = answer(holder, "COMMITTX")
    took = time.time() - t0
    for t in threads:
        t.join(60)
    check(done == b"OK" and took < 2, "the holder's COMMITTX isn't held up (%.2f s)" % took)
    check(sorted(got) == list(range(1001, 1009)) and int_at(client().get("w"), 0) == 1008,
          "every waiter went on after it, from the committed value (%s)" % sorted(map(str, got))[:3])

    print("an in-place write in a space with a change log")
    w = client(LOGGED)
    w.execute_command("configuration:SET", LOGGED + ".aof_dir", LOGS)
    w.execute_command("configuration:SAVE")
    p.kill()                                    # the log is opened with the space
    p.wait()
    p = start()
    w = client(LOGGED)
    w.execute_command("SETF", "put", FUNCTIONS["put"])
    answer(w, "CALLF", "put", "k", 1, 0)
    answer(w, "CALLF", "put", "k", 2, 0)        # the same size, so it could go in place
    p.kill()
    p.wait()
    p = start()
    got = int_at(client(LOGGED).get("k"), 0)
    check(got == 2, "the second write is back after a kill -9 (%s)" % got)
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
