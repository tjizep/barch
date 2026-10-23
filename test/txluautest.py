#!/usr/bin/env python3
"""
Transactions from Luau: barch.tx and the barch.store locks - TODO 732.

A stored function gets barch.tx.begin(pessimistic), commit(), abort(), open() and
run(fn, pessimistic, ...), which runs fn as one transaction and runs it again on
TXDEADLOCK, TXLOCKTIMEOUT or TXCONFLICT. In a pessimistic transaction barch.store
locks the way the string commands do: get, fetch and exists take a key's shared lock,
set and remove its exclusive one, and get(key, true) takes the exclusive one at once,
as GETFORUPDATE does. The calls that reach past single keys are refused there. And a
CALLF is now taken inside a client's BEGINTX PESSIMISTIC.

Checked against one barchd with no Raft, tx_lock_wait_ms 1500:
  - begin, open, commit and abort do what the commands do;
  - run commits, returns what fn returned, passes fn its arguments, and an abort in
    fn means nothing is kept;
  - an error in fn aborts and comes back as it was;
  - barch.store.set waits for another transaction's lock, and goes on after it;
  - get(k, true) and the space handle's get(k, true) lock k for writing;
  - read-then-write increments with get(k, true) never deadlock and none are lost,
    while with a plain get they deadlock and run retries them;
  - count, range and the like are refused in a pessimistic transaction;
  - a CALLF inside a client's pessimistic transaction runs in it: its writes are the
    client's, locked until COMMITTX, and gone on ABORTTX;
  - a function that times out inside run, interpreted or AOT, leaves no transaction
    open and no lock held (TODO 743); one that returns with one open leaves it for the
    client, and a timeout inside the client's own leaves the client's open.

A wait inside a stored function parks now (TODO 731, test/txfunctionparktest.py), so
which I/O threads the connections here land on doesn't matter.
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
PORT = scale.port(default=14819)
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
DATA = os.path.join(os.getcwd(), "txluau_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txluau.log"), "w")
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

SPACE = "txluau_" + str(int(time.time()))


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


def deadlocks(watch):
    m = re.search(r"^tx_deadlocks:(\d+)$", watch.execute_command("INFO", "transactions"), re.M)
    return int(m.group(1)) if m else -1


SPIN = """
function call(k)
    return barch.tx.run(function()
        barch.store.set(k, "spun")
        local x = 0
        while true do x = x + 1 end
    end, true)
end"""

FUNCTIONS = {
    # what open() says at each step, and what the key holds after each
    "tx_steps": """
function call(k)
    local seen = {}
    table.insert(seen, tostring(barch.tx.open()))
    barch.tx.begin(true)
    table.insert(seen, tostring(barch.tx.open()))
    barch.store.set(k, "kept")
    barch.tx.commit()
    table.insert(seen, tostring(barch.tx.open()))
    barch.tx.begin()
    barch.store.set(k, "dropped")
    barch.tx.abort()
    table.insert(seen, tostring(barch.tx.open()))
    table.insert(seen, barch.store.get(k))
    return table.concat(seen, " ")
end""",
    "tx_incr": """
function call(k, for_update)
    return barch.tx.run(function(key)
        local v = tonumber(barch.store.get(key, for_update == "yes")) or 0
        barch.store.set(key, tostring(v + 1))
        return v + 1
    end, true, k)
end""",
    "tx_handle_incr": """
function call(k)
    return barch.tx.run(function()
        local sp = barch.current()
        local v = tonumber(sp:get(k, true)) or 0
        sp:set(k, tostring(v + 1))
        return v + 1
    end, true)
end""",
    "tx_transfer": """
function call(src, dst, cents)
    cents = tonumber(cents)
    return barch.tx.run(function()
        local have = tonumber(barch.store.get(src, true))
        if have < cents then barch.tx.abort() return "refused" end
        barch.store.set(src, tostring(have - cents))
        barch.store.set(dst, tostring(tonumber(barch.store.get(dst, true)) + cents))
        return "moved"
    end, true)
end""",
    "tx_raise": """
function call(k)
    return barch.tx.run(function()
        barch.store.set(k, "half")
        error("not today", 0)
    end, true)
end""",
    "tx_count": """
function call()
    return barch.tx.run(function() return barch.store.count("a", "z") end, true)
end""",
    # spins inside run until the deadline: the timeout fires again outside run's pcall
    "tx_spin": SPIN,
    "tx_left_open": """
function call(k)
    barch.tx.begin(true)
    barch.store.set(k, "left")
    return "left"
end""",
    "tx_set": """
function call(k, v)
    return barch.tx.run(function() barch.store.set(k, v) return barch.store.get(k) end, true)
end""",
}

watch = client()
try:
    for name, body in FUNCTIONS.items():
        r = run(watch, "SETF", name, body)
        check(r == "OK", "SETF %s (%s)" % (name, r))

    print("begin, open, commit, abort")
    check(run(watch, "CALLF", "tx_steps", "steps") == "false true false false kept",
          "open() follows them, commit keeps and abort drops (%s)" % run(watch, "GET", "steps"))

    print("run")
    check(run(watch, "CALLF", "tx_incr", "n", "yes") == 1, "returns what fn returned")
    check(watch.get("n") == "1", "and committed it")
    watch.mset({"src": "100", "dst": "0"})
    check(run(watch, "CALLF", "tx_transfer", "src", "dst", "30") == "moved", "a transfer")
    check(run(watch, "CALLF", "tx_transfer", "src", "dst", "300") == "refused", "a refused one, aborted in fn")
    check(watch.mget("src", "dst") == ["70", "30"], "only the first kept (%s)" % watch.mget("src", "dst"))
    watch.set("r", "whole")
    r = run(watch, "CALLF", "tx_raise", "r")
    check("not today" in str(r), "an error comes back as it was (%s)" % r)
    check(watch.get("r") == "whole", "and what fn wrote is gone")
    check(run(watch, "BEGINTX") == "OK" and run(watch, "ABORTTX") == "OK", "nothing left open")

    print("barch.store.set waits for another transaction's lock")
    holder, waiter = client(), client()
    run(holder, "BEGINTX", "PESSIMISTIC")
    run(holder, "SET", "w", "holder")
    out = {}
    t = later(waiter, out, "w", "CALLF", "tx_set", "w", "function")
    time.sleep(0.4)
    check("w" not in out, "the function waits")
    run(holder, "COMMITTX")
    t.join()
    check(out.get("w") == "function" and out.get("w_s", 9) < 1.4, "and goes on after COMMITTX (%s)" % out.get("w"))
    check(watch.get("w") == "function", "its write kept")

    print("get(k, true) locks for writing")
    holder, reader = client(), client()
    run(holder, "BEGINTX", "PESSIMISTIC")
    run(holder, "GET", "g")
    out = {}
    t = later(reader, out, "g", "CALLF", "tx_incr", "g", "yes")
    time.sleep(0.4)
    check("g" not in out, "it waits for a shared reader")
    run(holder, "COMMITTX")
    t.join()
    check(out.get("g") == 1, "and goes on when it's done (%s)" % out.get("g"))
    a, b = client(), client()
    out = {}
    run(a, "BEGINTX", "PESSIMISTIC")
    check(run(a, "CALLF", "tx_handle_incr", "h") == 1, "the space handle's get(k, true) too, in a's transaction")
    run(b, "BEGINTX", "PESSIMISTIC")
    t = later(b, out, "h", "GET", "h")
    time.sleep(0.4)
    check("h" not in out, "a GET in another transaction waits for it")
    run(a, "COMMITTX")
    t.join()
    check(out.get("h") == "1", "and reads it after COMMITTX")
    run(b, "COMMITTX")

    print("read then write, four functions at once")

    def increments(for_update, clients=4, each=25):
        conns = [client() for _ in range(clients)]
        gave_up = [0]

        def worker(c):
            for _ in range(each):
                for attempt in range(50):
                    r = run(c, "CALLF", "tx_incr", "m", for_update)
                    if isinstance(r, int):
                        break
                    gave_up[0] += 1
                    time.sleep(0.002 * attempt)
        threads = [threading.Thread(target=worker, args=(c,)) for c in conns]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return gave_up[0]

    watch.set("m", "0")
    before = deadlocks(watch)
    increments("no")
    check(int(watch.get("m")) == 100, "with get(k) every increment is kept (%s)" % watch.get("m"))
    check(deadlocks(watch) > before, "they deadlock and run retries (%d)" % (deadlocks(watch) - before))
    watch.set("m", "0")
    before = deadlocks(watch)
    gave_up = increments("yes")
    check(int(watch.get("m")) == 100, "with get(k, true) every increment is kept (%s)" % watch.get("m"))
    check(deadlocks(watch) == before and gave_up == 0, "and none deadlock (%d)" % (deadlocks(watch) - before))

    print("refused in a pessimistic transaction")
    r = run(watch, "CALLF", "tx_count")
    check("isn't taken in a pessimistic transaction" in str(r), "barch.store.count (%s)" % r)

    print("CALLF inside a client's BEGINTX PESSIMISTIC")
    c, other = client(), client()
    watch.set("j", "5")
    run(c, "BEGINTX", "PESSIMISTIC")
    r = run(c, "CALLF", "tx_incr", "j", "yes")
    check(r == 6, "it's taken (%s)" % r)
    check(run(c, "GET", "j") == "6", "the client sees its write")
    out = {}
    run(other, "BEGINTX", "PESSIMISTIC")
    t = later(other, out, "j", "GET", "j")
    time.sleep(0.4)
    check("j" not in out, "another transaction waits on the lock it took")
    check(watch.get("j") == "5", "and outside one it isn't seen yet")
    check(run(c, "COMMITTX") == "OK", "the client commits")
    t.join()
    check(out.get("j") == "6", "and the write is there")
    run(other, "COMMITTX")
    run(c, "BEGINTX", "PESSIMISTIC")
    run(c, "CALLF", "tx_incr", "j", "yes")
    run(c, "ABORTTX")
    check(watch.get("j") == "6", "ABORTTX drops it")

    # TODO 743: a timeout fires again at the next interrupt, outside run's pcall, so
    # run's ABORTTX never ran and the connection kept the transaction and its locks
    print("a function that times out inside run")
    check(run(watch, "SETF", "tx_spin_aot", SPIN, "AOT") == "OK", "SETF tx_spin_aot, AOT")
    check(run(watch, "CONFIG", "SET", "function_deadline_ms", "300") == "OK", "a short deadline")
    for fn in ("tx_spin", "tx_spin_aot"):
        c, other = client(), client()
        key = "spun_" + fn
        r = run(c, "CALLF", fn, key)
        check("FUNCTION timeout" in str(r), "%s: times out (%s)" % (fn, str(r)[:50]))
        run(other, "BEGINTX", "PESSIMISTIC")
        t0 = time.time()
        r = run(other, "SET", key, "other")
        took = time.time() - t0
        check(r is True and took < 1.0, "%s: and holds no lock on the key (%s, %.2f s)" % (fn, str(r)[:50], took))
        run(other, "COMMITTX")
        r = run(c, "ABORTTX")
        check("without BEGINTX" in str(r), "%s: nor leaves a transaction open (%s)" % (fn, str(r)[:50]))
        check(watch.get(key) == "other", "%s: and nothing of it was kept" % fn)
        c.close()
        other.close()
    c = client()
    check(run(c, "CALLF", "tx_left_open", "lo") == "left" and run(c, "GET", "lo") == "left",
          "one that returns with it open leaves it for the client")
    check(run(c, "ABORTTX") == "OK" and watch.get("lo") is None, "which the client aborts")
    run(c, "BEGINTX", "PESSIMISTIC")
    run(c, "SET", "mine", "1")
    r = run(c, "CALLF", "tx_spin", "mine2")
    check("FUNCTION timeout" in str(r) and run(c, "GET", "mine") == "1",
          "a timeout in the client's own transaction leaves the client's open")
    check(run(c, "ABORTTX") == "OK", "for the client to end")
    c.close()
    run(watch, "CONFIG", "SET", "function_deadline_ms", "1000")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
