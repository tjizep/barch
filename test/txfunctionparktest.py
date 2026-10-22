#!/usr/bin/env python3
"""
A stored function's key lock wait parks - TODO 731.

A function whose command or barch.store call needs a lock another transaction holds
used to wait for it in place, holding its thread. A CALLF's first slice runs on the
connection's I/O thread, so a client transaction holding the lock on the same I/O
thread couldn't get its COMMITTX in, and both sat there until the function's time
limit. Now the function parks on the lock, the way a client's command does, and runs
the call again when it's let go of.

Checked against one barchd with no Raft, tx_lock_wait_ms 1500:
  - a client holds a key and a CALLF wants it, 0 to 9 connections apart, so one pair
    is on the same I/O thread: the holder's COMMITTX answers at once and the function
    goes on, through barch.call, barch.store, a space handle and an AOT function;
  - while it waits, other connections on its thread are answered, and INFO
    transactions lists it waiting on the lock;
  - a lock never let go of: TXLOCKTIMEOUT after tx_lock_wait_ms, and the holder is
    answered all the while;
  - a plain barch.call SET outside a transaction parks on a held key too;
  - a client that closes a ring with a parked function gets TXDEADLOCK, and the
    function goes on;
  - six functions parked on one key all go on when it's let go of, each passing the
    turn on, and none of their increments is lost; three readers go on together;
  - a parked function whose connection closes doesn't take the server with it.
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
PORT = scale.port(default=14820)
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
DATA = os.path.join(os.getcwd(), "txfunctionpark_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txfunctionpark.log"), "w")
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

SPACE = "txfunctionpark_" + str(int(time.time()))


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)
    c.response_callbacks.pop("INFO", None)
    c.execute_command("USE", SPACE)
    return c


def run(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except (redis.ResponseError, redis.ConnectionError, redis.TimeoutError) as e:
        return "ERROR " + str(e)


def later(c, out, name, *cmd):
    def go():
        t0 = time.time()
        out[name] = run(c, *cmd)
        out[name + "_s"] = time.time() - t0
    t = threading.Thread(target=go)
    t.start()
    return t


def listed(watch):
    return [line for line in watch.execute_command("INFO", "transactions").splitlines()
            if line.startswith("tx") and line[2:3].isdigit()]


INCR_STORE = """
function call(k)
    return barch.tx.run(function()
        local v = tonumber(barch.store.get(k, true)) or 0
        barch.store.set(k, tostring(v + 1))
        return v + 1
    end, true)
end"""

FUNCTIONS = {
    "fp_call": """
function call(k)
    return barch.tx.run(function()
        local v = tonumber(barch.call("GETFORUPDATE", k)) or 0
        barch.call("SET", k, v + 1)
        return v + 1
    end, true)
end""",
    "fp_store": INCR_STORE,
    "fp_handle": """
function call(k)
    return barch.tx.run(function()
        local sp = barch.current()
        local v = tonumber(sp:get(k, true)) or 0
        sp:set(k, tostring(v + 1))
        return v + 1
    end, true)
end""",
    "fp_read": """
function call(k)
    return barch.tx.run(function() return barch.store.get(k) end, true)
end""",
    "fp_plain": """
function call(k, v)
    barch.call("SET", k, v)
    return barch.call("GET", k)
end""",
    # no run: run would try again on TXLOCKTIMEOUT
    "fp_once": """
function call(k)
    barch.tx.begin(true)
    local ok, got = pcall(barch.call, "GETFORUPDATE", k)
    barch.tx.abort()
    if not ok then
        error(got, 0)
    end
    return got
end""",
    "fp_two": """
function call(a, b)
    return barch.tx.run(function()
        barch.store.set(a, "function")
        return barch.store.get(b, true)
    end, true)
end""",
}

watch = client()
try:
    for name, body in FUNCTIONS.items():
        check(run(watch, "SETF", name, body) == "OK", "SETF %s" % name)
    check(run(watch, "SETF", "fp_aot", INCR_STORE, "AOT") == "OK", "SETF fp_aot, AOT")

    print("a client holds the key, a CALLF 0 to 9 connections later wants it")
    for fn in ("fp_call", "fp_store", "fp_handle", "fp_aot"):
        key = "same_" + fn
        run(watch, "CALLF", fn, key)    # compiled (natively for AOT) before anything is timed
        watch.set(key, "0")
        worst_commit, worst_call, right = 0.0, 0.0, True
        for gap in range(10):
            holder = client()
            others = [client() for _ in range(gap)]
            waiter = client()
            run(holder, "BEGINTX", "PESSIMISTIC")
            run(holder, "SET", key, str(gap * 10))
            out = {}
            t = later(waiter, out, "r", "CALLF", fn, key)
            time.sleep(0.2)
            t0 = time.time()
            committed = run(holder, "COMMITTX")
            worst_commit = max(worst_commit, time.time() - t0)
            t.join()
            worst_call = max(worst_call, out["r_s"])
            right = right and committed == "OK" and out["r"] == gap * 10 + 1
            for c in [holder, waiter] + others:
                c.close()
        check(right, "%s: every one read the holder's write and added one" % fn)
        check(worst_commit < 0.5, "%s: the holder's COMMITTX answered at once (worst %.2f s)" % (fn, worst_commit))
        check(worst_call < 1.0, "%s: and the function went on (worst %.2f s)" % (fn, worst_call))

    print("while it waits")
    holder = client()
    neighbours = [client() for _ in range(7)]
    waiter = client()           # on the holder's thread
    after = client()            # on the next one along, the same as neighbours[0]
    run(holder, "BEGINTX", "PESSIMISTIC")
    run(holder, "SET", "w", "1")
    out = {}
    t = later(waiter, out, "w", "CALLF", "fp_store", "w")
    time.sleep(0.2)
    t0 = time.time()
    # the holder's in a transaction, which doesn't take PING: a GET then
    pinged = [run(c, "PING") for c in neighbours + [after]] + [run(holder, "GET", "w") == "1"]
    check(all(r is True for r in pinged) and time.time() - t0 < 0.5,
          "every other connection is answered (%s, %.2f s)" % (pinged, time.time() - t0))
    rows = [r for r in listed(watch) if "waiting=a lock on w," in r]
    check(len(rows) == 1 and "command=barch.store.get," in rows[0],
          "INFO transactions lists it waiting on the lock (%s)" % (rows[0][:90] if rows else "none"))
    run(holder, "COMMITTX")
    t.join()
    check(out.get("w") == 2, "and it goes on after COMMITTX (%s)" % out.get("w"))
    rows = [r for r in listed(watch) if "waiting=a lock on w," in r]
    check(not rows, "no longer listed waiting")
    for c in [holder, waiter, after] + neighbours:
        c.close()

    print("a lock never let go of")
    holder = client()
    neighbours = [client() for _ in range(7)]
    waiter = client()
    run(holder, "BEGINTX", "PESSIMISTIC")
    run(holder, "SET", "t", "1")
    out = {}
    t = later(waiter, out, "t", "CALLF", "fp_once", "t")
    time.sleep(0.3)
    t0 = time.time()
    check(run(holder, "GET", "t") == "1" and time.time() - t0 < 0.3, "the holder is answered while it waits")
    t.join()
    check("TXLOCKTIMEOUT" in str(out.get("t")), "TXLOCKTIMEOUT (%s)" % str(out.get("t"))[:70])
    check(1.3 < out.get("t_s", 0) < 2.5, "after tx_lock_wait_ms (%.2f s)" % out.get("t_s", 0))
    check(run(holder, "COMMITTX") == "OK" and watch.get("t") == "1", "the holder commits, nothing else written")
    for c in [holder, waiter] + neighbours:
        c.close()

    print("a plain write in a function")
    holder = client()
    neighbours = [client() for _ in range(7)]
    waiter = client()
    run(holder, "BEGINTX", "PESSIMISTIC")
    run(holder, "SET", "p", "holder")
    out = {}
    t = later(waiter, out, "p", "CALLF", "fp_plain", "p", "function")
    time.sleep(0.3)
    check("p" not in out, "barch.call SET outside a transaction waits")
    t0 = time.time()
    check(run(holder, "COMMITTX") == "OK" and time.time() - t0 < 0.5, "the holder's COMMITTX answers at once")
    t.join()
    check(out.get("p") == "function", "and the function's SET goes after it (%s)" % out.get("p"))
    for c in [holder, waiter] + neighbours:
        c.close()

    print("a ring through a parked function")
    x, f = client(), client()
    watch.mset({"ra": "0", "rb": "x's"})
    run(x, "BEGINTX", "PESSIMISTIC")
    run(x, "SET", "rb", "x's")
    out = {}
    t = later(f, out, "f", "CALLF", "fp_two", "ra", "rb")
    time.sleep(0.3)
    check("f" not in out, "the function holds ra and waits for rb")
    r = run(x, "SET", "ra", "x")
    check(str(r).startswith("ERROR TXDEADLOCK"), "x, closing the ring, gets TXDEADLOCK (%s)" % str(r)[:40])
    run(x, "ABORTTX")
    t.join()
    check(out.get("f") == "x's" and out.get("f_s", 9) < 1.0, "and the function goes on (%s)" % out.get("f"))
    check(watch.get("ra") == "function", "its write kept")
    x.close()
    f.close()

    print("six on one key")
    watch.set("six", "0")
    holder = client()
    waiters = [client() for _ in range(6)]
    run(holder, "BEGINTX", "PESSIMISTIC")
    run(holder, "SET", "six", "100")
    out = {}
    threads = [later(c, out, "w%d" % i, "CALLF", "fp_store", "six") for i, c in enumerate(waiters)]
    time.sleep(0.3)
    check(not [k for k in out if not k.endswith("_s")], "all six wait")
    t0 = time.time()
    run(holder, "COMMITTX")
    for t in threads:
        t.join()
    took = time.time() - t0
    got = sorted(out["w%d" % i] for i in range(6))
    check(got == [101, 102, 103, 104, 105, 106], "each went on in turn (%s)" % got)
    check(watch.get("six") == "106" and took < 1.0, "none lost, and quickly (%.2f s)" % took)
    for c in [holder] + waiters:
        c.close()

    print("three readers behind a writer")
    holder = client()
    readers = [client() for _ in range(3)]
    run(holder, "BEGINTX", "PESSIMISTIC")
    run(holder, "SET", "rd", "written")
    out = {}
    threads = [later(c, out, "r%d" % i, "CALLF", "fp_read", "rd") for i, c in enumerate(readers)]
    time.sleep(0.3)
    run(holder, "COMMITTX")
    for t in threads:
        t.join()
    check(all(out["r%d" % i] == "written" and out["r%d_s" % i] < 1.0 for i in range(3)),
          "all three read the write as soon as it's committed")
    for c in [holder] + readers:
        c.close()

    print("a parked function's connection closes")
    holder = client()
    waiter = client()
    run(holder, "BEGINTX", "PESSIMISTIC")
    run(holder, "SET", "gone", "1")
    raw = socket.create_connection(("127.0.0.1", PORT))
    raw.sendall(("*2\r\n$3\r\nUSE\r\n$%d\r\n%s\r\n" % (len(SPACE), SPACE)).encode())
    raw.recv(100)
    raw.sendall(b"*3\r\n$5\r\nCALLF\r\n$8\r\nfp_store\r\n$4\r\ngone\r\n")
    time.sleep(0.3)
    raw.close()
    time.sleep(0.2)
    check(run(holder, "COMMITTX") == "OK", "the holder commits")
    time.sleep(0.3)
    check(run(watch, "PING") is True and run(waiter, "CALLF", "fp_store", "gone") in (2, 3),
          "the server goes on (%s)" % watch.get("gone"))
    holder.close()
    waiter.close()

    # TODO 734: the space handle's methods can park now, which made a callback they
    # run yieldable, and a slice running out inside one took the server down
    print("a slice spent inside a callback")
    check(run(watch, "CONFIG", "SET", "function_slice_insns", "1000") == "OK", "a short slice")
    for i in range(50):
        watch.set("cb:%d" % i, "v" * 40)
    copy = client()
    copy.execute_command("USE", SPACE + "_copy")
    copy.set("there", "1")
    spin = "local function spin() local x = 0 for i = 1, 20000 do x = x + i end return x end\n"
    callbacks = {
        "fp_cb_save": """
function call(into)
    local sp = barch.current()
    local blocks = {}
    sp:call("BEGIN")
    local n = sp:save(function(buf, block, shard)
        spin()
        blocks[shard .. ":" .. block] = buf
    end)
    sp:call("COMMIT")
    local read = barch.space[into]:load(function(block, shard)
        spin()
        return blocks[shard .. ":" .. block]
    end)
    return n .. " " .. read
end""",
        "fp_cb_leaves": """
function call()
    return barch.current():leaves(function(page, buf, shard) spin() end)
end""",
        "fp_cb_stats": """
function call()
    return barch.current():stats(function(stats, shard) spin() end)
end""",
    }
    for name, body in callbacks.items():
        check(run(watch, "SETF", name, spin + body) == "OK", "SETF %s" % name)
    r = run(watch, "CALLF", "fp_cb_save", SPACE + "_copy")
    check(re.match(r"^\d+ \d+$", str(r)) is not None, "sp:save and sp:load with slow callbacks (%s)" % r)
    check(copy.get("cb:7") == "v" * 40 and copy.get("there") is None, "and the copy is the space")
    r = run(watch, "CALLF", "fp_cb_leaves")
    check(isinstance(r, int) and r > 0, "sp:leaves (%s)" % r)
    r = run(watch, "CALLF", "fp_cb_stats")
    check(isinstance(r, int) and r > 0, "sp:stats (%s)" % r)
    check(p.poll() is None and run(watch, "PING") is True, "the server is still up")
    run(watch, "CONFIG", "SET", "function_slice_insns", "1000000")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
