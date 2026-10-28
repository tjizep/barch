#!/usr/bin/env python3
"""
barch.tx.savepoint(fn, ...) - take back what fn wrote when it raises - TODO 766.

lent's case: inside a transaction a write is refused, the caller catches the error and
carries on, but the writes the hooks made before the refusal stay in the transaction
and get committed. A savepoint runs fn, and if fn raises, every key fn wrote goes back
to what the transaction held for it before fn started, then the error is raised again.

One barchd:
  - fn overwrites an earlier write, overwrites and deletes keys the space has, deletes
    an earlier write, adds a key, INCRs one, writes a composite key, writes over one with a
    deadline and raises: inside the transaction and once committed, only the writes from
    before the savepoint are there;
  - one that doesn't raise keeps everything, and answers what fn returned;
  - nested: an inner one that raises leaves the outer one's writes, and an outer one
    that raises takes back an inner one that didn't;
  - a key fn only read is still checked by COMMITTX, so another client's write to it
    is a TXCONFLICT;
  - fn that ends the transaction itself, then raises, raises as it came;
  - outside a transaction it's refused.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14828)
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


DATA = os.path.join(os.getcwd(), "savepoint_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "savepoint.log"), "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "savepoint_" + str(int(time.time()))


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


KEYS = ["a", "b", "c", "d", "n", "cnt", "x", "y", "z", "r a", "t"]

FUNCTIONS = {
    # the writes a refused one's hooks would have made, then the refusal
    "hooks": """
local function hooks()
    barch.store.set("a", "inside")          -- over an earlier write
    barch.store.set("c", "inside")          -- over a key the space has
    barch.store.remove("d")                 -- one the space has
    barch.store.remove("b")                 -- an earlier write
    barch.store.set("n", "new")             -- one nobody has
    barch.call("INCR", "cnt")
    barch.store.set("r a", "inside")        -- a composite, in a region of its own
    barch.store.set("t", "inside")          -- over a key the space has with a deadline
    error("refused by the hook")
end
function call()
    barch.store.set("a", "before")
    barch.store.set("b", "before")
    local ok, e = pcall(barch.tx.savepoint, hooks)
    return {tostring(ok), tostring(e)}
end""",
    "keeps": """
function call()
    return barch.tx.savepoint(function(v)
        barch.store.set("a", v)
        barch.store.remove("c")
        return "kept " .. v
    end, "inside")
end""",
    "inner_raises": """
function call()
    return barch.tx.savepoint(function()
        barch.store.set("x", "outer")
        local ok = pcall(barch.tx.savepoint, function()
            barch.store.set("x", "inner")
            barch.store.set("y", "inner")
            error("no")
        end)
        barch.store.set("z", "outer")
        return tostring(ok)
    end)
end""",
    "outer_raises": """
function call()
    local ok, e = pcall(barch.tx.savepoint, function()
        barch.store.set("x", "outer")
        barch.tx.savepoint(function()
            barch.store.set("y", "inner")
            barch.store.remove("a")
        end)
        error("outer gives up")
    end)
    return tostring(ok) .. " " .. tostring(e)
end""",
    "reads": """
function call()
    local ok = pcall(barch.tx.savepoint, function()
        local v = barch.store.get("c")
        barch.store.set("x", "from " .. tostring(v))
        error("no")
    end)
    return tostring(ok)
end""",
    "ends_it": """
function call()
    local ok, e = pcall(barch.tx.savepoint, function()
        barch.store.set("x", "gone")
        barch.tx.abort()
        error("after the abort")
    end)
    return tostring(ok) .. " " .. tostring(e)
end""",
    "outside": """
function call()
    local ok, e = pcall(barch.tx.savepoint, function() barch.store.set("x", "outside") end)
    return tostring(ok) .. " " .. tostring(e)
end""",
}


def reset(c):
    c.delete(*KEYS)
    c.mset({"c": "space", "d": "space", "cnt": "10", "r a": "space"})
    c.set("t", "space", px=600000)


def state(c):
    return {k: c.get(k) for k in KEYS}


try:
    c = client()
    for name, body in FUNCTIONS.items():
        check(answer(c, "SETF", name, body) == "OK", "SETF %s" % name)

    want = {"a": "before", "b": "before", "c": "space", "d": "space", "n": None, "cnt": "10",
            "x": None, "y": None, "z": None, "r a": "space", "t": "space"}

    print("a savepoint that raises")
    reset(c)
    c.execute_command("BEGINTX")
    got = answer(c, "CALLF", "hooks")
    check(isinstance(got, list) and got[0] == "false" and "refused by the hook" in got[1],
          "the error comes back to the caller (%s)" % got)
    check(state(c) == want, "inside the transaction, only the writes from before it (%s)" % state(c))
    check(answer(c, "COMMITTX") == "OK", "it commits")
    check(state(client()) == want, "and that's what's committed (%s)" % state(client()))
    # put back without it, t wouldn't be what was read, and would be committed as a write
    check(client().pttl("t") > 0, "with the deadline it wrote over put back (%s)" % client().pttl("t"))

    reset(c)
    got = answer(c, "CALLF", "hooks")
    # through barch.tx.run, which the function doesn't use: outside one it's refused
    check(isinstance(got, list) and got[0] == "false" and "transaction" in got[1],
          "outside a transaction it's refused (%s)" % got)
    check(answer(c, "CALLF", "outside").startswith("false") and c.get("x") is None,
          "and fn doesn't run (%s)" % answer(c, "CALLF", "outside"))

    print("one that doesn't raise")
    reset(c)
    c.execute_command("BEGINTX")
    got = answer(c, "CALLF", "keeps")
    check(got == "kept inside", "answers what fn returned (%s)" % got)
    check(c.get("a") == "inside" and c.get("c") is None, "and keeps its writes")
    check(answer(c, "COMMITTX") == "OK" and client().get("a") == "inside" and client().get("c") is None,
          "committed too")

    print("nested")
    reset(c)
    c.execute_command("BEGINTX")
    got = answer(c, "CALLF", "inner_raises")
    check(got == "false", "the inner one raised (%s)" % got)
    s = state(c)
    check(s["x"] == "outer" and s["y"] is None and s["z"] == "outer",
          "the outer one's writes stay, the inner one's go (%s %s %s)" % (s["x"], s["y"], s["z"]))
    answer(c, "COMMITTX")
    s = state(client())
    check(s["x"] == "outer" and s["y"] is None and s["z"] == "outer", "committed that way")

    reset(c)
    c.set("a", "space")
    c.execute_command("BEGINTX")
    got = answer(c, "CALLF", "outer_raises")
    check(got.startswith("false") and "outer gives up" in got, "the outer one raised (%s)" % got)
    s = state(c)
    check(s["x"] is None and s["y"] is None and s["a"] == "space",
          "and took back the inner one that didn't (%s)" % s)
    answer(c, "COMMITTX")
    check(state(client()) == s, "committed that way")

    print("reads inside one")
    reset(c)
    c.execute_command("BEGINTX")
    check(answer(c, "CALLF", "reads") == "false", "fn read c and raised")
    check(c.get("x") is None, "its write went")
    client().set("c", "changed by someone else")
    c.execute_command("SET", "z", "mark")
    got = answer(c, "COMMITTX")
    check("TXCONFLICT" in str(got) and client().get("z") is None,
          "the read still counts at COMMITTX (%s)" % str(got)[:40])

    print("fn that ends the transaction")
    reset(c)
    c.execute_command("BEGINTX")
    got = answer(c, "CALLF", "ends_it")
    check(got.startswith("false") and "after the abort" in got, "raises as it came (%s)" % got)
    check(c.get("x") is None and "ERR" in str(answer(c, "COMMITTX")), "and the transaction is gone")
    check(answer(c, "PING") == "PONG" or answer(c, "PING") is True, "barchd still answers")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
