#!/usr/bin/env python3
"""
barch.store.getBuffers - TODO 910.

getBuffers(keys [, offset]) reads many keys' bytes in one call. It answers one slot
per key, so the array keeps #keys: a buffer, barch.tomb, or false where getBufferAt
would answer nil. In a transaction it is one MGET: one routing step and one lock
request for the batch, and each read is logged as GET's is.

Checked against one barchd, a space without Raft, tx_lock_wait_ms 0 so a held lock
is refused at once rather than waited for:
  - outside a transaction each slot is what getBufferAt answers, an offset included;
  - in the configuration space a key ending in "password" reads as false, as get
    hides it there, and the ones around it still read;
  - a space handle has it too;
  - in a transaction it sees the transaction's own writes, and not after an abort;
  - a key changed beneath since getBuffers read it makes COMMITTX answer TXCONFLICT;
  - in a pessimistic transaction it takes a shared lock on every key, so another
    transaction's write to any of them is refused;
  - a key that isn't a string is refused.
"""
import os
import shutil
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14931)
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


DATA = os.path.join(os.getcwd(), "getbuffers_data")
shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)
log = open(os.path.join(os.getcwd(), "getbuffers.log"), "a")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000", "-c", "tx_lock_wait_ms=0"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "gbs" + str(int(time.time()))


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=30, single_connection_client=True)
    c.execute_command("USE", SPACE)
    return c


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


# each slot as a string: its bytes, "tomb" or "false", so a reply shows the array's shape
SHOW = """
    local function show(got)
        local out = {}
        for i = 1, #got do
            local v = got[i]
            if v == false then out[i] = "false"
            elseif v == barch.tomb then out[i] = "tomb"
            else out[i] = buffer.tostring(v) end
        end
        return out
    end
"""

FUNCTIONS = {
    "many": SHOW + """
        function call(off, ...)
            return show(barch.store.getBuffers({...}, tonumber(off)))
        end
    """,
    "one": """
        function call(off, ...)
            local out = {}
            for i, k in {...} do
                local v = barch.store.getBufferAt(k, tonumber(off))
                out[i] = v == nil and "false" or v == barch.tomb and "tomb" or buffer.tostring(v)
            end
            return out
        end
    """,
    "handle": SHOW + """
        function call(...)
            return show(barch.current():getBuffers({...}))
        end
    """,
    "secrets": SHOW + """
        function call(...)
            return show(barch.space.configuration:getBuffers({...}))
        end
    """,
    "put": "function call(k, s) barch.store.setBufferAt(k, buffer.fromstring(s), 0) return 'ok' end\n",
    "put_read": SHOW + """
        function call(k, s, ...)
            local keys = {...}
            return barch.tx.run(function()
                barch.store.setBufferAt(k, buffer.fromstring(s), 0)
                return show(barch.store.getBuffers(keys))
            end)
        end
    """,
    "notstring": """
        function call()
            local ok, err = pcall(function() return barch.store.getBuffers({"a", {}}) end)
            return ok and "accepted" or "refused"
        end
    """,
}

try:
    c = client()
    for name, body in FUNCTIONS.items():
        got = answer(c, "SETF", name, body)
        check(got == b"OK", "SETF %s (%s)" % (name, str(got)[:60]))
    c.set("a", "alpha")
    c.set("b", "be")
    c.delete("none")

    print("outside a transaction")
    keys = ["a", "none", "b", "a"]
    for off in (0, 2, 3):
        got = answer(c, "CALLF", "many", off, *keys)
        want = answer(c, "CALLF", "one", off, *keys)
        check(isinstance(got, list) and got == want, "offset %d is getBufferAt's answer per key (%s)" % (off, got))
    got = answer(c, "CALLF", "many", 0, *keys)
    check(isinstance(got, list) and len(got) == len(keys), "one slot per key, a missing one included")
    c.execute_command("configuration:SET", "gbs.endpoint", "http://here")
    c.execute_command("configuration:SET", "gbs.password", "hunter2")
    got = answer(c, "CALLF", "secrets", "gbs.endpoint", "gbs.password", "gbs.endpoint")
    check(got == [b"http://here", b"false", b"http://here"], "a configuration secret reads as false (%s)" % got)
    check(answer(c, "CALLF", "many", 0) == [], "no keys, an empty array")
    got = answer(c, "CALLF", "handle", "b", "none", "a")
    check(got == [b"be", b"false", b"alpha"], "a space handle has it (%s)" % got)
    check(answer(c, "CALLF", "notstring") == b"refused", "a key that isn't a string is refused")

    print("in a transaction")
    got = answer(c, "CALLF", "put_read", "b", "new", "a", "b")
    check(got == [b"alpha", b"new"], "it sees the transaction's own write (%s)" % got)
    c.set("b", "be")
    c.execute_command("BEGINTX")
    answer(c, "CALLF", "put", "a", "inside")
    got = answer(c, "CALLF", "many", 0, "a", "b")
    check(got == [b"inside", b"be"], "a client's transaction too (%s)" % got)
    check(client().get("a") == b"alpha", "another client doesn't see it")
    answer(c, "ABORTTX")
    check(answer(c, "CALLF", "many", 0, "a") == [b"alpha"], "and after ABORTTX it's gone")

    c.execute_command("BEGINTX")
    got = answer(c, "CALLF", "many", 0, "a", "b")
    answer(c, "CALLF", "put", "x", "written")
    client().set("b", "beneath")
    got = answer(c, "COMMITTX")
    check("TXCONFLICT" in str(got), "a key changed beneath its read: TXCONFLICT (%s)" % str(got)[:60])
    check(client().get("b") == b"beneath" and client().get("x") is None, "and the other client's write stands")

    print("in a pessimistic transaction")
    holder, other = client(), client()
    holder.execute_command("BEGINTX", "PESSIMISTIC")
    got = answer(holder, "CALLF", "many", 0, "a", "b")
    check(got == [b"alpha", b"beneath"], "it reads (%s)" % got)
    other.execute_command("BEGINTX", "PESSIMISTIC")
    got = answer(other, "SET", "b", "taken")
    check(str(got).startswith("ERR"), "another transaction's write to its last key is refused (%s)" % str(got)[:60])
    answer(other, "ABORTTX")
    other.execute_command("BEGINTX", "PESSIMISTIC")
    got = answer(other, "GET", "a")
    check(got == b"alpha", "a read of one of its keys isn't (%s)" % got)
    answer(other, "ABORTTX")
    check(answer(holder, "COMMITTX") is not None, "COMMITTX")
    check(client().get("b") == b"beneath", "the refused write never landed")
finally:
    p.terminate()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
