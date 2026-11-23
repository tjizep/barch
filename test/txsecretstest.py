"""A transaction's reads on configuration hide secrets as a plain call's do - TODO 963.

A key ending in "password" reads as absent through barch.store in the configuration
space. In a transaction the store is built on the overlay, which isn't named
configuration, and only the cursor hid them. What has to hold: each read below answers
inside a transaction what it answers outside one, and neither answer holds the secret.
"""
import os
import shutil
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=19780)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-70s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1
    return ok


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def short(v):
    return str(v)[:90]


SHOW = """
local function show(v)
    if v == nil then return "nil" end
    if v == false then return "false" end
    if v == barch.tomb then return "tomb" end
    if type(v) == "buffer" then return buffer.tostring(v) end
    return tostring(v)
end
local function guarded(f, ...)
    local ok, got = pcall(f, ...)
    if ok then return got end
    return {"error: " .. tostring(got)}
end
"""

FUNCTIONS = {
    "get": SHOW + """
        function call(lo, hi, ...)
            return guarded(function(...)
                local out = {}
                for i, k in {...} do out[i] = show(barch.store.get(k)) end
                return out
            end, ...)
        end
    """,
    "getJson": SHOW + """
        function call(lo, hi, ...)
            return guarded(function(...)
                local out = {}
                for i, k in {...} do out[i] = show(barch.store.getJson(k)) end
                return out
            end, ...)
        end
    """,
    "getBufferAt": SHOW + """
        function call(lo, hi, ...)
            return guarded(function(...)
                local out = {}
                for i, k in {...} do out[i] = show(barch.store.getBufferAt(k, 0)) end
                return out
            end, ...)
        end
    """,
    "getBuffers": SHOW + """
        function call(lo, hi, ...)
            return guarded(function(...)
                local out = {}
                for i, v in barch.store.getBuffers({...}) do out[i] = show(v) end
                return out
            end, ...)
        end
    """,
    "rangeValues": SHOW + """
        function call(lo, hi)
            return guarded(function()
                local ks, vs = barch.store.rangeValues(lo, hi, 100)
                local out = {}
                for i, k in ks do out[i] = show(k) .. "=" .. show(vs[i]) end
                return out
            end)
        end
    """,
    "walk": SHOW + """
        function call(lo, hi)
            return guarded(function()
                local out = {}
                for row in barch.current() do
                    local k = show(row.key)
                    if k >= lo and k < hi then table.insert(out, k .. "=" .. show(row.value)) end
                end
                return out
            end)
        end
    """,
    "incrInt64ByAt": SHOW + """
        function call(lo, hi, ...)
            return guarded(function(...)
                local out = {}
                for i, k in {...} do out[i] = show(barch.store.incrInt64ByAt(k, 0, 1)) end
                return out
            end, ...)
        end
    """,
}

DATA = os.path.join(os.getcwd(), "txsecrets_data")
shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)
log = open(os.path.join(os.getcwd(), "txsecrets.log"), "a")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
BASE = "txsecrets" + str(int(time.time()))
# a number, so getJson parses it and an increment that reached it would give it away
SECRET = "41"
SHOWN = '"http://here"'  # JSON too, so getJson has something to answer


def client(space):
    c = redis.Redis(host="127.0.0.1", port=PORT, single_connection_client=True,
                    socket_timeout=None, retry=None)
    c.execute_command("USE", space)
    return c


try:
    scale.wait_for_port(PORT, proc=p)
    c = client("configuration")
    c.execute_command("SET", BASE + ".endpoint", SHOWN)
    c.execute_command("SET", BASE + ".password", SECRET)
    for name, body in FUNCTIONS.items():
        got = answer(c, "SETF", name, body)
        check(got in (b"OK", "OK"), "SETF %s (%s)" % (name, short(got)))
    for name in FUNCTIONS:
        # an increment only asks for the secret: the endpoint's write isn't the question
        keys = (BASE + ".password",) if name == "incrInt64ByAt" else (BASE + ".endpoint", BASE + ".password")
        outside = answer(c, "CALLF", name, BASE, BASE + "~", *keys)
        c.execute_command("BEGINTX")
        inside = answer(c, "CALLF", name, BASE, BASE + "~", *keys)
        answer(c, "ABORTTX")
        shown = [v.decode() if isinstance(v, bytes) else str(v) for v in outside] \
            if isinstance(outside, list) else [str(outside)]
        check(not any(v == SECRET or v.endswith("=" + SECRET) for v in shown) and
              (name == "incrInt64ByAt" or any("http://here" in v for v in shown)),
              "%s outside a transaction reads no secret (%s)" % (name, short(outside)))
        check(inside == outside, "%s in a transaction answers the same (%s)" % (name, short(inside)))
    got = c.execute_command("GET", BASE + ".password")
    check(got == SECRET.encode(), "the secret is still there, as it was (%s)" % short(got))
finally:
    p.terminate()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
