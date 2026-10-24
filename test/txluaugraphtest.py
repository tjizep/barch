#!/usr/bin/env python3
"""
barch.graph in a transaction, the same as GRAPH - TODO 756.

barch.graph used to be a second copy of GRAPH, written against the graph store
directly, so when GRAPH learned transactions (TODO 750) barch.graph didn't, and was
refused inside one. Now each barch.graph function turns into a GRAPH command and runs
the way barch.call runs it, so what holds for GRAPH holds here too. This runs the
transaction cases of txgraphtest.py through stored functions:
  - the rows barch.graph.list and stat hand back say what GRAPH LS and STAT say;
  - optimistic: its own writes are seen inside, not outside, and land at COMMITTX;
    every function runs; ABORTTX drops the lot;
  - a plain write into a directory it listed gives TXCONFLICT;
  - pessimistic: a plain GRAPH write waits for COMMITTX while a function's read holds
    the graph, and a function's write waits (parked) for a GRAPH read, then goes on;
  - a function's plain write that waits too long gets TXLOCKTIMEOUT;
  - barch.call("GRAPH", ...) in a transaction answers the same as barch.graph.
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
PORT = scale.port(default=14849)
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
HELD = 0.4
DATA = os.path.join(os.getcwd(), "txluaugraph_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txluaugraph.log"), "a")
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

SPACE = "txluaugraph_" + str(int(time.time()))

# each one a barch.graph function, answering in GRAPH's own shape so the two compare
FUNCTIONS = {
    "gls": """function call(p)
        local out = {}
        for _, e in ipairs(barch.graph.list(p)) do
            out[#out + 1] = (e.dir and 'dir' or 'leaf') .. ' ' .. e.edge .. ' ' .. e.id .. ' ' .. e.refs .. ' ' .. e.name
        end
        return out
    end""",
    "gstat": """function call(p)
        local e = barch.graph.stat(p)
        if e == nil then return nil end
        local s = 'path=' .. e.path .. ' kind=' .. (e.dir and 'dir' or 'leaf') .. ' id=' .. e.id .. ' refs=' .. e.refs
        if e.path ~= '/' then s = s .. ' edge=' .. e.edge end
        if not e.dir then
            s = s .. ' size=' .. e.size .. ' chunk=' .. e.chunk .. ' chunks=' .. e.chunks
                  .. ' version=' .. e.version .. ' type=' .. e.type
        end
        return s
    end""",
    "gget": "function call(p) return barch.graph.get(p) end",
    "gput": "function call(p, v) barch.graph.put(p, v) return 'OK' end",
    "gmkdir": "function call(p) barch.graph.mkdir(p) return 'OK' end",
    "grm": "function call(p) barch.graph.remove(p) return 'OK' end",
    "gmv": "function call(a, b) barch.graph.rename(a, b) return 'OK' end",
    "gcp": "function call(a, b) barch.graph.copy(a, b) return 'OK' end",
    "glink": "function call(id, p) barch.graph.link(tonumber(id), p) return 'OK' end",
    "gunlink": "function call(p) barch.graph.unlink(p) return 'OK' end",
    "gbfs": """function call(p)
        local out = {}
        for _, e in ipairs(barch.graph.bfs(p)) do out[#out + 1] = e.id .. ' ' .. e.path end
        return out
    end""",
    "gdfs": """function call(p)
        local out = {}
        for _, e in ipairs(barch.graph.dfs(p)) do out[#out + 1] = e.id .. ' ' .. e.path end
        return out
    end""",
    "gcall": "function call(...) return barch.call('GRAPH', ...) end",
}


def client(space=SPACE):
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True, retry=redis.retry.Retry(redis.backoff.NoBackoff(), 0))
    c.execute_command("USE", space)
    return c


def run(c, *cmd):
    """the answer, or the error's text"""
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERROR " + str(e)


def lua(c, fn, *args):
    """a barch.graph function's answer; an empty list comes back as nil"""
    got = run(c, "CALLF", fn, *args)
    return [] if got is None and fn in ("gls", "gbfs", "gdfs") else got


def later(c, out, name, *cmd):
    def go():
        out[name] = run(c, *cmd)
        out[name + "_at"] = time.time()
    t = threading.Thread(target=go)
    t.start()
    return t


def waiting(t, out, name):
    t.join(HELD)
    return t.is_alive() and name not in out


def names(listing):
    """the names off LS lines"""
    return sorted(line.split(" ", 4)[4] for line in listing) if isinstance(listing, list) else listing


try:
    watch, a, b, plain = client(), client(), client(), client()
    for name, body in FUNCTIONS.items():
        check(run(watch, "SETF", name, body) == "OK", "SETF %s" % name)
    run(watch, "GRAPH", "MKDIR", "/d")
    run(watch, "GRAPH", "PUT", "/d/old", "before")

    print("the same rows as GRAPH")
    check(lua(watch, "gls", "/d") == run(watch, "GRAPH", "LS", "/d"), "list is LS (%s)" % lua(watch, "gls", "/d"))
    for path in ("/", "/d", "/d/old"):
        check(lua(watch, "gstat", path) == run(watch, "GRAPH", "STAT", path), "stat %s is STAT" % path)
    check(lua(watch, "gbfs", "/") == run(watch, "GRAPH", "BFS", "/"), "bfs is BFS")
    check(lua(watch, "gdfs", "/") == run(watch, "GRAPH", "DFS", "/"), "dfs is DFS")

    print("optimistic: its own writes")
    check(run(a, "BEGINTX") == "OK", "BEGINTX")
    got = lua(a, "gput", "/d/f", "hello")
    check(got == "OK", "barch.graph.put is taken (%s)" % str(got)[:70])
    check(lua(a, "gmkdir", "/d/sub") == "OK", "mkdir")
    check(lua(a, "gput", "/d/sub/g", "deep") == "OK", "put under it")
    check(lua(a, "gget", "/d/f") == "hello", "it reads its own put")
    check(run(a, "GRAPH", "GET", "/d/f") == "hello", "  and so does GRAPH in the transaction")
    check(names(lua(a, "gls", "/d")) == ["f", "old", "sub"], "and lists it")
    check(lua(a, "gls", "/d") == run(a, "GRAPH", "LS", "/d"), "  the same as LS does")
    check(lua(a, "gstat", "/d/f") == run(a, "GRAPH", "STAT", "/d/f"), "  and stat the same as STAT")
    check(run(watch, "GRAPH", "GET", "/d/f") is None and lua(watch, "gget", "/d/f") is None, "nobody else sees it")
    check(names(lua(watch, "gls", "/d")) == ["old"], "  nor lists it")
    check(lua(a, "gcp", "/d/f", "/d/copy") == "OK", "copy")
    check(lua(a, "gmv", "/d/copy", "/d/moved") == "OK", "rename")
    check(lua(a, "grm", "/d/old") == "OK", "remove")
    fstat = str(lua(a, "gstat", "/d/f"))
    fid = fstat.split(" id=")[1].split(" ")[0] if " id=" in fstat else "0"
    check(lua(a, "glink", fid, "/d/sub/again") == "OK", "link")
    check(lua(a, "gunlink", "/d/sub/again") == "OK", "unlink")
    walk = lua(a, "gbfs", "/d")
    check(any(w.endswith("/d/sub/g") for w in walk) and walk == run(a, "GRAPH", "BFS", "/d"), "bfs walks it")
    check(lua(a, "gdfs", "/d") == run(a, "GRAPH", "DFS", "/d"), "dfs too")
    check(lua(a, "gcall", "LS", "/d") == run(a, "GRAPH", "LS", "/d"), "barch.call('GRAPH') answers the same")
    check(run(watch, "GRAPH", "GET", "/d/old") == "before", "the remove isn't seen outside")
    check(run(a, "COMMITTX") == "OK", "COMMITTX")
    check(names(run(watch, "GRAPH", "LS", "/d")) == ["f", "moved", "sub"], "then it's all there")
    check(run(watch, "GRAPH", "GET", "/d/moved") == "hello" and run(watch, "GRAPH", "GET", "/d/sub/g") == "deep",
          "  with its bytes")

    print("ABORTTX")
    run(a, "BEGINTX")
    lua(a, "gput", "/d/gone", "x")
    lua(a, "grm", "/d/f")
    check(run(a, "ABORTTX") == "OK", "ABORTTX")
    check(names(run(watch, "GRAPH", "LS", "/d")) == ["f", "moved", "sub"], "drops its writes")

    print("an optimistic conflict")
    run(a, "BEGINTX")
    lua(a, "gls", "/d")
    run(plain, "GRAPH", "PUT", "/d/sneak", "1")
    lua(a, "gput", "/d/mine", "1")
    got = run(a, "COMMITTX")
    check(str(got).startswith("ERROR TXCONFLICT"), "a plain write into what it listed (%s)" % str(got)[:60])

    print("pessimistic")
    run(a, "BEGINTX", "PESSIMISTIC")
    check(isinstance(lua(a, "gls", "/d"), list), "list in one")
    out = {}
    t = later(plain, out, "put", "GRAPH", "PUT", "/d/waited", "1")
    check(waiting(t, out, "put"), "a plain GRAPH PUT waits")
    check("waited" not in names(lua(a, "gls", "/d")), "it doesn't see it yet")
    committed = time.time()
    check(run(a, "COMMITTX") == "OK", "COMMITTX")
    t.join(10)
    check(out.get("put") == "OK" and out.get("put_at", 0) >= committed, "then the PUT goes on (%s)" % out.get("put"))

    run(a, "BEGINTX", "PESSIMISTIC")
    run(a, "GRAPH", "LS", "/d")
    out = {}
    t = later(plain, out, "put", "CALLF", "gput", "/d/parked", "1")
    check(waiting(t, out, "put"), "a function's plain put waits for GRAPH LS in one")
    committed = time.time()
    check(run(a, "COMMITTX") == "OK", "COMMITTX")
    t.join(10)
    check(out.get("put") == "OK" and out.get("put_at", 0) >= committed, "then it goes on (%s)" % out.get("put"))
    check(run(watch, "GRAPH", "GET", "/d/parked") == "1", "  and stands")

    run(a, "BEGINTX", "PESSIMISTIC")
    lua(a, "gstat", "/d")
    t0 = time.time()
    got = lua(plain, "gmkdir", "/late")
    took = time.time() - t0
    check("TXLOCKTIMEOUT" in str(got) and took >= WAIT_MS / 1000.0 * 0.9,
          "TXLOCKTIMEOUT after tx_lock_wait_ms (%.1fs, %s)" % (took, str(got)[:60]))
    run(a, "ABORTTX")
    check(lua(plain, "gmkdir", "/late") == "OK", "which ABORTTX lets go")

    run(a, "BEGINTX", "PESSIMISTIC")
    check(lua(a, "gput", "/d/pess", "w") == "OK", "a put in one")
    run(b, "BEGINTX", "PESSIMISTIC")
    out = {}
    t = later(b, out, "ls", "CALLF", "gls", "/d")
    check(waiting(t, out, "ls"), "another's list waits")
    check(run(a, "COMMITTX") == "OK", "the writer commits")
    t.join(10)
    check("pess" in names(out.get("ls") or []), "then the list sees it (%s)" % names(out.get("ls")))
    run(b, "COMMITTX")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
