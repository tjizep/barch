#!/usr/bin/env python3
"""
GRAPH in a transaction - TODO 750.

GRAPH runs on the transaction's overlay like the string commands do: it sees its own
writes, nobody else does until COMMITTX, and ABORTTX drops them. Its node and inode ids
come from the space itself, so a writer outside the transaction can't be handed the same
ones. In a BEGINTX PESSIMISTIC every GRAPH locks the whole graph, shared, and a write
locks it exclusive too.

Checked against one barchd with no Raft, tx_lock_wait_ms 1500:
  - optimistic: its own writes are seen inside, not outside, and land at COMMITTX;
    every subcommand runs; ABORTTX drops the lot;
  - ids: a transaction's PUT and many plain MKDIRs into another directory while
    it's open: it commits, and each keeps its own ids;
  - optimistic conflicts: a plain write into a directory it listed, and two making
    the same path, each give TXCONFLICT;
  - pessimistic: a plain GRAPH write waits for COMMITTX, or gets TXLOCKTIMEOUT, while
    a plain GRAPH read doesn't wait;
  - a pessimistic writer holds up another transaction's GRAPH read, which then sees it;
  - an optimistic GRAPH commit gets TXCONFLICT while a pessimistic one holds the graph;
  - two pessimistic readers that both go on to write: one TXDEADLOCK;
  - several parked GRAPH writers all go on after ABORTTX.
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
PORT = scale.port(default=14839)
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
DATA = os.path.join(os.getcwd(), "txgraph_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txgraph.log"), "a")
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

SPACE = "txgraph_" + str(int(time.time()))


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
    """the names off GRAPH LS lines"""
    return sorted(line.split(" ", 4)[4] for line in listing) if isinstance(listing, list) else listing


try:
    watch, a, b, plain = client(), client(), client(), client()
    run(watch, "GRAPH", "MKDIR", "/d")
    run(watch, "GRAPH", "PUT", "/d/old", "before")

    print("optimistic: its own writes")
    check(run(a, "BEGINTX") == "OK", "BEGINTX")
    got = run(a, "GRAPH", "PUT", "/d/f", "hello")
    check(got == "OK", "GRAPH PUT is taken (%s)" % str(got)[:70])
    check(run(a, "GRAPH", "MKDIR", "/d/sub") == "OK", "MKDIR")
    check(run(a, "GRAPH", "PUT", "/d/sub/g", "deep") == "OK", "PUT under it")
    check(run(a, "GRAPH", "GET", "/d/f") == "hello", "it reads its own PUT")
    check(names(run(a, "GRAPH", "LS", "/d")) == ["f", "old", "sub"], "and lists it")
    check(run(watch, "GRAPH", "GET", "/d/f") is None, "nobody else sees it")
    check(names(run(watch, "GRAPH", "LS", "/d")) == ["old"], "  nor lists it")
    stat = run(a, "GRAPH", "STAT", "/d/f")
    check(isinstance(stat, str) and "leaf" in stat, "STAT (%s)" % stat)
    check(run(a, "GRAPH", "CP", "/d/f", "/d/copy") == "OK", "CP")
    check(run(a, "GRAPH", "MV", "/d/copy", "/d/moved") == "OK", "MV")
    check(run(a, "GRAPH", "RM", "/d/old") == "OK", "RM")
    walk = run(a, "GRAPH", "BFS", "/d")
    check(isinstance(walk, list) and any(w.endswith("/d/sub/g") for w in walk), "BFS walks it (%s)" % walk)
    walk = run(a, "GRAPH", "DFS", "/d")
    check(isinstance(walk, list) and any(w.endswith("/d/moved") for w in walk), "DFS too")
    check(run(watch, "GRAPH", "GET", "/d/old") == "before", "the RM isn't seen outside")
    check(run(a, "COMMITTX") == "OK", "COMMITTX")
    check(names(run(watch, "GRAPH", "LS", "/d")) == ["f", "moved", "sub"], "then it's all there")
    check(run(watch, "GRAPH", "GET", "/d/moved") == "hello" and run(watch, "GRAPH", "GET", "/d/sub/g") == "deep",
          "  with its bytes")

    print("ABORTTX")
    run(a, "BEGINTX")
    run(a, "GRAPH", "PUT", "/d/gone", "x")
    run(a, "GRAPH", "RM", "/d/f")
    check(run(a, "ABORTTX") == "OK", "ABORTTX")
    check(names(run(watch, "GRAPH", "LS", "/d")) == ["f", "moved", "sub"], "drops its writes")

    print("ids")
    # a fresh space, so its id block starts small: the transaction's PUT and the plain
    # MKDIRs after it need ids from past the first block
    ids_space = SPACE + "_ids"
    iw, ia = client(ids_space), client(ids_space)
    run(iw, "GRAPH", "MKDIR", "/a")
    run(iw, "GRAPH", "MKDIR", "/b")
    run(ia, "BEGINTX")
    # into another directory, so the plain writes don't conflict with what it read
    run(ia, "GRAPH", "PUT", "/b/tx", "from the transaction")
    for i in range(80):
        run(iw, "GRAPH", "MKDIR", "/a/p%02d" % i)
    got = run(ia, "COMMITTX")
    check(got == "OK", "COMMITTX after 80 plain MKDIRs (%s)" % str(got)[:60])
    listed = run(iw, "GRAPH", "LS", "/a")
    check(isinstance(listed, list) and len(listed) == 80, "all 80 MKDIRs are there (%d)"
          % (len(listed) if isinstance(listed, list) else -1))
    check(names(run(iw, "GRAPH", "LS", "/b")) == ["tx"], "and the PUT")
    lines = (listed if isinstance(listed, list) else []) + (run(iw, "GRAPH", "LS", "/b") or [])
    edges = [line.split(" ")[1] for line in lines]
    nodes = [line.split(" ")[2] for line in lines]
    check(len(set(edges + nodes)) == 2 * len(lines), "each with its own node and edge ids")
    check(run(iw, "GRAPH", "GET", "/b/tx") == "from the transaction", "and the PUT's bytes are its own")
    for i in range(40):
        run(iw, "GRAPH", "PUT", "/a/f%02d" % i, "plain %d" % i)
    check(run(iw, "GRAPH", "GET", "/b/tx") == "from the transaction"
          and run(iw, "GRAPH", "GET", "/a/f39") == "plain 39", "plain PUTs after don't land on it")

    print("optimistic conflicts")
    run(a, "BEGINTX")
    run(a, "GRAPH", "LS", "/d")
    run(plain, "GRAPH", "PUT", "/d/sneak", "1")
    run(a, "GRAPH", "PUT", "/d/mine", "1")
    got = run(a, "COMMITTX")
    check(str(got).startswith("ERROR TXCONFLICT"), "a plain write into what it listed (%s)" % str(got)[:60])
    run(a, "BEGINTX")
    run(b, "BEGINTX")
    run(a, "GRAPH", "MKDIR", "/same")
    run(b, "GRAPH", "MKDIR", "/same")
    first, second = run(a, "COMMITTX"), run(b, "COMMITTX")
    check(first == "OK" and str(second).startswith("ERROR TXCONFLICT"), "two making one path (%s)" % str(second)[:60])
    check(names(run(watch, "GRAPH", "LS", "/")).count("same") == 1, "  and there's one of it")

    print("pessimistic: plain GRAPH writes wait")
    run(a, "BEGINTX", "PESSIMISTIC")
    check(isinstance(run(a, "GRAPH", "LS", "/d"), list), "LS in one")
    t0 = time.time()
    check(isinstance(run(watch, "GRAPH", "LS", "/d"), list) and time.time() - t0 < HELD, "a plain read doesn't wait")
    out = {}
    t = later(plain, out, "put", "GRAPH", "PUT", "/d/waited", "1")
    check(waiting(t, out, "put"), "a plain GRAPH PUT waits")
    again = names(run(a, "GRAPH", "LS", "/d"))
    check("waited" not in again, "it doesn't see it yet")
    committed = time.time()
    check(run(a, "COMMITTX") == "OK", "COMMITTX")
    t.join(10)
    check(out.get("put") == "OK" and out.get("put_at", 0) >= committed, "then the PUT goes on (%s)" % out.get("put"))
    run(a, "BEGINTX", "PESSIMISTIC")
    run(a, "GRAPH", "STAT", "/d")
    t0 = time.time()
    got = run(plain, "GRAPH", "MKDIR", "/late")
    took = time.time() - t0
    check(str(got).startswith("ERROR TXLOCKTIMEOUT") and took >= WAIT_MS / 1000.0 * 0.9,
          "TXLOCKTIMEOUT after tx_lock_wait_ms (%.1fs, %s)" % (took, str(got)[:60]))
    run(a, "ABORTTX")
    t0 = time.time()
    check(run(plain, "GRAPH", "MKDIR", "/late") == "OK" and time.time() - t0 < HELD, "which ABORTTX lets go")
    t0 = time.time()
    run(a, "BEGINTX", "PESSIMISTIC")
    run(a, "GRAPH", "LS", "/d")
    got = run(plain, "SET", "graph:n:plain", "1")
    check(str(got).startswith("ERROR TXLOCKTIMEOUT"), "  a SET into the graph's keys waits too (%s)" % str(got)[:50])
    run(a, "ABORTTX")

    print("pessimistic writer, another transaction reading")
    run(a, "BEGINTX", "PESSIMISTIC")
    check(run(a, "GRAPH", "PUT", "/d/pess", "w") == "OK", "PUT in one")
    run(b, "BEGINTX", "PESSIMISTIC")
    out = {}
    t = later(b, out, "ls", "GRAPH", "LS", "/d")
    check(waiting(t, out, "ls"), "another's LS waits")
    got = run(a, "COMMITTX")
    check(got == "OK", "the writer commits (%s)" % str(got)[:60])
    t.join(10)
    check("pess" in names(out.get("ls", [])), "then the LS sees it (%s)" % names(out.get("ls")))
    run(b, "COMMITTX")

    print("an optimistic GRAPH commit")
    o = client()
    run(a, "BEGINTX", "PESSIMISTIC")
    run(a, "GRAPH", "LS", "/")
    run(o, "BEGINTX")
    run(o, "GRAPH", "MKDIR", "/opt")
    got = run(o, "COMMITTX")
    check(str(got).startswith("ERROR TXCONFLICT"), "TXCONFLICT while one holds the graph (%s)" % str(got)[:60])
    run(a, "COMMITTX")
    run(o, "BEGINTX")
    run(o, "GRAPH", "MKDIR", "/opt")
    check(run(o, "COMMITTX") == "OK", "after it, it commits")

    print("a ring")
    run(a, "BEGINTX", "PESSIMISTIC")
    run(b, "BEGINTX", "PESSIMISTIC")
    run(a, "GRAPH", "LS", "/")
    run(b, "GRAPH", "LS", "/")
    out = {}
    ta = later(a, out, "a", "GRAPH", "MKDIR", "/ra")
    time.sleep(0.1)
    tb = later(b, out, "b", "GRAPH", "MKDIR", "/rb")
    ta.join(10)
    tb.join(10)
    said = [str(out.get("a")), str(out.get("b"))]
    dead = [s for s in said if "TXDEADLOCK" in s]
    check(len(dead) == 1 and "OK" in said, "one TXDEADLOCK and one goes on (%s)" % [s[:50] for s in said])
    run(a, "COMMITTX")
    run(b, "ABORTTX")

    print("several parked GRAPH writers")
    run(a, "BEGINTX", "PESSIMISTIC")
    run(a, "GRAPH", "LS", "/")
    writers = [client() for _ in range(4)]
    out = {}
    ts = [later(w, out, "w%d" % i, "GRAPH", "MKDIR", "/park%d" % i) for i, w in enumerate(writers)]
    time.sleep(HELD)
    check(not any(("w%d" % i) in out for i in range(4)), "all wait")
    run(a, "ABORTTX")
    for t in ts:
        t.join(10)
    check(all(out.get("w%d" % i) == "OK" for i in range(4)), "and all go on after ABORTTX (%s)" % out)
    check(all("park%d" % i in names(run(watch, "GRAPH", "LS", "/")) for i in range(4)), "  and are there")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
