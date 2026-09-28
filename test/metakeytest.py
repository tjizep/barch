# The fs id counter is a meta key: no client sees it or changes it, and it goes
# wherever the data goes - TODO 527.
#
# It used to be a plain key, `ids:fs`. A client could DEL or SET it and eviction
# could take it (TODO 528), and then the next file got an id that was still live
# and wrote over another file's inode and chunks. Now it's kept under art::tmeta,
# which no command's key can be, and walks, eviction and compression leave alone.
#
# Every check below comes down to the same thing: a file written later never
# overwrites one written earlier. The ids are handed out from an in-memory block,
# so the counter itself is only read again after a restart - which is why most
# checks restart before they write.
import os
import shutil
import signal
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14527)
OTHER = PORT + 1
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "metakey_data")
OTHER_DATA = os.path.join(os.getcwd(), "metakey_other")
EXPORTED = os.path.join(os.getcwd(), "metakey.export")
ARGS = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000"]
SPACE = "mk"

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def start(port, data):
    p = subprocess.Popen([BINARY, "--port", str(port), "--bind", "127.0.0.1", "--dir", data,
                          "--no-save-on-exit"] + ARGS,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    end = time.time() + 60
    while True:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            return p
        except OSError:
            if time.time() > end or p.poll() is not None:
                raise AssertionError("barchd did not start")
            time.sleep(0.1)


def kill(p):
    if p is not None and p.poll() is None:
        p.send_signal(signal.SIGKILL)
        p.wait(timeout=30)


def client(port):
    return redis.Redis(host="127.0.0.1", port=port, protocol=2, socket_timeout=60)


def cmd(r, *args):
    return r.execute_command(SPACE + ":" + args[0], *args[1:])


written = {}


def put(r, path):
    body = "body of %s, %d" % (path, len(written))
    cmd(r, "FS", "PUT", path, body)
    written[path] = body.encode()


def intact(r):
    """paths whose body isn't what was written"""
    return [p for p, b in written.items() if cmd(r, "FS", "GET", p) != b]


for d in (DATA, OTHER_DATA):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
if os.path.exists(EXPORTED):
    os.remove(EXPORTED)

server = other = None
try:
    server = start(PORT, DATA)
    r = client(PORT)
    # one shard, so the eviction below can empty the space of plain keys (TODO 530)
    r.execute_command("configuration:SET", SPACE + ".shards", "1")
    r.execute_command("configuration:SAVE")

    print("the counter is hidden and can't be touched", flush=True)
    for i in range(10):
        put(r, "/f%d" % i)
    check(cmd(r, "GET", "ids:fs") is None, "GET ids:fs finds nothing")
    keys = [k.decode(errors="replace") for k in cmd(r, "KEYS", "*")]
    check(keys and all(k.startswith("fs:") and k != "fs:layout" for k in keys),
          "KEYS * shows only the files' keys (%d, e.g. %s)" % (len(keys), keys[:3]))
    cursor, scanned = 0, []
    while True:
        cursor, batch = cmd(r, "SCAN", cursor, "COUNT", 100)
        scanned += [k.decode(errors="replace") for k in batch]
        if int(cursor) == 0:
            break
    check(sorted(scanned) == sorted(keys), "SCAN shows the same")
    top = cmd(r, "MAX")
    check(top is not None and top.decode(errors="replace").startswith("fs:"),
          "MAX is a file's key (%r)" % top)
    randoms = [cmd(r, "RANDOMKEY") for _ in range(50)]
    check(all(k is not None and k.decode(errors="replace").startswith("fs:") for k in randoms),
          "RANDOMKEY answers with a file's key, 50 of 50")
    check(cmd(r, "DEL", "ids:fs") == 0, "DEL ids:fs removes nothing")
    cmd(r, "SET", "ids:fs", "1")            # a plain key by that name, behind the counter
    # restarted, so the counter is read again rather than served from the block
    cmd(r, "SAVE")
    kill(server)
    server = start(PORT, DATA)
    r = client(PORT)
    put(r, "/g0")
    bad = intact(r)
    check(not bad, "after a plain SET ids:fs 1 and a restart, a new file overwrites nothing (%s)"
          % bad[:3])

    print("the layout markers are meta keys too", flush=True)
    check(cmd(r, "GRAPH", "MKDIR", "/n") == b"OK"
          and cmd(r, "GRAPH", "PUT", "/n/leaf", "graph bytes") == b"OK", "a graph node is written")
    keys = [k.decode(errors="replace") for k in cmd(r, "KEYS", "*")]
    check("fs:layout" not in keys and "graph:layout" not in keys,
          "KEYS * shows neither layout marker")
    check(cmd(r, "GET", "fs:layout") is None and cmd(r, "GET", "graph:layout") is None,
          "GET finds neither")
    # a plain marker still counts, and the more careful answer wins: a layout this
    # build doesn't know is refused, as it would be from an older or newer store
    cmd(r, "SET", "graph:layout", "9")
    try:
        cmd(r, "GRAPH", "MKDIR", "/nine")
        refused = False
    except redis.exceptions.ResponseError:
        refused = True
    check(refused, "a plain graph:layout 9 is still refused")
    cmd(r, "DEL", "graph:layout")
    check(cmd(r, "GRAPH", "MKDIR", "/n/ok") == b"OK", "and with it gone, the meta marker is back in charge")
    # the file store reads its marker the same way - TODO 537. It used to be
    # written and never read, so a write went ahead and stamped "2" over it
    cmd(r, "SET", "fs:layout", "9")
    try:
        cmd(r, "FS", "PUT", "/nine", "x")
        refused = False
    except redis.exceptions.ResponseError:
        refused = True
    check(refused, "a plain fs:layout 9 is refused too")
    check(cmd(r, "GET", "fs:layout") == b"9", "and the refused write left the marker alone")
    cmd(r, "DEL", "fs:layout")
    put(r, "/f-after-nine")
    check(not intact(r), "with it gone, files are written again")

    print("the counter survives a restart", flush=True)
    cmd(r, "SAVE")
    kill(server)
    server = start(PORT, DATA)
    r = client(PORT)
    put(r, "/g1")
    bad = intact(r)
    check(not bad, "after SAVE and kill -9, a new file overwrites nothing (%s)" % bad[:3])
    check(cmd(r, "GRAPH", "GET", "/n/leaf") == b"graph bytes", "and the graph reads as it was")

    print("the counter survives eviction", flush=True)
    cmd(r, "KSPACE", "OPTION", "SET", "LRU", "ON")
    for i in range(500):
        cmd(r, "SET", "plain%d" % i, "x" * 64)
    r.execute_command("CONFIG", "SET", "maxmemory", "4096")
    deadline = time.time() + 60
    left = 500
    while time.time() < deadline:
        left = sum(1 for i in range(500) if cmd(r, "EXISTS", "plain%d" % i))
        if left == 0:
            break
        time.sleep(0.5)
    check(left == 0, "eviction took every plain key (%d left)" % left)
    r.execute_command("CONFIG", "SET", "maxmemory", str(1 << 34))
    cmd(r, "SAVE")
    kill(server)
    server = start(PORT, DATA)
    r = client(PORT)
    put(r, "/g2")
    bad = intact(r)
    check(not bad, "after that, a SAVE and a restart, a new file overwrites nothing (%s)" % bad[:3])

    print("the counter goes through EXPORT and IMPORT", flush=True)
    check(cmd(r, "EXPORT", EXPORTED) > 0, "EXPORT writes the space")
    other = start(OTHER, OTHER_DATA)
    o = client(OTHER)
    o.execute_command("configuration:SET", SPACE + ".shards", "1")
    cmd(o, "IMPORT", EXPORTED)
    put(o, "/h0")
    bad = intact(o)
    check(not bad, "in a fresh server after IMPORT, a new file overwrites nothing (%s)" % bad[:3])
    check(cmd(o, "GRAPH", "GET", "/n/leaf") == b"graph bytes", "the graph reads there")
    check(cmd(o, "GRAPH", "MKDIR", "/n/there") == b"OK", "and takes a write")
    check(all(cmd(o, "GET", k) is None for k in ("ids:fs", "fs:layout", "graph:layout")),
          "after which the counter and the markers are hidden there too")

    print("RANDOMKEY finds a key past a shard holding only hidden ones", flush=True)
    # sixteen shards and one file: three visible keys and the meta keys, so most
    # shards hold nothing and some hold only a meta key. RANDOMKEY picked one of
    # those about a third of the time and answered null - TODO 536
    r.execute_command("configuration:SET", "mk16.shards", "16")
    r.execute_command("mk16:FS", "PUT", "/only", "one file")
    visible = set(r.execute_command("mk16:KEYS", "*"))
    randoms = [r.execute_command("mk16:RANDOMKEY") for _ in range(300)]
    nulls = sum(1 for k in randoms if k is None)
    check(nulls == 0, "RANDOMKEY answers a key, 300 of 300 (%d null)" % nulls)
    check(all(k in visible for k in randoms if k is not None), "and every one is a visible key")
finally:
    kill(other)
    kill(server)

print("\n%s" % ("all meta key checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
