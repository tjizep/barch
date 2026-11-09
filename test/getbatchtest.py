# Pipelined GETs answered as a batch give GET's own replies - TODO 851.
#
# A run of GETs in one read is answered together, so their cache misses overlap.
# The first cut encoded each key straight from the socket buffer. encode_key
# reads the byte after the key and expects a 0; the buffer had '\r' there, so
# every key in a batch was another key and every GET missed.
#
# Each case is one raw pipeline, so the GETs land in one read:
#   hits and misses mixed, a list key (WRONGTYPE), lowercase get, a long key,
#   a SET in the middle of the run, a GET with the wrong arity after it
# Both with ordered_keys on and off.
import os
import signal
import socket
import subprocess
import sys
import time

import scale

scale.workdir()
PORT = scale.port(default=14487)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "getbatch_data")


def start(ordered):
    os.makedirs(DATA, exist_ok=True)
    for name in os.listdir(DATA):
        os.remove(os.path.join(DATA, name))
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA, "--no-save-on-exit",
                          "-c", "ordered_keys=" + ordered],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            raise AssertionError("barchd exited with %s" % p.returncode)
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % PORT)


def stop(p):
    p.terminate()
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        scale.stop_hung(p, signal.SIGTERM, 60)


def resp(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a if isinstance(a, bytes) else a.encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


def replies(sock, count):
    """the raw replies, one per request: enough RESP2 for these commands"""
    buf = b""
    got = []
    while len(got) < count:
        while b"\r\n" not in buf:
            chunk = sock.recv(65536)
            if not chunk:
                raise AssertionError("connection closed after %d replies" % len(got))
            buf += chunk
        at = buf.index(b"\r\n")
        head = buf[:at]
        if head[:1] == b"$" and head != b"$-1":
            n = int(head[1:])
            while len(buf) < at + 2 + n + 2:
                buf += sock.recv(65536)
            got.append(buf[at + 2:at + 2 + n])
            buf = buf[at + 2 + n + 2:]
        else:
            got.append(head)
            buf = buf[at + 2:]
    return got


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


LONG = "k" * 300

for ordered in ("no", "yes"):
    print("ordered_keys=%s" % ordered)
    proc = start(ordered)
    try:
        sock = socket.create_connection(("127.0.0.1", PORT), timeout=60)
        setup = [resp("SET", "a", "1"), resp("SET", "b", "2"), resp("RPUSH", "l", "x"),
                 resp("SET", LONG, "big")]
        sock.sendall(b"".join(setup))
        replies(sock, len(setup))

        cases = [
            (resp("GET", "a"), b"1"),
            (resp("GET", "nope"), b"$-1"),
            (resp("GET", "b"), b"2"),
            (resp("GET", "l"), b"-WRONGTYPE"),
            (resp("get", "a"), b"1"),
            (resp("GET", LONG), b"big"),
            (resp("GET", "nope2"), b"$-1"),
            (resp("GET", "nope3"), b"$-1"),
            (resp("SET", "a", "9"), b"+OK"),
            (resp("GET", "a"), b"9"),
            (resp("GET", "b"), b"2"),
            (resp("GET", "a", "b"), b"-ERR"),
            (resp("GET", "b"), b"2"),
        ]
        sock.sendall(b"".join(c for c, _ in cases))
        got = replies(sock, len(cases))
        for (c, want), g in zip(cases, got):
            what = c.split(b"\r\n")[2::2]
            check(g.startswith(want), "%s -> %s" % (b" ".join(w[:12] for w in what).decode(), g[:40]))

        # many hits in one run, each its own value
        keys = ["h%05d" % i for i in range(2000)]
        sock.sendall(b"".join(resp("SET", k, k + "v") for k in keys))
        replies(sock, len(keys))
        sock.sendall(b"".join(resp("GET", k) for k in keys))
        got = replies(sock, len(keys))
        bad = sum(1 for k, g in zip(keys, got) if g != (k + "v").encode())
        check(bad == 0, "2000 pipelined GETs, %d wrong" % bad)
        sock.close()
    finally:
        stop(proc)

print("\n%s" % ("all GET batch checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
