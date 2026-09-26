# A big pipeline gets every reply back, whole and in order - TODO 475.
#
# After a read the server started an async write of its reply buffer and went
# straight back to reading. The next read cleared that same buffer and filled it
# with the next replies while the first write was still going out. Nothing shows
# while the socket takes each write in one go, but once the client falls behind
# (around 400KB of replies on loopback) a write goes out in pieces, and the later
# pieces came from a buffer that now held something else. redis-py saw value
# bytes where a reply's type byte should be.
#
# Every value here starts with its own key, so a reply that is cut, repeated or
# out of order is caught, not only one that breaks the protocol.
#
#   redis-py pipelines of 1000 up to 20000 GETs, 200 byte values
#   a raw socket that sends 20000 GETs and reads the replies back slowly, so the
#   server's writes are sure to go out in pieces
import os
import socket
import subprocess
import sys
import threading
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14482)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "pipelinereply_data")
os.makedirs(DATA, exist_ok=True)
for name in os.listdir(DATA):
    os.remove(os.path.join(DATA, name))

KEYS = 50000
ARGS = ["-c", "save_interval=86400000"]


def key(i):
    return "k%08d" % i


def value(i):
    k = key(i)
    return (k + "v" * (200 - len(k))).encode()


def start():
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA] + ARGS,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            out = p.stdout.read().decode(errors="replace")
            raise AssertionError("barchd exited with %s:\n%s" % (p.returncode, out[-2000:]))
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
        p.kill()
        p.wait(timeout=30)


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=120)


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def wrong(got, count):
    """how many of the first `count` replies aren't their key's value"""
    return sum(1 for i in range(count) if got[i] != value(i))


def pipelined_gets(r, count):
    p = r.pipeline(transaction=False)
    for i in range(count):
        p.get(key(i))
    try:
        got = p.execute()
    except redis.RedisError as e:
        return "%s: %.40s" % (type(e).__name__, e)
    if len(got) != count:
        return "%d replies for %d GETs" % (len(got), count)
    bad = wrong(got, count)
    return "%d wrong replies" % bad if bad else None


class Reader:
    """just enough RESP2 to read bulk strings back off a raw socket"""

    def __init__(self, sock):
        self.sock = sock
        self.buf = b""

    def more(self):
        # small reads with a pause in between, so the client stays behind the
        # server and its writes can't go out in one piece
        time.sleep(0.0005)
        chunk = self.sock.recv(8192)
        if not chunk:
            raise AssertionError("connection closed")
        self.buf += chunk

    def line(self):
        while b"\r\n" not in self.buf:
            self.more()
        at = self.buf.index(b"\r\n")
        out, self.buf = self.buf[:at], self.buf[at + 2:]
        return out

    def bulk(self):
        head = self.line()
        if head[:1] != b"$":
            raise AssertionError("expected a bulk string, got %r" % head[:30])
        n = int(head[1:])
        while len(self.buf) < n + 2:
            self.more()
        out, self.buf = self.buf[:n], self.buf[n + 2:]
        return out


def slow_raw_gets(count):
    sock = socket.create_connection(("127.0.0.1", PORT), timeout=120)
    try:
        payload = b"".join(b"*2\r\n$3\r\nGET\r\n$9\r\n" + key(i).encode() + b"\r\n"
                           for i in range(count))
        # sent from another thread: the client reads while it sends, the way any
        # pipelining client has to once the replies back up
        sender = threading.Thread(target=sock.sendall, args=(payload,))
        sender.start()
        reader = Reader(sock)
        try:
            got = [reader.bulk() for _ in range(count)]
        except (AssertionError, OSError) as e:
            return str(e)
        finally:
            if not sender.is_alive():
                sender.join()
        bad = wrong(got, count)
        return "%d wrong replies" % bad if bad else None
    finally:
        sock.close()


proc = start()
try:
    r = client()
    for base in range(0, KEYS, 5000):
        p = r.pipeline(transaction=False)
        for i in range(base, base + 5000):
            p.set(key(i), value(i))
        p.execute()
    check(r.dbsize() == KEYS, "%d keys written" % KEYS)

    for count in (1000, 2000, 5000, 20000):
        err = pipelined_gets(r, count)
        check(err is None, "redis-py pipeline of %d GETs (%s)" % (count, err or "all right"))
        if err is not None:
            # the connection is past saving after a bad reply
            r = client()
        check(r.get(key(7)) == value(7), "  a plain GET after it")

    err = slow_raw_gets(20000)
    check(err is None, "slowly read raw pipeline of 20000 GETs (%s)" % (err or "all right"))
    check(r.ping(), "the server still answers")
finally:
    stop(proc)

print("\n%s" % ("all pipeline reply checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
