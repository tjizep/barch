"""
HGETALL answers a map under RESP3 - TODO 936.

HGETALL built its reply with start_array, so a RESP3 client got a flat array of
fields and values where Redis sends a map. CONFIG GET and HELLO already used
start_map. RESP2 has no map, so there both are a flat array.

Checked on raw sockets, so the wire type is what is asserted:
  - HGETALL under RESP3 opens with '%' and holds each field with its value;
  - HGETALL under RESP2 is still a flat array;
  - CONFIG GET under RESP3 is still a map.
"""
import os
import socket
import subprocess
import sys
import time

import scale

scale.workdir()
PORT = scale.port(default=14936)
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


def cmd(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


class Conn:
    """one connection and a reader that keeps each reply's wire type"""

    def __init__(self):
        self.s = socket.create_connection(("127.0.0.1", PORT), timeout=60)
        self.buf = b""

    def line(self):
        while b"\r\n" not in self.buf:
            part = self.s.recv(4096)
            if not part:
                raise EOFError("the server closed the connection")
            self.buf += part
        out, self.buf = self.buf.split(b"\r\n", 1)
        return out

    def exact(self, n):
        while len(self.buf) < n + 2:
            part = self.s.recv(4096)
            if not part:
                raise EOFError("the server closed the connection")
            self.buf += part
        out, self.buf = self.buf[:n], self.buf[n + 2:]
        return out

    def reply(self):
        """a reply as (type, value); an aggregate's value is a list of replies"""
        head = self.line()
        kind, rest = head[:1], head[1:]
        if kind == b"$":
            n = int(rest)
            return kind, None if n < 0 else self.exact(n)
        if kind in (b"*", b"%", b"~"):
            n = int(rest)
            if n < 0:
                return kind, None
            count = n * 2 if kind == b"%" else n
            return kind, [self.reply() for _ in range(count)]
        return kind, rest

    def call(self, *args):
        self.s.sendall(cmd(*args))
        return self.reply()

    def close(self):
        self.s.close()


def flat(items):
    return [v for _, v in items]


DATA = os.path.join(os.getcwd(), "resp3map_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "resp3map.log"), "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

try:
    r2 = Conn()
    r2.call("HSET", "h", "f1", "v1", "f2", "v2")

    print("HGETALL under RESP3")
    r3 = Conn()
    kind, _ = r3.call("HELLO", "3")
    check(kind == b"%", "HELLO 3 answers a map")
    kind, items = r3.call("HGETALL", "h")
    check(kind == b"%", "HGETALL opens with a map header (%r)" % (kind,))
    pairs = flat(items or [])
    check(dict(zip(pairs[0::2], pairs[1::2])) == {b"f1": b"v1", b"f2": b"v2"},
          "each field is paired with its value (%r)" % (pairs,))

    print("HGETALL under RESP2")
    kind, items = r2.call("HGETALL", "h")
    check(kind == b"*", "HGETALL is a flat array (%r)" % (kind,))
    check(sorted(flat(items or [])) == [b"f1", b"f2", b"v1", b"v2"], "it holds four values")

    print("CONFIG GET under RESP3")
    kind, items = r3.call("CONFIG", "GET", "port")
    check(kind == b"%" and flat(items or []) == [b"port", str(PORT).encode()],
          "CONFIG GET is a map (%r %r)" % (kind, items))
    r2.close()
    r3.close()
finally:
    p.kill()
    p.wait()
    log.close()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
