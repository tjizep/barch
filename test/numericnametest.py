"""
Names that look like numbers keep their bytes - TODO 952.

barchd stored any key, member or field that parsed as a number as a number key. So
`010` came back as the integer 10, `1e3` as 1000 and `1.50` as 1.5, and `010` and `10`
were one key: SET 010 v, then GET 10, answered v. A 2^63 integer became a double, so
near big integers shared a key too. Valkey keeps the bytes it is given.

barch still stores a name as a number when the text is exactly how that number prints,
so `10`, `-3` and `1.5` keep their numeric order. Everything else is a string.

Each expected shape below is what Valkey 8.1 answered on a raw socket, under RESP2 and
RESP3. Checked:
  - KEYS, SCAN, GET and DBSIZE on keys `010`, `10`, `1e3`, `1.50`, `+1`, `-0`, a 2^63
    integer and its neighbour;
  - ZRANGE, ZSCAN and ZSCORE on the same names as members, and ZCARD;
  - HKEYS, HSCAN and HGET on the same names as fields, and HLEN;
  - ZINCRBY on a member `10` that ZADD wrote changes that member, not a new one;
  - ZRANGEBYLEX finds a member `10`;
  - score bounds still read `1e0` as 1.
"""
import os
import socket
import subprocess
import sys
import time

import scale

scale.workdir()
PORT = scale.port(default=14952)
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
    """one connection; replies come back as shape strings"""

    def __init__(self):
        self.s = socket.create_connection(("127.0.0.1", PORT), timeout=60)
        self.buf = b""

    def line(self):
        while b"\r\n" not in self.buf:
            part = self.s.recv(65536)
            if not part:
                raise EOFError("the server closed the connection")
            self.buf += part
        out, self.buf = self.buf.split(b"\r\n", 1)
        return out

    def exact(self, n):
        while len(self.buf) < n + 2:
            part = self.s.recv(65536)
            if not part:
                raise EOFError("the server closed the connection")
            self.buf += part
        out, self.buf = self.buf[:n], self.buf[n + 2:]
        return out

    def read(self):
        head = self.line()
        kind, rest = head[:1].decode(), head[1:].decode()
        if kind == "$":
            n = int(rest)
            return "nil$" if n < 0 else "$" + self.exact(n).decode()
        if kind in "*%~":
            n = int(rest)
            if n < 0:
                return "nil*"
            parts = [self.read() for _ in range(n * 2 if kind == "%" else n)]
            return kind + "[" + " ".join(parts) + "]"
        return kind + rest

    def call(self, *args):
        self.s.sendall(cmd(*args))
        return self.read()

    def close(self):
        self.s.close()


NAMES = ["010", "10", "1e3", "1.50", "+1", "-0", "9223372036854775808", "9223372036854775809"]


def bulk_set(shape):
    """the bulk strings of a flat array shape, as a sorted list"""
    inner = shape[2:-1]
    return sorted(inner.split(" ")) if inner else []


def scan_bulks(shape):
    """the bulk strings inside a one call SCAN-style reply `*[$0 *[...]]`"""
    head = "*[$0 *["
    if not shape.startswith(head):
        return None
    inner = shape[len(head):-2]
    return sorted(inner.split(" ")) if inner else []


WANT = sorted("$" + n for n in NAMES)

DATA = os.path.join(os.getcwd(), "numericname_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "numericname.log"), "w")
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
    for proto in (2, 3):
        print("RESP%d" % proto)
        c = Conn()
        if proto == 3:
            c.call("HELLO", "3")
        c.call("FLUSHALL")

        print(" keys")
        for i, n in enumerate(NAMES):
            c.call("SET", n, "v%d" % i)
        got = c.call("DBSIZE")
        check(got == ":%d" % len(NAMES), "DBSIZE counts each name once (%s)" % got)
        got = c.call("KEYS", "*")
        check(bulk_set(got) == WANT, "KEYS gives each name back as its bytes (%s)" % got)
        got = scan_bulks(c.call("SCAN", "0", "COUNT", "100"))
        check(got == WANT, "SCAN gives each name back as its bytes (%s)" % got)
        bad = [n for i, n in enumerate(NAMES) if c.call("GET", n) != "$v%d" % i]
        check(not bad, "GET of each name finds its own value (%s)" % bad)
        got = c.call("GET", "1000")
        check(got in ("nil$", "_"), "GET 1000 does not find 1e3 (%s)" % got)

        print(" members")
        args = []
        for i, n in enumerate(NAMES):
            args += [str(i), n]
        c.call("ZADD", "z", *args)
        got = c.call("ZCARD", "z")
        check(got == ":%d" % len(NAMES), "ZCARD counts each member once (%s)" % got)
        got = c.call("ZRANGE", "z", "0", "-1")
        want = "*[" + " ".join("$" + n for n in NAMES) + "]"
        check(got == want, "ZRANGE gives each member back as its bytes (%s)" % got)
        got = scan_bulks(c.call("ZSCAN", "z", "0", "NOSCORES"))
        check(got == WANT, "ZSCAN gives each member back as its bytes (%s)" % got)
        score = "$1" if proto == 2 else ",1"
        got = c.call("ZSCORE", "z", "10")
        check(got == score, "ZSCORE of 10 is its own score, not 010's (%s)" % got)

        print(" fields")
        args = []
        for i, n in enumerate(NAMES):
            args += [n, "v%d" % i]
        c.call("HSET", "h", *args)
        got = c.call("HLEN", "h")
        check(got == ":%d" % len(NAMES), "HLEN counts each field once (%s)" % got)
        got = c.call("HKEYS", "h")
        check(bulk_set(got) == WANT, "HKEYS gives each field back as its bytes (%s)" % got)
        got = c.call("HSCAN", "h", "0", "NOVALUES")
        got = scan_bulks(got)
        check(got == WANT, "HSCAN gives each field back as its bytes (%s)" % got)
        bad = [n for i, n in enumerate(NAMES) if c.call("HGET", "h", n) != "$v%d" % i]
        check(not bad, "HGET of each field finds its own value (%s)" % bad)

        print(" sorted set paths")
        c.call("DEL", "y")
        c.call("ZADD", "y", "1", "10", "0", "a")
        c.call("ZINCRBY", "y", "1", "10")
        got = c.call("ZCARD", "y")
        check(got == ":2", "ZINCRBY on a member ZADD wrote changes it, not a new one (%s)" % got)
        two = "$2" if proto == 2 else ",2"
        got = c.call("ZSCORE", "y", "10")
        check(got == two, "and its score is 2 (%s)" % got)
        c.call("DEL", "x")
        c.call("ZADD", "x", "0", "10", "0", "a")
        got = c.call("ZRANGEBYLEX", "x", "[10", "[10")
        check(got == "*[$10]", "ZRANGEBYLEX [10 [10 finds member 10 (%s)" % got)
        got = c.call("ZCOUNT", "z", "1e0", "1e1")
        check(got == ":7", "ZCOUNT z 1e0 1e1 reads the bounds as scores (%s)" % got)
        got = c.call("ZRANGEBYSCORE", "z", "1e0", "2")
        check(got == "*[$10 $1e3]", "ZRANGEBYSCORE z 1e0 2 reads the bound as a score (%s)" % got)
        c.close()
finally:
    p.kill()
    p.wait()
    log.close()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
