"""
ZSCAN walks one sorted set - TODO 951.

Redis and Valkey answer `ZSCAN key cursor [MATCH pattern] [COUNT n] [NOSCORES]`.
barchd answered an unknown command.

Each expected shape below is what Valkey 8.1 answered on a raw socket. The reply is
flat under both protocols, and a score is a bulk string under both.

Checked:
  - the shapes of a one call scan, MATCH, NOSCORES, a missing key, and the errors;
  - a full iteration at COUNT 7 reports every member once with its ZSCORE;
  - MATCH works on members that look like numbers;
  - SCAN's promise: a member there throughout an iteration is reported, even when
    every score drops between calls. A walk in score order would miss members
    moved behind the cursor;
  - ZSCAN refuses a string key.
"""
import os
import socket
import subprocess
import sys
import time

import scale

scale.workdir()
PORT = scale.port(default=14951)
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
    """one connection; replies come back as shape strings or as Python values"""

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
        """(shape, value)"""
        head = self.line()
        kind, rest = head[:1].decode(), head[1:].decode()
        if kind == "$":
            n = int(rest)
            if n < 0:
                return "nil$", None
            v = self.exact(n).decode()
            return "$" + v, v
        if kind in "*%~":
            n = int(rest)
            if n < 0:
                return "nil*", None
            parts = [self.read() for _ in range(n * 2 if kind == "%" else n)]
            return kind + "[" + " ".join(p[0] for p in parts) + "]", [p[1] for p in parts]
        if kind == "-":
            return kind + rest, Exception(rest)
        return kind + rest, rest

    def call(self, *args):
        self.s.sendall(cmd(*args))
        return self.read()

    def shape(self, *args):
        return self.call(*args)[0]

    def value(self, *args):
        return self.call(*args)[1]

    def close(self):
        self.s.close()


def scan_all(c, key, *opts, between=None):
    """a full iteration: {member: score text}, how many times each came back, and calls"""
    seen, times, cursor, calls = {}, {}, "0", 0
    while True:
        got = c.value("ZSCAN", key, cursor, *opts)
        if isinstance(got, Exception):
            raise got
        cursor, flat = got
        calls += 1
        step = 1 if "NOSCORES" in opts else 2
        for i in range(0, len(flat), step):
            m = flat[i]
            seen[m] = flat[i + 1] if step == 2 else None
            times[m] = times.get(m, 0) + 1
        if cursor == "0":
            return seen, times, calls
        if between:
            between()
        if calls > 10000:
            raise RuntimeError("the cursor never came back to 0")


# (command, shape under both protocols), run in this order on {c:-1, a:1, b:2.5}
SHAPES = [
    (("ZSCAN", "z", "0"), "*[$0 *[$c $-1 $a $1 $b $2.5]]"),
    (("ZSCAN", "z", "0", "MATCH", "a*"), "*[$0 *[$a $1]]"),
    (("ZSCAN", "z", "0", "NOSCORES"), "*[$0 *[$c $a $b]]"),
    (("ZSCAN", "nokey", "0"), "*[$0 *[]]"),
    (("ZSCAN", "z", "x"), "-ERR invalid cursor"),
    (("ZSCAN", "z", "0", "COUNT", "0"), "-ERR syntax error"),
    (("ZSCAN", "z", "0", "BOGUS"), "-ERR syntax error"),
    (("ZADD", "zinf", "inf", "i"), ":1"),
    (("ZSCAN", "zinf", "0"), "*[$0 *[$i $inf]]"),
]


def same_members(shape_got, shape_want, noscores):
    """a one call reply lists its members in any order, as SCAN allows"""
    if shape_got == shape_want:
        return True
    head = "*[$0 *["
    if not (shape_got.startswith(head) and shape_want.startswith(head)):
        return False
    g = shape_got[len(head):-2].split(" ")
    w = shape_want[len(head):-2].split(" ")
    if noscores:
        return sorted(g) == sorted(w)
    pair = lambda xs: sorted(zip(xs[::2], xs[1::2]))
    return pair(g) == pair(w)


DATA = os.path.join(os.getcwd(), "zscan_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "zscan.log"), "w")
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
        print("RESP%d shapes" % proto)
        c = Conn()
        if proto == 3:
            c.call("HELLO", "3")
        c.call("FLUSHALL")
        c.call("ZADD", "z", "1", "a", "2.5", "b", "-1", "c")
        for args, want in SHAPES:
            got = c.shape(*args)
            ok = same_members(got, want, "NOSCORES" in args)
            check(ok, "%s: %s (%s)" % (" ".join(args), want, got))
        got = c.shape("ZSCAN", "z")
        check(got.startswith("-ERR wrong number of arguments"), "ZSCAN z: arity error (%s)" % got)
        c.call("SET", "s", "v")
        got = c.shape("ZSCAN", "s", "0")
        check(got.startswith("-WRONGTYPE"), "ZSCAN of a string: WRONGTYPE (%s)" % got)
        c.close()

    print("a full iteration")
    c = Conn()
    c.call("FLUSHALL")
    members = ["m%03d" % i for i in range(200)] + ["10", "-3.5", "1.5"]
    for i, m in enumerate(members):
        c.call("ZADD", "big", str(i * 0.5), m)
    seen, times, calls = scan_all(c, "big", "COUNT", "7")
    check(set(seen) == set(members), "every member comes back (%d of %d)" % (len(seen), len(members)))
    check(all(t == 1 for t in times.values()), "each comes back once when nothing changes")
    check(calls > 1, "COUNT 7 takes more than one call (%d)" % calls)
    bad = [m for m in members if m in seen and seen[m] != c.value("ZSCORE", "big", m)]
    check(not bad, "each score is its ZSCORE (%s)" % bad[:3])
    seen, _, _ = scan_all(c, "big", "COUNT", "7", "MATCH", "m1*")
    check(set(seen) == {m for m in members if m.startswith("m1")}, "MATCH m1* finds the m1 members")
    seen, _, _ = scan_all(c, "big", "COUNT", "7", "MATCH", "1*")
    check(set(seen) == {"10", "1.5"}, "MATCH 1* finds the members that look like numbers (%s)" % sorted(seen))
    seen, _, _ = scan_all(c, "big", "COUNT", "7", "NOSCORES")
    check(set(seen) == set(members), "NOSCORES walks every member")

    print("scores drop between calls")
    other = Conn()

    def drop():
        for m in members:
            other.call("ZINCRBY", "big", "-1000", m)

    seen, _, calls = scan_all(c, "big", "COUNT", "7", between=drop)
    missed = sorted(set(members) - set(seen))
    check(not missed, "no member is missed (%d missed over %d calls)" % (len(missed), calls))
    other.close()
    c.close()
finally:
    p.kill()
    p.wait()
    log.close()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
