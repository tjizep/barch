"""
Pairs nest under RESP3, and HGETALL of a missing key is empty - TODO 948.

Under RESP3, Redis 7 and Valkey answer a list of member-score or field-value
pairs as an array of two-element arrays. barchd sent them flat, as in RESP2.
HGETALL of a missing key answered a null. Redis answers an empty array (RESP2)
or an empty map (RESP3).

Each expected shape below is what Valkey 8.1 answered to the same commands on a
raw socket. The shape string keeps the wire type: `*[...]` an array, `%[...]` a
map, `$x` a bulk string, `,x` a double, `_` a null.

Checked:
  - under RESP3, every list of pairs nests each pair, and a lone pair does not;
  - under RESP2, the same replies stay flat;
  - HGETALL of a missing key is `*[]` or `%[]`;
  - a function called from a RESP3 connection still sees flat pairs, and an
    empty table for the missing key.
"""
import os
import socket
import subprocess
import sys
import time

import scale

scale.workdir()
PORT = scale.port(default=14948)
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
    """one connection and a reader that writes each reply as a shape string"""

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

    def shape(self):
        head = self.line()
        kind, rest = head[:1].decode(), head[1:].decode()
        if kind == "$":
            n = int(rest)
            return "nil$" if n < 0 else "$" + self.exact(n).decode()
        if kind in "*%~":
            n = int(rest)
            if n < 0:
                return "nil*"
            count = n * 2 if kind == "%" else n
            return kind + "[" + " ".join(self.shape() for _ in range(count)) + "]"
        return kind + rest

    def call(self, *args):
        self.s.sendall(cmd(*args))
        return self.shape()

    def close(self):
        self.s.close()


SETUP = [
    ("HSET", "h", "f", "v"),
    ("ZADD", "z", "1", "a", "2.5", "b"),
    ("ZADD", "z1", "1", "a"),
    ("ZADD", "zb", "3", "b", "4", "c"),
]

# (command, RESP2 shape, RESP3 shape), run in this order
CASES = [
    (("HGETALL", "nokey"), "*[]", "%[]"),
    (("HGETALL", "h"), "*[$f $v]", "%[$f $v]"),
    (("HRANDFIELD", "h", "1", "WITHVALUES"), "*[$f $v]", "*[*[$f $v]]"),
    (("HRANDFIELD", "h", "1"), "*[$f]", "*[$f]"),
    (("ZRANGE", "z", "0", "-1", "WITHSCORES"), "*[$a $1 $b $2.5]", "*[*[$a ,1] *[$b ,2.5]]"),
    (("ZRANGE", "z", "0", "-1"), "*[$a $b]", "*[$a $b]"),
    (("ZRANGE", "z", "0", "-1", "REV", "WITHSCORES"), "*[$b $2.5 $a $1]", "*[*[$b ,2.5] *[$a ,1]]"),
    (("ZRANGE", "z", "-inf", "+inf", "BYSCORE", "WITHSCORES"), "*[$a $1 $b $2.5]",
     "*[*[$a ,1] *[$b ,2.5]]"),
    (("ZRANGE", "nokey", "0", "-1", "WITHSCORES"), "*[]", "*[]"),
    (("ZRANGEBYSCORE", "z", "-inf", "+inf", "WITHSCORES"), "*[$a $1 $b $2.5]", "*[*[$a ,1] *[$b ,2.5]]"),
    (("ZREVRANGE", "z", "0", "-1", "WITHSCORES"), "*[$b $2.5 $a $1]", "*[*[$b ,2.5] *[$a ,1]]"),
    (("ZREVRANGEBYSCORE", "z", "+inf", "-inf", "WITHSCORES"), "*[$b $2.5 $a $1]",
     "*[*[$b ,2.5] *[$a ,1]]"),
    (("ZRANDMEMBER", "z1", "1", "WITHSCORES"), "*[$a $1]", "*[*[$a ,1]]"),
    (("ZRANDMEMBER", "z1", "1"), "*[$a]", "*[$a]"),
    (("ZUNION", "2", "z", "zb", "WITHSCORES"), "*[$a $1 $c $4 $b $5.5]", "*[*[$a ,1] *[$c ,4] *[$b ,5.5]]"),
    (("ZINTER", "2", "z", "zb", "WITHSCORES"), "*[$b $5.5]", "*[*[$b ,5.5]]"),
    (("ZDIFF", "2", "z", "zb", "WITHSCORES"), "*[$a $1]", "*[*[$a ,1]]"),
    # without a count a pop answers one pair, which stays flat
    (("ZPOPMIN", "z"), "*[$a $1]", "*[$a ,1]"),
    (("ZADD", "z", "1", "a"), ":1", ":1"),
    (("ZPOPMIN", "z", "2"), "*[$a $1 $b $2.5]", "*[*[$a ,1] *[$b ,2.5]]"),
    (("ZADD", "z", "1", "a", "2.5", "b"), ":2", ":2"),
    (("ZPOPMAX", "z"), "*[$b $2.5]", "*[$b ,2.5]"),
    (("ZPOPMAX", "z", "1"), "*[$a $1]", "*[*[$a ,1]]"),
]


def run(proto):
    c = Conn()
    if proto == 3:
        c.call("HELLO", "3")
    c.call("FLUSHALL")
    for s in SETUP:
        c.call(*s)
    for args, two, three in CASES:
        want = three if proto == 3 else two
        got = c.call(*args)
        check(got == want, "%s: %s (%s)" % (" ".join(args), want, got))
    return c


DATA = os.path.join(os.getcwd(), "resp3pair_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "resp3pair.log"), "w")
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
    print("RESP2")
    run(2).close()
    print("RESP3")
    r3 = run(3)

    print("a function called from a RESP3 connection")
    r3.call("ZADD", "z", "1", "a", "2.5", "b")
    got = r3.call("SETF", "pairs", '''
function call()
    local r = barch.call("ZRANGE", "z", "0", "-1", "WITHSCORES")
    return #r .. " " .. type(r[1]) .. " " .. type(barch.call("HGETALL", "nokey"))
end
''')
    check(got == "+OK", "SETF takes it (%s)" % got)
    got = r3.call("CALLF", "pairs")
    check(got == "$4 string table", "it sees four flat values and an empty table (%s)" % got)
    r3.close()
finally:
    p.kill()
    p.wait()
    log.close()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
