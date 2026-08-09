# The exact bytes each command answers with.
#
# Everything else in this directory drives barch through the embedded interface or
# through redis-py. Both are useful and neither can see what this one looks at: the
# reply as it appears on the wire. redis-py in particular hides the difference, because
# it parses a bulk string and a one element array holding a bulk string into values that
# compare equal in python.
#
# That blind spot is not hypothetical. Every defect in DONE 33 was invisible to the suite
# as it stood - HGET answering with a one element array, HEXISTS with an array holding an
# empty array, TTL with its two negatives the wrong way round, SET ... GET answering with
# the key instead of the previous value. All of them were found by reading the raw reply,
# and all of them would come back unnoticed without this.
#
# So the assertions here are deliberately written as literal bytes rather than as decoded
# values. If a reply shape changes, this test fails and says exactly what changed, which
# is the whole point of it.
#
# RESP2 is what a connection speaks unless it asks for 3 with HELLO, and this test never
# does, so these are the RESP2 encodings. Where a command answers differently under RESP3
# that belongs in a test of its own - see the note at the bottom.
import socket
import barch

PORT = 14074

barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)


def encode(*args):
    """a command as a RESP array of bulk strings, which is what a client sends"""
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = str(a).encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


class Wire:
    """one connection, kept open, reading exactly one reply at a time"""

    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.buf = b""

    def close(self):
        self.sock.close()

    def _more(self):
        d = self.sock.recv(65536)
        if not d:
            raise AssertionError("connection closed by the server mid reply")
        self.buf += d

    def _line(self):
        while b"\r\n" not in self.buf:
            self._more()
        line, _, self.buf = self.buf.partition(b"\r\n")
        return line

    def _reply(self):
        """read one complete reply and give back the bytes it occupied"""
        line = self._line()
        kind = line[:1]
        if kind in (b"+", b"-", b":", b",", b"#", b"_"):
            return line + b"\r\n"
        if kind in (b"$", b"="):
            n = int(line[1:])
            if n == -1:
                return line + b"\r\n"
            while len(self.buf) < n + 2:
                self._more()
            body, self.buf = self.buf[:n + 2], self.buf[n + 2:]
            return line + b"\r\n" + body
        if kind in (b"*", b"%", b"~"):
            n = int(line[1:])
            if n == -1:
                return line + b"\r\n"
            if kind == b"%":
                n *= 2
            out = line + b"\r\n"
            for _ in range(n):
                out += self._reply()
            return out
        raise AssertionError("unknown reply type %r" % line[:1])

    def cmd(self, *args):
        self.sock.sendall(encode(*args))
        return self._reply()


passed = 0


def shape(w, args, expect, why):
    """assert the exact bytes `args` answers with"""
    global passed
    got = w.cmd(*args)
    assert got == expect, "%s\n  %s\n  expected %r\n  got      %r" % (
        why, " ".join(str(a) for a in args), expect, got)
    passed += 1


print("start resp shape test")

w = Wire(PORT)
# a space of its own, so a rerun against a saved directory starts from the same place
assert w.cmd("USE", "respshapes") == b"+OK\r\n"
assert w.cmd("FLUSHDB") == b"+OK\r\n"

# --- keys: what SET answers, and what GET takes back out -------------------------
shape(w, ("SET", "a", "1"), b"+OK\r\n", "a plain SET answers OK")
shape(w, ("GET", "a"), b"$1\r\n1\r\n", "GET answers with a bulk string")
shape(w, ("GET", "missing"), b"$-1\r\n", "a key that is not there is nil, not an error")

# SET ... GET answers with the value that was replaced. It used to answer with the key
shape(w, ("SET", "a", "2", "GET"), b"$1\r\n1\r\n", "SET GET answers with the previous value")
shape(w, ("GET", "a"), b"$1\r\n2\r\n", "and the new value is the one that stuck")
shape(w, ("SET", "brand_new", "v", "GET"), b"$-1\r\n",
      "SET GET on a key that did not exist is nil")

# NX and XX suppress the write rather than failing it
shape(w, ("SET", "a", "3", "NX"), b"$-1\r\n", "NX on an existing key answers nil")
shape(w, ("GET", "a"), b"$1\r\n2\r\n", "and NX left the value alone")
shape(w, ("SET", "absent_xx", "1", "XX"), b"$-1\r\n", "XX on a missing key answers nil")

# --- MSET: OK, and pairs only ----------------------------------------------------
shape(w, ("MSET", "x", "1", "y", "2"), b"+OK\r\n", "MSET answers OK, not an integer")
shape(w, ("MSET", "k1", "v1", "k2"), b"-Wrong Arity\r\n",
      "an odd MSET is a wrong arity, not an out of range from walking off argv")

# --- EXISTS counts, it does not answer a single boolean --------------------------
shape(w, ("EXISTS", "a"), b":1\r\n", "EXISTS of one key that exists")
shape(w, ("EXISTS", "missing"), b":0\r\n", "EXISTS of one key that does not")
shape(w, ("EXISTS", "a", "x", "missing"), b":2\r\n", "EXISTS counts how many were found")
shape(w, ("EXISTS", "a", "a"), b":2\r\n", "a key named twice counts twice, as in redis")

# --- TTL: -1 is present with no expiry, -2 is no such key ------------------------
shape(w, ("TTL", "a"), b":-1\r\n", "a key with no expiry is -1")
shape(w, ("TTL", "missing"), b":-2\r\n", "a key that is not there is -2")

# --- DEL counts; REM answers with the value it removed ---------------------------
shape(w, ("DEL", "x", "missing"), b":1\r\n", "DEL answers with how many it removed")
shape(w, ("DEL", "missing"), b":0\r\n", "removing nothing is not an error")
shape(w, ("REM", "y"), b"$1\r\n2\r\n", "REM keeps its own shape and answers with the value")
shape(w, ("REM", "missing"), b"$-1\r\n", "REM of a missing key is nil")

# --- hashes: the shapes that were wrong --------------------------------------------
shape(w, ("HSET", "h", "f1", "v1", "f2", "v2"), b":2\r\n",
      "HSET counts the fields that were new")
shape(w, ("HSET", "h", "f1", "other"), b":0\r\n", "updating an existing field adds none")
shape(w, ("HSET", "h", "f3"), b"-Wrong Arity\r\n", "an odd field list is a wrong arity")

# HGET is a bulk string. It used to be a one element array holding one
shape(w, ("HGET", "h", "f1"), b"$5\r\nother\r\n", "HGET answers with a bulk string")
shape(w, ("HGET", "h", "nofield"), b"$-1\r\n", "a field that is not there is nil")
shape(w, ("HGET", "nohash", "f1"), b"$-1\r\n", "a hash that is not there is nil too")

# HMGET is the one that legitimately answers with an array
shape(w, ("HMGET", "h", "f1", "nofield"), b"*2\r\n$5\r\nother\r\n$-1\r\n",
      "HMGET answers one entry per field asked for, nils included")

# HEXISTS is a plain integer. It used to be an array holding an empty array
shape(w, ("HEXISTS", "h", "f1"), b":1\r\n", "HEXISTS answers 1 for a field that exists")
shape(w, ("HEXISTS", "h", "nofield"), b":0\r\n", "and 0 for one that does not")

# HINCRBY used to answer 0 and change nothing at all
shape(w, ("HINCRBY", "h", "ctr", "3"), b":3\r\n", "HINCRBY creates the field at the increment")
shape(w, ("HINCRBY", "h", "ctr", "3"), b":6\r\n", "and adds to it when it is there")
shape(w, ("HGET", "h", "ctr"), b"$1\r\n6\r\n", "and the field really holds the total")
# a field holding something that is not a number is refused, and keeps its value. This
# used to be coerced to zero and overwritten by the increment, which destroyed it
shape(w, ("HINCRBY", "h", "f1", "1"), b"-hash value is not an integer\r\n",
      "a non numeric field is refused")
shape(w, ("HGET", "h", "f1"), b"$5\r\nother\r\n",
      "and the value it held is still there")

shape(w, ("HDEL", "h", "f2"), b":1\r\n", "HDEL counts what it removed")
shape(w, ("HDEL", "h", "nofield"), b":0\r\n", "removing a field that is absent is not an error")
shape(w, ("HLEN", "h"), b":2\r\n", "HLEN counts the fields left")

# --- ordered sets: the pair order --------------------------------------------------
shape(w, ("ZADD", "z", "1", "m1"), b":1\r\n", "ZADD counts what it added")
shape(w, ("ZADD", "z", "2", "m2"), b":1\r\n", "and again for the second member")
shape(w, ("ZCARD", "z"), b":2\r\n", "ZCARD counts the members")
# member first, then the score. It used to be the other way round
shape(w, ("ZPOPMIN", "z"), b"*2\r\n$2\r\nm1\r\n$1\r\n1\r\n",
      "ZPOPMIN answers member first, then score")
shape(w, ("ZPOPMAX", "z"), b"*2\r\n$2\r\nm2\r\n$1\r\n2\r\n",
      "ZPOPMAX answers member first, then score")


# --- the type error that used to destroy the value -------------------------------
shape(w, ("SET", "notnum", "abc"), b"+OK\r\n", "a string value")
shape(w, ("INCR", "notnum"), b"-value is not an integer or out of range\r\n",
      "INCR on a string is refused")
shape(w, ("GET", "notnum"), b"$3\r\nabc\r\n", "and the string is untouched")
shape(w, ("INCRBY", "notnum", "5"), b"-value is not an integer or out of range\r\n",
      "INCRBY likewise")
shape(w, ("DECR", "notnum"), b"-value is not an integer or out of range\r\n",
      "and DECR")
shape(w, ("GET", "notnum"), b"$3\r\nabc\r\n", "still untouched after all three")

# --- SET options in any order ----------------------------------------------------
shape(w, ("SET", "oo", "1"), b"+OK\r\n", "seed")
shape(w, ("SET", "oo", "2", "GET", "XX"), b"$1\r\n1\r\n", "GET before XX")
shape(w, ("SET", "oo", "3", "XX", "GET"), b"$1\r\n2\r\n",
      "XX before GET - the order that used to be a syntax error")
shape(w, ("SET", "oo", "4", "NX", "XX"), b"-Syntax Error\r\n", "NX and XX together")
shape(w, ("SET", "oo", "4", "NONSENSE"), b"-Syntax Error\r\n", "an unknown option")
shape(w, ("SET", "oo", "4", "H"), b"-Syntax Error\r\n",
      "the H flag is gone and is refused like any other unknown word")

# --- EXPIRE's condition word -----------------------------------------------------
shape(w, ("EXPIRE", "oo", "100", "NONSENSE"), b"-Syntax Error\r\n",
      "a word that is not a condition is refused rather than ignored")

# --- ZRANGE by position ----------------------------------------------------------
w.cmd("DEL", "zz")
shape(w, ("ZADD", "zz", "1", "m1"), b":1\r\n", "seed")
shape(w, ("ZADD", "zz", "2", "m2"), b":1\r\n", "seed")
shape(w, ("ZADD", "zz", "3", "m3"), b":1\r\n", "seed")
shape(w, ("ZRANGE", "zz", "0", "-1"),
      b"*3\r\n$2\r\nm1\r\n$2\r\nm2\r\n$2\r\nm3\r\n",
      "0 to -1 is the whole set - it used to answer empty")
shape(w, ("ZRANGE", "zz", "0", "0"), b"*1\r\n$2\r\nm1\r\n", "first only")
shape(w, ("ZRANGE", "zz", "-2", "-1"),
      b"*2\r\n$2\r\nm2\r\n$2\r\nm3\r\n", "negatives count from the end")
shape(w, ("ZRANGE", "zz", "5", "9"), b"*0\r\n", "past the end is empty, not an error")
shape(w, ("ZREVRANGE", "zz", "0", "-1"),
      b"*3\r\n$2\r\nm3\r\n$2\r\nm2\r\n$2\r\nm1\r\n", "reversed")

# --- ZRANK is a member position now ----------------------------------------------
shape(w, ("ZRANK", "zz", "m1"), b":0\r\n", "first member is rank 0")
shape(w, ("ZRANK", "zz", "m3"), b":2\r\n", "last member")
shape(w, ("ZRANK", "zz", "nosuch"), b"$-1\r\n", "a member that is not there is nil")

# --- LPOP and RPOP answer with what they removed ---------------------------------
w.cmd("DEL", "ll")
# RPUSH appends and LPUSH prepends, as in redis - they used to be the other way round
shape(w, ("RPUSH", "ll", "a"), b":1\r\n", "append a")
shape(w, ("RPUSH", "ll", "b"), b":2\r\n", "append b, so the list is a b")
shape(w, ("LPUSH", "ll", "z"), b":3\r\n", "prepend z, so the list is z a b")
shape(w, ("LLEN", "ll"), b":3\r\n", "three entries")
shape(w, ("LFRONT", "ll"), b"$1\r\nz\r\n", "the head is what LPUSH put there")
shape(w, ("LBACK", "ll"), b"$1\r\nb\r\n", "the tail is what RPUSH put there")
shape(w, ("LPOP", "ll"), b"$1\r\nz\r\n",
      "LPOP takes from the head; no count means one bulk string")
shape(w, ("RPOP", "ll"), b"$1\r\nb\r\n", "RPOP takes from the tail")
shape(w, ("LPOP", "ll", "5"), b"*1\r\n$1\r\na\r\n",
      "with a count: an array, shorter than asked for when the list runs out")
shape(w, ("LPOP", "ll"), b"$-1\r\n", "an empty list is nil")

# --- SELECT takes a number -------------------------------------------------------
shape(w, ("SELECT", "0"), b"+OK\r\n", "0 is the default space")
shape(w, ("SELECT", "1"), b"+OK\r\n", "a number above zero is the database db1")
# barch also lets SELECT take a name, which is a superset of redis rather than a
# departure from it - spacethreadtest.py relies on it
shape(w, ("SELECT", "namedspace"), b"+OK\r\n", "and a name selects that space")
shape(w, ("SELECT", "-1"), b"-DB index is out of range\r\n", "a negative index is refused")
assert w.cmd("USE", "respshapes") == b"+OK\r\n"

# --- the dispatcher's own answers ---------------------------------------------------
shape(w, ("PING",), b"+PONG\r\n", "PING is the redis health check")
shape(w, ("NOSUCHCOMMAND",), b"-unknown command\r\n",
      "a name that is not registered is refused by the dispatcher")

w.cmd("FLUSHDB")
w.close()

print("resp shape test ok - %d reply shapes asserted" % passed)

# Not covered here, on purpose:
#
#   - RESP3. A connection that sends HELLO 3 gets maps and sets with their own wire
#     types where this one gets flat arrays, so those encodings want a test that sets
#     the protocol first rather than extra branches in this one.
#   - the commands in the dangerous ACL category, and START, STOP, PULL and RETRIEVE,
#     which cannot be exercised against the server running the test without disrupting
#     it. Their replies in the documentation come from reading the handler.
