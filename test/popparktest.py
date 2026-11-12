"""
A blocking pop isn't missed by a push, and only its timeout answers nil - TODO 879.

Two faults:
  - A pop looked at its key, let the shard go, and only then went on the key's list of
    waiters. A push in between woke nobody, and the pop waited with the value there.
  - A list waiter woken with nothing left answered nil at once. Two BLPOPs and one push:
    the second got nil. And a BLPOP on two keys, both filled when it woke, popped both
    and answered twice.

barchd runs with BARCH_TEST_PARK_HOLD_FILE. While that file exists, a session about to
go on its keys' lists waits, and makes <file>.held to say so. So a push lands in the gap
every time, with no timing:
  - each blocking pop, held after its look: the push lands, and it answers the value;
  - two waiters and one push: the second parks again, is held there, takes a second
    push, and answers that;
  - one BLPOP on two keys, both pushed in a MULTI: one answer, the other value left.

A pop pipelined behind KEYS runs on the worker pool, and used to park from there, with
its session on the keys' lists and two threads on it - TODO 880. The held file says
which thread parks: it has to be the connection's own.

Two more - TODO 890:
  - a BLPOP whose key turns into a string as it wakes stays parked and takes a later
    push. Redis 8.10 does the same.
  - a waiter that parks again keeps its first deadline. A short BLPOP behind a long one
    is held past its deadline, then woken: it answers nil, and the push stays. This
    case waits on the clock, because a deadline has to go by.

The waits are long so a slow machine doesn't time one out. A pop that fails waits one
out: POPPARK_WAIT shortens it for a run against a server known to fail.
"""
import os
import select
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14826)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)
WAIT = os.environ.get("POPPARK_WAIT", "30")

failures = 0


def check(ok, what):
    global failures
    print("  %-74s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


DATA = os.path.join(os.getcwd(), "poppark_data")
os.makedirs(DATA, exist_ok=True)
HOLD = os.path.join(os.getcwd(), "poppark.hold")
HELD = HOLD + ".held"
for f in (HOLD, HELD):
    if os.path.exists(f):
        os.remove(f)
log = open(os.path.join(os.getcwd(), "poppark.log"), "w")
env = dict(os.environ, BARCH_TEST_PARK_HOLD_FILE=HOLD)
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"],
                     stdout=log, stderr=log, env=env)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "poppark_" + str(int(time.time()))


class Raw:
    """one connection, each reply read whole, so a reply too many shows"""

    def __init__(self):
        # the deadline only stops a broken server hanging the test
        self.s = socket.create_connection(("127.0.0.1", PORT), timeout=120)
        self.buf = b""
        self.ask("USE", SPACE)

    def send(self, *args):
        out = b"*%d\r\n" % len(args)
        for a in args:
            a = a.encode() if isinstance(a, str) else a
            out += b"$%d\r\n%s\r\n" % (len(a), a)
        self.s.sendall(out)

    def line(self):
        while b"\r\n" not in self.buf:
            got = self.s.recv(4096)
            if not got:
                raise ConnectionError("closed")
            self.buf += got
        i = self.buf.index(b"\r\n")
        out, self.buf = self.buf[:i].decode(), self.buf[i + 2:]
        return out

    def reply(self):
        head = self.line()
        if head[0] == "*":
            n = int(head[1:])
            return [self.reply() for _ in range(n)] if n >= 0 else None
        if head[0] == "$":
            n = int(head[1:])
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                self.buf += self.s.recv(4096)
            out, self.buf = self.buf[:n].decode(), self.buf[n + 2:]
            return out
        return head

    def ask(self, *args):
        self.send(*args)
        return self.reply()

    def answered(self):
        """a reply is waiting to be read"""
        return bool(self.buf) or bool(select.select([self.s], [], [], 0)[0])

    def close(self):
        self.s.close()


def hold():
    open(HOLD, "w").close()


def release():
    for f in (HOLD, HELD):
        if os.path.exists(f):
            os.remove(f)


def wait_held(parked=None):
    """until a session is held before it parks. False if `parked` answered instead"""
    while not os.path.exists(HELD):
        if parked is not None and parked.answered():
            return False
        time.sleep(0.005)
    return True


# connections share a few io threads, and a held session holds its thread. So a push
# goes out on whichever of these answers a PING while the hold is on
pool = []
owed = []


def free_conn():
    for c in pool:
        c.send("PING")
    ready, _, _ = select.select([c.s for c in pool], [], [])
    pick = next(c for c in pool if c.s in ready)
    pick.reply()
    owed[:] = [c for c in pool if c is not pick]
    return pick


def settle():
    """the PONGs the held thread's connections owe, read once it's let go"""
    for c in owed:
        c.reply()
    owed.clear()


def blocked():
    return int(admin.execute_command("INFO", "CLIENTS")["blocked_clients"])


def wait_blocked(n):
    while blocked() != n:
        time.sleep(0.005)


def then_pong(r):
    try:
        return r.ask("PING")
    except (OSError, ConnectionError) as e:
        return "no reply (%s)" % e


def gap(title, park, push, want):
    """`park` held after its look, `push` lands, and it answers `want`"""
    print(title)
    a = Raw()
    hold()
    a.send(*park)
    wait_held()
    b = free_conn()
    b.ask(*push)
    release()
    settle()
    got = a.reply()
    check(got == want, "the push in the gap isn't missed (%s)" % (got,))
    nxt = then_pong(a)
    check(nxt == "+PONG", "and it answers once (%s)" % (nxt,))
    a.close()


def turn(title, park, push1, want1, push2, want2):
    """two waiters, `push1`: the first answers, the second parks again and takes `push2`"""
    print(title)
    w1, w2 = Raw(), Raw()
    w1.send(*park)
    wait_blocked(1)
    w2.send(*park)
    wait_blocked(2)
    hold()
    admin.execute_command(*push1)
    got = w1.reply()
    check(got == want1, "the first waiter takes the push (%s)" % (got,))
    # the second is woken with nothing left: it parks again, and is held there
    if not wait_held(w2):
        got = w2.reply()
        check(False, "the second waiter stays parked (it answered %s)" % (got,))
        release()
        w1.close()
        w2.close()
        return
    check(True, "the second waiter stays parked")
    b = free_conn()
    b.ask(*push2)
    release()
    settle()
    got = w2.reply()
    check(got == want2, "and takes a push that landed as it parked again (%s)" % (got,))
    nxt = then_pong(w2)
    check(nxt == "+PONG", "and answers once (%s)" % (nxt,))
    w1.close()
    w2.close()


try:
    admin = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=120, decode_responses=True,
                        single_connection_client=True)
    admin.execute_command("USE", SPACE)
    pool = [Raw() for _ in range(16)]

    gap("BLPOP, a push between its look and its registration",
        ("BLPOP", "g1", WAIT), ("LPUSH", "g1", "v"), ["g1", "v"])
    gap("BRPOP", ("BRPOP", "g2", WAIT), ("RPUSH", "g2", "v"), ["g2", "v"])
    gap("BLMPOP", ("BLMPOP", WAIT, "1", "g3", "LEFT"), ("LPUSH", "g3", "v"), ["g3", ["v"]])
    gap("BLMOVE", ("BLMOVE", "g4", "g4to", "LEFT", "RIGHT", WAIT), ("LPUSH", "g4", "v"), "v")
    gap("BZPOPMIN", ("BZPOPMIN", "g5", WAIT), ("ZADD", "g5", "1", "m"), ["g5", "m", "1"])
    gap("BZMPOP", ("BZMPOP", WAIT, "1", "g6", "MIN"), ("ZADD", "g6", "1", "m"), ["g6", [["m", "1"]]])

    turn("two BLPOPs, one push",
         ("BLPOP", "t1", WAIT), ("LPUSH", "t1", "a"), ["t1", "a"], ("LPUSH", "t1", "b"), ["t1", "b"])
    turn("two BLMPOPs, one push",
         ("BLMPOP", WAIT, "1", "t2", "LEFT"), ("LPUSH", "t2", "a"), ["t2", ["a"]],
         ("LPUSH", "t2", "b"), ["t2", ["b"]])
    turn("two BLMOVEs, one push",
         ("BLMOVE", "t3", "t3to", "LEFT", "RIGHT", WAIT), ("LPUSH", "t3", "a"), "a",
         ("LPUSH", "t3", "b"), "b")
    turn("two BZPOPMINs, one member",
         ("BZPOPMIN", "t4", WAIT), ("ZADD", "t4", "1", "a"), ["t4", "a", "1"],
         ("ZADD", "t4", "2", "b"), ["t4", "b", "2"])

    print("a BLPOP pipelined behind KEYS, which runs on the worker pool")
    a = Raw()
    hold()
    a.s.sendall(b"*2\r\n$4\r\nKEYS\r\n$8\r\nnomatch*\r\n*3\r\n$5\r\nBLPOP\r\n$2\r\nb1\r\n$%d\r\n%s\r\n"
                % (len(WAIT), WAIT.encode()))
    wait_held()
    where = open(HELD).read()
    check(where == "home", "it parks on its connection's thread (%s)" % where)
    b = free_conn()
    b.ask("LPUSH", "b1", "v")
    release()
    settle()
    got = a.reply()
    check(got == [], "KEYS answers first (%s)" % (got,))
    got = a.reply()
    check(got == ["b1", "v"], "then the pop, with the push from the gap (%s)" % (got,))
    nxt = then_pong(a)
    check(nxt == "+PONG", "and it answers once (%s)" % (nxt,))
    a.close()

    print("a BLPOP whose key turns into a string as it wakes stays parked, as Redis does")
    a = Raw()
    a.send("BLPOP", "y1", WAIT)
    wait_blocked(1)
    admin.execute_command("MULTI")
    admin.execute_command("LPUSH", "y1", "v")
    admin.execute_command("LPOP", "y1")
    admin.execute_command("SET", "y1", "s")
    admin.execute_command("EXEC")
    admin.execute_command("DEL", "y1")
    admin.execute_command("LPUSH", "y1", "after")
    got = a.reply()
    check(got == ["y1", "after"], "it takes the next push, not an error or nil (%s)" % (got,))
    nxt = then_pong(a)
    check(nxt == "+PONG", "and answers once (%s)" % (nxt,))
    a.close()

    # The one case here that waits on the clock: a deadline has to go by. The second
    # waiter parks, half its time goes by, a push wakes it with nothing left, and it is
    # held as it parks again until its first deadline is past. Armed afresh, it would
    # still be waiting then, and take the push sent after.
    print("a waiter that parks again keeps its first deadline")
    short = 4.0
    w1, w2 = Raw(), Raw()
    w1.send("BLPOP", "d1", WAIT)
    wait_blocked(1)
    w2.send("BLPOP", "d1", str(short))
    wait_blocked(2)
    parked_at = time.time()
    while time.time() < parked_at + short / 2:
        time.sleep(0.01)
    hold()
    admin.execute_command("LPUSH", "d1", "a")
    got = w1.reply()
    check(got == ["d1", "a"], "the first waiter takes the push (%s)" % (got,))
    if not wait_held(w2):
        check(False, "the second parks again (it answered %s)" % (w2.reply(),))
    else:
        while time.time() < parked_at + short + 0.2:
            time.sleep(0.01)
        release()
        admin.execute_command("LPUSH", "d1", "late")
        got = w2.reply()
        check(got is None, "its first deadline answers nil (%s)" % (got,))
        nxt = then_pong(w2)
        check(nxt == "+PONG", "and answers once (%s)" % (nxt,))
        check(admin.execute_command("LLEN", "d1") == 1, "the late push is still there")
    release()
    w1.close()
    w2.close()

    print("one BLPOP on two keys, both filled when it wakes")
    a = Raw()
    a.send("BLPOP", "m1", "m2", WAIT)
    wait_blocked(1)
    admin.execute_command("MULTI")
    admin.execute_command("LPUSH", "m1", "a")
    admin.execute_command("LPUSH", "m2", "b")
    admin.execute_command("EXEC")
    got = a.reply()
    check(got in (["m1", "a"], ["m2", "b"]), "it pops one (%s)" % (got,))
    nxt = then_pong(a)
    check(nxt == "+PONG", "and answers once (%s)" % (nxt,))
    left = admin.execute_command("LLEN", "m1") + admin.execute_command("LLEN", "m2")
    check(left == 1, "the other value is still there (%s left)" % left)
    a.close()
finally:
    release()
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
