"""
A parked command whose time runs out just as a wake reaches it answers once - TODO 878.

The timer's handler and a wake's continuation both ran the command, and both answered.
Every later reply on the connection was then one behind: an MGET got the "OK" of an
earlier SET. barchd runs with BARCH_TEST_WAKE_AT_BLOCK_TIMEOUT, so a waiter whose time
runs out wakes its own key first, as a commit on another thread landing then would.
Each case reads the one answer, then sends PING, and the next reply has to be PONG:
  - a BLPOP on an empty list;
  - a SET in a transaction, parked on a key lock another transaction holds;
  - a plain SET parked on the same lock.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14816)
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


DATA = os.path.join(os.getcwd(), "blockwake_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "blockwake.log"), "w")
env = dict(os.environ, BARCH_TEST_WAKE_AT_BLOCK_TIMEOUT="1")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000", "-c", "tx_lock_wait_ms=100"],
                     stdout=log, stderr=log, env=env)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "blockwake_" + str(int(time.time()))


class Raw:
    """one connection, each reply read whole, so a reply too many shows"""

    def __init__(self):
        # the deadline only stops a broken server hanging the test
        self.s = socket.create_connection(("127.0.0.1", PORT), timeout=60)
        self.buf = b""

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
        if head[0] in "*":
            n = int(head[1:])
            return [self.reply() for _ in range(max(n, 0))] if n >= 0 else None
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

    def close(self):
        self.s.close()


def then_pong(r):
    try:
        return r.ask("PING")
    except (OSError, ConnectionError) as e:
        return "no reply (%s)" % e


try:
    print("a BLPOP whose time runs out as it's woken")
    r = Raw()
    r.ask("USE", SPACE)
    got = r.ask("BLPOP", "empty", "0.05")
    check(got is None, "it times out (%s)" % got)
    nxt = then_pong(r)
    check(nxt == "+PONG", "and answers once: the next reply is PING's (%s)" % nxt)
    r.close()

    holder = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                         single_connection_client=True)
    holder.execute_command("USE", SPACE)
    holder.execute_command("BEGINTX", "PESSIMISTIC")
    holder.execute_command("SET", "k", "holder")

    print("a transaction's SET whose lock wait runs out as it's woken")
    r = Raw()
    r.ask("USE", SPACE)
    r.ask("BEGINTX", "PESSIMISTIC")
    got = r.ask("SET", "k", "tx")
    check(got.startswith("-TXLOCKTIMEOUT"), "it times out (%s)" % got[:40])
    # the failed transaction answers anything but its end with that same error
    nxt = r.ask("ABORTTX")
    check(nxt == "+OK", "and answers once: the next reply is ABORTTX's (%s)" % str(nxt)[:60])
    nxt = then_pong(r)
    check(nxt == "+PONG", "then PING's (%s)" % str(nxt)[:60])
    r.close()

    print("a plain SET whose lock wait runs out as it's woken")
    r = Raw()
    r.ask("USE", SPACE)
    got = r.ask("SET", "k", "plain")
    check(got.startswith("-TXLOCKTIMEOUT"), "it times out (%s)" % got[:40])
    nxt = then_pong(r)
    check(nxt == "+PONG", "and answers once: the next reply is PING's (%s)" % str(nxt)[:60])
    r.close()

    holder.execute_command("COMMITTX")
    check(holder.get("k") == "holder", "the holder's write stands (%s)" % holder.get("k"))
    holder.close()
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
