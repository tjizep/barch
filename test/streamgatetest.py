"""
A client that stops reading a streamed reply holds up no commit - TODO 925.

KEYS streams its reply from a worker. Once a space has had a transaction, a read
command runs holding the space's tx_gate shared, so it sees a commit whole or not at
all (TODO 693). KEYS wrote to the socket from inside that call. When the client had
stopped reading and bytes were already queued ahead of KEYS, the worker waited in
stream_wait with the gate held. A COMMITTX in that space waits for the gate's readers
to go, so it waited for that client: rpc_client_max_wait_ms (30 s) and the client
closed, or for ever at 0.

The stalled client pipelines a big MGET and then KEYS *, and reads nothing. Once INFO
says a worker waits on it, another client commits a transaction. Checked:
  - the commit answers OK while the worker is still waiting;
  - the stalled client then gets all of both replies;
  - INFO's stream_wait_locked is 0: no wait on a client held a gate or a latch.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14818)
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


class Reader:
    """RESP replies off a raw socket, read whole"""

    def __init__(self, s):
        self.s = s
        self.buf = b""

    def fill(self):
        chunk = self.s.recv(1 << 20)
        if not chunk:
            raise ConnectionError("closed by the server")
        self.buf += chunk

    def line(self):
        while b"\r\n" not in self.buf:
            self.fill()
        head, self.buf = self.buf.split(b"\r\n", 1)
        return head

    def exact(self, n):
        while len(self.buf) < n + 2:
            self.fill()
        body, self.buf = self.buf[:n], self.buf[n + 2:]
        return body

    def reply(self):
        head = self.line()
        kind, rest = head[:1], head[1:]
        if kind == b"$":
            n = int(rest)
            return None if n < 0 else self.exact(n)
        if kind == b"*":
            return [self.reply() for _ in range(int(rest))]
        return head


def stream_waiting(r):
    return int(r.info("clients").get("stream_waiting", 0))


DATA = os.path.join(os.getcwd(), "streamgate_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "streamgate.log"), "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"],
                     stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "streamgate_" + str(int(time.time()))
# The MGET reply goes with KEYS to the worker, and is queued before KEYS runs. It has
# to be past rpc_stream_high_water (256 KB) and the socket's buffers, so KEYS waits,
# and under rpc_output_high_water (1 MB), or the session stops and lets it drain first
BIG = 450000
BIGS = ["big%d" % i for i in range(2)]
KEYS = 2000
stalled = None
try:
    # failsafe only: a commit stuck behind the gate answers after the client is closed
    r = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=120, single_connection_client=True)
    r.execute_command("USE", SPACE)
    for name in BIGS:
        r.set(name, b"b" * BIG)
    for start in range(0, KEYS, 500):
        r.mset({"k:%05d" % i: "v" for i in range(start, start + 500)})
    # a transaction once, so the space's reads take its gate from now on
    r.execute_command("BEGINTX")
    r.set("t", "0")
    r.execute_command("COMMITTX")
    names = set(r.execute_command("KEYS", "*"))
    check(b"big0" in names and len(names) == KEYS + len(BIGS) + 1, "the space holds %d keys" % len(names))

    print("a client stalls on a pipelined MGET and KEYS")
    stalled = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    stalled.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    # a network's segment size rather than loopback's 64 KB, so the server's send
    # buffer stays as small as it is to a real client. On loopback it grows past
    # what the session queues ahead of KEYS, and KEYS never waits
    stalled.setsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG, 536)
    stalled.connect(("127.0.0.1", PORT))
    stalled.sendall(cmd("USE", SPACE) + cmd("MGET", *BIGS) + cmd("KEYS", "*"))
    # until a worker waits on it. The deadline only keeps a broken run from hanging
    end = time.time() + 60
    while stream_waiting(r) < 1 and time.time() < end:
        time.sleep(0.05)
    check(stream_waiting(r) >= 1, "a worker waits for it to read")

    print("another client commits in the same space")
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=120, single_connection_client=True)
    c.execute_command("USE", SPACE)
    c.execute_command("BEGINTX")
    c.set("t", "1")
    got = c.execute_command("COMMITTX")
    still = stream_waiting(r)
    check(got in (b"OK", "OK", True), "COMMITTX answers (%r)" % (got,))
    check(still >= 1, "while the worker still waits on the stalled client (%d waiting)" % still)
    c.close()

    print("the stalled client reads")
    rd = Reader(stalled)
    try:
        use = rd.reply()
        value = rd.reply()
        listed = rd.reply()
        whole = True
    except (ConnectionError, OSError) as e:
        use, value, listed, whole = None, None, None, False
        print("  read failed: %s" % e)
    check(whole, "its connection is still open")
    check(use == b"+OK", "USE answered (%r)" % (use,))
    check(isinstance(value, list) and [len(v) for v in value] == [BIG] * len(BIGS),
          "MGET answered all %d values" % len(BIGS))
    check(isinstance(listed, list) and set(listed) == names,
          "KEYS answered every key (%s)" % (len(listed) if isinstance(listed, list) else listed))
    end = time.time() + 60
    while stream_waiting(r) > 0 and time.time() < end:
        time.sleep(0.05)
    check(stream_waiting(r) == 0, "and no worker waits any more")
    locked = int(r.info("clients").get("stream_wait_locked", -1))
    check(locked == 0, "no wait on a client held a gate or a latch (%d did)" % locked)
    r.close()
finally:
    if stalled is not None:
        stalled.close()
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
