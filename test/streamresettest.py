"""
A client reset mid-stream is let go - TODO 943.

KEYS streams its reply from a worker, and the worker waits in stream_wait while the
client is behind. A client that resets with input the server has not read fails
the write that is out. That completion only marked the stream failed. The worker gave up and returned, the read chain stayed down, and nothing set let_go.
The collector frees a session on let_go or on a peek of 0, and a reset socket with
unread input peeks as that input for ever. The session and its fd stayed.

The client pipelines a big MGET and KEYS *, reads nothing, and waits until INFO says a
worker waits on it. It pipelines more commands, which sit unread at the server, and
resets the connection. Checked:
  - the worker stops waiting;
  - INFO's redis_sessions drops back to what it was before the client came.
"""
import os
import socket
import struct
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14943)
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


def stream_waiting(r):
    return int(r.info("clients").get("stream_waiting", 0))


def sessions(r):
    return int(r.info("SERVER")["redis_sessions"])


DATA = os.path.join(os.getcwd(), "streamreset_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "streamreset.log"), "w")
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

SPACE = "streamreset_" + str(int(time.time()))
# sized as in streamgatetest.py: past rpc_stream_high_water and the socket buffers, so
# KEYS waits, and under rpc_output_high_water
BIG = 450000
BIGS = ["big%d" % i for i in range(2)]
KEYS = 2000
stalled = None
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=120, single_connection_client=True)
    r.execute_command("USE", SPACE)
    for name in BIGS:
        r.set(name, b"b" * BIG)
    for start in range(0, KEYS, 500):
        r.mset({"k:%05d" % i: "v" for i in range(start, start + 500)})
    # the probe connection above is gone once the collector has seen it
    end = time.time() + 60
    while sessions(r) > 1 and time.time() < end:
        time.sleep(0.05)
    before = sessions(r)
    check(before == 1, "one session before the client comes (%d)" % before)

    print("a client stalls on a pipelined MGET and KEYS")
    stalled = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    stalled.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    stalled.setsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG, 536)
    stalled.connect(("127.0.0.1", PORT))
    stalled.sendall(cmd("USE", SPACE) + cmd("MGET", *BIGS) + cmd("KEYS", "*"))
    # the deadline only keeps a broken run from hanging
    end = time.time() + 60
    while stream_waiting(r) < 1 and time.time() < end:
        time.sleep(0.05)
    check(stream_waiting(r) >= 1, "a worker waits for it to read")
    check(sessions(r) == before + 1, "it has a session (%d)" % sessions(r))

    print("it sends more, then resets")
    # the batch is running, so the read chain is down and these stay unread
    stalled.sendall(cmd("PING") * 100)
    stalled.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    stalled.close()
    stalled = None

    end = time.time() + 60
    while stream_waiting(r) > 0 and time.time() < end:
        time.sleep(0.05)
    check(stream_waiting(r) == 0, "the worker stops waiting")
    end = time.time() + 60
    while sessions(r) > before and time.time() < end:
        time.sleep(0.05)
    check(sessions(r) == before, "its session is let go (%d sessions)" % sessions(r))
    check(r.ping(), "the server still answers")
    r.close()
finally:
    if stalled is not None:
        stalled.close()
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
