"""
A batch stops when its reply can't be written - TODO 935.

A batch runs each call on a worker, then writes the call's reply with write_then and
runs the next call when that write completes. The write's completion ran the next
call whether the write worked or not. So a client that reset still had the rest of
its batch run: each command, its writes included, with nowhere to send the reply.
The other write failures in a batch (stream_out, send_socket_tail, stream_flush)
stop it.

The client pipelines KEYS * COUNT and a SET behind it. KEYS is asynchronous, so both
are one batch, and its count is a short reply that write_then writes. A hold
(BARCH_TEST_BATCH_WRITE_HOLD_FILE) stops the worker before it writes that reply. The client resets, and the hold
lets go. Checked:
  - the SET behind the failed write does not run;
  - the client's session is let go;
  - the server still answers, and a batch on a live client runs to its end.
"""
import os
import re
import socket
import struct
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14935)
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


def sessions(r):
    return int(r.info("SERVER")["redis_sessions"])


def wait_until(what, limit=60):
    # the deadline only keeps a broken run from hanging
    end = time.time() + limit
    while not what() and time.time() < end:
        time.sleep(0.02)
    return what()


DATA = os.path.join(os.getcwd(), "batchwrite_data")
os.makedirs(DATA, exist_ok=True)
HOLD = os.path.join(DATA, "write.hold")
for f in (HOLD, HOLD + ".held"):
    if os.path.exists(f):
        os.remove(f)
log = open(os.path.join(os.getcwd(), "batchwrite.log"), "w")
env = dict(os.environ, BARCH_TEST_BATCH_WRITE_HOLD_FILE=HOLD)
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

SPACE = "batchwrite_" + str(int(time.time()))
client = None
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=120, single_connection_client=True)
    r.execute_command("USE", SPACE)
    r.set("k", "v")
    check(wait_until(lambda: sessions(r) == 1), "one session before the client comes")
    before = sessions(r)

    print("a client pipelines KEYS COUNT and a SET, then resets")
    open(HOLD, "w").close()
    client = socket.create_connection(("127.0.0.1", PORT))
    client.sendall(cmd("USE", SPACE) + cmd("KEYS", "*", "COUNT") + cmd("SET", "after", "ran"))
    check(wait_until(lambda: os.path.exists(HOLD + ".held")), "the worker holds before it writes")
    client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    client.close()
    client = None
    os.remove(HOLD)

    check(wait_until(lambda: sessions(r) == before), "its session is let go (%d sessions)" % sessions(r))
    check(r.get("after") is None, "the SET behind the failed write did not run")

    print("a batch on a live client runs to its end")
    # by hand, on r's own connection: a pipeline from redis-py takes a new one,
    # which is not in the space
    live = socket.create_connection(("127.0.0.1", PORT), timeout=60)
    live.sendall(cmd("USE", SPACE) + cmd("KEYS", "*", "COUNT") + cmd("SET", "live", "ran"))
    got = b""
    while got.count(b"\r\n") < 3:
        part = live.recv(100)
        if not part:
            break
        got += part
    live.close()
    check(re.fullmatch(rb"\+OK\r\n:\d+\r\n\+OK\r\n", got) is not None, "all three replies (%r)" % (got,))
    check(r.get("live") == b"ran", "the SET ran")
    r.close()
finally:
    if client is not None:
        client.close()
    if os.path.exists(HOLD):
        os.remove(HOLD)
    p.kill()
    p.wait()
    log.close()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
