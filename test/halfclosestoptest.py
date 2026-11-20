"""
A half closed session stays where stop() can reach it - TODO 933.

The collector dropped a session once a peek of its socket read 0. A client that
sends its FIN while a streamed reply waits on it peeks as 0, but a worker still
waits for that client to take its reply. The collector dropped its reference, so
CLIENT LIST lost the session and stop() could not tell the worker to give up.
With rpc_client_max_wait_ms at 0 the worker waited for ever, and so did SIGTERM.

The client stalls a streamed KEYS behind two large values, so a worker waits in
stream_wait (INFO clients stream_waiting). It then shuts its write side. The test
waits for two collector sweeps (INFO clients collector_sweeps), not for a time:
  - the session is still in CLIENT LIST;
  - a client that half closes with nothing out still leaves the list;
  - SIGTERM ends barchd. The wait on it is only there so a broken build fails
    instead of hanging.
"""
import os
import signal
import socket
import subprocess
import sys
import time

import redis

import scale

scale.workdir()
PORT = scale.port(default=14933)
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


def clients(r, field):
    return int(r.info("clients").get(field, 0))


def two_sweeps(r):
    """wait until a whole sweep has started and ended after this call"""
    start = clients(r, "collector_sweeps")
    while clients(r, "collector_sweeps") < start + 2:
        time.sleep(0.02)


def listed(r, port):
    return (":%d " % port) in r.execute_command("CLIENT", "LIST").decode()


DATA = os.path.join(os.getcwd(), "halfclosestop_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "halfclosestop.log"), "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

stopped = False
try:
    r = redis.Redis(port=PORT, single_connection_client=True, socket_timeout=60)
    r.execute_command("USE", "sp")
    for n in ("big0", "big1"):
        r.set(n, b"b" * 450000)
    for s in range(0, 2000, 500):
        r.mset({"k:%05d" % i: "v" for i in range(s, s + 500)})
    r.execute_command("CONFIG", "SET", "rpc_client_max_wait_ms", "0")

    print("a half close with nothing out")
    plain = socket.create_connection(("127.0.0.1", PORT), timeout=60)
    plain_port = plain.getsockname()[1]
    plain.sendall(cmd("PING"))
    check(plain.recv(64).startswith(b"+PONG"), "the client is answered")
    check(listed(r, plain_port), "it is in CLIENT LIST")
    plain.shutdown(socket.SHUT_WR)
    while listed(r, plain_port):
        two_sweeps(r)
    check(True, "it leaves CLIENT LIST")
    plain.close()

    print("a half close while a streamed reply waits")
    c = socket.socket()
    c.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    c.setsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG, 536)
    c.connect(("127.0.0.1", PORT))
    c_port = c.getsockname()[1]
    c.sendall(cmd("USE", "sp") + cmd("MGET", "big0", "big1") + cmd("KEYS", "*"))
    while clients(r, "stream_waiting") < 1:
        time.sleep(0.02)
    check(True, "a worker waits on the client")
    c.shutdown(socket.SHUT_WR)
    two_sweeps(r)
    check(listed(r, c_port), "the session is still in CLIENT LIST")
    r.close()

    p.send_signal(signal.SIGTERM)
    try:
        p.wait(timeout=120)
        stopped = True
    except subprocess.TimeoutExpired:
        pass
    check(stopped, "SIGTERM ends barchd")
    c.close()
finally:
    if p.poll() is None:
        p.kill()
    p.wait()
    log.close()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
