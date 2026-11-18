"""
A command that changes the connection, sent behind one that runs as a batch, changes
it for what comes after - TODO 931.

KEYS runs as a batch, on the worker pool. Each call in a batch ran on a copy of the
session's caller, made when the batch was read. A USE, HELLO or MULTI there changed
its copy only. The calls after it had been copied before it ran, and the session
never saw it: KEYS; USE other; SET wrote to the old space and stayed there, HELLO 3
left the next reply in RESP2, and MULTI after KEYS queued nothing. Under MULTI a KEYS
went to a copy too, so its queued reply was lost, and the EXEC after it ran on a copy
and left the session in MULTI.

Checked:
  - USE after KEYS: the SET behind it, and the next read, are in the space it named;
  - HELLO 3 after KEYS: the reply behind it is RESP3;
  - MULTI after KEYS queues what follows, and EXEC answers it;
  - KEYS under MULTI is queued, and EXEC answers it with the rest;
  - KEYS first under MULTI: EXEC answers both, and the connection is out of MULTI;
  - USE after KEYS, then a BLPOP: it parks in the named space, and a push there
    answers it;
  - INFO clients' chain_mode_errors is 0 at the end.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14833)
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


def blocked(r):
    return int(r.info("clients").get("blocked_clients", -1))


def wait_for(r, n):
    """until INFO says so. The deadline only keeps a broken run from hanging"""
    end = time.time() + 60
    while time.time() < end:
        if blocked(r) == n:
            return True
        time.sleep(0.02)
    return False


def cmd(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a.encode() if isinstance(a, str) else a
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


def read_exactly(s, n):
    """n bytes, or what came before the failsafe"""
    got = b""
    try:
        while len(got) < n:
            chunk = s.recv(min(1 << 20, n - len(got)))
            if not chunk:
                break
            got += chunk
    except socket.timeout:
        pass
    return got


def read_until(s, want):
    """read until `want` has come. The timeout is a failsafe: a broken server holds
    the bytes for ever, and this is how that run ends"""
    got = b""
    try:
        while not got.startswith(want):
            if len(got) >= len(want) and not want.startswith(got[:len(want)]):
                break
            chunk = s.recv(65536)
            if not chunk:
                break
            got += chunk
    except socket.timeout:
        pass
    return got


DATA = os.path.join(os.getcwd(), "batchstate_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "batchstate.log"), "w")
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

tag = str(int(time.time()))
OTHER = "bother_" + tag
HOME = "bshome_" + tag
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=120, single_connection_client=True)
    other = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=120, single_connection_client=True)
    other.execute_command("USE", OTHER)

    def conn():
        """a raw connection in its own space, so KEYS sees nothing of other tests"""
        s = socket.create_connection(("127.0.0.1", PORT), timeout=20)
        s.sendall(cmd("USE", HOME))
        read_until(s, b"+OK\r\n")
        return s

    def ask(s, data, want):
        s.sendall(data)
        return read_until(s, want)

    keys = cmd("KEYS", "nope_*")
    none = b"*0\r\n"

    print("USE behind KEYS")
    s = conn()
    want = none + b"+OK\r\n+OK\r\n$2\r\nin\r\n"
    got = ask(s, keys + cmd("USE", OTHER) + cmd("SET", "kk", "in") + cmd("GET", "kk"), want)
    check(got == want, "KEYS, USE, SET, GET are answered (%r)" % got)
    check(other.get("kk") == b"in", "the SET is in %s (%r)" % (OTHER, other.get("kk")))
    want = b"+OK\r\n"
    got = ask(s, cmd("SET", "k3", "x"), want)
    check(other.get("k3") == b"x", "and so is a SET read after (%r)" % other.get("k3"))
    s.close()

    print("HELLO 3 behind KEYS")
    other.zadd("z", {"a": 1})
    s = conn()
    s.sendall(keys + cmd("HELLO", "3") + cmd(OTHER + ":ZSCORE", "z", "a"))
    # the HELLO map's length isn't fixed, so read until either kind of score ends it
    rest = b""
    try:
        while not rest.endswith(b"\r\n,1\r\n") and not rest.endswith(b"\r\n$1\r\n1\r\n"):
            c = s.recv(65536)
            if not c:
                break
            rest += c
    except socket.timeout:
        pass
    check(rest.startswith(none + b"%"), "KEYS, then the HELLO map (%r)" % rest[:8])
    check(rest.endswith(b"\r\n,1\r\n"), "the ZSCORE behind it is a RESP3 double (%r)" % rest[-12:])
    s.close()

    print("MULTI behind KEYS")
    s = conn()
    want = none + b"+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n$1\r\n1\r\n"
    got = ask(s, keys + cmd("MULTI") + cmd("SET", "m1", "1") + cmd("EXEC") + cmd("GET", "m1"), want)
    check(got == want, "the SET is queued and EXEC answers it (%r)" % got)
    s.close()

    print("KEYS under MULTI")
    s = conn()
    want = b"+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n*1\r\n$2\r\nm2\r\n"
    got = ask(s, cmd("MULTI") + cmd("SET", "m2", "1") + cmd("KEYS", "m2") + cmd("EXEC"), want)
    check(got == want, "EXEC answers the SET and the KEYS (%r)" % got)
    s.close()

    print("KEYS first under MULTI")
    s = conn()
    want = b"+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n*0\r\n+OK\r\n$1\r\n1\r\n"
    got = ask(s, cmd("MULTI") + cmd("KEYS", "m3") + cmd("SET", "m3", "1") + cmd("EXEC") + cmd("GET", "m3"), want)
    check(got == want, "EXEC answers both, and the GET after is not queued (%r)" % got)
    s.close()

    print("USE behind KEYS, then a park")
    s = conn()
    s.sendall(keys + cmd("USE", OTHER) + cmd("BLPOP", "qk", "0"))
    got = read_until(s, none + b"+OK\r\n")
    check(got == none + b"+OK\r\n", "KEYS and USE are answered (%r)" % got)
    check(wait_for(r, 1), "the BLPOP parks")
    other.rpush("qk", "v")
    want = b"*2\r\n$2\r\nqk\r\n$1\r\nv\r\n"
    got = read_until(s, want)
    check(got == want, "a push in %s answers it (%r)" % (OTHER, got))
    s.close()
    check(wait_for(r, 0), "and nothing is parked after")

    errors = int(r.info("clients").get("chain_mode_errors", -1))
    check(errors == 0, "no read chain was in a mode that can't happen (%d)" % errors)
    r.close()
    other.close()
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
