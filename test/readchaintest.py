"""
Replies to commands sent ahead of a blocking one go out before it parks - TODO 930.

A pipeline of SET, GET and then BLPOP ran all three in one read. The SET and GET
replies sat in the session's stream while the BLPOP parked, and left only with the
BLPOP's own answer: a client waiting on its GET waited on a push to another key.
Redis sends them at once.

Checked:
  - SET and GET ahead of a BLPOP that parks are answered while it is still parked;
  - the BLPOP's answer follows them, and a command sent after it waits for it;
  - the same when the park ends by timing out instead of a push;
  - a cross space park (`space:BLPOP`) does the same;
  - a park part way through a batch (KEYS runs as one) does the same, and its
    answer is not lost. The SET's "+OK" left the session's caller marked as having
    sent its reply, and the wake took the BLPOP's answer as sent too: the value
    was popped and dropped;
  - a park behind replies big enough to pause the chain parks once they drain;
  - INFO clients' chain_mode_errors is 0 at the end: no session's read chain was
    seen in a mode that can't happen.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14832)
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


DATA = os.path.join(os.getcwd(), "readchain_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "readchain.log"), "w")
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
OTHER = "rother_" + tag
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=120, single_connection_client=True)
    other = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=120, single_connection_client=True)
    other.execute_command("USE", OTHER)
    r.delete("qk", "a", "big")

    ahead = b"+OK\r\n$1\r\n1\r\n"

    print("replies ahead of a park go out while it is parked")
    s = socket.create_connection(("127.0.0.1", PORT), timeout=20)
    s.sendall(cmd("SET", "a", "1") + cmd("GET", "a") + cmd("BLPOP", "qk", "0") + cmd("PING"))
    check(wait_for(r, 1), "BLPOP parks")
    got = read_until(s, ahead)
    check(got == ahead, "SET and GET are answered before the push (%r)" % got)
    r.rpush("qk", "v")
    answer = b"*2\r\n$2\r\nqk\r\n$1\r\nv\r\n+PONG\r\n"
    got = read_until(s, answer)
    check(got == answer, "the BLPOP's answer, then the PING after it (%r)" % got)
    s.close()
    check(wait_for(r, 0), "and nothing is parked after")

    print("the same when the park times out")
    s = socket.create_connection(("127.0.0.1", PORT), timeout=20)
    s.sendall(cmd("SET", "a", "1") + cmd("GET", "a") + cmd("BLPOP", "qk", "0.05"))
    want = ahead + b"$-1\r\n"
    got = read_until(s, want)
    check(got == want, "SET and GET, then nil (%r)" % got)
    s.close()

    print("the same for a cross space park")
    s = socket.create_connection(("127.0.0.1", PORT), timeout=20)
    s.sendall(cmd("SET", "a", "1") + cmd("GET", "a") + cmd(OTHER + ":BLPOP", "qk", "0"))
    check(wait_for(r, 1), "%s:BLPOP parks" % OTHER)
    got = read_until(s, ahead)
    check(got == ahead, "SET and GET are answered before the push (%r)" % got)
    other.rpush("qk", "w")
    answer = b"*2\r\n$2\r\nqk\r\n$1\r\nw\r\n"
    got = read_until(s, answer)
    check(got == answer, "then the answer from %s (%r)" % (OTHER, got))
    s.close()
    check(wait_for(r, 0), "and nothing is parked after")

    print("the same for a park part way through a batch")
    s = socket.create_connection(("127.0.0.1", PORT), timeout=20)
    s.sendall(cmd("SET", "a", "1") + cmd("KEYS", "a") + cmd("BLPOP", "qk", "0") + cmd("PING"))
    check(wait_for(r, 1), "BLPOP parks in the batch")
    keys = b"+OK\r\n*1\r\n$1\r\na\r\n"
    got = read_until(s, keys)
    check(got == keys, "SET and KEYS are answered before the push (%r)" % got)
    r.rpush("qk", "x")
    answer = b"*2\r\n$2\r\nqk\r\n$1\r\nx\r\n+PONG\r\n"
    got = read_until(s, answer)
    check(got == answer, "then the answer and the PING (%r)" % got)
    s.close()
    check(wait_for(r, 0), "and nothing is parked after")

    print("a park behind replies that pause the chain")
    big = b"z" * 200000
    r.set("big", big)
    one = b"$%d\r\n%s\r\n" % (len(big), big)
    s = socket.create_connection(("127.0.0.1", PORT), timeout=20)
    # eight of them pass rpc_output_high_water; the chain pauses before the BLPOP
    s.sendall(cmd("GET", "big") * 8 + cmd("BLPOP", "qk", "0") + cmd("PING"))
    got = read_exactly(s, 8 * len(one))
    check(got == one * 8, "the eight GETs are answered (%d bytes)" % len(got))
    check(wait_for(r, 1), "and the BLPOP parks once they drain")
    r.rpush("qk", "y")
    answer = b"*2\r\n$2\r\nqk\r\n$1\r\ny\r\n+PONG\r\n"
    got = read_until(s, answer)
    check(got == answer, "then the answer and the PING (%r)" % got)
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
