"""
A connection that chose a space with USE, and the space is dropped - TODO 945.

The connection holds the space it chose. KSPACE DROP takes the space out of the
registry and marks its shards to delete their files when the last holder lets go.
Three holes, one cause:
  - closing the connection deleted the files of a space made again under the name;
  - while it stayed open the files stayed, so the space made again under the name
    loaded the dropped data back;
  - its next command ran in the dropped space. A write went nowhere anyone could
    read, and a BLPOP parked where no push reaches.

Checked:
  - a space made again after the drop doesn't hold the dropped keys;
  - the connection's next write lands in the space now under the name;
  - its BLPOP is answered by a push from another connection;
  - closing it leaves the new space whole: its key is there after a restart.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14946)
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


def read_reply(s):
    """the bytes of one reply, or None if none came in the backstop"""
    s.settimeout(60)
    got = b""
    try:
        while True:
            chunk = s.recv(65536)
            if not chunk:
                return got or None
            got += chunk
            if got[:1] in (b"-", b"+", b":") and got.endswith(b"\r\n"):
                return got
            if got.startswith(b"*") and got.count(b"\r\n") >= 5:
                return got
            if got.startswith(b"$") and got.count(b"\r\n") >= 2:
                return got
    except socket.timeout:
        return got or None


def until(what, test):
    """wait for a condition the server reports, with a 60 s backstop"""
    end = time.time() + 60
    while time.time() < end:
        if test():
            return True
        time.sleep(0.05)
    print("  gave up waiting for", what, flush=True)
    return False


def clients(r, field):
    return int(r.info("clients").get(field, 0))


def sessions(r):
    return int(r.info("SERVER")["redis_sessions"])


DATA = os.path.join(os.getcwd(), "usedrop_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "usedrop.log"), "w")


def start():
    proc = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA,
                             "--no-save-on-exit", "-c", "save_interval=86400000"],
                            stdout=log, stderr=log)
    scale.wait_for_port(PORT, proc=proc, what="barchd")
    return proc


def stop():
    if p.poll() is not None:
        return
    p.terminate()
    try:
        p.wait(timeout=30)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()


p = start()

STAMP = str(int(time.time()))
try:
    # everything but the user is space:CMD from the default space, so only the user
    # holds DR by choice
    r = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True)
    DR = "usedrop_" + STAMP

    print("a connection that chose a space, and the space is dropped")
    user = socket.create_connection(("127.0.0.1", PORT))
    user.sendall(cmd("USE", DR))
    check(read_reply(user) == b"+OK\r\n", "it chose the space")
    user.sendall(cmd("SET", "old", "1"))
    check(read_reply(user) == b"+OK\r\n", "it wrote a key")
    r.execute_command(DR + ":SAVE")
    r.execute_command("KSPACE", "DROP", DR)

    print("the space made again under the name")
    check(r.execute_command(DR + ":GET", "old") is None, "it doesn't hold the dropped key")
    r.execute_command(DR + ":SET", "kept", "yes")
    r.execute_command(DR + ":SAVE")

    print("the connection's next commands")
    user.sendall(cmd("SET", "fromuser", "1"))
    check(read_reply(user) == b"+OK\r\n", "its write was answered")
    check(r.execute_command(DR + ":GET", "fromuser") == "1", "its write is in the space under the name")
    user.sendall(cmd("BLPOP", "q", "0"))
    check(until("the user to park", lambda: clients(r, "blocked_clients") == 1), "its BLPOP parked")
    r.execute_command(DR + ":RPUSH", "q", "v")
    got = read_reply(user)
    check(got is not None and got.startswith(b"*2"), "a push answered it: %r" % (got,))

    print("the connection closes")
    was = sessions(r)
    user.close()
    check(until("the connection to go", lambda: sessions(r) < was), "it went")

    # every holder lets go by the time the server stops, so a late delete is seen here
    print("the space made again, after a restart")
    stop()
    p = start()
    r = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True)
    check(r.execute_command(DR + ":GET", "kept") == "yes", "its key is still there")
    check(r.execute_command(DR + ":GET", "old") is None, "the dropped key is still gone")
finally:
    stop()
    log.close()

print("usedroptest:", "FAIL" if failures else "OK", flush=True)
sys.exit(1 if failures else 0)
