"""
A blocked client and a space that goes away - TODO 944.

A park keeps the space it was made in, so the wake runs the handler there. UNLOAD and
KSPACE DROP take the space out of the registry and wake every waiter it has. Two holes:
  - the woken BLPOP found the old space's list still empty and parked again, on the old
    space's lists. A push goes to the space opened again under that name, so nothing
    woke it. At timeout 0 it waited for ever;
  - the caller kept the park's space after the answer, until it parked again. An idle
    client that once blocked in a dropped space held it. The last holder of a dropped
    space deletes its files, so closing that client deleted the files of the space made
    again under the same name.

Checked:
  - a BLPOP parked in a space that is unloaded is told so;
  - no key is left blocked after it;
  - a client that once blocked in a dropped space, closed, leaves the space made again
    under that name whole: its key is there after a restart;
  - a CALLF parked in a space that is unloaded still answers what its job returns. Its
    wake key is woken by pointer, and its job owns the session's state until it ends.
"""
import http.server
import socketserver
import threading
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14944)
WEB_PORT = scale.port(1, default=14945)
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
            # an error, or the two element array BLPOP answers with
            if got.startswith(b"-") and got.endswith(b"\r\n"):
                return got
            if got.startswith(b"*") and got.count(b"\r\n") >= 5:
                return got
            if got.startswith(b"+") and got.endswith(b"\r\n"):
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


def open_client(space):
    s = socket.create_connection(("127.0.0.1", PORT))
    s.sendall(cmd("USE", space))
    assert read_reply(s) == b"+OK\r\n"
    return s


# the latch: a request to the web server waits until the test lets it go
arrived = threading.Event()
release = threading.Event()


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        arrived.set()
        # bounded only so a broken run can't keep the thread for good
        release.wait(300)
        raw = b"held"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


web = Server(("127.0.0.1", WEB_PORT), Handler)
threading.Thread(target=web.serve_forever, daemon=True).start()

DATA = os.path.join(os.getcwd(), "blockunload_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "blockunload.log"), "w")


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
    r = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True)

    print("BLPOP parked in a space that is unloaded")
    UN = "blockun_" + STAMP
    admin = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True)
    admin.execute_command("USE", UN)
    admin.set("there", "1")
    waiter = open_client(UN)
    waiter.sendall(cmd("BLPOP", "k", "0"))
    check(until("the waiter to park", lambda: clients(r, "blocked_clients") == 1), "the waiter parked")
    r.execute_command("UNLOAD", UN)
    # a connection keeps the space it chose, so the push comes from a new one. The
    # space opens again from its files, and the push lands there
    admin.close()
    admin = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True)
    admin.execute_command("USE", UN)
    admin.rpush("k", "v")
    got = read_reply(waiter)
    check(got is not None and got.startswith(b"-ERR") and b"unloaded" in got,
          "the waiter was told the space went: %r" % (got,))
    check(until("no blocked keys", lambda: clients(r, "blocked_keys") == 0), "no key is left blocked")
    waiter.close()
    admin.close()

    print("an idle client that once blocked in a dropped space")
    DR = "blockdrop_" + STAMP
    # everything here is space:CMD from the default space. A connection that chose DR
    # holds it as well, and whichever holder goes last deletes the files
    idle = socket.create_connection(("127.0.0.1", PORT))
    idle.sendall(cmd(DR + ":BLPOP", "k", "0"))
    check(until("the client to park", lambda: clients(r, "blocked_clients") == 1), "the client parked")
    r.execute_command(DR + ":RPUSH", "k", "v")
    got = read_reply(idle)
    check(got is not None and got.startswith(b"*2"), "it was answered: %r" % (got,))
    r.execute_command("KSPACE", "DROP", DR)
    # the same name again, with a key, saved
    r.execute_command(DR + ":SET", "kept", "yes")
    r.execute_command(DR + ":SAVE")
    was = sessions(r)
    idle.close()
    check(until("the idle client to go", lambda: sessions(r) < was), "the idle client went")

    print("CALLF parked in a space that is unloaded")
    CF = "blockcallf_" + STAMP
    admin = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True)
    admin.execute_command("USE", CF)
    admin.execute_command("SETF", "held", 'function call(url) local res = http.request(url)'
                                          ':timeout(600000):get() return res.body end')
    caller = open_client(CF)
    caller.sendall(cmd("CALLF", "held", "http://127.0.0.1:%d/hold" % WEB_PORT))
    check(arrived.wait(60), "the job holds its request")
    check(until("the call to park", lambda: clients(r, "blocked_clients") == 1), "the call parked")
    r.execute_command("UNLOAD", CF)
    release.set()
    got = read_reply(caller)
    check(got == b"$4\r\nheld\r\n", "it answered what its job returned: %r" % (got,))
    check(until("no blocked keys", lambda: clients(r, "blocked_keys") == 0), "no key is left blocked")
    caller.close()
    admin.close()

    # every holder lets go by the time the server stops, so a late delete is seen here
    print("the space made again after the drop, after a restart")
    stop()
    p = start()
    r = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True)
    check(r.execute_command(DR + ":GET", "kept") == "yes", "its key is still there")
finally:
    release.set()
    web.shutdown()
    stop()
    log.close()

print("blockunloadtest:", "FAIL" if failures else "OK", flush=True)
sys.exit(1 if failures else 0)
