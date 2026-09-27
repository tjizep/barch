# A server restart while a queue handler is out - TODO 517.
#
# server::start() replaces the queue consumer every time, and START (or a listen
# port change) reaches it at runtime. stop() used to wait up to 10 s for handlers
# and then give up on them: the late completion was thrown away, so a handler
# that worked never got its message removed, and the new consumer, starting with
# nothing marked running, handed the same message out again while the first copy
# was still working on it.
#
# Now the claim on a queue's head lives in the queue registry, which a restart
# doesn't replace, and a delivery settles itself on the queue however late it is.
import http.server
import os
import shutil
import signal
import socket
import socketserver
import subprocess
import sys
import threading
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14390)
WEB_PORT = scale.port(1, default=14391)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

ROOT = os.path.join(os.getcwd(), "queuerestart")
shutil.rmtree(ROOT, ignore_errors=True)
DATA, QDIR = os.path.join(ROOT, "data"), os.path.join(ROOT, "queues")
os.makedirs(DATA)
os.makedirs(QDIR)
LOG_PATH = os.path.join(ROOT, "barchd.log")

HOLD = 15           # longer than stop() used to wait, under the handler's deadline
T0 = time.time()
events = []


class Slow(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        events.append(("start", time.time() - T0))
        time.sleep(HOLD)
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")
        events.append(("end", time.time() - T0))


class Web(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


threading.Thread(target=Web(("127.0.0.1", WEB_PORT), Slow).serve_forever, daemon=True).start()


def connect(timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", PORT), 0.5).close()
            return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=60)
        except OSError:
            time.sleep(0.1)
    raise AssertionError("barchd did not listen on %d" % PORT)


def status_of(r, name):
    for line in r.execute_command("QUEUE", "STATUS").decode().splitlines():
        if (" name=%s " % name) in line + " ":
            return dict(kv.split("=", 1) for kv in line.split(" ") if "=" in kv)
    return {}


def wait_until(pred, timeout=30.0):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.05)
    return pred()


print("start queue restart test with %s" % BINARY, flush=True)
log = open(LOG_PATH, "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA,
                      "--config", "queue_dir=" + QDIR], stdout=log, stderr=subprocess.STDOUT)
try:
    r = connect()
    r.execute_command("SETF", "SLOW", '''--@barch {"deadline_ms": 30000}
        function call(message, sequence, attempts)
            http.request("http://127.0.0.1:%d/"):timeout(60000):get()
            local n = tonumber(barch.store.get("done") or "0") or 0
            barch.store.set("done", tostring(n + 1))
            return "ok"
        end''' % WEB_PORT)
    r.execute_command("configuration:SETF", "queues/slow", '''
        function transport()
            return { kind = "queue", name = "slow", space = "default", call = "SLOW",
                     user = "default", durability = "each", max_attempts = 5, poll = "1s" }
        end''')
    r.execute_command("QUEUE", "PUSH", "slow", "once")
    assert wait_until(lambda: len(events) == 1, 20), "the handler never ran: %s" % events

    print("restarting the server while the handler waits", flush=True)
    r.execute_command("START", "127.0.0.1", str(PORT))
    r = connect()

    # past when the first copy finishes, and past when a second would have started
    assert wait_until(lambda: len([e for e in events if e[0] == "end"]) == 1, HOLD + 20), events
    time.sleep(3)
    starts = [round(t, 1) for kind, t in events if kind == "start"]
    assert len(starts) == 1, "the message was handed out %d times, at %s" % (len(starts), starts)
    assert wait_until(lambda: r.get("done") == b"1", 10), r.get("done")
    assert wait_until(lambda: status_of(r, "slow").get("waiting") == "0", 10), \
        "the first copy's success didn't remove the message: %s" % status_of(r, "slow")
    assert status_of(r, "slow").get("running") == "no", status_of(r, "slow")
    print("delivered once, and its success removed it after the restart", flush=True)
finally:
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()
    log.close()

with open(LOG_PATH, errors="replace") as f:
    out = f.read()
assert "server stopped on" in out, "the server never restarted, so this tested nothing"
assert "WARNING: ThreadSanitizer" not in out and "ERROR: AddressSanitizer" not in out, \
    out[out.find("Sanitizer") - 200:][:4000]
print("queue restart test passed")
