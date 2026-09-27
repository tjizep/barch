# A queue across restarts: sequences and unfinished deliveries - TODO 511.
#
# Two things used to be lost when barchd restarted:
#   - the sequence. It carried on from the highest one left in the queue file,
#     so a queue that had drained started again at 1, and a handler that dedupes
#     on the sequence (the obvious thing under at-least-once) threw new messages
#     away as repeats.
#   - attempts, which were only counted in memory, and only when a handler
#     reported back. A handler that took the process down never did, so its
#     message came back at attempt 0 on every start and never reached the dead
#     letter queue.
#
# Now a marker record in the queue file says a delivery started, and the last
# record in the file always carries the sequence. Here the handler "takes the
# process down" by waiting on a slow HTTP server while barchd is killed.
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
PORT = scale.port(default=14380)
WEB_PORT = scale.port(1, default=14381)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

ROOT = os.path.join(os.getcwd(), "queuecrash")
shutil.rmtree(ROOT, ignore_errors=True)
DATA, QDIR = os.path.join(ROOT, "data"), os.path.join(ROOT, "queues")
os.makedirs(DATA)
os.makedirs(QDIR)

# every barchd this starts writes here: most are killed, so a sanitizer's exit
# code never shows, and its reports are read from this at the end
LOG = open(os.path.join(ROOT, "barchd.log"), "w")

hits = []


class Slow(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        hits.append(self.path)
        time.sleep(60)          # longer than the test waits: barchd dies first


class Web(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


web = Web(("127.0.0.1", WEB_PORT), Slow)
threading.Thread(target=web.serve_forever, daemon=True).start()


def start():
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA,
                          "--config", "queue_dir=" + QDIR],
                         stdout=LOG, stderr=subprocess.STDOUT)
    end = time.time() + 60
    while time.time() < end:
        assert p.poll() is None, "barchd exited with %s" % p.returncode
        try:
            socket.create_connection(("127.0.0.1", PORT), 0.5).close()
            return p, redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=60)
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % PORT)


def wait_until(pred, timeout=20.0):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.05)
    return pred()


def status_of(r, name):
    for line in r.execute_command("QUEUE", "STATUS").decode().splitlines():
        if (" name=%s " % name) in line + " ":
            return dict(kv.split("=", 1) for kv in line.split(" ") if "=" in kv)
    return {}


print("start queue crash test with %s" % BINARY, flush=True)
p, r = start()
try:
    r.execute_command("SETF", "TAKE", '''
        function call(message, sequence, attempts)
            barch.store.set("last_seq", tostring(sequence))
            return "ok"
        end''')
    r.execute_command("configuration:SETF", "queues/work", '''
        function transport()
            return { kind = "queue", name = "work", space = "default", call = "TAKE",
                     user = "default", durability = "each", max_attempts = 3, poll = "1h" }
        end''')
    r.execute_command("SETF", "SLOW", '''
        function call(message, sequence, attempts)
            http.request("http://127.0.0.1:%d/?seq=" .. tostring(sequence)
                         .. "&attempts=" .. tostring(attempts)):timeout(120000):get()
            return "ok"
        end''' % WEB_PORT)
    r.execute_command("configuration:SETF", "queues/slow", '''
        function transport()
            return { kind = "queue", name = "slow", space = "default", call = "SLOW",
                     user = "default", durability = "each", max_attempts = 2, poll = "1s" }
        end''')

    # --- 1. a drained queue keeps its sequence over a restart ----------------------
    print("a drained queue doesn't start its sequence again", flush=True)
    first = r.execute_command("QUEUE", "PUSH", "work", "a")
    second = r.execute_command("QUEUE", "PUSH", "work", "b")
    assert wait_until(lambda: status_of(r, "work").get("waiting") == "0"), status_of(r, "work")
    r.execute_command("SAVEALL")
finally:
    p.send_signal(signal.SIGTERM)
    p.wait(timeout=60)

p, r = start()
try:
    third = r.execute_command("QUEUE", "PUSH", "work", "c")
    assert third > second, "pushed %s and %s, drained, restarted, and got %s" % (first, second, third)
    assert wait_until(lambda: r.get("last_seq") == str(third).encode()), r.get("last_seq")
    print("sequences %s %s, then %s after the restart" % (first, second, third), flush=True)

    # --- 2. a handler that takes the process down is counted -----------------------
    print("a delivery the process died in counts as an attempt", flush=True)
    r.execute_command("QUEUE", "PUSH", "slow", "poison")
    r.execute_command("SAVEALL")
except BaseException:
    p.kill()
    raise

seen = []
for round in range(3):
    got = wait_until(lambda: len(hits) > len(seen), 20)
    if got:
        seen.append(hits[-1])
    p.kill()
    p.wait()
    p, r = start()
    if not got:
        break

try:
    assert seen == ["/?seq=1&attempts=0", "/?seq=1&attempts=1"], \
        "with max_attempts 2 the deliveries were %s" % seen
    assert wait_until(lambda: status_of(r, "slow").get("waiting") == "0"), status_of(r, "slow")
    assert status_of(r, "slow").get("dead") == "1", status_of(r, "slow")
    print("delivered %s, then dead lettered" % seen, flush=True)

    # --- 3. a timer queue is synced, and a torn one still opens - TODO 515 --------
    print("a durability=timer queue gets synced on the maintenance tick", flush=True)
    r.execute_command("configuration:SETF", "queues/held", '''
        function transport()
            return { kind = "queue", name = "held", space = "default", call = "TAKE",
                     user = "default", durability = "timer", poll = "1h", enabled = false }
        end''')
    unsynced = []
    for m in ("h1", "h2", "h3"):
        r.execute_command("QUEUE", "PUSH", "held", m)
        unsynced.append(int(status_of(r, "held").get("unsynced", "-1")))
    assert max(unsynced) > 0, "nothing was ever unsynced after a push: %s" % unsynced
    assert wait_until(lambda: status_of(r, "held").get("unsynced") == "0", 10), status_of(r, "held")
    print("unsynced after the pushes %s, then 0" % unsynced, flush=True)
    r.execute_command("SAVEALL")
finally:
    p.send_signal(signal.SIGTERM)
    p.wait(timeout=60)

print("a queue whose last length is garbage still opens", flush=True)
# three 18 byte records ("hN" behind a 16 byte header) in 22 byte elements after the
# 32 byte file header: at 32, 54 and 76. The last one's length is what a crash
# below `each` can leave as whatever was on the disk
with open(os.path.join(QDIR, "held.queue"), "r+b") as f:
    f.seek(76)
    f.write(b"\xff\xff\xff\xf0")
p, r = start()
try:
    got = r.execute_command("QUEUE", "PUSH", "held", "h4")
    assert isinstance(got, int), got
    # waiting comes from what the consumer last published, so give it a tick
    assert wait_until(lambda: status_of(r, "held").get("waiting") == "3", 10), \
        "h1, h2 and h4 should be waiting: %s" % status_of(r, "held")
    print("it opened, kept the two before the damage, and took a push", flush=True)
finally:
    p.send_signal(signal.SIGTERM)
    p.wait(timeout=60)

LOG.close()
with open(os.path.join(ROOT, "barchd.log"), errors="replace") as f:
    out = f.read()
assert "WARNING: ThreadSanitizer" not in out and "ERROR: AddressSanitizer" not in out, \
    out[out.find("Sanitizer") - 200:][:4000]
print("queue crash test passed")
