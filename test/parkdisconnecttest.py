# A parked CALLF whose client disconnects - TODO 559.
#
# A function that waits on I/O parks, and its connection waits for it. If the
# client goes away meanwhile, the session collector frees the session and the
# caller in it. The job still resumes later on a pool thread, and its interface
# held the caller by reference: `barch.space` read the freed caller's rights
# (and could write into it), and `barch.call` did the same. Under ASan that's a
# heap-use-after-free; in a normal build it corrupts the heap quietly.
#
# The call interface now holds a guard instead, so a resumed job whose caller
# has gone just stops finding spaces and commands. This shows up under a
# sanitizer; in a plain build it checks the server is still answering.
import http.server
import socket
import threading
import time

import scale
import redis
import barch

scale.workdir()
PORT = scale.port(default=14590)
WEB = scale.port(1, default=14591)
DROPS = 20


class Slow(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        time.sleep(0.5)
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *a):
        pass


web = http.server.ThreadingHTTPServer(("127.0.0.1", WEB), Slow)
threading.Thread(target=web.serve_forever, daemon=True).start()

print("start park disconnect test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)

failures = []


def check(ok, what):
    print("  %-70s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures.append(what)


try:
    r.execute_command("USE", "pdother")
    r.execute_command("SET", "k", "v")
    r.execute_command("USE", "pdhome")
    assert r.execute_command("SETF", "PARKER", '''
function call()
    local got = http.request("http://127.0.0.1:%d/slow"):timeout(5000):get()
    local v = barch.space["pdother"]:get("k")
    barch.call("PING")
    return tostring(got.status) .. v
end
''' % WEB) == b"OK"
    check(r.execute_command("CALLF", "PARKER") == b"200v",
          "a parked call opens another space and calls a command when it resumes")

    print("clients that leave while their call is parked")
    for i in range(DROPS):
        s = socket.create_connection(("127.0.0.1", PORT))
        s.sendall(b"*2\r\n$3\r\nUSE\r\n$6\r\npdhome\r\n")
        s.recv(100)
        s.sendall(b"*2\r\n$5\r\nCALLF\r\n$6\r\nPARKER\r\n")
        time.sleep(0.02)
        s.close()
    # let every one of them resume, and the collector take their sessions
    time.sleep(2.5)
    check(r.ping(), "%d dropped clients later, the server answers" % DROPS)
    check(r.execute_command("CALLF", "PARKER") == b"200v", "and a call still works")
finally:
    barch.stop()
    web.shutdown()

print()
if failures:
    print("FAILURES: %d" % len(failures))
    for f in failures:
        print("  " + f)
    raise SystemExit(1)
print("all park disconnect checks pass")
