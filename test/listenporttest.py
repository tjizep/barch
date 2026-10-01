# listen_port is the port it is set to.
#
# SetListenPort read the number out of `external_host` instead of the value it was
# given, so `--config external_host=127.0.0.1 --config listen_port=14500` made the
# server try to listen on port 127 (the leading digits of the address) and log
# "failed to start server bind: Permission denied". A host with no leading digits
# left the port at 0.
#
# This starts a barchd with both settings and checks that it serves on its own port
# without a failed bind, that CONFIG GET returns the port that was given, and that a
# value that is not a port is refused. A live CONFIG SET of a valid port restarts the
# listener, which configtest.py leaves alone for the same reason, so only the
# refusals are tried live.
import os
import signal
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14510)
ADVERTISED = PORT + 1

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "listenport_data")
LOG = os.path.join(os.getcwd(), "listenport.log")
os.makedirs(DATA, exist_ok=True)

failures = []


def check(ok, what):
    print(("ok   " if ok else "FAIL ") + what, flush=True)
    if not ok:
        failures.append(what)


proc = subprocess.Popen(
    [BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA,
     "--config", "external_host=127.0.0.1",
     "--config", "listen_port=%d" % ADVERTISED],
    stdout=open(LOG, "wb"), stderr=subprocess.STDOUT)
try:
    scale.wait_for_port(PORT, proc=proc, what="barchd")
    r = redis.Redis(host="127.0.0.1", port=PORT, protocol=2, decode_responses=True)
    check(r.ping(), "serves on its own port")

    got = r.execute_command("CONFIG", "GET", "listen_port")
    check(got[1] == str(ADVERTISED), "listen_port reads back as given, not as 127 (got %s)" % got[1])

    time.sleep(0.5)
    log = open(LOG, "rb").read()
    check(b"failed to start server bind" not in log, "no failed bind in the log")

    for bad in ("65536", "70000", "abc", "12x", "-1", "127.0.0.1"):
        try:
            r.execute_command("CONFIG", "SET", "listen_port", bad)
            check(False, "CONFIG SET listen_port %s is refused" % bad)
        except redis.ResponseError:
            check(True, "CONFIG SET listen_port %s is refused" % bad)
    got = r.execute_command("CONFIG", "GET", "listen_port")
    check(got[1] == str(ADVERTISED), "a refused value leaves the port as it was")
    check(r.ping(), "still serving after the refusals")
finally:
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        proc.kill()

if failures:
    print("%d failed" % len(failures))
    sys.exit(1)
print("listen_port test passed")
