"""
COMMITTX doesn't answer OK when its marker may not be on the disk - TODO 881.

A commit appends its writes and its marker to the change log, syncs, and answers OK.
`aof::log::sync()` returned quietly once the file was broken, and the commit took that
as synced. Under a byte durability the marker's own append syncs. When that sync
failed, the failure was kept back, the file was broken, and the commit's sync after it
said nothing: OK for a transaction a restart could drop.

barchd runs with BARCH_TEST_AOF_FAIL_MARKER_SYNC, which fails the sync a marker's
append makes, as EIO would, and with aof_durability=1 so every append syncs:
  - a plain SET before it is fine;
  - COMMITTX answers UNKNOWN, not OK;
  - the log takes no more writes after, and says why.
"""
import os
import shutil
import signal
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14836)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "txsyncfail_data")
LOGS = os.path.join(os.getcwd(), "txsyncfail_logs")
for d in (DATA, LOGS):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
SPACE = "sf"
NO_SAVES = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000"]

failures = 0


def check(ok, what):
    global failures
    print("  %-74s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def start(name, extra=(), env=None):
    out = open(os.path.join(os.getcwd(), name), "w")
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA] + NO_SAVES
                         + list(extra), stdout=out, stderr=out, env=env)
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            raise AssertionError("barchd exited with %s, see %s" % (p.returncode, name))
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % PORT)


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=120, decode_responses=True,
                       single_connection_client=True)


# the opt in, saved with nothing failing
proc = start("txsyncfail0.log")
try:
    r = client()
    r.execute_command("configuration:SET", SPACE + ".aof_dir", LOGS)
    r.execute_command("configuration:SAVE")
finally:
    proc.send_signal(signal.SIGTERM)
    proc.wait()

proc = start("txsyncfail1.log", ["-c", "aof_durability=1"],
             dict(os.environ, BARCH_TEST_AOF_FAIL_MARKER_SYNC="1"))
try:
    r = client()
    r.execute_command("USE", SPACE)
    print("a commit whose marker's sync fails")
    check(r.execute_command("SET", "plain", "1") in (True, "OK"), "a plain SET before it is logged")
    r.execute_command("BEGINTX")
    r.execute_command("SET", "k", "v")
    try:
        got = str(r.execute_command("COMMITTX"))
    except redis.ResponseError as e:
        got = "error: " + str(e)
    check("UNKNOWN" in got, "COMMITTX answers UNKNOWN, not OK (%s)" % got[:60])
    try:
        got = str(r.execute_command("SET", "after", "2"))
    except redis.ResponseError as e:
        got = "error: " + str(e)
    check("no more changes" in got, "the log takes no more writes, and says why (%s)" % got[:60])
finally:
    proc.kill()
    proc.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
