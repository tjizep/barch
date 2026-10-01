# One process to a data directory - TODO 571.
#
# Nothing stopped two processes from using the same directory. A second barchd
# on the same --dir started, loaded the first one's keys, took 1,000 writes and
# said OK to SAVE; the first one then saved, and after a restart none of the
# second one's keys were left. Startup also removes `.wal` files it didn't write,
# which could be the other process's save in progress.
#
# Now the data directory is held with flock on the directory itself, and so is
# each change log directory. Five cases:
#   1. a second barchd on a held directory exits, and says who holds it
#   2. the first one is untouched and goes on working
#   3. after a clean stop the directory is free again
#   4. after kill -9 it's free too - the kernel drops the lock, nothing stale
#   5. a space whose change log directory another process holds is refused,
#      and a space logging into its own data directory isn't
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
PORT = scale.port(default=14982)
PORT2 = scale.port(1, default=14983)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

ROOT = os.getcwd()
DATA = os.path.join(ROOT, "lock_data")
OTHER = os.path.join(ROOT, "lock_other")
LOGS = os.path.join(ROOT, "lock_logs")
for d in (DATA, OTHER, LOGS):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def launch(port, data):
    return subprocess.Popen([BINARY, "--port", str(port), "--bind", "127.0.0.1", "--dir", data],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def wait_up(p, port):
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            return False
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            return True
        except OSError:
            time.sleep(0.1)
    return False


def start(port, data):
    p = launch(port, data)
    if not wait_up(p, port):
        out = p.stdout.read().decode(errors="replace") if p.poll() is not None else ""
        p.kill()
        raise AssertionError("barchd on %s did not start:\n%s" % (data, out[-2000:]))
    return p


def stop(p, sig=signal.SIGTERM):
    if p.poll() is not None:
        return
    p.send_signal(sig)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=30)


def client(port):
    return redis.Redis(host="127.0.0.1", port=port, protocol=2, socket_timeout=60)


print("TestDataDirLock")
first = start(PORT, DATA)
try:
    a = client(PORT)
    a.set("a", "1")

    # 1. a second one on the same directory
    second = launch(PORT2, DATA)
    try:
        code = second.wait(timeout=60)
    except subprocess.TimeoutExpired:
        code = None
        stop(second)
    said = second.stdout.read().decode(errors="replace")
    check(code not in (None, 0), "a second barchd on a held directory exits (%s)" % code)
    check("held" in said and str(first.pid) in said,
          "and says the directory is held, and by which process")

    # 2. the first one never noticed
    a.set("b", "2")
    a.execute_command("SAVE")
    check(a.get("a") == b"1" and a.get("b") == b"2", "the first one goes on working")
finally:
    stop(first)

# 3. free after a clean stop
again = start(PORT2, DATA)
r = client(PORT2)
check(r.get("a") == b"1" and r.get("b") == b"2", "after a clean stop another one starts, with the keys")
# 4. free after kill -9 too
stop(again, signal.SIGKILL)
again = start(PORT2, DATA)
check(True, "after kill -9 another one starts")
stop(again)

# 5. change log directories
first = start(PORT, DATA)
second = start(PORT2, OTHER)
try:
    a = client(PORT)
    b = client(PORT2)
    for c in (a, b):
        c.execute_command("configuration:SET", "lg.aof_dir", LOGS)
        c.execute_command("configuration:SAVE")
    a.execute_command("USE", "lg")
    a.set("x", "1")
    try:
        b.execute_command("USE", "lg")
        b.set("y", "1")
        refused = None
    except redis.exceptions.ResponseError as e:
        refused = str(e)
    check(refused is not None and "held" in (refused or ""),
          "a space whose change log directory is held elsewhere is refused (%s)" % refused)

    # a log kept inside the process's own data directory isn't a second claim
    a.execute_command("configuration:SET", "own.aof_dir", DATA)
    a.execute_command("configuration:SAVE")
    try:
        a.execute_command("USE", "own")
        a.set("z", "1")
        ok = a.get("z") == b"1"
    except redis.exceptions.ResponseError as e:
        ok = False
        print("   ", e)
    check(ok, "a change log in the process's own data directory works")
finally:
    stop(second)
    stop(first)

print("%s: %d failure(s)" % ("FAIL" if failures else "PASS", failures))
sys.exit(1 if failures else 0)
