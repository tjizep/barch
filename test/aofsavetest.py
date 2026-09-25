# Writes made while SAVE runs survive a crash - TODO 452.
#
# SAVE writes a hash-sharded space one shard after another while writes carry
# on, then checkpoints the change log and trims it. The checkpoint used to mean
# "everything before me is saved", so a write to a shard that had already been
# saved was trimmed from the log and was in no shard file, and a kill -9 after
# the save lost it. Now the checkpoint covers only where the log was when the
# save began.
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14460)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "aofsave_data")
LOGS = os.path.join(os.getcwd(), "aofsave_logs")
for d in (DATA, LOGS):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)


def start():
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA],
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            out = p.stdout.read().decode(errors="replace")
            raise AssertionError("barchd exited with %s:\n%s" % (p.returncode, out[-2000:]))
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % PORT)


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=120)


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


SPACE = "sv"
FILL = int(os.environ.get("AOFSAVE_FILL", "400000"))
ROUNDS = 3

proc = start()
try:
    r = client()
    # the opt in and the shard count are data in the configuration space, and
    # have to be on disk before the kill or the restart won't open the log
    r.execute_command("configuration:SET", SPACE + ".aof_dir", LOGS)
    r.execute_command("configuration:SET", SPACE + ".shards", "16")
    r.execute_command("configuration:SAVE")

    pipe = r.pipeline(transaction=False)
    for i in range(FILL):
        pipe.execute_command(SPACE + ":SET", "fill%07d" % i, "x" * 40)
        if i % 5000 == 4999:
            pipe.execute()
    pipe.execute()
    check(r.execute_command(SPACE + ":DBSIZE") == FILL, "the space is filled")

    # write steadily from another connection while SAVE runs, and keep only
    # the writes that were answered before the save finished
    during = []
    for round_ in range(ROUNDS):
        stop = threading.Event()
        written = []

        def writer():
            w = client()
            n = 0
            while not stop.is_set():
                k = "during%d_%07d" % (round_, n)
                w.execute_command(SPACE + ":SET", k, "v" + k)
                written.append(k)
                n += 1

        t = threading.Thread(target=writer)
        t.start()
        time.sleep(0.05)
        r.execute_command(SPACE + ":SAVE")
        stop.set()
        t.join()
        during.extend(written)
    print("  %d writes landed while %d saves ran" % (len(during), ROUNDS), flush=True)
    check(len(during) > 0, "some writes landed while the saves ran")
finally:
    # no shutdown save: what comes back has to come from the shard files and
    # the change log alone
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=30)

proc = start()
try:
    r = client()
    missing = [k for k in during if r.execute_command(SPACE + ":GET", k) != ("v" + k).encode()]
    if missing:
        print("  first missing:", missing[:5], flush=True)
    check(not missing, "every write made during a save is back (%d missing)" % len(missing))
    check(r.execute_command(SPACE + ":DBSIZE") == FILL + len(during),
          "and nothing else went missing")
finally:
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        proc.kill()

print("\n%s" % ("all aof save checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
