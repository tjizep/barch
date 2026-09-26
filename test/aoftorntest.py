# A torn record in the change log doesn't hide the writes after it - TODO 466.
#
# A crash under durability weaker than `each` can leave the last record of the
# change log half written. Replay applies what comes before it and stops there,
# which is right. But the bad record used to stay in the file, with new writes
# appended after it. Replay, the checkpoint scan and the trim all stop at the
# first record that doesn't decode, so from then on every write after it was
# skipped on every restart, `each` or not, and the log was never trimmed again.
# Now the log cuts the bad record off as it opens.
#
# The torn record is made by hand: the last write carries a value nothing else
# has, and one byte of it is flipped in the file after a kill -9, so its checksum
# fails the way a torn write's would.
#
#   run 1  writes A, then the marker write, kill -9, flip a byte in the marker
#   run 2  A is back and the marker isn't. Writes B under `each`, kill -9
#   run 3  B is back. SAVE, then C, kill -9
#   run 4  C is back, and the log no longer stops at a bad record
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
PORT = scale.port(default=14466)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "aoftorn_data")
LOGS = os.path.join(os.getcwd(), "aoftorn_logs")
for d in (DATA, LOGS):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)

SPACE = "tt"
COUNT = 200
MARKER = b"TORN-TAIL-" + bytes(range(0x41, 0x5b)) * 4
STOPS = b"stops at sequence"
CUT = b"were cut off"
# no interval saves, so the log is the only way back; `each` so every answered
# write is on disk before the kill
ARGS = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000",
        "-c", "aof_durability=each"]


def start():
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA] + ARGS,
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


def stop(p, sig=signal.SIGTERM):
    """stop it, and return what it printed"""
    p.send_signal(sig)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=30)
    return p.stdout.read()


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=120)


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def write(r, prefix):
    for i in range(COUNT):
        r.execute_command(SPACE + ":SET", "%s%04d" % (prefix, i), prefix)


def missing(r, prefix):
    return [i for i in range(COUNT)
            if r.execute_command(SPACE + ":GET", "%s%04d" % (prefix, i)) != prefix.encode()]


def tear():
    """flip a byte inside the marker, in whichever log file holds it"""
    for root, _, files in os.walk(LOGS):
        for name in files:
            path = os.path.join(root, name)
            with open(path, "rb") as f:
                data = bytearray(f.read())
            at = data.find(MARKER)
            if at < 0:
                continue
            data[at + len(MARKER) // 2] ^= 0xFF
            with open(path, "wb") as f:
                f.write(data)
            print("  tore %s at byte %d" % (name, at + len(MARKER) // 2), flush=True)
            return True
    return False


# --- run 0: the change log, on disk -------------------------------------------------
proc = start()
try:
    r = client()
    r.execute_command("configuration:SET", SPACE + ".aof_dir", LOGS)
    r.execute_command("configuration:SAVE")
finally:
    stop(proc)

# --- run 1: A, then the record that gets torn --------------------------------------
proc = start()
try:
    r = client()
    write(r, "a")
    r.execute_command(SPACE + ":SET", "marker", MARKER)
finally:
    stop(proc, signal.SIGKILL)
check(tear(), "the marker record was found in the log and torn")

# --- run 2: replay stops at it; B goes in after ------------------------------------
proc = start()
try:
    r = client()
    lost = missing(r, "a")
    check(not lost, "every write before the torn record is back (%d lost)" % len(lost))
    check(r.execute_command(SPACE + ":GET", "marker") is None, "the torn write isn't")
    write(r, "b")
finally:
    out = stop(proc, signal.SIGKILL)
check(CUT in out, "opening the log said it cut off a bad record")

# --- run 3: B is back; SAVE, then C -----------------------------------------------
proc = start()
try:
    r = client()
    lost = missing(r, "b")
    check(not lost, "every write after the restart is back (%d of %d lost)"
          % (len(lost), COUNT))
    lost = missing(r, "a")
    check(not lost, "and the ones before it still are (%d lost)" % len(lost))
    r.execute_command(SPACE + ":SAVE")
    write(r, "c")
finally:
    out = stop(proc, signal.SIGKILL)
check(STOPS not in out and CUT not in out, "the next start found nothing bad in the log")

# --- run 4: the SAVE's checkpoint was found and the log trimmed --------------------
proc = start()
try:
    r = client()
    lost = missing(r, "c")
    check(not lost, "every write after the SAVE is back (%d of %d lost)"
          % (len(lost), COUNT))
finally:
    out = stop(proc)
check(STOPS not in out and CUT not in out, "and neither did the one after the SAVE")

print("\n%s" % ("all torn change log checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
