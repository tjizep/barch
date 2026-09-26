# A write the change log could not record is not applied either - TODO 460.
#
# The shard applies a write to memory first and appends it to the change log
# second. When the append throws - a full disk is the usual reason - the client
# gets an error but the key is already in the tree, so other clients read it and
# a crash loses it. A DEL that fails the same way is gone in memory and comes
# back on replay.
#
# There's no small filesystem to fill without root, so the server gets one
# through RLIMIT_FSIZE instead: past the limit a write or a truncate fails with
# EFBIG, which reaches the change log the same way ENOSPC would. SIGXFSZ is
# ignored in the child so the limit fails the write instead of killing the
# process.
#
# What's checked is that an error means "not applied": before the crash, where
# other clients would see it, and after it, where the log decides. And that an
# OK means applied, on both sides.
import os
import resource
import shutil
import signal
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14461)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "aoffail_data")
LOGS = os.path.join(os.getcwd(), "aoffail_logs")
for d in (DATA, LOGS):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)

SPACE = "lf"
# how big any file barchd writes may get during the run that fills the log. The
# log starts at 4 KiB and doubles, so the doubling past this is what fails
FILE_LIMIT = 1 << 20
# no interval saves: the shard files would hit the limit too, and the point is
# that only the log does
NO_SAVES = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000"]


def start(file_limit=None):
    def limit():
        if file_limit is not None:
            signal.signal(signal.SIGXFSZ, signal.SIG_IGN)   # inherited through exec
            resource.setrlimit(resource.RLIMIT_FSIZE, (file_limit, file_limit))

    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA] + NO_SAVES,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                         preexec_fn=limit)
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
    p.send_signal(sig)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=30)


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=120)


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def long_name(i):
    # longer than any tiny key below, so its erase record is bigger than the
    # last tiny write that fit - a DEL of one of these needs the log to grow
    return "long_%06d_" % i + "n" * 40


# --- run 0: the opt in, on disk, with no limit ----------------------------------
# the configuration space is saved here rather than under the limit, so the only
# file that can fail in run 1 is the change log
proc = start()
try:
    r = client()
    r.execute_command("configuration:SET", SPACE + ".aof_dir", LOGS)
    r.execute_command("configuration:SAVE")
finally:
    stop(proc)

# --- run 1: fill the log until it can't grow -------------------------------------
acked = {}            # key -> value the server said OK to
refused_sets = {}     # key -> value the server refused
refused_dels = []     # keys whose DEL was refused
deleted = []          # keys whose DEL was answered

proc = start(file_limit=FILE_LIMIT)
try:
    r = client()
    for i in range(50):
        k = long_name(i)
        r.execute_command(SPACE + ":SET", k, "long" + k)
        acked[k] = "long" + k

    # big values until one is refused: the log wanted to double past the limit
    big = "b" * 4096
    i = 0
    while not refused_sets and i < 10 * FILE_LIMIT // len(big):
        k = "big%06d" % i
        try:
            r.execute_command(SPACE + ":SET", k, big)
            acked[k] = big
        except redis.ResponseError as e:
            print("  first refused SET: %s (%s)" % (k, e), flush=True)
            refused_sets[k] = big
        i += 1
    check(bool(refused_sets), "a SET was refused once the log couldn't grow")

    # small ones until the space that's left is used up, so even a short record
    # needs the log to grow
    i = 0
    tiny_refused = 0
    while tiny_refused < 3 and i < 100000:
        k = "t%05d" % i
        try:
            r.execute_command(SPACE + ":SET", k, "v")
            acked[k] = "v"
        except redis.ResponseError:
            refused_sets[k] = "v"
            tiny_refused += 1
        i += 1
    check(tiny_refused == 3, "small SETs are refused too once it's full")

    for i in range(10):
        k = long_name(i)
        try:
            r.execute_command(SPACE + ":DEL", k)
            deleted.append(k)
            acked.pop(k, None)
        except redis.ResponseError:
            refused_dels.append(k)
    check(bool(refused_dels), "a DEL was refused once the log couldn't grow")

    # what another client sees now, before any crash
    other = client()
    leaked = [k for k in refused_sets if other.execute_command(SPACE + ":GET", k) is not None]
    if leaked:
        print("  refused but readable:", leaked[:3], flush=True)
    check(not leaked, "a refused SET isn't readable (%d are)" % len(leaked))
    gone = [k for k in refused_dels if other.execute_command(SPACE + ":GET", k) is None]
    if gone:
        print("  refused but deleted:", gone[:3], flush=True)
    check(not gone, "a refused DEL leaves the key in place (%d gone)" % len(gone))
finally:
    # no shutdown save: what comes back comes from the change log alone
    stop(proc, signal.SIGKILL)

# --- run 2: no limit, and the log decides what's there ---------------------------
proc = start()
try:
    r = client()
    lost = [k for k, v in acked.items() if r.execute_command(SPACE + ":GET", k) != v.encode()]
    if lost:
        print("  acknowledged but lost:", lost[:3], flush=True)
    check(not lost, "every acknowledged SET is back (%d lost)" % len(lost))
    back = [k for k in refused_sets if r.execute_command(SPACE + ":GET", k) is not None]
    check(not back, "no refused SET is back (%d are)" % len(back))
    kept = [k for k in refused_dels if r.execute_command(SPACE + ":GET", k) is None]
    check(not kept, "a key whose DEL was refused is still there (%d gone)" % len(kept))
    undead = [k for k in deleted if r.execute_command(SPACE + ":GET", k) is not None]
    check(not undead, "a key whose DEL was answered stays gone (%d back)" % len(undead))
    check(r.execute_command(SPACE + ":DBSIZE") == len(acked),
          "and nothing else is there")
finally:
    stop(proc)

print("\n%s" % ("all aof failure checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
