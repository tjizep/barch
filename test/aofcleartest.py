# FLUSHDB and FLUSHALL are in the change log - TODO 478.
#
# They cleared the shards and wrote nothing to the log, so after a kill -9 the
# replay put back every write since the last checkpoint on top of the last saved
# files, and every flushed key came back. Now a `clear` record goes in, under
# every shard's write latch, so the log has the clear exactly where it happened.
#
# Checked for a hash-sharded and a range-sharded space:
#   - writes, SAVE, writes, FLUSHDB, writes, kill -9: only the last writes
#   - then SAVE, writes, kill -9: the checkpoint covers the clear
#   - a writer running while FLUSHDB happens, then kill -9: exactly the keys that
#     were live before the kill, so the clear landed between the right writes
# And FLUSHALL over two spaces with logs.
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
PORT = scale.port(default=14478)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "aofclear_data")
LOGS = os.path.join(os.getcwd(), "aofclear_logs")
N = 500
# `timer` is enough for kill -9, which leaves the page cache alone
ARGS = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000",
        "-c", "aof_durability=timer"]


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
    p.send_signal(sig)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=30)


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=60)


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def write(r, space, prefix, n=N):
    pipe = r.pipeline(transaction=False)
    for i in range(n):
        pipe.execute_command(space + ":SET", "%s%05d" % (prefix, i), prefix)
    pipe.execute()


def present(r, space, prefix, n=N):
    pipe = r.pipeline(transaction=False)
    for i in range(n):
        pipe.execute_command(space + ":GET", "%s%05d" % (prefix, i))
    return sum(1 for v in pipe.execute() if v == prefix.encode())


def configure(spaces):
    for d in (DATA, LOGS):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)
    proc = start()
    try:
        r = client()
        for space, range_sharded in spaces:
            r.execute_command("configuration:SET", space + ".shards", "2")
            r.execute_command("configuration:SET", space + ".aof_dir", LOGS)
            if range_sharded:
                r.execute_command("configuration:SET", space + ".ordered", "1")
                r.execute_command("configuration:SET", space + ".range_sharded", "1")
        r.execute_command("configuration:SAVE")
    finally:
        stop(proc)


def flushdb(r, space):
    r.execute_command("USE", space)
    r.execute_command("FLUSHDB")


def run_space(space, range_sharded):
    print("%s space" % ("range-sharded" if range_sharded else "hash-sharded"), flush=True)
    configure([(space, range_sharded)])

    # writes, SAVE, writes, FLUSHDB, writes, kill -9
    proc = start()
    try:
        r = client()
        write(r, space, "a")
        r.execute_command(space + ":SAVE")
        write(r, space, "b")
        flushdb(r, space)
        write(r, space, "c")
    finally:
        stop(proc, signal.SIGKILL)
    proc = start()
    try:
        r = client()
        a, b, c = present(r, space, "a"), present(r, space, "b"), present(r, space, "c")
        check(a == 0 and b == 0, "nothing from before FLUSHDB came back (%d saved, %d logged)"
              % (a, b))
        check(c == N, "everything after it did (%d of %d)" % (c, N))
        check(r.execute_command(space + ":DBSIZE") == N, "and nothing else")

        # SAVE covers the clear, and the log after it still replays
        r.execute_command(space + ":SAVE")
        write(r, space, "d")
    finally:
        stop(proc, signal.SIGKILL)
    proc = start()
    try:
        r = client()
        c, d = present(r, space, "c"), present(r, space, "d")
        check(c == N and d == N, "after a SAVE and more writes, both are back (%d, %d)" % (c, d))
        check(r.execute_command(space + ":DBSIZE") == 2 * N, "and nothing else")
    finally:
        stop(proc, signal.SIGKILL)

    # a writer running while FLUSHDB happens
    proc = start()
    try:
        r = client()
        stop_at = threading.Event()
        written = []

        def writer():
            c = client()
            i = 0
            while not stop_at.is_set():
                c.execute_command(space + ":SET", "w%07d" % i, "w")
                written.append(i)
                i += 1

        t = threading.Thread(target=writer)
        t.start()
        time.sleep(0.3)
        flushdb(r, space)
        time.sleep(0.3)
        stop_at.set()
        t.join(timeout=60)
        pipe = r.pipeline(transaction=False)
        for i in written:
            pipe.execute_command(space + ":EXISTS", "w%07d" % i)
        live = {i for i, e in zip(written, pipe.execute()) if e}
        check(0 < len(live) < len(written),
              "FLUSHDB landed in the middle of the writes (%d of %d live)" % (len(live), len(written)))
    finally:
        stop(proc, signal.SIGKILL)
    proc = start()
    try:
        r = client()
        pipe = r.pipeline(transaction=False)
        for i in written:
            pipe.execute_command(space + ":EXISTS", "w%07d" % i)
        back = {i for i, e in zip(written, pipe.execute()) if e}
        check(back == live, "after kill -9, exactly the keys that were live (%d extra, %d missing)"
              % (len(back - live), len(live - back)))
        check(r.execute_command(space + ":DBSIZE") == len(live), "and nothing else")
    finally:
        stop(proc)


def run_flushall():
    print("FLUSHALL over two spaces", flush=True)
    configure([("fh", False), ("fg", True)])
    proc = start()
    try:
        r = client()
        for s in ("fh", "fg"):
            write(r, s, "a")
            r.execute_command(s + ":SAVE")
            write(r, s, "b")
        r.execute_command("FLUSHALL")
        for s in ("fh", "fg"):
            write(r, s, "c")
    finally:
        stop(proc, signal.SIGKILL)
    proc = start()
    try:
        r = client()
        for s in ("fh", "fg"):
            a, b, c = present(r, s, "a"), present(r, s, "b"), present(r, s, "c")
            check(a == 0 and b == 0 and c == N,
                  "%s: only what came after FLUSHALL (%d, %d, %d)" % (s, a, b, c))
    finally:
        stop(proc)


run_space("ch", False)
run_space("cg", True)
run_flushall()

print("\n%s" % ("all change log clear checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
