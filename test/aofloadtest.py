# LOAD and a failed RETRIEVE are in step with the change log - TODO 479.
#
# LOAD replaces a space from its shard files and told the change log nothing, so
# after a kill -9 the replay put the writes from before the LOAD back on top of
# the files. After a LOAD the files are the state, so the fix is a checkpoint at
# the LOAD, with the log mark taken while every shard is locked.
#
# RETRIEVE cleared every shard when it failed, with nothing in the log, the way
# FLUSHDB did before TODO 478.
#
# Checked for a hash-sharded and a range-sharded space:
#   - writes, SAVE, writes, LOAD, writes, kill -9: the saved and the last writes,
#     none from between
#   - LOAD of a space never saved, then writes, kill -9: only those writes
#   - a writer running through LOAD, then kill -9: exactly what was live
#   - a RETRIEVE that fails, then writes, kill -9: only those writes
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
PORT = scale.port(default=14479)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "aofload_data")
LOGS = os.path.join(os.getcwd(), "aofload_logs")
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


def answer(r, *args):
    try:
        return r.execute_command(*args)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def ok(reply):
    return reply in (b"OK", "OK")


def configure(space, range_sharded):
    for d in (DATA, LOGS):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)
    proc = start()
    try:
        r = client()
        r.execute_command("configuration:SET", space + ".shards", "2")
        r.execute_command("configuration:SET", space + ".aof_dir", LOGS)
        if range_sharded:
            r.execute_command("configuration:SET", space + ".ordered", "1")
            r.execute_command("configuration:SET", space + ".range_sharded", "1")
        r.execute_command("configuration:SAVE")
    finally:
        stop(proc)


def run_space(space, range_sharded):
    print("%s space" % ("range-sharded" if range_sharded else "hash-sharded"), flush=True)

    # writes, SAVE, writes, LOAD, writes, kill -9
    configure(space, range_sharded)
    proc = start()
    try:
        r = client()
        write(r, space, "a")
        r.execute_command(space + ":SAVE")
        write(r, space, "b")
        check(ok(answer(r, space + ":LOAD")), "LOAD is OK")
        check(present(r, space, "b") == 0, "and takes back what wasn't saved")
        write(r, space, "c")
    finally:
        stop(proc, signal.SIGKILL)
    proc = start()
    try:
        r = client()
        a, b, c = present(r, space, "a"), present(r, space, "b"), present(r, space, "c")
        check(a == N and c == N, "after kill -9, the saved and the last writes (%d, %d)" % (a, c))
        check(b == 0, "and none from before the LOAD (%d came back)" % b)
        check(r.execute_command(space + ":DBSIZE") == 2 * N, "and nothing else")
    finally:
        stop(proc, signal.SIGKILL)

    # LOAD of a space never saved: the files say empty
    configure(space, range_sharded)
    proc = start()
    try:
        r = client()
        write(r, space, "b")
        check(ok(answer(r, space + ":LOAD")), "LOAD of a space never saved is OK")
        check(r.execute_command(space + ":DBSIZE") == 0, "and leaves it empty, as its files are")
        write(r, space, "c")
    finally:
        stop(proc, signal.SIGKILL)
    proc = start()
    try:
        r = client()
        b, c = present(r, space, "b"), present(r, space, "c")
        check(b == 0 and c == N, "after kill -9, only what came after it (%d, %d)" % (b, c))
    finally:
        stop(proc, signal.SIGKILL)

    # a writer running through LOAD
    configure(space, range_sharded)
    proc = start()
    try:
        r = client()
        write(r, space, "a")
        r.execute_command(space + ":SAVE")
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
        r.execute_command(space + ":LOAD")
        time.sleep(0.3)
        stop_at.set()
        t.join(timeout=60)
        pipe = r.pipeline(transaction=False)
        for i in written:
            pipe.execute_command(space + ":EXISTS", "w%07d" % i)
        live = {i for i, e in zip(written, pipe.execute()) if e}
        check(0 < len(live) < len(written),
              "LOAD landed in the middle of the writes (%d of %d live)" % (len(live), len(written)))
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
        check(present(r, space, "a") == N, "and the saved ones")
    finally:
        stop(proc, signal.SIGKILL)

    # a RETRIEVE that fails clears the space
    configure(space, range_sharded)
    proc = start()
    try:
        r = client()
        write(r, space, "a")
        r.execute_command(space + ":SAVE")
        write(r, space, "b")
        reply = answer(r, space + ":RETRIEVE", "127.0.0.1", "1")
        check(not ok(reply), "RETRIEVE from nowhere fails")
        check(r.execute_command(space + ":DBSIZE") == 0, "and clears the space")
        write(r, space, "c")
    finally:
        stop(proc, signal.SIGKILL)
    proc = start()
    try:
        r = client()
        a, b, c = present(r, space, "a"), present(r, space, "b"), present(r, space, "c")
        check(a == 0 and b == 0 and c == N,
              "after kill -9, only what came after it (%d, %d, %d)" % (a, b, c))
    finally:
        stop(proc)


run_space("lh", False)
run_space("lg", True)

print("\n%s" % ("all change log load checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
