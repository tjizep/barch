# barch's files stay where they started when the working directory moves - TODO 526.
#
# Shard files, dictionaries and the replication files were named relative to the
# working directory and resolved again on every open, while the change log keeps
# its file open. So after `CONFIG SET dir` in Valkey, or an `os.chdir()` in Python,
# a SAVE wrote the shard files in the new directory, the log stayed in the old
# one, and its checkpoint said the save held everything. A restart where the
# process started found the old files and an empty log: every write since the
# move was gone. Now the directory is taken once, as an absolute path, and every
# data file is named from it.
#
# The embedded module runs in a forked child, so it's gone before barchd starts:
#   - in A: a space with a change log in a relative `aof_dir`, keys written, SAVE
#   - os.chdir(B): more keys, SAVE
# Then B holds none of the space's files, and barchd started in A has every key.
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
PORT = scale.port(default=14526)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

ROOT = os.getcwd()
A = os.path.join(ROOT, "datadir_a")
B = os.path.join(ROOT, "datadir_b")
N = 100

CHILD = r'''
import os, sys
sys.path.insert(0, %(test)r)
import redis, barch
os.chdir(%(a)r)
barch.start("0.0.0.0", %(port)d)
barch.ping("127.0.0.1", %(port)d)
r = redis.Redis(host="127.0.0.1", port=%(port)d, protocol=2)
r.execute_command("configuration:SET", "cw.aof_dir", "logs")    # relative on purpose
r.execute_command("configuration:SAVE")
for i in range(%(n)d):
    r.execute_command("cw:SET", "one%%d" %% i, "one%%d" %% i)
r.execute_command("cw:SAVE")
os.chdir(%(b)r)                         # what CONFIG SET dir or os.chdir does
for i in range(%(n)d):
    r.execute_command("cw:SET", "two%%d" %% i, "two%%d" %% i)
r.execute_command("cw:SAVE")
r.close()
barch.stop()
''' % {"test": os.path.dirname(os.path.abspath(__file__)), "a": A, "b": B, "port": PORT, "n": N}

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


print("a working directory that moves while barch runs (TODO 526)", flush=True)
for d in (A, B):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
# forked rather than a fresh interpreter: under a sanitizer the runtime is preloaded
# into this process, and a new python started from here loads it too late to import
# the module. Nothing here has imported barch or started a thread yet
pid = os.fork()
if pid == 0:
    code = 0
    try:
        exec(CHILD)
    except BaseException:
        import traceback
        traceback.print_exc()
        code = 1
    sys.stdout.flush()
    os._exit(code)
_, status = os.waitpid(pid, 0)
code = os.waitstatus_to_exitcode(status)
check(code == 0, "the embedded run finished (exit %d)" % code)

in_b = sorted(f for f in os.listdir(B) if "cw" in f or f == "logs")
check(not in_b, "nothing of the space was written where the directory moved to (%s)"
      % (in_b[:4] or "none"))
check(os.path.isdir(os.path.join(A, "logs")), "the change log is in the start directory")

p = subprocess.Popen([BINARY, "--port", str(PORT + 1), "--bind", "127.0.0.1", "--dir", A,
                      "--no-save-on-exit"],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    end = time.time() + 60
    while True:
        try:
            socket.create_connection(("127.0.0.1", PORT + 1), timeout=0.5).close()
            break
        except OSError:
            if time.time() > end or p.poll() is not None:
                raise AssertionError("barchd did not start")
            time.sleep(0.1)
    r = redis.Redis(host="127.0.0.1", port=PORT + 1, protocol=2)
    one = sum(1 for i in range(N) if r.execute_command("cw:GET", "one%d" % i) == b"one%d" % i)
    two = sum(1 for i in range(N) if r.execute_command("cw:GET", "two%d" % i) == b"two%d" % i)
    check(one == N and two == N,
          "barchd started there has every key (%d of %d before the move, %d of %d after)"
          % (one, N, two, N))
finally:
    p.send_signal(signal.SIGKILL)
    p.wait(timeout=30)

print("\n%s" % ("all data directory checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
