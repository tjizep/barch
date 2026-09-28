# A change log replay never puts an older value back over a newer one that a
# shard file already has - TODO 520.
#
# Replay applied every record past the last checkpoint over the shard files, and
# counted on that being harmless. It is only while no file is ahead of the log.
# Now each shard file names the log it was saved against and that log's mark,
# and a shard only takes the records past it.
#
# Checked, against barchd with a two shard space:
#   - the log loses its tail after a shard file was saved (the log is put back
#     to a copy taken before the last write, the way a crash under a weak
#     durability loses what wasn't synced): the key keeps the newer value from
#     the file, not the older one the log still has
#   - after that, new writes are replayed normally on the next crash, so the
#     log's numbers didn't go backwards
#   - a run with the log's opt in lost while its file stays (TODO 522): the file
#     is left alone until the first save, which moves it aside, so when the opt
#     in comes back its stale records aren't replayed over what that run saved
#
# The stamp alone can't cover that last one: a file saved with no log is
# replayed over as before, because it can't be told from one a RETRIEVE copied
# in, and the log after a RETRIEVE holds writes newer than that file.
import glob
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
PORT = scale.port(default=14520)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "aofahead_data")
LOGS = os.path.join(os.getcwd(), "aofahead_logs")
SPACE = "t520"
QUIET = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000"]
WITH_LOG = ["-c", "aof_dir=" + LOGS, "-c", "aof_durability=each"]


def start(args):
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA, "--no-save-on-exit"] + args,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            raise AssertionError("barchd exited with %s" % p.returncode)
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % PORT)


def kill(p):
    p.send_signal(signal.SIGKILL)
    p.wait(timeout=30)


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=60)


failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def cmd(r, *args):
    return r.execute_command(SPACE + ":" + args[0], *args[1:])


def fresh():
    for d in (DATA, LOGS):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)
    p = start(QUIET + WITH_LOG)
    try:
        r = client()
        r.execute_command("configuration:SET", SPACE + ".shards", "2")
        r.execute_command("configuration:SET", SPACE + ".aof", "on")
        r.execute_command("configuration:SAVE")
    finally:
        kill(p)


def log_file():
    files = glob.glob(os.path.join(LOGS, SPACE + "*.aof"))
    assert files, "no change log in %s: %s" % (LOGS, os.listdir(LOGS))
    return files[0]


def shard_files():
    return {f: os.stat(f).st_mtime_ns for f in glob.glob(os.path.join(DATA, "leaves_" + SPACE + "*.dat"))}


print("the log loses its tail after a shard file was saved", flush=True)
fresh()
p = start(QUIET + WITH_LOG)
try:
    r = client()
    cmd(r, "SET", "k", "a")
    cmd(r, "SAVE")                              # a checkpoint, and k=a in the files
    cmd(r, "SET", "k", "b")                     # past the checkpoint
    tail = log_file() + ".copy"
    shutil.copyfile(log_file(), tail)           # the log as it is with k=b the last record
    cmd(r, "SET", "k", "c")                     # the write the log will lose
    before = shard_files()
    # a shard saved on its own, the way the interval save does it: no checkpoint
    r.execute_command("CONFIG", "SET", "save_interval", "100")
    end = time.time() + 20
    while time.time() < end and shard_files() == before:
        time.sleep(0.1)
    check(shard_files() != before, "a shard saved on its own with k=c in it")
finally:
    kill(p)
shutil.move(tail, log_file())                   # the tail after k=b is gone
p = start(QUIET + WITH_LOG)
try:
    r = client()
    got = cmd(r, "GET", "k")
    check(got == b"c", "k keeps the value from its file, not the log's older one (%r)" % got)
    cmd(r, "SET", "k", "d")                     # logged, not saved
finally:
    kill(p)
p = start(QUIET + WITH_LOG)
try:
    got = cmd(client(), "GET", "k")
    check(got == b"d", "a write after that is replayed after a crash (%r)" % got)
finally:
    kill(p)

print("a run with the log's opt in lost, then the log is back", flush=True)
fresh()
p = start(QUIET + WITH_LOG)
try:
    r = client()
    cmd(r, "SET", "k", "a")
    cmd(r, "SAVE")
    cmd(r, "SET", "k", "b")                     # in the log only
    # the opt in goes, the way it does when the configuration space isn't saved
    r.execute_command("configuration:SET", SPACE + ".aof", "off")
    r.execute_command("configuration:SAVE")
finally:
    kill(p)
logged = log_file()
p = start(QUIET + WITH_LOG)                     # the server still has aof_dir
try:
    r = client()
    check(os.path.exists(logged), "the unused log is left alone until something saves")
    cmd(r, "SET", "k", "c")
    cmd(r, "SAVE")                              # k=c in the files, saved without the log
    stale = glob.glob(logged + ".stale-*")
    check(not os.path.exists(logged) and len(stale) == 1,
          "the first save moved it aside (%s)" % [os.path.basename(f) for f in stale])
    r.execute_command("configuration:SET", SPACE + ".aof", "on")
    r.execute_command("configuration:SAVE")
finally:
    kill(p)
p = start(QUIET + WITH_LOG)
try:
    r = client()
    got = cmd(r, "GET", "k")
    check(got == b"c", "the old log's k=b isn't replayed over the file's k=c (%r)" % got)
    cmd(r, "SET", "k", "d")
finally:
    kill(p)
p = start(QUIET + WITH_LOG)
try:
    got = cmd(client(), "GET", "k")
    check(got == b"d", "the new log is replayed after a crash (%r)" % got)
finally:
    kill(p)

if failures:
    print("change log ahead test: %d FAILED" % failures)
    sys.exit(1)
print("change log ahead test passed")
