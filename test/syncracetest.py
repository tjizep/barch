# The git poller: when it runs, how often, and alone - TODOs 583 and 584.
#
# The poller ran a repository's sync without the lock FUNCTIONS SYNC takes, so the
# two could run in the same checkout together: two git resets at once (one of them
# fails on git's index.lock) and unlocked writes to the sync's own bookkeeping
# (ThreadSanitizer reports those, and barchd then exits 66).
#
# Checked here, on one running server:
#   1. a repository set up at run time is synced without FUNCTIONS SYNC (TODO 584)
#   2. its poll interval holds after the first run; it fell back to the one minute
#      idle cap (TODO 583)
#   3. clearing functions_dir does not stop the poller (TODO 584)
#   4. polled every 20 ms while a client sends FUNCTIONS SYNC as fast as it answers,
#      every sync succeeds, none starts while another runs, and barchd stops cleanly
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14989)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "syncrace_data")
shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)
LOG = os.path.join(DATA, "barchd.log")
SECONDS = scale.scaled_seconds(6.0, floor=2.0)
base = tempfile.mkdtemp(prefix="bdrace")
origin = os.path.join(base, "origin")

print("start sync race test with %s" % BINARY, flush=True)


def git(*args):
    subprocess.check_call(["git", "-C", origin, *args],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


subprocess.check_call(["git", "init", "-q", "-b", "main", origin])
git("config", "user.email", "t@t")
git("config", "user.name", "t")
os.makedirs(os.path.join(origin, "race"))
for i in range(20):
    with open(os.path.join(origin, "race", "f%02d.luau" % i), "w") as f:
        f.write('function call() return "%d" end\n' % i)
    with open(os.path.join(origin, "race", "k%02d.txt" % i), "w") as f:
        f.write("value %d\n" % i)
git("add", "-A")
git("commit", "-q", "-m", "v1")

def start():
    # a file and not a pipe, so a busy log cannot block the server
    logfile = open(LOG, "ab")
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA],
                         stdout=logfile, stderr=subprocess.STDOUT)
    logfile.close()
    scale.wait_for_port(PORT, proc=p, what="barchd")
    return p


def stop(p):
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        raise AssertionError("barchd did not stop")
    # a sanitizer report inside barchd is its exit status
    assert p.returncode == 0, "barchd exited with %s:\n%s" % (
        p.returncode, open(LOG, "rb").read().decode(errors="replace")[-3000:])


def wait_for(r, want, what, seconds=10):
    end = time.time() + seconds
    got = None
    while time.time() < end:
        try:
            got = r.execute_command("race.F07")
        except redis.exceptions.ResponseError:
            got = None                  # not there yet
        if got == want:
            return
        time.sleep(0.05)
    raise AssertionError("%s: race.F07 is %r after %d s" % (what, got, seconds))


def commit(body, msg):
    with open(os.path.join(origin, "race", "f07.luau"), "w") as f:
        f.write('function call() return "%s" end\n' % body)
    git("add", "-A")
    git("commit", "-q", "-m", msg)


errors = []
syncs = 0
try:
    proc = start()
    try:
        r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
        # set up while the server runs, and never asked for: the poller only
        # started at boot when a repository was already there - TODO 584
        print("a repository set up at run time is cloned without being asked", flush=True)
        for setting, value in (("url", origin), ("pull", "on"), ("ms", "200")):
            r.execute_command("configuration:SET", "git/repositories/race/" + setting, value)
        wait_for(r, b"7", "the new repository was not synced")

        # the interval was only kept for a repository's first run: after it the
        # poller slept the whole idle minute, so ms=200 polled once a minute - TODO 583
        print("a 200 ms poll picks a new commit up without being asked", flush=True)
        commit("seven", "v2")
        wait_for(r, b"seven", "the poll did not run")

        # clearing the old functions_dir stopped the poller even with repositories
        # configured the new way - TODO 584
        print("CONFIG SET functions_dir off leaves the poller running", flush=True)
        r.execute_command("CONFIG", "SET", "functions_dir", "functions")
        r.execute_command("CONFIG", "SET", "functions_dir", "off")
        commit("sieben", "v3")
        wait_for(r, b"sieben", "the poll stopped with functions_dir")

        r.execute_command("configuration:SET", "git/repositories/race/ms", "20")
        print("FUNCTIONS SYNC against a 20 ms poll for %.0f s" % SECONDS, flush=True)
        end = time.time() + SECONDS
        while time.time() < end:
            try:
                r.execute_command("FUNCTIONS", "SYNC", "race")
            except redis.exceptions.ResponseError as e:
                errors.append(str(e))
            syncs += 1
        assert r.execute_command("race.F07") == b"sieben"
        r.close()
    finally:
        stop(proc)
finally:
    shutil.rmtree(base, ignore_errors=True)

assert not errors, "%d of %d syncs failed, the first: %s" % (len(errors), syncs, errors[0])
# the overlap itself: git did not trip on it and ThreadSanitizer did not see it, so the
# sync says so in the log when one starts while another is running
log = open(LOG, "rb").read().decode(errors="replace")
overlaps = log.count("started while another sync was running")
assert overlaps == 0, "%d syncs started while another was running" % overlaps
print("sync race test complete, %d syncs" % syncs, flush=True)
