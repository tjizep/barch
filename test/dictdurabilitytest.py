# A space's compressed values and its dictionary stay together - TODO 518, 527.
#
# The dictionary used to live in its own file beside the shard files, and TODO 518
# was about keeping the two in step: a missing or torn file, a new dictionary saved
# over the old one, values that read back as "". Now it's a meta key in the space
# (TODO 527), saved, logged and copied with the values that need it, so there is no
# second file to lose. What's left to check is that it's stored before anything is
# compressed with it, whichever way it was made, and that a store written by an
# older build, with the file, comes across.
#
# Checked, each on a fresh data directory:
#   1. TRAIN, compressed values, SAVE, kill -9: every value reads back, the space
#      has the same dictionary, and there's no dictionary file anywhere
#   2. the same with a change log and no SAVE: the log brings the dictionary back
#      ahead of the values that need it
#   3. a dictionary trained by compressing, not by TRAIN: it's stored by the
#      space's maintenance tick before anything is compressed with it, and the
#      values and the dictionary read back after SAVE and kill -9
#   4. a different dictionary put with DICTIONARY SET is refused
#   5. with BARCHD_OLD naming a build from before TODO 527: a store it wrote,
#      dictionary file and all, reads back here, and the file is moved into the
#      space and set aside
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
PORT = scale.port(default=14518)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)
OLD = os.environ.get("BARCHD_OLD", "")

DATA = os.path.join(os.getcwd(), "dictdurability_data")
LOGS = os.path.join(os.getcwd(), "dictdurability_logs")
ARGS = ["-c", "compression=zstd", "-c", "save_interval=86400000",
        "-c", "max_modifications_before_save=1000000000"]
N = 200


def start(binary=BINARY):
    p = subprocess.Popen([binary, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA] + ARGS,
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
    if p is not None and p.poll() is None:
        p.send_signal(signal.SIGKILL)
        p.wait(timeout=30)


def stop(p):
    if p is None or p.poll() is not None:
        return
    p.send_signal(signal.SIGTERM)
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
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def cmd(r, space, *args):
    return r.execute_command(space + ":" + args[0], *args[1:])


def value(i, seed=1):
    words = ["alpha", "beta", "gamma", "delta"] if seed == 1 else ["red", "green", "blue", "cyan"]
    return ('{"id": %d, "name": "record number %d", "tags": ["%s", "%s", "%s"], '
            '"note": "the quick brown fox jumps over the lazy dog %d", '
            '"address": {"street": "%d %s street", "city": "%s", "zip": "%05d"}}'
            % (i, i, words[0], words[1], words[2], i, i % 97, words[3], words[i % 4], i % 99991)).encode()


def train(r, space, seed=1):
    # records shaped like the values, so the dictionary is one they shrink with.
    # The seed changes the words, so two seeds train two different dictionaries
    left, n = 512000, 0
    while left > 0:
        chunk = b"".join(value(n * 100 + i, seed) for i in range(100))
        left = cmd(r, space, "TRAIN", chunk)
        n += 1
        assert n < 60, "the dictionary never trained"


def fill(r, space):
    """N values, each compressed; answers how many were"""
    done = 0
    for i in range(N):
        cmd(r, space, "SET", "k%04d" % i, value(i))
        done += cmd(r, space, "COMPRESS", "k%04d" % i)
    return done


def readable(r, space):
    good = 0
    for i in range(N):
        try:
            if cmd(r, space, "GET", "k%04d" % i) == value(i):
                good += 1
        except redis.exceptions.ResponseError:
            pass
    return good


def dict_files():
    return sorted(os.path.basename(f) for f in glob.glob(os.path.join(DATA, "barch_dict*.dat")))


def fresh():
    for d in (DATA, LOGS):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)


server = None
try:
    print("1. TRAIN, compressed values, SAVE, kill -9", flush=True)
    fresh()
    server = start()
    r = client()
    train(r, "dd")
    d1 = cmd(r, "dd", "DICTIONARY", "GET")
    check(d1 is not None and fill(r, "dd") == N, "the space trained a dictionary and compressed with it")
    cmd(r, "dd", "SAVE")
    kill(server)
    server = start()
    r = client()
    check(readable(r, "dd") == N, "every value reads back after the restart")
    check(cmd(r, "dd", "DICTIONARY", "GET") == d1, "with the same dictionary")
    check(not dict_files(), "and no dictionary file anywhere (%s)" % dict_files())

    print("4. a different dictionary is refused", flush=True)
    try:
        cmd(r, "dd", "DICTIONARY", "SET", d1[:-1] + bytes([d1[-1] ^ 0xFF]))
        refused = False
    except redis.exceptions.ResponseError:
        refused = True
    check(refused, "DICTIONARY SET of another one is refused")
    kill(server)

    print("2. with a change log and no SAVE", flush=True)
    fresh()
    server = start()
    r = client()
    r.execute_command("configuration:SET", "dl.aof_dir", LOGS)
    r.execute_command("configuration:SAVE")
    train(r, "dl")
    check(fill(r, "dl") == N, "values compressed")
    time.sleep(1)                           # past the default timer's sync
    kill(server)
    server = start()
    r = client()
    check(readable(r, "dl") == N, "the log brings the dictionary back ahead of the values")
    kill(server)

    print("3. a dictionary trained by compressing", flush=True)
    fresh()
    server = start()
    r = client()
    # COMPRESS feeds the training until there's a dictionary; the one it trains
    # waits for the maintenance tick to store it, and only then does COMPRESS work
    deadline = time.time() + 60
    first = None
    i = 0
    while time.time() < deadline and first is None:
        cmd(r, "dp", "SET", "t%05d" % i, value(10000 + i))
        if cmd(r, "dp", "COMPRESS", "t%05d" % i):
            first = i
        i += 1
        if i % 2000 == 0:
            time.sleep(0.2)                 # give the tick a moment once it could have trained
    check(first is not None, "compressing trained a dictionary and then used it (after %s values)" % first)
    check(fill(r, "dp") == N, "and compresses with it")
    d3 = cmd(r, "dp", "DICTIONARY", "GET")
    cmd(r, "dp", "SAVE")
    kill(server)
    server = start()
    r = client()
    check(readable(r, "dp") == N and cmd(r, "dp", "DICTIONARY", "GET") == d3,
          "after SAVE and kill -9 the values read back, with the same dictionary")
    kill(server)

    if OLD:
        print("5. a store written by %s" % OLD, flush=True)
        fresh()
        server = start(OLD)
        r = client()
        train(r, "dm")
        check(fill(r, "dm") == N, "the old build compressed the values")
        d_old = cmd(r, "dm", "DICTIONARY", "GET")
        cmd(r, "dm", "SAVE")
        stop(server)
        before = dict_files()
        check(bool(before), "and wrote its dictionary to a file (%s)" % before)
        server = start()
        r = client()
        check(readable(r, "dm") == N, "every value reads back with this build")
        check(cmd(r, "dm", "DICTIONARY", "GET") == d_old, "with the old build's dictionary")
        moved = sorted(os.path.basename(f) for f in glob.glob(os.path.join(DATA, "barch_dict*.moved-*")))
        check("barch_dict_dm_.dat" not in dict_files() and moved,
              "the space's file was moved into it and set aside (%s)" % moved)
        cmd(r, "dm", "SAVE")
        kill(server)
        server = start()
        r = client()
        check(readable(r, "dm") == N, "and it reads back again from the space alone")
        kill(server)
    else:
        print("5. skipped: set BARCHD_OLD to a build from before TODO 527 to check a store it wrote",
              flush=True)
finally:
    kill(server)

print("\n%s" % ("all dictionary durability checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
