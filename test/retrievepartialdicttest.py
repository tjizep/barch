# A RETRIEVE that installs only some shards leaves every value readable - TODO 534.
#
# A space's dictionary used to be one, replaced by the source's as a RETRIEVE
# went in. A shard that failed to install kept values compressed with the old
# one, which was gone, and they answered an error. A kill -9 between two installs
# left the same mix on disk. Now a space keeps every dictionary it has seen, a
# value is read with the one its zstd frame names, and every shard file carries
# the space's dictionaries, so any mix of files brings what its values need.
#
# Checked, against two barchd with compression on, four shards each and a
# dictionary of their own, no change log: a directory where one shard's `.wal`
# goes makes that shard fail to install. Each of the four shards is made the
# failing one in turn, so one of the runs fails the shard holding the `dict` key.
# Every value, the source's on the installed shards and the local ones left on
# the failed shard, reads back; a value compressed afterwards reads back; and
# all of it after a SAVE and kill -9, when the files on disk are the mix.
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
LOCAL = scale.port(default=14534)
REMOTE = LOCAL + 1

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

LOCAL_DATA = os.path.join(os.getcwd(), "retrievepartial_local")
REMOTE_DATA = os.path.join(os.getcwd(), "retrievepartial_remote")
ARGS = ["-c", "compression=zstd", "-c", "save_interval=86400000",
        "-c", "max_modifications_before_save=1000000000"]
SPACE = "rd"
N = 120
SHARDS = 4


def start(port, data):
    p = subprocess.Popen([BINARY, "--port", str(port), "--bind", "127.0.0.1",
                          "--dir", data, "--no-save-on-exit"] + ARGS,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            raise AssertionError("barchd exited with %s" % p.returncode)
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % port)


def kill(p):
    if p is not None and p.poll() is None:
        p.send_signal(signal.SIGKILL)
        p.wait(timeout=30)


def client(port):
    return redis.Redis(host="127.0.0.1", port=port, protocol=2, socket_timeout=120)


failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def cmd(r, *args):
    return r.execute_command(SPACE + ":" + args[0], *args[1:])


def value(i, seed=1):
    words = ["alpha", "beta", "gamma", "delta"] if seed == 1 else ["red", "green", "blue", "cyan"]
    return ('{"id": %d, "name": "record number %d", "tags": ["%s", "%s", "%s"], '
            '"note": "the quick brown fox jumps over the lazy dog %d", '
            '"address": {"street": "%d %s street", "city": "%s", "zip": "%05d"}}'
            % (i, i, words[0], words[1], words[2], i, i % 97, words[3], words[i % 4], i % 99991)).encode()


def train(r, seed):
    left, n = 512000, 0
    while left > 0:
        chunk = b"".join(value(n * 100 + i, seed) for i in range(100))
        left = cmd(r, "TRAIN", chunk)
        n += 1
        assert n < 60, "the dictionary never trained"


def fill(r, prefix, seed):
    """N values, each compressed; answers how many compressed"""
    done = 0
    for i in range(N):
        cmd(r, "SET", "%s%04d" % (prefix, i), value(i, seed))
        done += cmd(r, "COMPRESS", "%s%04d" % (prefix, i))
    return done


def readable(r, prefix, seed):
    good = 0
    for i in range(N):
        try:
            if cmd(r, "GET", "%s%04d" % (prefix, i)) == value(i, seed):
                good += 1
        except redis.exceptions.ResponseError:
            pass
    return good


def fresh():
    for d in (LOCAL_DATA, REMOTE_DATA):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)




def shard_of(r, key):
    text = cmd(r, "INFO", "SHARD", key).decode()
    return int(re.search(r"number:(\d+)", text).group(1))


def run(failing):
    print("shard %d fails to install" % failing, flush=True)
    fresh()
    remote = start(REMOTE, REMOTE_DATA)
    local = start(LOCAL, LOCAL_DATA)
    try:
        rr, lr = client(REMOTE), client(LOCAL)
        for r in (rr, lr):
            r.execute_command("configuration:SET", SPACE + ".shards", str(SHARDS))
        train(rr, 1)
        check(fill(rr, "r", 1) == N, "the source compressed all %d values" % N)
        train(lr, 2)
        check(fill(lr, "l", 2) == N, "the local space compressed %d with its own" % N)
        cmd(lr, "SAVE")
        stayed = [i for i in range(N) if shard_of(lr, "l%04d" % i) == failing]
        came = [i for i in range(N) if shard_of(lr, "r%04d" % i) != failing]
        planted = os.path.join(LOCAL_DATA, "leaves_%s_%d.dat.wal" % (SPACE, failing))
        os.makedirs(planted)
        with open(os.path.join(planted, "x"), "w") as f:
            f.write("x")
        try:
            cmd(lr, "RETRIEVE", "127.0.0.1", str(REMOTE))
            refused = False
        except redis.exceptions.ResponseError:
            refused = True
        check(refused, "RETRIEVE fails on shard %d" % failing)

        def reads(key, want):
            try:
                return cmd(lr, "GET", key) == want
            except redis.exceptions.ResponseError:
                return False            # a value it can't decompress

        def read_all(tag):
            good_l = sum(1 for i in stayed if reads("l%04d" % i, value(i, 2)))
            good_r = sum(1 for i in came if reads("r%04d" % i, value(i, 1)))
            check(good_l == len(stayed), "%s: the local values left on shard %d read (%d of %d)"
                  % (tag, failing, good_l, len(stayed)))
            check(good_r == len(came), "%s: the source's values that went in read (%d of %d)"
                  % (tag, good_r, len(came)))

        read_all("after the RETRIEVE")
        cmd(lr, "SET", "after", value(99999, 2))
        cmd(lr, "COMPRESS", "after")
        check(reads("after", value(99999, 2)), "a value compressed afterwards reads back")
        shutil.rmtree(planted)
        cmd(lr, "SAVE")
        kill(local)
        local = start(LOCAL, LOCAL_DATA)
        lr = client(LOCAL)
        read_all("after kill -9")
        check(reads("after", value(99999, 2)), "after kill -9: the later value reads")
    finally:
        kill(local)
        kill(remote)


for failing in range(SHARDS):
    run(failing)

if failures:
    print("partial retrieve dictionary test: %d FAILED" % failures)
    sys.exit(1)
print("partial retrieve dictionary test passed")
