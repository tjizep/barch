# A RETRIEVE brings the source's zstd dictionary with the space - TODO 521.
#
# RETRIEVE sent the shard files as they were, compressed leaves included, and a
# replica has its own dictionary, so every compressed value that arrived was
# unreadable there. Before DONE 486 they read back as "". Now the dictionary
# comes after the shards, and the receiving side takes it before the files go
# in, whatever it had.
#
# Two servers, compression on both:
#   - into a space with no dictionary: every compressed value reads back byte for
#     byte, the space has the source's dictionary, and both hold over kill -9
#   - into a space that trained a different dictionary of its own and has
#     compressed values with it: the same, and a value compressed afterwards
#     uses the new dictionary and reads back, which means each thread's copy of
#     the old one was dropped
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
LOCAL = scale.port(default=14521)
REMOTE = LOCAL + 1

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

LOCAL_DATA = os.path.join(os.getcwd(), "retrievedict_local")
REMOTE_DATA = os.path.join(os.getcwd(), "retrievedict_remote")
ARGS = ["-c", "compression=zstd", "-c", "save_interval=86400000",
        "-c", "max_modifications_before_save=1000000000"]
SPACE = "rd"
N = 300


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


def run(label, local_trains):
    print(label, flush=True)
    fresh()
    remote = start(REMOTE, REMOTE_DATA)
    local = start(LOCAL, LOCAL_DATA)
    try:
        rr, lr = client(REMOTE), client(LOCAL)
        train(rr, 1)
        source_dict = cmd(rr, "DICTIONARY", "GET")
        check(fill(rr, "r", 1) == N, "the source compressed all %d values" % N)
        if local_trains:
            train(lr, 2)
            own = cmd(lr, "DICTIONARY", "GET")
            check(own is not None and own != source_dict, "the local space trained its own dictionary")
            check(fill(lr, "l", 2) == N, "and compressed values of its own with it")
        reply = cmd(lr, "RETRIEVE", "127.0.0.1", str(REMOTE))
        check(reply == b"OK", "RETRIEVE works (%r)" % reply)
        got = readable(lr, "r", 1)
        check(got == N, "every compressed value reads back here (%d of %d)" % (got, N))
        check(cmd(lr, "DICTIONARY", "GET") == source_dict, "and the space has the source's dictionary")
        # compressed here, after the swap: each thread's copy has to be the new one
        cmd(lr, "SET", "after", value(99999, 1))
        check(cmd(lr, "COMPRESS", "after") == 1 and cmd(lr, "GET", "after") == value(99999, 1),
              "a value compressed afterwards uses it and reads back")
        cmd(lr, "SAVE")
    finally:
        kill(local)
    local = start(LOCAL, LOCAL_DATA)
    try:
        lr = client(LOCAL)
        got = readable(lr, "r", 1)
        check(got == N and cmd(lr, "GET", "after") == value(99999, 1),
              "all of it holds over kill -9 (%d of %d)" % (got, N))
    finally:
        kill(local)
        kill(remote)


run("into a space with no dictionary", False)
run("into a space with a different dictionary of its own", True)

if failures:
    print("retrieve dictionary test: %d FAILED" % failures)
    sys.exit(1)
print("retrieve dictionary test passed")
