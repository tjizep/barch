# SET over a list, hash or ordered set removes it - TODO 843.
#
# Every SET that makes a new key used to probe three shards for a container of the
# same name. Now a process that never made or loaded a container key skips the
# probe. The flag that says so is set where leaves are built and where shard files
# are loaded. A restart starts with it false, so the second half needs the load to
# set it: without that, SET leaves the loaded container in place.
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
PORT = scale.port(default=14396)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

ROOT = os.path.join(os.getcwd(), "containersetover")
shutil.rmtree(ROOT, ignore_errors=True)
DATA = os.path.join(ROOT, "data")
os.makedirs(DATA)
LOG_PATH = os.path.join(ROOT, "barchd.log")


def start(log, *extra):
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA]
                         + list(extra), stdout=log, stderr=subprocess.STDOUT)
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            raise AssertionError("barchd exited with %s" % p.returncode)
        try:
            socket.create_connection(("127.0.0.1", PORT), 0.5).close()
            return p, redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=60)
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % PORT)


def stop(p):
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()


def size(r, name, tag):
    # a string by the same name answers WRONGTYPE, which means no container
    try:
        return {"l": r.llen, "h": r.hlen, "z": r.zcard}[name](name + tag)
    except redis.exceptions.ResponseError as e:
        if "WRONGTYPE" not in str(e):
            raise
        return 0


def make(r, tag, names):
    if "l" in names:
        r.rpush("l" + tag, "a", "b")
    r.hset("h" + tag, "f", "v")
    r.zadd("z" + tag, {"m": 1})
    for name in names:
        assert size(r, name, tag), name + tag


def overwrite(r, tag, names):
    for name in names:
        key = name + tag
        assert r.set(key, "s"), key
        assert r.get(key) == b"s", (key, r.get(key))
        assert size(r, name, tag) == 0, "SET left the container %s in place" % key


print("start container SET-over test with %s" % BINARY, flush=True)
# with ordered keys off, LLEN answers 0 after RPUSH, before TODO 843 too, so the
# hashed run leaves lists out
MODES = {"ordered": (["ordered_keys=yes", "hybrid_keys=off"], "lhz"),
         "hybrid": (["ordered_keys=yes", "hybrid_keys=on"], "lhz"),
         "hashed": (["ordered_keys=no"], "hz")}
for mode, (configs, names) in MODES.items():
    shutil.rmtree(DATA, ignore_errors=True)
    os.makedirs(DATA)
    log = open(LOG_PATH, "w")
    extra = [a for c in configs for a in ("--config", c)]
    p, r = start(log, *extra)
    try:
        # made by commands in this process
        make(r, "1", names)
        overwrite(r, "1", names)
        make(r, "2", names)
        r.execute_command("SAVE")
    finally:
        stop(p)

    # loaded from the shard files by a fresh process
    p, r = start(log, *extra)
    try:
        for name in names:
            assert size(r, name, "2"), "the load lost %s2" % name
        overwrite(r, "2", names)
    finally:
        stop(p)
        log.close()
    with open(LOG_PATH, errors="replace") as f:
        out = f.read()
    assert "WARNING: ThreadSanitizer" not in out and "ERROR: AddressSanitizer" not in out, \
        out[out.find("Sanitizer") - 200:][:4000]
    print("%s keys: SET removed made and loaded containers" % mode, flush=True)

print("container SET-over test passed")
