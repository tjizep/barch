#!/usr/bin/env python3
"""
A pessimistic commit checks a container's keys in the shard they live in - TODO 955.

A container's keys live in the shard of its name, not in the shard their own bytes
hash to. DEL of a list reads its header, and the read is kept in the name's shard.
The commit checked each read by routing its key again, by the key's own hash. For a
container key that is another shard: one the commit doesn't hold. It found nothing
there, so it said the key had changed and refused, every time. And it read-locked a
shard it didn't hold, so two commits could each wait on the other's shard until the
lock wait ran out.

Checked against one barchd with no Raft:
  - DEL of a list, a hash and an ordered set in BEGINTX PESSIMISTIC commits, and each
    is gone after. Several names, so some have their header in another shard;
  - the check still sees a real change: a list pushed to from outside after the
    transaction's DEL read it is TXCONFLICT at COMMITTX.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14901)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-74s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


DATA = os.path.join(os.getcwd(), "txcontainerread_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txcontainerread.log"), "a")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "txcontainerread_" + str(int(time.time()))


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", SPACE)
    return c


def commit(c):
    try:
        return c.execute_command("COMMITTX")
    except redis.ResponseError as e:
        return str(e)


def abort(c):
    # a refused COMMITTX may have ended it already
    try:
        c.execute_command("ABORTTX")
    except redis.ResponseError:
        pass


try:
    a = client()
    outside = client()
    names = ["box:%d" % n for n in range(24)]

    print("DEL of a container in a pessimistic transaction")
    for kind, fill, there in (
            ("list", lambda n: outside.rpush(n, "x", "y"), lambda n: outside.llen(n)),
            ("hash", lambda n: outside.hset(n, "f", "v"), lambda n: outside.hlen(n)),
            ("ordered set", lambda n: outside.zadd(n, {"m": 1}), lambda n: outside.zcard(n))):
        refused = []
        left = []
        for name in names:
            key = kind.replace(" ", "") + ":" + name
            fill(key)
            a.execute_command("BEGINTX", "PESSIMISTIC")
            a.execute_command("DEL", key)
            said = commit(a)
            if said not in ("OK", True):
                refused.append((key, said))
                abort(a)
            if there(key):
                left.append(key)
        check(not refused, "each %s's DEL commits (%d refused%s)"
              % (kind, len(refused), ", first: %s %s" % refused[0] if refused else ""))
        check(not left, "each %s is gone after (%d left)" % (kind, len(left)))

    print("a real change to a list it read")
    seen = 0
    for name in names:
        key = "pushed:" + name
        outside.rpush(key, "x")
        a.execute_command("BEGINTX", "PESSIMISTIC")
        a.execute_command("DEL", key)
        outside.rpush(key, "y")
        said = commit(a)
        if "TXCONFLICT" in said:
            seen += 1
        else:
            abort(a)
    check(seen == len(names), "a push after the read is TXCONFLICT (%d of %d)" % (seen, len(names)))
finally:
    p.kill()
    p.wait()

print("%d failure(s)" % failures)
sys.exit(1 if failures else 0)
