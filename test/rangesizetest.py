#!/usr/bin/env python3
"""
DBSIZE on a range sharded space is exact while the rebalancer moves keys - TODO 857.
So are SPACES, SIZEALL and INFO memory's barch_keys, which count the same way -
TODO 862.

DBSIZE added up each shard's count with that shard's lock alone, letting it go before
taking the next. The rebalancer moves keys between neighbours. A move from a shard not
counted yet into one already counted lost those keys from the answer, and a move the
other way counted them twice. COUNT had the same race and was fixed in TODO 703.
TestSaveCopyOnWrite hit it after a restart, while the rebalancer spread keys the log
replay had added.

A fresh range sharded space puts every new key in shard 0, and the rebalancer spreads
them upward. The keys are written in batches on one connection with a DBSIZE after each
batch, in one pipeline. A connection's commands run in order, so each DBSIZE has one
right answer: the keys written so far. Then they're deleted the same way, which moves
keys again. No answer depends on how far the rebalancer got.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=15100)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

KEYS = scale.env_int("RANGESIZE_KEYS", 200000, floor=20000)
BATCH = 500
ROUNDS = 3
SHARDS = 8

failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


DATA = os.path.join(os.getcwd(), "rangesize_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "rangesize.log"), "a")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA,
                      "--no-save-on-exit", "-c", "save_interval=86400000"],
                     stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "rsize" + str(int(time.time()))
try:
    c = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True, socket_timeout=300)
    for k, v in ((".ordered", "1"), (".shards", str(SHARDS)), (".range_sharded", "1")):
        c.execute_command("configuration:SET", SPACE + k, v)
    names = ["k%08d" % i for i in range(KEYS)]
    batches = [names[i:i + BATCH] for i in range(0, KEYS, BATCH)]

    def counts(pipe):
        """the four answers to how many keys, asked in this order after a batch"""
        pipe.execute_command(SPACE + ":DBSIZE")
        pipe.execute_command("SPACES")
        pipe.execute_command("SIZEALL")
        pipe.execute_command("INFO", "memory")

    def read(answers):
        """the four answers as numbers. SPACES and SIZEALL count every space, and
        INFO every shard, so those three are taken from the start of the round"""
        size, spaces, sizeall, info = answers
        mine = [int(n) for name, n in zip(spaces[::2], spaces[1::2]) if name == SPACE]
        return int(size), mine[0] if mine else -1, int(sizeall), int(info["barch_keys"])

    NAMES = ("DBSIZE", "SPACES", "SIZEALL", "INFO barch_keys")
    print("counts after each batch of %d, %d keys in %d shards, %d rounds"
          % (BATCH, KEYS, SHARDS, ROUNDS))
    asked = 0
    wrong = {n: [] for n in NAMES}
    for _ in range(ROUNDS):
        pipe = c.pipeline(transaction=False)
        counts(pipe)
        base = read(pipe.execute())
        base = (0, 0, base[2], base[3])
        # filled from the top down, so each batch lands below every key in the space
        # and the rebalancer has to push keys up through every shard
        pipe = c.pipeline(transaction=False)
        for b in reversed(batches):
            pipe.execute_command(SPACE + ":MSET", *[x for k in b for x in (k, "v")])
            counts(pipe)
        out = pipe.execute()
        answers = [read(out[n + 1:n + 5]) for n in range(0, len(out), 5)]
        want = [BATCH * (n + 1) for n in range(len(batches))]
        # and emptied from the bottom up, so keys move down to fill shard 0 again
        pipe = c.pipeline(transaction=False)
        for b in batches:
            pipe.execute_command(SPACE + ":DEL", *b)
            counts(pipe)
        out = pipe.execute()
        answers += [read(out[n + 1:n + 5]) for n in range(0, len(out), 5)]
        want += [KEYS - BATCH * (n + 1) for n in range(len(batches))]
        asked += len(want)
        for a, w in zip(answers, want):
            for n, name in enumerate(NAMES):
                if a[n] != base[n] + w:
                    wrong[name].append((a[n] - base[n], w))
    for name in NAMES:
        print("  %s: %d asked, %d wrong%s" % (name, asked, len(wrong[name]),
              (", first (got, want) %s" % wrong[name][:4]) if wrong[name] else ""), flush=True)
        check(not wrong[name], "every %s was the keys written so far" % name)
finally:
    p.terminate()
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
    log.close()

print("\n%s" % ("all range count checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
