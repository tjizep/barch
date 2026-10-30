#!/usr/bin/env python3
"""
Transaction throughput in a space of many shards - TODO 773.

While a transaction is open, the first write to a key after each BEGINTX saves what the
key held, in that version's layer. A layer used to be a whole space with the base's
shard count, made for each version that saw a write and thrown away when the oldest
snapshot moved past it. With 32 clients that was most of a commit's cost: a space of 64
shards did less than half the commits of a space of one.

Now a layer is made of shards only where a write lands, and those shards are emptied and
used again. Checked against one barchd, a space without Raft:
  - a write while a snapshot is open makes one layer shard in a space of 64, not 64,
    and INFO transactions says so in tx_layer_shards;
  - the layer shards go when the snapshot does;
  - a few hundred transactions over the same key make next to no new ones
    (tx_layer_shards_made), since the old ones are used again;
  - a snapshot still reads what it saw, the before-image is in the layer shard;
  - with several clients each moving between two keys, a space of 64 shards does at
    least 70% of the commits a space of one does. It did 45% before, and does about
    110% now. A sanitizer build only prints the two: it runs everything many times
    slower, and the ratio says nothing there.

COMMITTX holds its space's commit gate alone, so the time it holds it for is what the
commits in a space go at - TODO 774. Under the gate a commit walked its writes four
times through a scratch map, and put what each key held before into another scratch
map, a scratch space of its own. Now a commit of a few small writes has them decoded in
memory before it takes the gate, and keeps the before images in memory too; a big one
still uses a scratch space for them, so its memory stays bounded. Checked by
scratch_spaces_taken in INFO transactions, which doesn't depend on how fast the machine
is: a commit of 10 writes takes fewer scratch spaces than one of 100. It took as many.
Since TODO 777 a commit of up to 64 small writes takes none at all, nor does a read only
or an aborted transaction, and a big one still takes one.
"""
import multiprocessing as mp
import os
import random
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=19260)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SANITIZED = bool(os.environ.get("BARCH_SANITIZE"))
KEYS = scale.scaled(20000, floor=2000)
SECONDS = scale.scaled_seconds(4.0, floor=2.0)
CLIENTS = 8
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def client(db):
    # a space by number, db1 and so on: a pipeline takes its own connection from the
    # pool, which selects it too, where a USE would hold for one connection only
    return redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=30, decode_responses=True,
                       single_connection_client=True, db=db)


def stats(c):
    # redis-py parses INFO into a dict
    got = c.execute_command("INFO", "transactions")
    return {k: v for k, v in got.items() if isinstance(v, int)}


def fill(space):
    c = client(space)
    p = c.pipeline(transaction=False)
    for i in range(KEYS):
        p.set("k%d" % i, "v")
        if i % 1000 == 999:
            p.execute()
    p.execute()


def work(space, i, secs, q):
    # two reads and two writes a transaction, in two round trips
    c = client(space)
    r = random.Random(i)
    ok = 0
    end = time.time() + secs
    while time.time() < end:
        a, b = "k%d" % r.randrange(KEYS), "k%d" % r.randrange(KEYS)
        try:
            p = c.pipeline(transaction=False)
            p.execute_command("BEGINTX")
            p.get(a)
            p.get(b)
            p.execute()
            p = c.pipeline(transaction=False)
            p.set(a, "x")
            p.set(b, "y")
            p.execute_command("COMMITTX")
            p.execute()
            ok += 1
        except redis.ResponseError:
            try:
                c.execute_command("ABORTTX")
            except redis.ResponseError:
                pass
    q.put(ok)


def commits_a_second(space, clients):
    q = mp.Queue()
    ps = [mp.Process(target=work, args=(space, i, SECONDS, q)) for i in range(clients)]
    for p in ps:
        p.start()
    total = sum(q.get() for _ in ps)
    for p in ps:
        p.join()
    return total / SECONDS


DATA = os.path.join(os.getcwd(), "txthroughput_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txthroughput.log"), "a")
server = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                           "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

# spaces db1 and db2, new in this barchd
ONE, MANY = 1, 2

try:
    admin = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=30, decode_responses=True)
    # a space's shard count is set before it's first used
    admin.execute_command("configuration:SET", "db%d.shards" % ONE, "1")
    admin.execute_command("configuration:SET", "db%d.shards" % MANY, "64")
    fill(ONE)
    fill(MANY)

    print("layer shards")
    reader, writer = client(MANY), client(MANY)
    reader.execute_command("BEGINTX")
    reader.get("k1")
    writer.set("k2", "changed")
    s = stats(writer)
    check(s.get("tx_layers") == 1, "one write while a snapshot is open makes one layer (%s)" % s.get("tx_layers"))
    check(s.get("tx_layer_shards") == 1,
          "and one layer shard of 64 (%s)" % s.get("tx_layer_shards"))
    check(reader.get("k2") == "v", "the snapshot reads what the key held before (%r)" % reader.get("k2"))
    for i in range(3, 13):
        writer.set("k%d" % i, "changed")
    s = stats(writer)
    check(1 <= s.get("tx_layer_shards", 0) <= 11,
          "ten more writes make at most ten more (%s)" % s.get("tx_layer_shards"))
    reader.execute_command("ABORTTX")
    s = stats(writer)
    check(s.get("tx_layers") == 0 and s.get("tx_layer_shards") == 0,
          "they go when the snapshot does (%s %s)" % (s.get("tx_layers"), s.get("tx_layer_shards")))

    made = stats(writer).get("tx_layer_shards_made", 0)
    rounds = scale.scaled(300, floor=50)
    for i in range(rounds):
        reader.execute_command("BEGINTX")
        reader.get("k1")
        writer.set("k2", "w%d" % i)
        reader.execute_command("ABORTTX")
    after = stats(writer)
    made = after["tx_layer_shards_made"] - made if "tx_layer_shards_made" in after else None
    check(made is not None and made <= 2,
          "%d snapshots, each with a write, make next to no new layer shards (%s)" % (rounds, made))

    print("before images")
    w = client(MANY)

    def scratch_a_commit(writes, rounds=20):
        taken = stats(w)["scratch_spaces_taken"]
        for i in range(rounds):
            p = w.pipeline(transaction=False)
            p.execute_command("BEGINTX")
            p.get("k0")
            for j in range(writes):
                p.set("k%d" % j, "b%d" % i)
            p.execute_command("COMMITTX")
            got = p.execute()
            if got[-1] != "OK":
                return None
        return (stats(w)["scratch_spaces_taken"] - taken) / rounds

    one_write, few, many_writes = scratch_a_commit(1), scratch_a_commit(10), scratch_a_commit(100)
    check(few is not None and many_writes is not None and few < many_writes,
          "a commit of 10 writes takes fewer scratch spaces than one of 100 (%s %s)" % (few, many_writes))
    # none at all, since TODO 777: a scratch container takes its space at its first write,
    # and a commit's writes stay in memory up to 64 of them and a megabyte
    check(one_write == 0 and few == 0, "commits of 1 and of 10 writes take none (%s %s)" % (one_write, few))
    check(many_writes is not None and many_writes >= 1, "one of 100 still takes one (%s)" % many_writes)
    taken = stats(w)["scratch_spaces_taken"]
    for i in range(20):
        w.execute_command("BEGINTX")
        w.get("k1")
        w.execute_command("COMMITTX")
        w.execute_command("BEGINTX")
        w.set("k1", "a%d" % i)
        w.execute_command("ABORTTX")
    check(stats(w)["scratch_spaces_taken"] == taken,
          "nor do read only and aborted ones (%d)" % (stats(w)["scratch_spaces_taken"] - taken))
    check(w.get("k99") == "b19" and w.get("k0") == "b19", "and both are made (%s %s)" % (w.get("k0"), w.get("k99")))
    p = w.pipeline(transaction=False)
    p.execute_command("BEGINTX")
    for j in range(4):
        p.set("big%d" % j, "x" * 400000)
    p.execute_command("COMMITTX")
    taken = stats(w)["scratch_spaces_taken"]
    check(p.execute()[-1] == "OK" and w.strlen("big3") == 400000,
          "a few writes of more than a megabyte between them commit too")
    check(stats(w)["scratch_spaces_taken"] > taken, "in a scratch space (%d)" % (stats(w)["scratch_spaces_taken"] - taken))

    print("throughput, %d clients" % CLIENTS)
    one = commits_a_second(ONE, CLIENTS)
    many = commits_a_second(MANY, CLIENTS)
    print("    1 shard: %.0f commits/s, 64 shards: %.0f commits/s" % (one, many), flush=True)
    if SANITIZED:
        print("    not compared: a %s sanitizer build" % os.environ["BARCH_SANITIZE"])
    else:
        check(one > 0 and many >= 0.7 * one,
              "64 shards do at least 70%% of what 1 does (%.0f%%)" % (100.0 * many / one if one else 0))
    s = stats(writer)
    check(s.get("tx_live") == 0 and s.get("tx_layers") == 0 and s.get("tx_layer_shards", 0) == 0,
          "nothing is left open afterwards (%s %s %s)"
          % (s.get("tx_live"), s.get("tx_layers"), s.get("tx_layer_shards")))
finally:
    server.kill()
    server.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
