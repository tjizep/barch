# RANDOMKEY against writers on the same shards - TODO 377.
#
# RANDOMKEY used to walk the shard it picked with no lock: the read lock it took only
# covered counting. TSan caught it in TestChaos reading a leaf while HINCRBY made one
# and RPOP freed one on the same shard. That test only hits it now and then, so this
# one aims at it: a space of two shards, so every reader and writer keeps meeting on
# the same trees, and readers doing nothing but RANDOMKEY. Under TSan a race fails
# the process; without it this is a quick check that answers stay sane.
import threading
import time

import scale

import redis
import barch

scale.workdir()
PORT = scale.port(default=14220)
SECONDS = scale.env_float("BARCH_RANDOMKEY_SECONDS", 4.0, 1.5)

print("start randomkey race test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)

# two shards, so the picked shard is nearly always one somebody is writing to
conf = barch.KeyValue("configuration")
conf.set("rk.shards", "2")


def conn():
    c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
    c.execute_command("USE", "rk")
    return c


seed = conn()
seed.execute_command("FLUSHDB")
for i in range(200):
    seed.set("k%03d" % i, "v")

stop = threading.Event()
errors = []
answers = [0]


def reader():
    c = conn()
    try:
        while not stop.is_set():
            got = c.execute_command("RANDOMKEY")
            if got is not None and not isinstance(got, bytes):
                errors.append("RANDOMKEY answered %r" % (got,))
            answers[0] += 1
    except Exception as e:
        errors.append("reader: %r" % e)


def writer(n):
    c = conn()
    i = 0
    try:
        while not stop.is_set():
            # a new leaf, a freed leaf, a replaced leaf and a removed key, every pass
            c.execute_command("HINCRBY", "h%d" % (i % 20), "f%d" % (i % 7), 1)
            c.execute_command("RPUSH", "l%d" % n, "x%d" % i)
            c.execute_command("RPOP", "l%d" % n)
            c.set("w%d_%d" % (n, i % 50), "v%d" % i)
            c.delete("w%d_%d" % (n, (i + 25) % 50))
            i += 1
    except Exception as e:
        errors.append("writer %d: %r" % (n, e))


try:
    threads = [threading.Thread(target=reader) for _ in range(4)] + \
              [threading.Thread(target=writer, args=(n,)) for n in range(4)]
    for t in threads:
        t.start()
    time.sleep(SECONDS)
    stop.set()
    for t in threads:
        t.join(timeout=30)
    assert not errors, errors[:5]
    assert answers[0] > 0, "no RANDOMKEY answered"
    print("  %d RANDOMKEY answers beside the writers" % answers[0], flush=True)
    print("randomkey race test complete", flush=True)
finally:
    barch.stop()
