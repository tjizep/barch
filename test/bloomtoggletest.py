# The static bloom filter switched on and off while the server is being read - TODO 581.
#
# A GET asks the shard's filter before it takes the shard's lock, because a miss that
# the filter rules out saves the lock. So the filter is read by threads that hold
# nothing, and CONFIG SET static_bloom_filter rebuilds it under the shard's write
# latch. Two things went wrong:
#
#   * turning it on sized the filter to all zeros and only then filled it from the
#     leaves, and a reader in between was told "not in the filter", so a key that
#     exists came back as a miss
#   * the setting itself, and the filter's bits, were plain memory written by one
#     thread and read by another - ThreadSanitizer reports it. That one is for the
#     sanitizer trees; this test is what puts a writer and the readers together.
#
# Checked here: while four clients read keys that exist, the filter is turned on and
# off over and over, and none of those reads may miss. Then, with it on, a key that
# was never set still reads as absent and one that was set is found.
import os
import signal
import subprocess
import sys
import threading
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14985)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "bloomtoggle_data")
os.makedirs(DATA, exist_ok=True)
for f in os.listdir(DATA):
    if f.endswith(".dat"):
        os.remove(os.path.join(DATA, f))

KEYS = scale.scaled(100000, floor=5000)
SECONDS = scale.scaled_seconds(8.0, floor=2.0)

print("start bloom toggle test with %s" % BINARY, flush=True)
# a file and not a pipe: every CONFIG SET logs a line, nobody reads a pipe until the
# end, and a full pipe blocks the logger and with it the server
LOG = os.path.join(DATA, "barchd.log")
logfile = open(LOG, "wb")
proc = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA],
                        stdout=logfile, stderr=subprocess.STDOUT)
scale.wait_for_port(PORT, proc=proc, what="barchd")


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=30)


try:
    r = client()
    assert r.execute_command("CONFIG", "GET", "static_bloom_filter")[1] == b"off"
    pipe = r.pipeline(transaction=False)
    for i in range(KEYS):
        pipe.set("k%d" % i, "v")
        if i % 5000 == 4999:
            pipe.execute()
    pipe.execute()

    print("reads that must not miss while the filter is toggled", flush=True)
    stop = threading.Event()
    missed = []
    errors = []

    def reader(seed):
        try:
            c = client()
            i = seed
            while not stop.is_set():
                i = (i * 1103515245 + 12345) % KEYS
                if c.get("k%d" % i) is None:
                    missed.append(i)
                    return
        except Exception as e:      # a reader that died is not a reader that passed
            errors.append(repr(e))

    readers = [threading.Thread(target=reader, args=(s,)) for s in range(1, 5)]
    for t in readers:
        t.start()
    toggles = 0
    end = time.time() + SECONDS
    while time.time() < end and not missed and not errors:
        r.execute_command("CONFIG", "SET", "static_bloom_filter", "on")
        r.execute_command("CONFIG", "SET", "static_bloom_filter", "off")
        toggles += 1
    stop.set()
    for t in readers:
        t.join()
    assert not errors, errors
    assert not missed, "key k%d exists and a read missed it while the filter was " \
                       "being turned on (%d toggles)" % (missed[0], toggles)
    assert toggles >= 1, toggles

    print("with it on, what was set is found and what was not is absent", flush=True)
    r.execute_command("CONFIG", "SET", "static_bloom_filter", "on")
    for i in range(0, KEYS, max(1, KEYS // 500)):
        assert r.get("k%d" % i) == b"v", i
    for i in range(500):
        assert r.get("never%d" % i) is None, i
    r.set("added-after", "x")
    assert r.get("added-after") == b"x"
    r.execute_command("DEL", "k0")
    assert r.get("k0") is None
    # and a flush starts the filter over without losing the next write
    r.execute_command("FLUSHDB")
    assert r.get("k1") is None
    r.set("after-flush", "y")
    assert r.get("after-flush") == b"y"
    r.close()
finally:
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        proc.kill()
        raise AssertionError("barchd did not exit on SIGTERM")

# a sanitizer finding inside barchd is its exit status, and this test is where the
# readers and the writer meet
logfile.close()
assert proc.returncode == 0, "barchd exited with %s:\n%s" % (
    proc.returncode, open(LOG, "rb").read().decode(errors="replace")[-3000:])
print("bloom toggle test complete", flush=True)
