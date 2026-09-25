# Merging a space with compressed values into another space - TODO 451.
#
# Dictionaries are per space. A value compressed in the source is in the source's
# dictionary, and the target reads with its own, so a merge has to decode it with
# the source's and, if asked to COMPRESS, encode it again with the target's. It
# used to copy compressed values across as they were, and read them back as
# garbage (or not at all) in the target. The two spaces here are trained on
# different data so their dictionaries really differ.
import os
import time

import scale

import barch
import redis

scale.workdir()
PORT = scale.port(default=15060)
print("start merge compress test", flush=True)
exec(open(f"{os.path.dirname(os.path.realpath(__file__))}/test_data.py").read())

barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.config_set("compression", "zstd")

failures = []


def check(ok, what):
    print("  %-64s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures.append(what)


def conn(space):
    c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
    c.execute_command("USE", space)
    c.flushdb()
    return c


def train(c, reverse):
    left = None
    for w in words:
        sample = train_set[w][::-1] if reverse else train_set[w]
        left = c.execute_command("TRAIN", sample)
    left = c.execute_command("TRAIN")
    return left


src = conn("mcsrc")
dst = conn("mcdst")
dst2 = conn("mcdst2")
check(train(src, False) == 0, "the source's dictionary is trained")
check(train(dst, True) == 0, "the target's, on different data")
check(train(dst2, True) == 0, "and a second target's")

# the source's values, compressed by the background pass before anything merges
before = barch.stats().value_bytes_compressed
for w in words:
    src.set("v:" + w, test_set[w])
deadline = time.time() + 60
while time.time() < deadline and barch.stats().value_bytes_compressed <= before:
    time.sleep(0.25)
check(barch.stats().value_bytes_compressed > before, "the source's values were compressed")
check(all(src.get("v:" + w) == test_set[w] for w in words), "and read back in the source")

# a plain merge: decoded with the source's dictionary on the way across
dst.set("v:" + words[0], b"overwritten")
src.execute_command("SPACES", "MERGE", "mcsrc", "INTO", "mcdst")
bad = [w for w in words if dst.get("v:" + w) != test_set[w]]
check(not bad, "a merge reads back byte for byte in the target: %d bad" % len(bad))

# with COMPRESS: encoded again with the target's dictionary
src.execute_command("SPACES", "MERGE", "mcsrc", "INTO", "mcdst2", "COMPRESS")
bad = [w for w in words if dst2.get("v:" + w) != test_set[w]]
check(not bad, "a COMPRESS merge reads back byte for byte: %d bad" % len(bad))

# and a merge from a space that is compressed that way, onward again
dst.execute_command("USE", "mcdst")
dst2.execute_command("SPACES", "MERGE", "mcdst2", "INTO", "mcsrc")
bad = [w for w in words if src.get("v:" + w) != test_set[w]]
check(not bad, "merged back from the COMPRESS target: %d bad" % len(bad))

barch.stop()
if failures:
    print("merge compress test FAILED: %d" % len(failures), flush=True)
    raise SystemExit(1)
print("merge compress test ok", flush=True)
