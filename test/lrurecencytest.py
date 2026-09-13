"""Does LRU eviction actually keep the keys that are being read?

lrutest.py asserts that eviction happens at all, which passed for years while
the leaf LRU bit was never set - the sweep was evicting whatever page it landed
on, at random, and losing keys is what a random sweep looks like from the
outside too. This asks the question that tells the two apart: write a cold set
and a hot set, keep reading the hot set while memory pressure builds, and see
which half survives. See TODO 305.

Two things about the shape of it, both learned the hard way:

  - The sets are interleaved. The sweep walks one whole page at a time and
    leaves land on pages roughly in insertion order, so writing all the cold
    keys and then all the hot ones would put them on separate pages and a random
    page walk would score well by luck. Interleaved, every page holds both and
    the bit is the only thing separating them.
  - The reads run continuously, with no sleep. The bit is a second chance, not a
    timestamp: a sweep clears it and the next sweep of the same page evicts what
    is still clear. Idle for a second and the sweeps drain the whole shard
    between reads, hot keys included, which is correct behaviour for a clock and
    tells you nothing about recency.
"""

import time

import barch
import scale

scale.workdir()

PAIRS = scale.env_int("BARCH_LRU_PAIRS", 60000, 10000)
# Long enough for the sweep to get through most of the cold half.
BUDGET = scale.env_float("BARCH_LRU_SECONDS", 20.0, 5.0)

barch.clear()
barch.save()
barch.setConfiguration("max_memory_bytes", "300m")
barch.setConfiguration("eviction_policy", "allkeys-lru")

k = barch.KeyValue()

for i in range(PAIRS):
    k.set("cold:%d" % i, str(i))
    k.set("hot:%d" % i, str(i))

assert barch.size() == PAIRS * 2, barch.size()

barch.setConfiguration("max_memory_bytes", "1m")

started = time.time()
passes = 0
while time.time() - started < BUDGET:
    for i in range(PAIRS):
        k.get("hot:%d" % i)
    passes += 1
    if barch.size() < PAIRS:
        break

# get() answers "" for a missing key, not None, so counting survivors has to go
# through exists() - with get() every key looks present and the test passes
# whatever eviction did.
hot_left = sum(1 for i in range(PAIRS) if k.exists("hot:%d" % i))
cold_left = sum(1 for i in range(PAIRS) if k.exists("cold:%d" % i))

print("passes %d, hot %d, cold %d, of %d each" % (passes, hot_left, cold_left, PAIRS))

# Without pressure the test proves nothing either way.
assert hot_left + cold_left < PAIRS * 2, "nothing was evicted"

# The real assertion. A random sweep leaves the halves within noise of each
# other - measured at 9160 against 9096 before the fix, and 60000 against 947
# after it. Two to one is far clear of the noise without asking an approximate
# LRU to be a perfect one.
assert hot_left > cold_left * 2, (
    "eviction is not following recency: hot %d, cold %d" % (hot_left, cold_left)
)
