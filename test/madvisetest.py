"""Freed arena memory goes back to the kernel - TODO 824.

The maintenance sweeps call MADV_DONTNEED on what they free, so the RAM an arena
holds drops while its mapping keeps its size. Two ways in:

- a page left empty by deletes or by defrag goes whole, in the defrag pass;
- the gap a value leaves when it is compressed in place goes in the compression
  pass, apart from the filler header at its start.

Each half waits for its pass with a poll and a cap, and the cap is only there so a
broken build fails rather than hangs. On a build without the release, the arena
keeps its RAM and both halves hit the cap.
"""
import scale

import random

import barch
import redis
import time

scale.workdir()

PORT = scale.port(default=15030)
print(f'running {__file__}')

barch.start("0.0.0.0", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
CAP = 60


def resident():
    rows = r.execute_command("KSRESIDENT")
    out = {}
    for at in range(0, len(rows), 2):
        name = rows[at].decode() if isinstance(rows[at], bytes) else rows[at]
        out[name] = rows[at + 1]
    return out


def wait_for(what, done):
    """what is the failure message, or a function that makes it when the cap is hit"""
    deadline = time.time() + CAP
    while not done():
        s = barch.stats()
        assert time.time() < deadline, "%s; %d bytes compressed, %d released" % (
            what() if callable(what) else what, s.value_bytes_compressed, s.vmm_bytes_released)
        time.sleep(0.1)


# --- whole pages, in the defrag pass ------------------------------------------
print("deleted keys give their pages back", flush=True)
r.config_set("active_defrag", "on")
r.flushdb()
# not scaled: 40 MB over 17 shards is a few 512K pages each, and fewer keys leave
# every shard on its last page, which the kept keys hold on to
n = 20000
value = "x" * 2000
p = r.pipeline(transaction=False)
for i in range(n):
    p.set("del%06d" % i, value)
    if i % 1000 == 999:
        p.execute()
p.execute()

before = resident()
released_before = barch.stats().vmm_bytes_released
# the newest keys stay, so every shard's last page is still in use and popping the
# trailing pages, which an old build does too, can't account for the drop
kept = n // 20
p = r.pipeline(transaction=False)
for i in range(n - kept):
    p.delete("del%06d" % i)
    if i % 1000 == 999:
        p.execute()
p.execute()

now = {}


def pages_released():
    now.update(resident())
    return now["leaves_resident_bytes"] * 2 < before["leaves_resident_bytes"]


wait_for(lambda: "freed pages kept their RAM: %s, then %s" % (before, now), pages_released)
after = resident()
print("leaves resident %d -> %d, mapped %d -> %d" % (
    before["leaves_resident_bytes"], after["leaves_resident_bytes"],
    before["leaves_mapped_bytes"], after["leaves_mapped_bytes"]), flush=True)
assert barch.stats().vmm_bytes_released > released_before, "nothing counted as released"
for i in range(n - kept, n):
    assert r.get("del%06d" % i) == value.encode(), "a kept key changed: del%06d" % i
# and a page given back can be used again
for i in range(kept):
    r.set("again%06d" % i, value)
for i in range(kept):
    assert r.get("again%06d" % i) == value.encode(), "a reused page lost again%06d" % i

# --- the gap a compressed value leaves, in the compression pass ----------------
print("compressing in place gives the gap back", flush=True)
# defrag off: moving keys out of fragmented pages frees whole pages, which would
# lower RSS on its own and hide whether the gaps went
r.config_set("active_defrag", "off")
r.config_set("compression", "zstd")
r.flushdb()
rnd = random.Random(824)
words = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
         "india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa"]


def text(size):
    out = []
    left = size
    while left > 0:
        w = rnd.choice(words) + " "
        out.append(w)
        left -= len(w)
    return "".join(out)[:size]


def rss_anon():
    """
    The process's own anonymous RSS. Not KSRESIDENT: the compression pass copies
    whole pages, which maps the kernel's zero page back over what was dropped, and
    mincore counts that as resident although it costs no RAM.
    """
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("RssAnon:"):
                return int(line.split()[1]) * 1024
    raise AssertionError("no RssAnon in /proc/self/status")


left = r.execute_command("TRAIN", text(2000))
while left > 0:
    left = r.execute_command("TRAIN", text(2000))
assert r.execute_command("TRAIN") == 0

m = scale.scaled(400, floor=80)


def compressed_since(at):
    return barch.stats().value_bytes_compressed - at


# a first round, so every maintenance thread has made its compressor before the
# baseline: each keeps one per space, tens of MB between them, and making them
# while RssAnon is watched would hide what the gaps give back
at = barch.stats().value_bytes_compressed
for i in range(m):
    r.set("warm%05d" % i, text(65536))
wait_for("the first round was never compressed", lambda: compressed_since(at) > m * 32768)

# compression off while the measured values go in, so the baseline is taken
# before the pass starts on them
r.config_set("compression", "none")
big = {}
for i in range(m):
    big["big%05d" % i] = text(65536)
    r.set("big%05d" % i, big["big%05d" % i])

before = resident()
rss_before = rss_anon()
compressed_before = barch.stats().value_bytes_compressed
released_before = barch.stats().vmm_bytes_released
r.config_set("compression", "zstd")


def gaps_released():
    compressed = compressed_since(compressed_before)
    if compressed <= m * 32768:
        return False
    if scale.SANITIZER_PRELOAD:
        # a sanitizer's shadow memory is anonymous too, and grows with the work the
        # pass does, so RssAnon can't show the drop; the count of what went back can
        return barch.stats().vmm_bytes_released - released_before > compressed // 2
    return rss_before - rss_anon() > compressed // 2


wait_for("compressed gaps kept their RAM: RssAnon was %d" % rss_before, gaps_released)
after = resident()
print("RssAnon %d -> %d after %d bytes compressed" % (
    rss_before, rss_anon(), barch.stats().value_bytes_compressed - compressed_before), flush=True)
assert after["leaves_mapped_bytes"] >= before["leaves_mapped_bytes"], \
    "the mapping shrank, so this is not the gaps going: %s then %s" % (before, after)
for k, v in big.items():
    assert r.get(k) == v.encode(), "value changed for %s" % k

# the fillers and the zeroed pages behind them survive a save and a load
r.execute_command("SAVE")
barch.clear()
barch.load()
for k, v in big.items():
    assert r.get(k) == v.encode(), "value changed for %s after a load" % k

r.config_set("active_defrag", "on")
barch.stop()
print("madvise test passed", flush=True)
