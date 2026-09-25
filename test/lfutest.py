# LFU eviction runs while keys are being written and read - TODO 454.
#
# The LFU path used to remove keys without the shard latch, racing every writer.
# A race can't be forced from here, so this is the shape that would show it:
# eviction and writes on the same shards at once, and every value read back is
# either gone or exactly what was written.
import scale
import barch
import time

scale.workdir()
MAXK = 300000
barch.clear()
barch.save()
barch.setConfiguration("max_memory_bytes", "300m")
barch.setConfiguration("eviction_policy", "allkeys-lfu")
k = barch.KeyValue()
for i in range(MAXK):
    k.set(str(i), "v" + str(i))
assert barch.size() == MAXK

# squeeze, and keep writing and reading while the maintenance thread evicts
barch.setConfiguration("max_memory_bytes", "1m")
bad = 0
reads = 0
end = time.time() + 4.0
i = MAXK
while time.time() < end:
    try:
        k.set(str(i), "v" + str(i))
    except Exception:
        pass            # a full space refusing the write is fine
    j = (i * 7919) % i
    got = k.get(str(j))
    reads += 1
    if got is not None and got != "" and got != "v" + str(j):
        bad += 1
        print("key", j, "read back as", repr(got))
    i += 1

print("size after", barch.size(), "reads", reads, "wrong", bad)
assert barch.size() < MAXK, "LFU eviction removed nothing"
assert bad == 0, "a value came back different from what was written"
barch.setConfiguration("max_memory_bytes", "300m")
barch.setConfiguration("eviction_policy", "none")
print("LFU eviction under writes: pass")
