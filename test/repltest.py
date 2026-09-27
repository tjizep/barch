import os
import scale
import barch
import time

# both ctest runs of this script share one directory: the second reads
# what the first saved
scale.workdir("repltest")
# this process publishes to itself, and a run that stopped partway leaves it
# following a stream that's gone, which it would rightly refuse - TODO 502. What's
# checked here is the traffic, not carrying on across runs, so each starts afresh
for f in ("repl_positions.dat", "repl_primary.dat"):
    if os.path.exists(f):
        os.remove(f)

PORT = str(scale.port(default=13000))
barch.start("127.0.0.1", PORT)
barch.stop()
barch.start("127.0.0.1", PORT)
barch.publish("127.0.0.1", PORT)
k = barch.KeyValue()
k.set("one","1")
k.set("two","2")
k.set("three","3")
COUNT = 200000
for i in range(COUNT):
    k.set(str(i),str(i))
    if i % 10000 == 0 :
        print("adding",i)
for i in range(COUNT):
    k.erase(str(i))
    if i % 10000 == 0 :
        print("removing",i)


# Replication sends what the shards did as REPLAPPLY batches now, not the SET and
# REM the binding was asked for - TODO 498. The binding's own calls aren't
# counted, so the wait used to be on the replicated SETs arriving. Now it's on
# the batches: some have arrived, and the queue has stayed empty for a second
deadline = time.time() + 300
last = -1
while time.time() < deadline:
    now = barch.calls("REPLAPPLY")
    if now > 0 and now == last and barch.repl_stats().out_queue_size == 0:
        break
    last = now
    time.sleep(1)

stats = barch.repl_stats()
assert barch.calls("REPLAPPLY") > 0
assert stats.barch_requests > 0
assert(stats.bytes_recv > 0)
assert(stats.bytes_sent > 0)
assert(stats.out_queue_size == 0)
assert(stats.instructions_failed == 0)

#print(barch.repl_stats().bytes_recv)

barch.save()
barch.stop()