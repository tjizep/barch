import scale
import barch
import time

# both ctest runs of this script share one directory: the second reads
# what the first saved
scale.workdir("repltest")

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


while (barch.calls("SET") < COUNT):
    time.sleep(1)
while (barch.calls("REM") < COUNT/10):
    time.sleep(1)

stats = barch.repl_stats()
assert barch.calls("SET") > 0
assert barch.calls("REM") > 0
assert stats.barch_requests > 0
assert(stats.bytes_recv > 0)
assert(stats.bytes_sent > 0)
assert(stats.out_queue_size == 0)
assert(stats.instructions_failed == 0)

#print(barch.repl_stats().bytes_recv)

barch.save()
barch.stop()