import barch
import time
barch.start("127.0.0.1","13000")
barch.stop()
barch.start("127.0.0.1","13000")
barch.publish("127.0.0.1","13000")
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