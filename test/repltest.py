from b import barch
import time
barch.start("127.0.0.1","13000")
barch.publish("127.0.0.1","13000")
k = barch.KeyValue()
k.set("one","1")
k.set("two","2")
k.set("three","3")
for i in range(1000000):
    k.set(str(i),str(i))
for i in range(1000000):
    k.erase(str(i))

time.sleep(3)
stats = barch.repl_stats()
assert(stats.key_add_recv > 0)
assert(stats.key_add_recv_applied == 0)
assert(stats.key_rem_recv > 0)
assert(stats.key_rem_recv_applied == 0)
assert(stats.bytes_recv > 0)
assert(stats.bytes_sent > 0)
assert(stats.out_queue_size == 0)
assert(stats.instructions_failed == 0)
assert(stats.insert_requests > 0)
assert(stats.remove_requests == 0)
#print(barch.repl_stats().bytes_recv)
barch.stop()