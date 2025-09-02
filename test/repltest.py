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
for i in range(100000):
    k.set(str(i),str(i))
    if i % 10000 == 0 :
        print("adding",i)
for i in range(100000):
    k.erase(str(i))
    if i % 1000 == 0 :
        print("removing",i)


time.sleep(2)
stats = barch.repl_stats()
assert(stats.key_add_recv > 0)
assert(stats.key_add_recv_applied > 0)
assert(stats.key_rem_recv > 0)
#assert(stats.key_rem_recv_applied == 0)
assert(stats.bytes_recv > 0)
assert(stats.bytes_sent > 0)
assert(stats.out_queue_size == 0)
assert(stats.instructions_failed == 0)
assert(stats.insert_requests > 0)
assert(stats.remove_requests > 0)
#print(barch.repl_stats().bytes_recv)

barch.save()
barch.stop()