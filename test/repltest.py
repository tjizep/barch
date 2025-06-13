from b import barch
barch.start("127.0.0.1","13000")
barch.publish("127.0.0.1","13000")
k = barch.KeyMap()
k.set("one","1")
k.set("two","2")
k.set("three","3")
#print(barch.repl_stats().bytes_recv)
barch.stop()