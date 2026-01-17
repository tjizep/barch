import barch
import time

kv = barch.KeyValue("/tmp/lbarch",0)
print(kv.get("message"))
kv.set("message","exit")