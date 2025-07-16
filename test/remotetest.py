import barch
barch.start("127.0.0.1",13000)
k = barch.KeyValue("127.0.0.1",13000)
#k = barch.KeyValue()
k.set("key1","value1")
for i in range(100000):
    k.set(str(i),str(i))
    if i%1000==0:
        print(i)
assert(k.get("key1")=="value1")
assert(k.get(str(1000))==str(1000))
l = barch.List("127.0.0.1",13000)
assert(l.push("l",["a1","a2"])==2)
assert(l.push("l",["b1","b2"])==4)

assert(l.len("l")==4)
assert(l.pop("l",1)==3)
assert(l.back("l")=="b1")
print(l.front("l"))
assert(l.front("l")=="a1")
assert(l.pop("l",1)==2)
assert(l.pop("l",2)==0)
