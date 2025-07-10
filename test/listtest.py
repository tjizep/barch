import barch
barch.clear()
l = barch.List()
assert(l.push("l",["a1","a2"])==2)
assert(l.push("l",["b1","b2"])==4)

assert(l.len("l")==4)
assert(l.pop("l",1)==3)
