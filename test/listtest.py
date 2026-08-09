import barch
barch.clear()
l = barch.List()
# push() is LPUSH, which prepends now, as in redis. So a1 then a2 leaves a2 at the head,
# and b1 then b2 leaves the list as b2 b1 a2 a1. pop() answers with what it removed
# rather than with the length left behind - len() is what reports that. See TODO 38
assert(l.push("l",["a1","a2"])==2)
assert(l.push("l",["b1","b2"])==4)

assert(l.len("l")==4)
assert(l.pop("l",1)[0].s()=="b2")
assert(l.len("l")==3)
assert(l.back("l")=="a1")
print(l.front("l"))
assert(l.front("l")=="b1")

import ctypes
import os

try:
    # Force gcov to write data to disk before Python exits
    ctypes.CDLL(None).__gcov_flush()
except Exception:
    pass