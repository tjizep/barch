import barch
barch.clear()
l = barch.List()
assert(l.push("l",["a1","a2"])==2)
assert(l.push("l",["b1","b2"])==4)

assert(l.len("l")==4)
assert(l.pop("l",1)==3)
assert(l.back("l")=="b1")
print(l.front("l"))
assert(l.front("l")=="a1")

import ctypes
import os

try:
    # Force gcov to write data to disk before Python exits
    ctypes.CDLL(None).__gcov_flush()
except Exception:
    pass