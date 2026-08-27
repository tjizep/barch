# Hybrid keys: ART owns the leaves, the overflow hash indexes them.
# GET and in-place SET should work through the hash, size must not
# double-count, range still walks the tree, and turning hybrid off
# leaves ART GET working.
import barch

print("start hybrid test")
barch.clear()
barch.setConfiguration("ordered_keys", "yes")
barch.setConfiguration("hybrid_keys", "yes")

k = barch.KeyValue()
assert barch.size() == 0

k.set("alpha", "one")
assert k.get("alpha") == "one"
assert k.exists("alpha")
assert barch.size() == 1

# same-size SET should be an in-place hash hit, not a new leaf
k.set("alpha", "two")
assert k.get("alpha") == "two"
assert barch.size() == 1

# a longer value replaces the leaf; the hash pointer has to move
k.set("alpha", "twothree")
assert k.get("alpha") == "twothree"
assert barch.size() == 1

N = 500
for i in range(N):
    k.set("k" + str(i), "v" + str(i))
assert barch.size() == 1 + N, "hybrid size double-counted the hash index"

for i in range(0, N, 17):
    assert k.get("k" + str(i)) == "v" + str(i)

k.erase("alpha")
assert not k.exists("alpha")
assert barch.size() == N

# INCR replaces through the ART trace, then reindexes
assert k.incr("counter") == 1
assert k.get("counter") == "1"
assert k.incr("counter") == 2
assert k.get("counter") == "2"

# integer names still round-trip; GET no longer needs the ART increment walk
k.set("0", "z")
k.set("1", "y")
assert k.get("0") == "z"
assert k.get("1") == "y"

# range is still the tree
assert k.min() is not None
assert k.max() is not None

barch.save()
assert k.get("k10") == "v10"

# hash dropped, ART still answers
barch.setConfiguration("hybrid_keys", "off")
assert k.get("k10") == "v10"
assert k.get("0") == "z"

# rebuild the index from the tree
barch.setConfiguration("hybrid_keys", "yes")
assert k.get("k10") == "v10"
assert k.exists("k11")

print("hybrid test ok")
