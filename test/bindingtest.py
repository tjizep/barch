import scale
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

# Exercises the language binding surface in src/swig_api.cpp, which reads replies
# through rpc_caller's flat view rather than over RESP.
#
# end_array stopped splicing an array reply into results and started keeping it as one
# nested value, so every site in swig_api.cpp was moved onto flat_size, flat_empty,
# flat_at and append_flat. Those accessors are meant to hand the bindings exactly the
# list they used to see. The substitution was mechanical and the compiler cannot check
# it - a nested array is a perfectly good Value, so a site that was missed returns one
# opaque value where it used to return several, silently.
#
# The check that catches that is the count: a method that should answer n values must
# answer n, not 1. Anything asserting only "not empty" would pass on a missed site.

print("start binding test")


def vals(result):
    """A vector<Value> comes back as a sequence; normalise it to a list of strings."""
    return [v.s() if hasattr(v, "s") else str(v) for v in result]


def one_of(result):
    """A method declared to return a single Value must not have become an array."""
    assert not isinstance(result, (list, tuple)), \
        f"expected a single Value, got a sequence of {len(result)}: {result!r}"
    return result


kv = barch.KeyValue()
kv.clear()

# ---------------------------------------------------------------- KeyValue
for i in range(10):
    kv.set(f"bk:{i:02d}", f"value{i}")
assert kv.size() == 10, f"expected 10 keys, got {kv.size()}"

# range and glob both answer a list, and the count is what proves the flattening
r = kv.range("bk:00", "bk:99", -1)
assert len(r) == 10, f"KeyValue.range should answer 10 values, answered {len(r)}"
assert "value0" in vals(r) or "bk:00" in vals(r), f"range returned {vals(r)[:4]}"

g = kv.glob("bk:*", 0)
assert len(g) == 10, f"KeyValue.glob should answer 10 values, answered {len(g)}"

# a glob matching exactly one key is the case that used to collapse to a bare scalar
g1 = kv.glob("bk:07", 0)
assert len(g1) == 1, f"KeyValue.glob of one key should answer 1 value, answered {len(g1)}"

g0 = kv.glob("nosuchprefix:*", 0)
assert len(g0) == 0, f"KeyValue.glob matching nothing should answer 0 values, answered {len(g0)}"

assert kv.exists("bk:03")
assert kv.ttl("bk:03") is not None
assert kv.expire("bk:03", 1000, "") in (True, False)

# ---------------------------------------------------------------- HashSet
hs = barch.HashSet()
hs.set("bh", ["f1", "v1", "f2", "v2", "f3", "v3"])

m = hs.mget("bh", ["f1", "f2", "f3"])
assert len(m) == 3, f"HashSet.mget of 3 fields should answer 3 values, answered {len(m)}"
m1 = hs.mget("bh", ["f1"])
assert len(m1) == 1, f"HashSet.mget of 1 field should answer 1 value, answered {len(m1)}"

ga = hs.getall("bh")
assert len(ga) == 6, f"HashSet.getall should answer 3 field/value pairs, answered {len(ga)}"

et = hs.expiretime("bh", ["f1", "f2"])
assert len(et) == 2, f"HashSet.expiretime of 2 fields should answer 2, answered {len(et)}"

t = hs.ttl("bh", ["f1", "f2", "f3"])
assert len(t) == 3, f"HashSet.ttl of 3 fields should answer 3, answered {len(t)}"

ex = hs.expire("bh", ["100"], ["f1", "f2"])
assert len(ex) == 2, f"HashSet.expire over 2 fields should answer 2, answered {len(ex)}"

# expireat always inserts its flags token, so it needs a real one - an empty string
# becomes an empty argument and the spec refuses to parse it
ea = hs.expireat("bh", 4102444800, "NX", ["f1", "f2", "f3"])
assert len(ea) == 3, f"HashSet.expireat over 3 fields should answer 3, answered {len(ea)}"

# these are declared to answer one Value, so a nested array reaching them would show up
one_of(hs.exists("bh", "f1"))
one_of(hs.incrby("bh", "counter", 5))
one_of(hs.remove("bh", ["f3"]))
one_of(hs.getdel("bh", ["f2"]))

# ---------------------------------------------------------------- OrderedSet
zs = barch.OrderedSet()
for i in range(5):
    zs.add("bz", [str(i + 1), f"m{i}"])

zr = zs.range("bz", 0, 100)
assert len(zr) == 5, f"OrderedSet.range should answer 5 members, answered {len(zr)}"
zr1 = zs.range("bz", 1, 1)
assert len(zr1) == 1, f"OrderedSet.range over one score should answer 1, answered {len(zr1)}"
# an empty span is not an empty vector here: range answers a single null, which is
# what it has always done - `if (sc.flat_empty()) return {nullptr};`. The point of
# pinning it is that flat_empty has to agree with the old results.empty() it replaced
zr0 = zs.range("bz", 900, 1000)
assert len(zr0) == 1 and zr0[0].isNull(), \
    f"OrderedSet.range over an empty span should answer a single null, answered {len(zr0)}"

# revrange takes its bounds the same way round as range - only the order of the
# answer is reversed - so the low score comes first here too
zv = zs.revrange("bz", 0, 100)
assert len(zv) == 5, f"OrderedSet.revrange should answer 5 members, answered {len(zv)}"

one_of(zs.card("bz"))
one_of(zs.rank("bz", 0, 100))
one_of(zs.popmin("bz"))
one_of(zs.popmax("bz"))
one_of(zs.incrby("bz", 2.5, "m1"))
one_of(zs.remove("bz", ["m1"]))

# the set operations, which run through the shared ZOPER body
zs.add("bz2", ["1", "m0"])
zs.add("bz2", ["9", "other"])
d = zs.diff(["bz", "bz2"], [])
assert isinstance(d, (list, tuple)), f"OrderedSet.diff should answer a sequence, got {d!r}"
i_ = zs.inter(["bz", "bz2"], [])
assert isinstance(i_, (list, tuple)), f"OrderedSet.inter should answer a sequence, got {i_!r}"
one_of(zs.intercard(["bz", "bz2"]))
one_of(zs.diffstore("bzd", ["bz", "bz2"]))
one_of(zs.interstore("bzi", ["bz", "bz2"]))
one_of(zs.remrangebylex("bz2", "a", "zzzz"))

# ---------------------------------------------------------------- List
ls = barch.List()
assert ls.push("bl", ["a", "b", "c"]) >= 0
assert ls.len("bl") == 3, f"List.len should be 3, got {ls.len('bl')}"

# ---------------------------------------------------------------- Caller.call
# the generic escape hatch, which appends whatever the command answered
c = barch.KeyValue()
called = c.call("KEYS", [barch.Value("bk:*")])
assert len(called) == 10, f"Caller.call KEYS should answer 10 values, answered {len(called)}"
called1 = c.call("KEYS", [barch.Value("bk:07")])
assert len(called1) == 1, f"Caller.call KEYS of one key should answer 1, answered {len(called1)}"
called0 = c.call("KEYS", [barch.Value("nosuchprefix:*")])
assert len(called0) == 0, f"Caller.call KEYS matching nothing should answer 0, answered {len(called0)}"

# it has to dispatch on the method it was handed, not on whatever the object was asked
# for last time. This used to read params[0], which still held the previous command -
# and on a freshly built object params was empty, so the read went off the end. Calling
# two different methods in a row is what shows it: the second would run the first.
fresh = barch.KeyValue()
size_first = fresh.call("DBSIZE", [])
assert len(size_first) == 1, f"Caller.call DBSIZE should answer one value, answered {len(size_first)}"
expected_size = kv.size()
assert size_first[0].i() == expected_size, \
    f"DBSIZE answered {size_first[0].i()}, expected {expected_size} - it ran the wrong command"
then_keys = fresh.call("KEYS", [barch.Value("bk:*")])
assert len(then_keys) == 10, \
    f"Caller.call KEYS after DBSIZE should answer 10, answered {len(then_keys)} - it ran the wrong command"
back_to_size = fresh.call("DBSIZE", [])
assert len(back_to_size) == 1 and back_to_size[0].i() == expected_size, \
    f"Caller.call DBSIZE after KEYS answered {back_to_size!r} - it ran the wrong command"

# ---------------------------------------------------------------- module level
assert barch.size() >= 0
assert barch.sizeAll() >= 0

kv.clear()
print("complete binding test")
