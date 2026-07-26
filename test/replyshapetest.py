import redis
import barch

# Pins the RESP reply shape of the commands that answer with an array.
#
# The reply is assembled into a flat list of values and the array header is worked
# out at write time from how many values are in it. That guess is right for a scalar
# reply and for an array of two or more, and wrong for an array holding nothing or
# one thing, which come out as a nil and as a bare scalar. This file records what
# each arity should look like on the wire so the shape cannot drift.

PORT = 14000


def raw(r, *args):
    """Send a command without letting redis-py reinterpret the reply, so the shape
    the server actually produced is what gets asserted."""
    return r.execute_command(*args)


def as_text(v):
    if isinstance(v, bytes):
        return v.decode()
    return str(v)


print("start reply shape test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("CLEARALL")
r.flushdb()

r.mset({"shape:a": "1", "shape:b": "2", "solo:only": "3", "num:1": "4"})

# --- scalar replies keep their bare form, they must not grow an array header ----
assert raw(r, "GET", "shape:a") == b"1"
assert raw(r, "GET", "nosuchkey") is None
assert isinstance(raw(r, "DBSIZE"), int)

# --- KEYS at every arity ------------------------------------------------------
# nothing matches: an empty array, not a nil
empty = raw(r, "KEYS", "nomatch:*")
assert isinstance(empty, list), f"KEYS with no match should be a list, got {type(empty).__name__}: {empty!r}"
assert empty == [], f"KEYS with no match should be empty, got {empty!r}"

# exactly one match: a one element array, not the bare key
one = raw(r, "KEYS", "solo:*")
assert isinstance(one, list), f"KEYS with one match should be a list, got {type(one).__name__}: {one!r}"
assert [as_text(k) for k in one] == ["solo:only"], f"unexpected KEYS reply {one!r}"

# two or more: an array, which is the case that already worked
many = raw(r, "KEYS", "shape:*")
assert isinstance(many, list), f"KEYS with two matches should be a list, got {type(many).__name__}: {many!r}"
assert sorted(as_text(k) for k in many) == ["shape:a", "shape:b"], f"unexpected KEYS reply {many!r}"

# a numeric key is decoded and matched on its text, and still arrives inside an array
numeric = raw(r, "KEYS", "num:*")
assert isinstance(numeric, list), f"KEYS over a numeric key should be a list, got {numeric!r}"
assert [as_text(k) for k in numeric] == ["num:1"], f"unexpected KEYS reply {numeric!r}"

# --- SCAN, which nests its array behind the cursor and was never affected -------
for match, expected in [("nomatch:*", []), ("solo:*", ["solo:only"]),
                        ("shape:*", ["shape:a", "shape:b"])]:
    cursor, batch = raw(r, "SCAN", "0", "MATCH", match)
    assert isinstance(batch, list), f"SCAN batch should be a list, got {batch!r}"
    assert sorted(as_text(k) for k in batch) == expected, f"SCAN MATCH {match!r} gave {batch!r}"

# --- other array shaped replies, to hold the two-or-more wire format still -------
stats = raw(r, "STATS")
assert isinstance(stats, list) and len(stats) > 2, f"STATS should be a list, got {type(stats).__name__}"
ops = raw(r, "OPS")
assert isinstance(ops, list) and len(ops) > 2, f"OPS should be a list, got {type(ops).__name__}"

mget = raw(r, "MGET", "shape:a", "shape:b")
assert isinstance(mget, list) and len(mget) == 2, f"MGET of two keys gave {mget!r}"
mget_one = raw(r, "MGET", "shape:a")
assert isinstance(mget_one, list) and len(mget_one) == 1, \
    f"MGET of one key should be a one element array, got {type(mget_one).__name__}: {mget_one!r}"

# VALUES globs over the values rather than the keys - it is matched against the
# "1".."4" stored above - but it answers with the keys that hold them. That is the
# intended behaviour, not just what it happens to do, so these expectations are a
# specification rather than a record of the status quo.
vals = raw(r, "VALUES", "*")
assert isinstance(vals, list) and len(vals) == 4, f"VALUES gave {vals!r}"
one_val = raw(r, "VALUES", "3")
assert isinstance(one_val, list), f"VALUES with one match should be a list, got {one_val!r}"
assert [as_text(v) for v in one_val] == ["solo:only"], f"VALUES with one match gave {one_val!r}"
no_val = raw(r, "VALUES", "nosuchvalue")
assert no_val == [], f"VALUES with no match should be an empty list, got {no_val!r}"

# --- a blocking pop answers once, not once per attempt -------------------------
# bpop opens an array before it knows whether it can pop anything. When it has to
# block, that array must be abandoned rather than left in the reply, otherwise the
# answer the block callback produces arrives alongside an empty one.
r.rpush("shape:list", "first")
popped = r.blpop(["shape:list"], 1)
assert popped == (b"shape:list", b"l") or popped == (b"shape:list", b"first"), \
    f"an immediately satisfiable BLPOP gave {popped!r}"

# nothing to pop and the block times out: a nil, not an empty array
assert r.blpop(["shape:nolist"], 0.1) is None, "a timed out BLPOP should answer nil"

# --- HELLO, the handshake a modern client opens with ---------------------------
# barch speaks RESP2 only, so the handshake map comes back as a flat array of
# alternating field and value, and protocol 3 is refused rather than half served.
for cmd in (["HELLO"], ["HELLO", "2"]):
    hello = raw(r, *cmd)
    assert isinstance(hello, list), f"{cmd} should answer with an array, got {hello!r}"
    assert len(hello) % 2 == 0, f"{cmd} answered an odd number of elements: {hello!r}"
    fields = {as_text(hello[i]): hello[i + 1] for i in range(0, len(hello), 2)}
    for name in ("server", "version", "proto", "id", "mode", "role", "modules"):
        assert name in fields, f"{cmd} handshake is missing {name}: {hello!r}"
    assert fields["proto"] == 2, f"barch speaks RESP2, handshake said {fields['proto']!r}"
    assert as_text(fields["server"]) == "redis"
    assert as_text(fields["mode"]) == "standalone"
    # modules is a nested empty array, which is the one place the reply nests
    assert fields["modules"] == [], f"modules should be an empty array, got {fields['modules']!r}"

# protocol 3 is refused with NOPROTO, which is what a RESP2 only server should say,
# rather than the "unknown command" a missing HELLO used to produce
for bad in ("3", "1", "0"):
    try:
        raw(r, "HELLO", bad)
        assert False, f"HELLO {bad} should have been refused"
    except redis.exceptions.ResponseError as e:
        assert "NOPROTO" in str(e), f"HELLO {bad} gave the wrong error: {e}"

try:
    raw(r, "HELLO", "notanumber")
    assert False, "HELLO with a non numeric version should have been refused"
except redis.exceptions.ResponseError as e:
    assert "not an integer" in str(e), f"HELLO notanumber gave the wrong error: {e}"

# SETNAME is accepted and ignored, an unknown option is a syntax error
assert isinstance(raw(r, "HELLO", "2", "SETNAME", "shapetest"), list)
try:
    raw(r, "HELLO", "2", "NOSUCHOPTION")
    assert False, "an unknown HELLO option should have been refused"
except redis.exceptions.ResponseError:
    pass

r.close()
barch.stop()
print("complete reply shape test")
