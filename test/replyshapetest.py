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

# --- every array shaped command, asked for nothing -------------------------------
# A command that opens an array and then pushes nothing has to answer an empty array.
# It used to answer nil, because the empty array was spliced into an empty reply and
# the writer could not tell the difference. bpop relied on exactly that to mean "no
# answer yet", so it is worth holding every one of these down rather than assuming
# bpop was the only place it mattered.
r.execute_command("ZADD", "shape:zz", "1", "member")
empty_cases = [
    (["KEYS", "nomatch:*"], []),
    (["VALUES", "nomatch:*"], []),
    (["RANGE", "zzzz1", "zzzz2", "-1"], []),
    (["HKEYS", "shape:nohash"], []),
    (["ZRANGE", "shape:nozset", "0", "-1"], []),
    (["ZRANGEBYSCORE", "shape:nozset", "-inf", "+inf"], []),
    (["ZPOPMIN", "shape:nozset"], []),
    (["ZPOPMAX", "shape:nozset"], []),
    (["ZINTER", "2", "shape:nozset", "shape:zz"], []),
]
for cmd, expected in empty_cases:
    got = raw(r, *cmd)
    assert isinstance(got, list), \
        f"{' '.join(cmd)} should answer an array when empty, got {type(got).__name__}: {got!r}"
    assert got == expected, f"{' '.join(cmd)} gave {got!r}"

# HGETALL is a map, so redis-py hands back a dict rather than a list
assert raw(r, "HGETALL", "shape:nohash") == {}, "HGETALL of a missing key should be empty"
# SCAN nests its array behind the cursor
assert raw(r, "SCAN", "0", "MATCH", "nomatch:*")[1] == []
# and the counting forms answer a number, not an array
assert raw(r, "ZCARD", "shape:nozset") == 0
assert raw(r, "ZINTERCARD", "2", "shape:nozset", "shape:zz") == 0
assert raw(r, "VALUES", "nomatch:*", "COUNT") == 0
r.execute_command("DEL", "shape:zz")

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

# only 2 and 3 exist, so anything outside that is NOPROTO. HELLO 3 is not sent on this
# connection: it would switch the server side to RESP3 and leave this client reading
# with the wrong parser - it is exercised on its own client further down
for bad in ("1", "0", "4"):
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

# --- the same server over RESP3 -------------------------------------------------
# RESP3 is a superset, so the shapes above are unchanged; what differs is that a map
# arrives as a map, and null, boolean and double have types of their own instead of
# being carried as a bulk string or an integer.
r3 = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=3)

hello3 = raw(r3, "HELLO", "3")
assert isinstance(hello3, dict), \
    f"a RESP3 handshake should arrive as a map, got {type(hello3).__name__}: {hello3!r}"
proto3 = hello3.get(b"proto", hello3.get("proto"))
assert proto3 == 3, f"handshake should have agreed on RESP3, said {proto3!r}"

# arrays keep their shape and arity at every count
assert raw(r3, "KEYS", "nomatch:*") == []
one3 = raw(r3, "KEYS", "solo:*")
assert isinstance(one3, list) and [as_text(k) for k in one3] == ["solo:only"], \
    f"KEYS with one match over RESP3 gave {one3!r}"
# the blocking pop section above left shape:list behind, so this asks for containment
# rather than an exact count and does not depend on what ran before it
many3 = {as_text(k) for k in raw(r3, "KEYS", "shape:*")}
assert {"shape:a", "shape:b"} <= many3, f"KEYS over RESP3 gave {many3!r}"

# null is '_' rather than '$-1', and still reaches the client as None
assert raw(r3, "GET", "nosuchkey") is None
assert raw(r3, "GET", "shape:a") == b"1"

# a boolean is '#t'/'#f' in RESP3, so it arrives as a bool rather than 1 or 0
assert raw(r3, "SPACES", "OPTION", "GET", "ORDERED") is True
assert raw(r3, "SPACES", "OPTION", "GET", "LRU") is False

# a double is ',' in RESP3 and a bulk string in RESP2, and has to survive either way.
# it used to go out as ':1' whatever its value, because the writer handed a const char*
# to an overload set where it bound to bool ahead of std::string
for client in (r, r3):
    client.execute_command("ZADD", "shape:z", "1", "member")
    assert client.execute_command("ZINCRBY", "shape:z", "1.5", "member") == 2.5, \
        "a double reply did not survive the round trip"
    client.execute_command("DEL", "shape:z")

r3.close()

# --- HELLO AUTH -----------------------------------------------------------------
# HELLO runs the ordinary AUTH and then takes its OK back off the reply, so what the
# client sees is the handshake alone. "default"/"empty" is the pair rpc_caller itself
# authenticates with when a session opens.
def fresh(proto=2):
    return redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=proto)


ra = fresh(2)
authed = raw(ra, "HELLO", "2", "AUTH", "default", "empty")
assert isinstance(authed, list), f"HELLO AUTH should answer the handshake, got {authed!r}"
# 7 pairs and nothing else: an AUTH OK travelling in front would make it 15
assert len(authed) == 14, f"HELLO AUTH leaked an extra element into the reply: {authed!r}"
assert as_text(authed[0]) == "server", f"handshake did not start where it should: {authed!r}"
ra.close()

# the options may be combined, and in either order
ra = fresh(2)
assert len(raw(ra, "HELLO", "2", "AUTH", "default", "empty", "SETNAME", "shapetest")) == 14
ra.close()

# a bad password fails the whole command - no handshake, just the error AUTH gave
for user, secret in (("default", "wrongpassword"), ("nosuchuser", "empty")):
    ra = fresh(2)
    try:
        raw(ra, "HELLO", "2", "AUTH", user, secret)
        assert False, f"HELLO AUTH {user} {secret} should have failed"
    except redis.exceptions.ResponseError as e:
        assert "authentication failed" in str(e), f"unexpected error for {user}: {e}"
    ra.close()

# AUTH needs both a username and a password
ra = fresh(2)
try:
    raw(ra, "HELLO", "2", "AUTH", "onlyone")
    assert False, "HELLO AUTH with one argument should have been refused"
except redis.exceptions.ResponseError:
    pass
ra.close()

# and the whole point of it: a client configured with credentials connects on its own,
# which is the handshake redis-py sends as HELLO <proto> AUTH <user> <pass>
cred = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=3,
                   username="default", password="empty")
cred.set("shape:authed", "yes")
assert cred.get("shape:authed") == b"yes", "a credentialled RESP3 client could not work"
cred.close()

barch.stop()
print("complete reply shape test")
