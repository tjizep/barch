import scale
import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

# One name holds one kind of collection, and the kinds do not see each other's entries.
#
# Before the container lead bytes (DONE 48) a list, hash and ordered set under one name
# were a single key range, so HLEN counted an ordered set's members as fields and ZCARD
# returned the favour. The kind is part of the key now, which fixes the counting for free
# and makes two kinds under one name possible instead - so the kind is claimed where a
# collection is created, and that is most of what this asserts.

PORT = scale.port(default=14100)

barch.start("0.0.0.0", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
# barch answers in wire terms; redis-py would rewrite some of these on the way back
r.response_callbacks = {}


def wrongtype(*args):
    """the command must be refused with redis's WRONGTYPE, not merely fail"""
    try:
        r.execute_command(*args)
    except redis.exceptions.ResponseError as e:
        assert "WRONGTYPE" in str(e), f"{args[0]} failed with {e}, wanted WRONGTYPE"
        return
    raise AssertionError(f"{args[0]} was allowed on a name held by another kind")


def ok(*args):
    return r.execute_command(*args)


print("start container kind test")

# --- a name claimed as a hash refuses the other kinds ---
ok("HSET", "ck:hash", "f", "v")
wrongtype("ZADD", "ck:hash", "1", "m")
wrongtype("ZINCRBY", "ck:hash", "1", "m")
wrongtype("LPUSH", "ck:hash", "x")
assert ok("HSET", "ck:hash", "g", "w") == 1, "the kind that owns the name still works"

# --- and as an ordered set ---
ok("ZADD", "ck:zset", "1", "m")
wrongtype("HSET", "ck:zset", "f", "v")
wrongtype("LPUSH", "ck:zset", "x")
assert ok("ZINCRBY", "ck:zset", "1", "m") == b"2"

# --- a plain value refuses all three, which worked before and must keep working ---
ok("SET", "ck:str", "s")
wrongtype("HSET", "ck:str", "f", "v")
wrongtype("ZADD", "ck:str", "1", "m")
wrongtype("LPUSH", "ck:str", "x")

# --- the miscount the lead bytes were added for ---
# two kinds cannot share a name any more, so the counts are checked on their own names.
# Each must count only its own entries, which is what a shared prefix used to break
ok("HSET", "ck:h2", "a", "1")
ok("HSET", "ck:h2", "b", "2")
ok("ZADD", "ck:z2", "1", "one")
ok("ZADD", "ck:z2", "2", "two")
ok("ZADD", "ck:z2", "3", "three")
assert ok("HLEN", "ck:h2") == 2, "hash counted something that was not a field"
assert ok("ZCARD", "ck:z2") == 3, "ordered set counted something that was not a member"

# --- the name is free again once the collection is gone ---
ok("DEL", "ck:hash")
assert ok("ZADD", "ck:hash", "1", "m") == 1, "a deleted hash still held its name"

# --- and DEL takes the whole ordered set with it, member index included.
# the index is keyed member to score and does not begin with the name, so a sweep of the
# name's prefix walks straight past it - a deleted set went on answering ZSCORE
ok("ZADD", "ck:gone", "1", "m")
ok("DEL", "ck:gone")
assert ok("ZCARD", "ck:gone") == 0, "ordered set survived its own deletion"
# ZRANK is the one that reads the member index rather than the score keys, which is
# exactly the range that used to be left behind
assert ok("ZRANK", "ck:gone", "m") is None, "the member index outlived the set"

# --- a collection holding exactly one entry ---
# the entry's key is alone in the shard it routes to, which makes that shard's tree a
# single leaf. art::range answered nothing for those, so a one field hash reported itself
# as empty while HGET happily returned the value - see DONE 49. Asserted against literals,
# since HLEN and HKEYS were wrong together and agreed with each other
ok("HSET", "ck:one", "f", "v")
assert ok("HLEN", "ck:one") == 1, "one field hash reported the wrong length"
assert ok("HKEYS", "ck:one") == [b"f"], "one field hash listed no fields"
assert ok("HGETALL", "ck:one") == [b"f", b"v"]
ok("ZADD", "ck:onez", "1", "m")
assert ok("ZCARD", "ck:onez") == 1
assert ok("ZRANGE", "ck:onez", "0", "-1") == [b"m"]
assert ok("ZCOUNT", "ck:onez", "0", "9") == 1

# --- what a keyspace command sees when the name holds a collection ---
# GET used to answer nil, as though the name were free, and EXISTS answered 0 because a
# collection has no plain key of its own. See DONE 51
ok("HSET", "ck:seen", "f", "v")
try:
    r.execute_command("GET", "ck:seen")
    raise AssertionError("GET read a hash as though it were a string")
except redis.exceptions.ResponseError as e:
    assert "WRONGTYPE" in str(e), f"GET on a hash said {e}"
assert ok("EXISTS", "ck:seen") == 1, "EXISTS could not see a hash"
assert ok("EXISTS", "ck:seen", "ck:nothing") == 1
ok("SET", "ck:plain", "v")
assert ok("GET", "ck:plain") == b"v", "GET stopped reading strings"
assert ok("GET", "ck:missing") is None, "a free name is not a wrong type"

# KEYS and SCAN report the name once, not one entry per field or member
ok("HSET", "ck:n:h", "a", "1")
ok("HSET", "ck:n:h", "b", "2")
ok("ZADD", "ck:n:z", "1", "m")
ok("SET", "ck:n:s", "x")
assert sorted(ok("KEYS", "ck:n:*")) == [b"ck:n:h", b"ck:n:s", b"ck:n:z"], "KEYS did not answer names"
assert ok("KEYS", "ck:n:h") == [b"ck:n:h"], "a pattern naming a hash did not find it"
seen = sorted(ok("SCAN", "0", "MATCH", "ck:n:*", "COUNT", "1000")[1])
assert seen == [b"ck:n:h", b"ck:n:s", b"ck:n:z"], f"SCAN did not answer names: {seen}"

# --- a key holding the separator survives being converted twice ---
# as_composite tokenised the caller's key in place, so the second conversion of one key
# saw no separator and built a string with an interior null - see DONE 50. Any command
# that checks the type before reading converts twice, which is all of these
ok("SET", "1.1 a", "1.1a")
assert ok("GET", "1.1 a") == b"1.1a", "a key holding a separator was rewritten by reading it"
assert ok("STRLEN", "1.1 a") == 4
assert ok("GETRANGE", "1.1 a", "0", "-1") == b"1.1a"
assert ok("GET", "1.1 a") == b"1.1a", "the second read differed from the first"

# --- HSCAN walks one hash, and a full iteration reports every field ---
# the cursor is scoped to the hash's prefix rather than to a shard, so it is the position
# in one contiguous run - see DONE 57
for i in range(25):
    ok("HSET", "ck:scan", "f%02d" % i, "v%d" % i)
seen, cursor, rounds = set(), b"0", 0
while True:
    cursor, batch = ok("HSCAN", "ck:scan", cursor, "COUNT", "5")
    rounds += 1
    for j in range(0, len(batch), 2):
        seen.add(batch[j])
    if cursor == b"0" or rounds > 40:
        break
assert rounds > 1, "COUNT 5 over 25 fields should have paged"
assert len(seen) == 25, "a full iteration missed fields: %d of 25" % len(seen)
assert ok("HSCAN", "ck:scan", "0", "COUNT", "100")[0] == b"0", "a small hash should finish in one call"
# MATCH is applied to the field name, which is stored encoded - a pattern that finds
# nothing at all is the signature of matching against the raw bytes instead
matched = ok("HSCAN", "ck:scan", "0", "COUNT", "100", "MATCH", "f0*")[1][::2]
assert sorted(matched) == [b"f0%d" % d for d in range(10)], "MATCH did not filter by field name"
assert len(ok("HSCAN", "ck:scan", "0", "COUNT", "100", "NOVALUES")[1]) == 25

# --- lex ranges: the bracket says whether an end is open ---
# `[a` includes a, `(a` excludes it, and - and + are the ends of the range. barch only ever
# understood a bare bound, which redis refuses; the bracket is stripped now and the
# exclusive form is honoured at both ends - see DONE 60. Nothing in the translated valkey
# tests covers this, which is why it is asserted here by hand
for _m in ("a", "b", "c", "d"):
    ok("ZADD", "ck:lex", "0", _m)
def lex(lo, hi):
    return [x.decode() for x in ok("ZRANGEBYLEX", "ck:lex", lo, hi)]
assert lex("[a", "[c") == ["a", "b", "c"], "an inclusive range lost an end"
assert lex("(a", "[c") == ["b", "c"], "an exclusive start still included its member"
assert lex("[a", "(c") == ["a", "b"], "an exclusive stop still included its member"
assert lex("(a", "(d") == ["b", "c"], "exclusive at both ends"
assert lex("-", "+") == ["a", "b", "c", "d"], "- and + should span the whole set"
assert lex("(b", "+") == ["c", "d"]
try:
    ok("ZRANGEBYLEX", "ck:lex", "a", "c")
    raise AssertionError("a bare lex bound should be refused")
except redis.exceptions.ResponseError as e:
    assert "not valid string range" in str(e), str(e)

# --- an ordered set is one name, not two ---
# its member index used to encode with no separator after the empty marker, which made the
# index key byte for byte identical to the key of a set whose name began with an 0x03. KEYS
# listed every set twice: once as itself and once as a phantom made of index bytes. The
# collision was in what was written, so no reader could have told them apart - see DONE 62
ok("ZADD", "ck:once", "1", "m", "2", "n")
listed = [x for x in ok("KEYS", "ck:once*")]
assert listed == [b"ck:once"], "an ordered set was listed as %r" % listed
scanned = ok("SCAN", "0", "MATCH", "ck:once*", "COUNT", "1000")[1]
assert scanned == [b"ck:once"], "SCAN answered %r" % scanned
# and the index still works for everything that reads through it
assert ok("ZSCORE", "ck:once", "m") == b"1"
assert ok("ZRANK", "ck:once", "n") == 1
assert ok("ZREM", "ck:once", "m") == 1
assert ok("ZRANGE", "ck:once", "0", "-1") == [b"n"]

print("container kind test passed")
