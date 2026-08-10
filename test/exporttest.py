import os

import redis
import barch

# A logical export is the commands that would rebuild the data, not a copy of the pages it
# happens to live in. That is the whole point of it: storage_version refuses a shard file
# written by a different build, and this is what a user does with the data in between.
#
# So the test is a round trip through an empty store. Anything that survives FLUSHALL and
# comes back the same is exported faithfully; anything that does not is a hole.

PORT = 14200

barch.start("0.0.0.0", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.response_callbacks = {}


def c(*args):
    return r.execute_command(*args)


print("start export test")

c("FLUSHALL")

# a value with an embedded null and a newline: the stream is RESP rather than lines for
# exactly this reason, and a line based export would lose it
awkward = b"a\x00b\nc\r\nd"
c("SET", "e:plain", "hello world")
c("SET", "e:binary", awkward)
c("SET", "e:empty", "")
c("SET", "e:ttl", "v", "EX", "1000")
c("HSET", "e:hash", "f1", "v1", "f2", "v2")
c("RPUSH", "e:list", "one", "two", "three")
c("ZADD", "e:zset", "1.5", "a", "2.5", "b", "-3", "c")

path = os.path.abspath("export_roundtrip.resp")
written = c("EXPORT", path)
assert written >= 7, "exported %r keys, expected at least the seven written" % written
assert os.path.getsize(path) > 0, "the export file is empty"

c("FLUSHALL")
assert c("GET", "e:plain") is None, "the store was not emptied before the import"

applied = c("IMPORT", path)
assert applied == written, "imported %r commands from an export of %r" % (applied, written)

assert c("GET", "e:plain") == b"hello world"
assert c("GET", "e:binary") == awkward, "a value holding a null or a newline did not survive"
assert c("GET", "e:empty") == b""
# the deadline travels as an absolute time, so it is still roughly what it was rather than
# being restarted from the moment of the import
ttl = c("TTL", "e:ttl")
assert 900 < ttl <= 1000, "the expiry came back as %r" % ttl

assert c("HGETALL", "e:hash") == [b"f1", b"v1", b"f2", b"v2"]
assert c("LRANGE", "e:list", "0", "-1") == [b"one", b"two", b"three"]
# scores come back as they went in, negative ones included, and in score order
assert c("ZRANGE", "e:zset", "0", "-1", "WITHSCORES") == [b"c", b"-3", b"a", b"1.5", b"b", b"2.5"]

# an ordered set is one ZADD, not one per member and not one per index entry - a set that
# exports twice is the member index being mistaken for data
c("FLUSHALL")
c("ZADD", "e:one", "1", "a", "2", "b")
n = c("EXPORT", path)
assert n == 1, "a two member set exported as %r commands" % n

os.remove(path)
print("export test passed")
