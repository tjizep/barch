# The range sharding option: that it can be set per key space, that it is only honoured
# where it means something, and that reading it back tells the truth.
#
# Nothing routes by range yet - see TODO 30. What is tested here is the option and its
# plumbing, which is what exists.
#
# A key space reads its options once, when it is first built, so every case below uses a
# name of its own and sets the configuration before touching the space.
import redis
import barch

PORT = 14071

barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

conf = barch.KeyValue("configuration")


def shard_info(conn, space, shard=0):
    """the INFO SHARD block for a shard of `space`, as a dict"""
    conn.execute_command("USE", space)
    # one string, not three arguments: redis-py recognises a command called INFO and
    # replaces the reply with a dict of its own parsing, which loses the block
    raw = conn.execute_command("INFO SHARD %d" % shard)
    if isinstance(raw, bytes):
        raw = raw.decode()
    out = {}
    for line in raw.split("\n"):
        if ":" in line:
            k, _, v = line.partition(":")
            out[k.strip()] = v.strip()
    return out


print("start range shard test")

# --- an ordered space that asks for it, gets it ----------------------------------
conf.set("rs_on.ordered", "1")
conf.set("rs_on.shards", "2")
conf.set("rs_on.range_sharded", "1")
on = barch.KeyValue("rs_on")
assert on.getOrdered(), "rs_on should be ordered"
assert on.getRangeSharded(), "an ordered space that asked for range sharding should have it"

# --- an unordered space is refused it ---------------------------------------------
# a range means nothing in a hash table, so the option is dropped rather than half
# honoured, and reading it back has to say so
conf.set("rs_off.ordered", "0")
conf.set("rs_off.shards", "2")
conf.set("rs_off.range_sharded", "1")
off = barch.KeyValue("rs_off")
assert not off.getOrdered(), "rs_off should be unordered"
assert not off.getRangeSharded(), \
    "range sharding needs ordered keys and should have been refused"

# --- not asking for it leaves it off ----------------------------------------------
conf.set("rs_default.ordered", "1")
conf.set("rs_default.shards", "1")
default = barch.KeyValue("rs_default")
assert default.getOrdered()
assert not default.getRangeSharded(), "the default is off"

# --- explicitly off stays off ------------------------------------------------------
conf.set("rs_zero.ordered", "1")
conf.set("rs_zero.shards", "1")
conf.set("rs_zero.range_sharded", "0")
zero = barch.KeyValue("rs_zero")
assert not zero.getRangeSharded()

# --- the default space is not configurable this way --------------------------------
# node takes its settings from the server, not from the configuration space, so it is
# off and setting the key does not change that
node = barch.KeyValue()
assert not node.getRangeSharded(), "the default node space should not be range sharded"
conf.set("node.range_sharded", "1")
assert not barch.KeyValue().getRangeSharded(), \
    "node ignores the configuration space, so this must still be off"

# --- and neither is the configuration space itself ---------------------------------
assert not conf.getRangeSharded(), "the configuration space should not be range sharded"

# --- INFO SHARD reports it, which is where somebody will look ----------------------
r = redis.Redis(host="127.0.0.1", port=PORT, protocol=2)

info_on = shard_info(r, "rs_on")
assert info_on["sharding"] == "range", f"rs_on should report range: {info_on}"
assert info_on["index_logical"] == "ordered", info_on

info_off = shard_info(r, "rs_off")
assert info_off["sharding"] == "hash", f"rs_off should report hash: {info_off}"
assert info_off["index_logical"] == "unordered", info_off

info_default = shard_info(r, "rs_default")
assert info_default["sharding"] == "hash", f"rs_default should report hash: {info_default}"

# the two ways of asking agree with each other, which is the point of the test
for space, kv in [("rs_on", on), ("rs_off", off), ("rs_default", default)]:
    reported = shard_info(r, space)["sharding"] == "range"
    assert reported == kv.getRangeSharded(), \
        f"{space}: INFO SHARD says {reported}, the binding says {kv.getRangeSharded()}"

# --- the space still works, whatever it says ---------------------------------------
on.set("a", "1")
assert on.get("a") == "1"
off.set("a", "1")
assert off.get("a") == "1"

r.close()
barch.stop()
print("complete range shard test")
