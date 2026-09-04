import scale
import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

# The content counters (leaf_nodes, the node size counts, logical_allocated) are gauges: they
# say what the server is holding right now. They used to be process globals with nothing
# recording which tree an allocation belonged to, which was invisible while they only ever
# went up together and wrong as soon as an operation was scoped to one shard. Clearing one
# key space zeroed the counts for every key space, so a store with data in it reported that
# it held nothing.
#
# So this measures a gauge against something independent of it - DBSIZE - across an
# operation that touches only part of the store.

PORT = scale.port(default=14300)

barch.start("0.0.0.0", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)

print("start shard statistics test")


def stats():
    flat = r.execute_command("STATS")
    out = {}
    for i in range(0, len(flat) - 1, 2):
        k = flat[i].decode() if isinstance(flat[i], bytes) else str(flat[i])
        out[k] = int(flat[i + 1])
    return out


def use(space):
    r.execute_command("USE", space)


N = 5000

use("statsalpha")
r.execute_command("FLUSHDB")
for i in range(N):
    r.execute_command("SET", f"alpha:{i}", "v")
alpha_only = stats()
assert alpha_only["leaf_nodes"] >= N, \
    "%r leaves for %r keys" % (alpha_only["leaf_nodes"], N)

use("statsbeta")
r.execute_command("FLUSHDB")
for i in range(N):
    r.execute_command("SET", f"beta:{i}", "v")
both = stats()
assert both["leaf_nodes"] >= alpha_only["leaf_nodes"] + N, \
    "the second key space added %r leaves, expected at least %r" % (
        both["leaf_nodes"] - alpha_only["leaf_nodes"], N)

# clearing beta takes away beta's leaves and nothing else. alpha still holds its keys, so a
# leaf count at or below what alpha alone had is the bug: the counter was zeroed rather than
# decremented by what the cleared shards actually held.
r.execute_command("FLUSHDB")
after = stats()
use("statsalpha")
assert r.execute_command("DBSIZE") == N, "alpha lost keys to beta's FLUSHDB"

assert after["leaf_nodes"] >= N, \
    "alpha still holds %r keys but the server reports %r leaves" % (N, after["leaf_nodes"])
assert after["leaf_nodes"] < both["leaf_nodes"], \
    "clearing beta did not take beta's leaves away: %r -> %r" % (
        both["leaf_nodes"], after["leaf_nodes"])
assert after["logical_allocated"] > 0, \
    "alpha still holds keys but logical_allocated is %r" % after["logical_allocated"]

# and clearing the rest takes the rest away, rather than going negative and wrapping
r.execute_command("FLUSHDB")
empty = stats()
assert 0 <= empty["leaf_nodes"] < N, \
    "an empty store reports %r leaves" % empty["leaf_nodes"]
assert empty["logical_allocated"] >= 0, \
    "logical_allocated went negative: %r" % empty["logical_allocated"]

print("shard statistics test passed")
