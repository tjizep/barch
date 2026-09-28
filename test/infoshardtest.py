# INFO SHARD <key> names the shard the key is on - TODO 531.
#
# It routed the argument as given, while a key is stored under its encoded form:
# a number goes in as an integer, and a key holding the space's separator as a
# composite. So for most keys it named some other shard - 12 of 200 agreed on a
# 17 shard space, about what chance gives. barch.store.shardNumber encodes first,
# the way every command does, so it's the reference here.
#
# For a hash-sharded space and a range-sharded one, and for plain words, integers,
# floats and keys holding a space, `|` or `:`.
import scale
import redis
import barch

scale.workdir()

PORT = scale.port(default=14531)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0)

print("start INFO SHARD test", flush=True)

conf = redis.Redis(host="127.0.0.1", port=PORT, db=0)
conf.execute_command("USE", "configuration")
conf.execute_command("SET", "isrange.ordered", "1")
conf.execute_command("SET", "isrange.range_sharded", "1")

KEYS = (["word%d" % i for i in range(150)]
        + [str(i) for i in range(-20, 80)]
        + ["%d.5" % i for i in range(20)]
        + ["with space %d" % i for i in range(20)]
        + ["with|bar|%d" % i for i in range(20)]
        + ["with:colon:%d" % i for i in range(20)])

failures = 0
for space in ("ishash", "isrange"):
    r.execute_command("USE", space)
    for k in KEYS:
        r.execute_command("SET", k, "v")
    r.execute_command("SETF", "shardof", """
        function call(k)
            return barch.store.shardNumber(k)
        end
    """)
    # A range-sharded space rebalances while this runs, so a key can move between
    # two answers. Only a key INFO SHARD names the same shard for before and after
    # shardNumber - nothing moved in between - and still disagrees counts as wrong
    wrong = []
    for k in KEYS:
        for attempt in range(20):
            before = int(r.execute_command("INFO", "SHARD", k)["number"])
            real = r.execute_command("shardof", k)
            after = int(r.execute_command("INFO", "SHARD", k)["number"])
            if before == after:
                break
        if before == after and before != real:
            wrong.append((k, before, real))
    ok = not wrong
    print("  %-10s INFO SHARD agrees with where %d keys are: %s%s"
          % (space, len(KEYS), "pass" if ok else "FAIL",
             "" if ok else " - %d wrong, e.g. %s" % (len(wrong), wrong[:3])), flush=True)
    if not ok:
        failures += 1

    # the #<n> form takes a shard number and has to keep doing so
    n = int(r.execute_command("INFO", "SHARD", "#0")["number"])
    if n != 0:
        print("  %-10s INFO SHARD #0 names shard %d: FAIL" % (space, n), flush=True)
        failures += 1

r.close()
conf.close()
barch.stop()
assert failures == 0, "%d INFO SHARD checks failed" % failures
print("complete INFO SHARD test", flush=True)
