# Turning range sharding on for a key space that already has data in it.
#
# The routing table is nothing but each shard's first key, so a load rebuilds it rather
# than reading it back. That only works if the shards are already an ordered partition -
# true if the space was range sharded the last time it was written, and false if it was
# hash sharded, where every shard holds keys from all over the order. Routing by the
# boundaries of that would find almost nothing, so the load repartitions instead.
#
# A key space reads its options once, when it is first built, and is then cached for the
# life of the process, so the two halves of this have to be two processes. The first
# writes the data hash sharded and saves it; the second turns the option on and opens the
# same space, which is where the conversion happens.
import os
import subprocess

import scale
import sys

import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

PORT = scale.port(default=14075)
SHARDS = 4
KEYS = 5000
SPACE = "rs_conv"


def key(i):
    return "c%08d" % i


def phase_write():
    """hash sharded, because range_sharded is never set"""
    conf = barch.KeyValue("configuration")
    conf.set(SPACE + ".ordered", "1")
    conf.set(SPACE + ".shards", str(SHARDS))
    conf.set(SPACE + ".range_sharded", "0")
    kv = barch.KeyValue(SPACE)
    assert not kv.getRangeSharded()
    for i in range(KEYS):
        kv.set(key(i), "v%d" % i)
    assert kv.save()
    print("wrote %d hash sharded keys" % KEYS)


def phase_convert():
    """the same space, opened with the option on: the load has to repartition it"""
    conf = barch.KeyValue("configuration")
    # every option again, not just the new one: the configuration space is not carried
    # between processes, so anything left unset here falls back to the server default -
    # which for the shard count is not the four the data was written with
    conf.set(SPACE + ".ordered", "1")
    conf.set(SPACE + ".shards", str(SHARDS))
    conf.set(SPACE + ".range_sharded", "1")
    kv = barch.KeyValue(SPACE)
    assert kv.getRangeSharded(), "the option should be on in this process"

    # every key survived being moved
    for i in range(KEYS):
        got = kv.get(key(i))
        assert got == "v%d" % i, "key %s came back as %r after converting" % (key(i), got)

    # and the shards are now in key order, which is what the conversion was for. The
    # ordered walk only returns a sorted, complete answer if they are
    import redis
    r = redis.Redis(host="127.0.0.1", port=PORT, protocol=2)
    r.execute_command("USE", SPACE)
    got = [k.decode() if isinstance(k, bytes) else k
           for k in r.execute_command("RANGE", key(0), "c~", -1)]
    assert got == [key(i) for i in range(KEYS)], \
        "ordered walk returned %d of %d keys" % (len(got), KEYS)
    assert kv.min() == key(0)
    assert kv.max() == key(KEYS - 1)
    r.close()
    print("converted and read back %d keys" % KEYS)


if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else "write"
    if phase == "write" and os.name == "nt":
        # There is no exec on windows: os.execv starts the new process and leaves this
        # one alive until it ends, and this one holds the directory (TODO 571), so the
        # second half would wait for a lock that is released only when it finishes.
        # So the write half runs as a child that exits, and this process, which has
        # not started a server or touched the space, carries on as the convert half.
        # See TODO 599.
        sys.stdout.flush()
        rc = subprocess.call([sys.executable, os.path.abspath(__file__), "write-only"])
        if rc != 0:
            sys.exit(rc)
        phase = "convert"
    barch.start("0.0.0.0", PORT)
    barch.ping("127.0.0.1", PORT)
    if phase in ("write", "write-only"):
        print("start range convert test")
        phase_write()
        barch.stop()
        if phase == "write-only":
            sys.exit(0)
        # the second half has to be its own process: this one has the space cached with
        # the option off, and no amount of configuration will change that. Exec rather
        # than wait for a child, because this process holds the directory for as long as
        # it runs, and the second half needs it (TODO 571). Its exit status is the test's
        sys.stdout.flush()
        os.execv(sys.executable, [sys.executable, os.path.abspath(__file__), "convert"])
    else:
        phase_convert()
        barch.stop()
        print("complete range convert test")
