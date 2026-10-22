#!/usr/bin/env python3
"""
KSPACE MERGE between spaces that don't share a layout - TODO 718.

MERGE merged shard N into shard N whatever either space's routing. Between two unrelated
range sharded spaces, or a hash and a range one, it answered OK and then about 3400 of
8000 GETs in the target were wrong: the keys sat in shards their routing doesn't send
them to. Now a range sharded target routes each merged key by its own table, and a hash
sharded one takes only a merge from a hash sharded space with its shard count, which
puts every key where it would have put it anyway. The rest is refused.

Checked against one barchd, 8 shard spaces unless said otherwise:
  - a range sharded space into an unrelated range sharded one, each rebalanced on its
    own first. The space merged from is itself a dependant, with tombstones over keys
    the target has, and number keys, which route by a stored form that isn't the
    command's: every key of both is right in the target, the deleted ones gone, and
    still right after the target rebalances what it took;
  - a hash sharded space into a range sharded one: every key right;
  - a range sharded space into a hash sharded one, and a hash sharded space of 8 shards
    into one of 4: refused, saying why, with the target as it was;
  - a hash sharded space into another of the same shard count: every key right.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14810)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-74s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


DATA = os.path.join(os.getcwd(), "mergeroute_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "mergeroute.log"), "a")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

TAG = str(int(time.time()))
K = ["k%05d" % i for i in range(4000)]
A = ["a%05d" % i for i in range(4000)]
N = [str(i) for i in range(1, 2001)]
WAIT = scale.scaled_seconds(30.0, floor=10.0)

try:
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)

    def space(name, ranged, shards=8):
        name = name + "_" + TAG
        for k, v in ((".ordered", "1"), (".shards", str(shards)), (".range_sharded", "1" if ranged else "0")):
            c.execute_command("configuration:SET", name + k, v)
        c.execute_command("USE", name)
        c.execute_command("SETF", "sn", "function call(k) return barch.store.shardNumber(k) end\n")
        return name

    def answer(*cmd):
        try:
            return c.execute_command(*cmd)
        except redis.ResponseError as e:
            return "ERR " + str(e)

    def put(name, keys, value):
        c.execute_command("USE", name)
        for i in range(0, len(keys), 500):
            c.execute_command("MSET", *[x for k in keys[i:i + 500] for x in (k, value)])

    def wrong(name, want):
        keys = sorted(want)
        c.execute_command("USE", name)
        got = []
        for i in range(0, len(keys), 500):
            got += c.execute_command("MGET", *keys[i:i + 500])
        return [k for k, v in zip(keys, got) if v != want[k]]

    def shard(name, key):
        c.execute_command("USE", name)
        return int(c.execute_command("CALLF", "sn", key))

    def wait_for(test):
        deadline = time.time() + WAIT
        while time.time() < deadline:
            if test():
                return True
            time.sleep(0.2)
        return False

    print("range sharded into an unrelated range sharded space")
    to = space("mrt", True)
    put(to, K, "t")
    check(wait_for(lambda: shard(to, K[0]) == 0 and shard(to, K[-1]) == 7), "the target spread its keys over its shards")
    # the space merged from depends on a third, so takes its boundaries, which are
    # cut over other keys than the target's
    B = ["b%05d" % i for i in range(4000)]
    under = space("mru", True)
    put(under, B + K[3000:3100], "u")
    check(wait_for(lambda: shard(under, B[0]) == 0 and shard(under, K[3099]) == 7), "a third space spread its keys")
    frm = space("mrf", True)
    check(answer("KSPACE", "DEPENDS", frm, "ON", under) in (True, "OK"), "the space merged from depends on it")
    put(frm, A, "a")
    put(frm, N, "n")
    put(frm, K[:200], "f")
    c.execute_command("USE", frm)
    c.execute_command("DEL", *K[3000:3100])           # tombstones, over keys the target has
    check(shard(frm, K[0]) != shard(to, K[0]),
          "the two put k00000 in different shards (%d, %d)" % (shard(frm, K[0]), shard(to, K[0])))
    check(answer("KSPACE", "MERGE", frm, "INTO", to) in (True, "OK"), "MERGE answers OK")
    want = {k: "t" for k in K}
    want.update({k: "a" for k in A})
    want.update({k: "n" for k in N})
    want.update({k: "f" for k in K[:200]})
    for k in K[3000:3100]:
        want[k] = None
    bad = wrong(to, want)
    check(not bad, "every key of both is right in the target (%d wrong, %s)" % (len(bad), bad[:3]))
    c.execute_command("USE", to)
    live = sum(1 for v in want.values() if v is not None)
    check(c.execute_command("DBSIZE") >= live, "DBSIZE counts them (%s, %d keys)" % (c.execute_command("DBSIZE"), live))
    check(c.execute_command("COUNT", "a", "l") == len(A) + len(K) - 100,
          "COUNT over them (%s)" % c.execute_command("COUNT", "a", "l"))
    before = [shard(to, k) for k in A[::400]]
    check(wait_for(lambda: [shard(to, k) for k in A[::400]] != before), "the target rebalances what it took")
    bad = wrong(to, want)
    check(not bad, "every key is still right after (%d wrong, %s)" % (len(bad), bad[:3]))

    print("hash sharded into range sharded")
    to = space("mrt2", True)
    put(to, K, "t")
    check(wait_for(lambda: shard(to, K[0]) == 0 and shard(to, K[-1]) == 7), "the target spread its keys over its shards")
    frm = space("mrf2", False)
    put(frm, A + N, "h")
    check(answer("KSPACE", "MERGE", frm, "INTO", to) in (True, "OK"), "MERGE answers OK")
    want = {k: "t" for k in K}
    want.update({k: "h" for k in A + N})
    bad = wrong(to, want)
    check(not bad, "every key of both is right in the target (%d wrong, %s)" % (len(bad), bad[:3]))

    print("into a hash sharded space")
    for what, frm_ranged, frm_shards in (("range sharded", True, 8), ("hash sharded, 8 shards into 4", False, 8)):
        to = space("mrh%d" % frm_ranged, False, 4 if not frm_ranged else 8)
        put(to, K, "t")
        frm = space("mrg%d" % frm_ranged, frm_ranged, frm_shards)
        put(frm, A, "g")
        got = answer("KSPACE", "MERGE", frm, "INTO", to)
        check("ERR" in str(got) and "hash sharded" in str(got), "%s: refused (%s)" % (what, str(got)[4:50]))
        want = {k: "t" for k in K}
        want.update({k: None for k in A})
        bad = wrong(to, want)
        check(not bad, "%s: the target is as it was (%d wrong)" % (what, len(bad)))
    to = space("mrh8", False)
    put(to, K, "t")
    frm = space("mrg8", False)
    put(frm, A + N, "g")
    put(frm, K[:100], "g")
    check(answer("KSPACE", "MERGE", frm, "INTO", to) in (True, "OK"), "hash sharded, the same shard count: MERGE answers OK")
    want = {k: "t" for k in K}
    want.update({k: "g" for k in A + N + K[:100]})
    bad = wrong(to, want)
    check(not bad, "every key of both is right in the target (%d wrong, %s)" % (len(bad), bad[:3]))
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
