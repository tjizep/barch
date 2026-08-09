# The same command through a local handle and a remote one must answer the same.
#
# This is a shape test rather than a value test, and it is deliberately not written as a
# list of expected replies. The defect it guards against (DONE 40) decoded every element
# of a remote reply after the first from one byte too far into the buffer, so the values
# came back as the wrong *types* - a three field HMGET answered ['v1', 'false', '0.0'].
# Any fixed expectation would have caught that, but so would this, and this keeps working
# when the commands underneath it change. Local is the reference: it shares no code with
# the reply decoder the remote path uses.
#
# Only replies with more than one value can show the fault. A single value decodes
# correctly either way, which is why the defect survived - every test that drove a binding
# remotely happened to ask for one thing at a time.
import barch

PORT = 14096
barch.start("127.0.0.1", PORT)
barch.ping("127.0.0.1", PORT)

print("start remote array test")


def flat(values):
    """a list of Values as plain strings, so local and remote compare directly"""
    return [v.s() for v in values]


checks = 0


def same(what, local_result, remote_result):
    global checks
    a, b = flat(local_result), flat(remote_result)
    assert a == b, "%s: local %r but remote %r" % (what, a, b)
    assert len(a) > 1, "%s: needs more than one value to be a useful check, got %r" % (what, a)
    checks += 1


# --- hash fields -------------------------------------------------------------------
lh, rh = barch.HashSet(), barch.HashSet("127.0.0.1", PORT)
lh.set("rat_h", ["f1", "v1", "f2", "v2", "f3", "v3"])
same("HMGET three fields",
     lh.mget("rat_h", ["f1", "f2", "f3"]),
     rh.mget("rat_h", ["f1", "f2", "f3"]))
same("HGETALL", lh.getall("rat_h"), rh.getall("rat_h"))

# --- ordered set -------------------------------------------------------------------
lz, rz = barch.OrderedSet(), barch.OrderedSet("127.0.0.1", PORT)
lz.add("rat_z", ["1", "one", "2", "two", "3", "three"])
same("ZRANGE by score",
     lz.range("rat_z", 0, 10, []),
     rz.range("rat_z", 0, 10, []))
same("ZRANGE with scores",
     lz.range("rat_z", 0, 10, ["WITHSCORES"]),
     rz.range("rat_z", 0, 10, ["WITHSCORES"]))

# --- key range and glob ------------------------------------------------------------
lk, rk = barch.KeyValue(), barch.KeyValue("127.0.0.1", PORT)
for k, v in [("rat_a", "1"), ("rat_b", "2"), ("rat_c", "3")]:
    lk.set(k, v)
same("RANGE over keys",
     lk.range("rat_a", "rat_d", 10),
     rk.range("rat_a", "rat_d", 10))
same("KEYS glob", lk.glob("rat_*", 100), rk.glob("rat_*", 100))

# --- list ---------------------------------------------------------------------------
# pop removes what it answers with, so the two handles cannot be pointed at the same list.
# Each gets its own, seeded identically, and the replies are compared
ll, rl = barch.List(), barch.List("127.0.0.1", PORT)
ll.push("rat_l_local", ["a1", "a2", "a3", "a4"])
ll.push("rat_l_remote", ["a1", "a2", "a3", "a4"])
same("LPOP two", ll.pop("rat_l_local", 2), rl.pop("rat_l_remote", 2))

print("remote array test ok - %d multi value replies agree local and remote" % checks)
