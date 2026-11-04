import scale
import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

# Guards the string writes in src/keys_api.cpp when ordered_keys is off - TODO 828.
#
# With ordered_keys off a read looks only in the hash. SET marked its key for the
# hash; the other string writes did not, so their keys went into the ART and no
# read found them. MSET answered OK and stored nothing a GET could see. Checked
# here: each string write, then GET, for ordered_keys off and on.

PORT = scale.port(default=14092)


def check(r, label):
    def want(key, value):
        got = r.get(key)
        assert got == value, f"{label}: {key} reads {got!r}, wanted {value!r}"

    assert r.execute_command("MSET", "ms1", "a", "ms2", "b")
    want("ms1", b"a")
    want("ms2", b"b")
    assert r.execute_command("MSETNX", "mn1", "a", "mn2", "b")
    want("mn1", b"a")
    want("mn2", b"b")
    assert r.execute_command("SETNX", "sn1", "a")
    want("sn1", b"a")
    r.execute_command("GETSET", "gs1", "a")
    want("gs1", b"a")
    r.set("gs2", "x")
    assert r.execute_command("GETSET", "gs2", "a") == b"x"
    want("gs2", b"a")
    r.execute_command("APPEND", "ap1", "a")
    want("ap1", b"a")
    r.set("ap2", "x")
    r.execute_command("APPEND", "ap2", "a")
    want("ap2", b"xa")
    r.execute_command("SETRANGE", "sr1", "0", "a")
    want("sr1", b"a")
    r.set("sr2", "xx")
    r.execute_command("SETRANGE", "sr2", "1", "a")
    want("sr2", b"xa")
    r.execute_command("INCR", "in1")
    want("in1", b"1")
    r.execute_command("INCR", "in1")
    want("in1", b"2")
    r.execute_command("INCRBY", "in2", "5")
    want("in2", b"5")
    r.execute_command("DECR", "in3")
    want("in3", b"-1")
    r.execute_command("INCRBYFLOAT", "if1", "1.5")
    want("if1", b"1.5")
    r.execute_command("SETEX", "se1", "100", "a")
    want("se1", b"a")
    r.execute_command("PSETEX", "pe1", "100000", "a")
    want("pe1", b"a")
    # every key above is one key, counted once
    assert r.dbsize() == 17, f"{label}: dbsize {r.dbsize()}"


print("start hash writes test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
    for ordered in ("no", "yes"):
        r.execute_command("CLEARALL")
        r.execute_command("CONFIG", "SET", "ordered_keys", ordered)
        check(r, f"ordered_keys={ordered}")
    r.close()
finally:
    barch.stop()
print("hash writes test passed")
