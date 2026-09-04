# KEYS over RESP writes each matching key to the socket as it is found, so the
# reply does not sit in Variables. The answers still have to be the same keys.
# A later bitmap behind glob_page_list (TODO 81) must still pass this file.
import scale
import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

PORT = scale.port(default=14082)
N = 200

barch.start("0.0.0.0", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, protocol=2)

print("start keys stream test")


def decoded(items):
    out = []
    for i in items:
        out.append(i.decode() if isinstance(i, bytes) else i)
    return out


r.execute_command("FLUSHDB")

assert r.keys("ks:*") == [], r.keys("ks:*")
assert r.execute_command("KEYS", "ks:*", "COUNT") == 0

for i in range(N):
    r.set("ks:%08d" % i, "v%d" % i)

expected = sorted(["ks:%08d" % i for i in range(N)])
got = sorted(decoded(r.keys("ks:*")))
assert got == expected, "KEYS gave %d of %d" % (len(got), N)

assert r.execute_command("KEYS", "ks:*", "COUNT") == N
assert sorted(decoded(r.keys("ks:0000000*"))) == expected[:10]

# VALUES is still built on the result stack; it must keep answering keys
values = sorted(decoded(r.execute_command("VALUES", "v1*")))
assert "ks:00000001" in values
assert "ks:00000010" in values

# a pipelined GET after KEYS still sees its own reply. MULTI/EXEC is not
# used here: KEYS is asynchronous, and a transaction would queue it on a
# copy of the caller that EXEC never sees.
pipe = r.pipeline(transaction=False)
pipe.keys("ks:0000000*")
pipe.get("ks:00000000")
out = pipe.execute()
assert sorted(decoded(out[0])) == expected[:10]
assert out[1] == b"v0"

r.close()
barch.stop()
print("complete keys stream test")
