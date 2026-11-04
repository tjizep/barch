import scale
import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

# Guards MGET's batched read in src/keys_api.cpp - TODO 827.
#
# MGET takes its keys a run of 32 at a time. It locks each shard in the run once, starts
# every key's reads, then searches. The replies must still come back in argument order,
# with a null for a missing key and for a refused one (the empty key), and a run must
# not leak into the next. Checked for the hash index, the ART, and inside MULTI.

PORT = scale.port(default=14090)
N = 200


def name(i: int) -> str:
    return f"mg:{i:05d}"


def want(args):
    out = []
    for a in args:
        if a == "" or int(a[3:]) % 3 == 0:
            out.append(None)
        else:
            out.append(("v" + a[3:]).encode())
    return out


def check(r, label):
    # every key whose number is not a multiple of 3 is there
    r.mset({name(i): "v" + name(i)[3:] for i in range(N) if i % 3})
    assert r.get(name(1)) == b"v00001", f"{label}: MSET did not land"
    for size in (1, 2, 31, 32, 33, 64, 65, N):
        args = [name(i) for i in range(size)]
        got = r.execute_command("MGET", *args)
        assert got == want(args), f"{label}: MGET of {size} keys: {got[:8]}"
    # refused keys at a run's edges and inside one
    args = [name(i) for i in range(70)]
    for at in (0, 5, 31, 32, 33, 69):
        args[at] = ""
    got = r.execute_command("MGET", *args)
    assert got == want(args), f"{label}: MGET with empty keys: {got[:8]}"
    # all refused, and repeats of one key across runs
    assert r.execute_command("MGET", "", "", "") == [None, None, None]
    args = [name(1)] * 40 + [name(3)] * 40
    assert r.execute_command("MGET", *args) == want(args), f"{label}: repeats"
    # the same inside MULTI, where a commit may hold shards already
    p = r.pipeline(transaction=True)
    args = [name(i) for i in range(N)]
    p.execute_command("MGET", *args)
    assert p.execute()[0] == want(args), f"{label}: MGET in MULTI"


print("start mget batch test")
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
print("mget batch test passed")
