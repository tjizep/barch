import threading
import time

import scale
import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

# Guards the asynchronous call path in src/rpc/asio_resp_session.h.
#
# KEYS and VALUES are marked asynchronous so a long scan is handed to the worker pool
# rather than occupying one of the few service threads. The session used to resume
# reading from two places when a batch contained one of them - the worker did it on
# completion and the io thread did it as well - which left two async_read_some chains
# filling one buffer and driving one parser. It survived small traffic and came apart
# on the next sizeable pipelined write: the parser threw "invalid array size", the
# connection stopped answering, and the client waited on a reply that never came, at
# zero CPU, looking exactly like a deadlock.
#
# Two things are checked here, and both need the asynchronous flag to be on to mean
# anything. If KEYS and VALUES are ever made synchronous again this file still passes,
# so it is worth knowing that it only guards the path while that flag is set.
#
#   1. a large write straight after a scan still works - the original symptom
#   2. replies come back in request order when a pipeline mixes synchronous commands
#      with asynchronous ones, which is what the batch has to preserve while it runs
#      the calls one at a time

PORT = scale.port(default=14000)
ROUNDS = scale.scaled(40, floor=4)
SEED_KEYS = scale.scaled(3000, floor=200)
WRITE_PER_ROUND = scale.scaled(400, floor=40)
AFTER_KEYS = scale.scaled(2000, floor=100)

print("start async pipeline test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
    r.execute_command("CLEARALL")
    r.flushdb()
    r.execute_command("CONFIG SET rpc_max_buffer 1m")
    r.mset({f"s:{i:05d}": f"v{i}" for i in range(SEED_KEYS)})

    # --- a big write straight after a scan ---------------------------------------
    # this is the shape that used to kill the connection: scan, then write enough that
    # the request spans what the parser reads in one go
    for _ in range(8):
        r.execute_command("VALUES", "*v1*", "COUNT")
    r.mset({f"after:{k:05d}": "y" * 500 for k in range(AFTER_KEYS)})
    assert r.get("after:00000") == b"y" * 500, "a write after a scan did not take"
    assert r.dbsize() == SEED_KEYS + AFTER_KEYS, f"expected {SEED_KEYS + AFTER_KEYS} keys, got {r.dbsize()}"
    # those keys stay; the pipeline section below counts from whatever is here now
    base = r.dbsize()

    # --- replies stay in request order across the sync/async boundary -------------
    for round_ in range(ROUNDS):
        p = r.pipeline(transaction=False)
        expected = []
        for j in range(6):
            p.get(f"s:{j:05d}")
            expected.append((f"GET s:{j:05d}", f"v{j}".encode()))
            p.execute_command("KEYS", f"s:0000{j}")
            expected.append((f"KEYS s:0000{j}", [f"s:0000{j}".encode()]))
            p.execute_command("DBSIZE")
            expected.append(("DBSIZE", base + WRITE_PER_ROUND * round_))
            p.execute_command("VALUES", f"v{j}")
            expected.append((f"VALUES v{j}", [f"s:0000{j}".encode()]))
        got = p.execute()

        assert len(got) == len(expected), (
            f"round {round_}: {len(got)} replies for {len(expected)} requests - "
            f"the batch lost or duplicated one")
        for (label, want), actual in zip(expected, got):
            assert actual == want, (
                f"round {round_}: {label} answered {actual!r}, expected {want!r} - "
                f"replies are out of order across the sync/async boundary")

        # and a sizeable write between rounds, so every round re-tests the original
        # symptom with an asynchronous call immediately before it
        r.mset({f"w:{round_}:{k:04d}": "x" * 500 for k in range(WRITE_PER_ROUND)})

    assert r.dbsize() == base + WRITE_PER_ROUND * ROUNDS, \
        f"expected {base + WRITE_PER_ROUND * ROUNDS} keys, got {r.dbsize()}"

    # --- a blocking command inside an asynchronous batch -------------------------
    # run_params turns every call after the first asynchronous one asynchronous too, so
    # a BLPOP behind a KEYS runs against a copy of the session caller. Its blocks used to
    # be registered on that copy and thrown away with it, so the command did not block at
    # all - it answered nil straight away. They are now handed back to the session, which
    # is what answers when the block resolves.
    # nothing here asserts on how long a timeout took. The block timer is loose - a one
    # second BLPOP has been seen to return anywhere from 0.14s to 0.9s - and a duration
    # bound tight enough to catch the bug would be flaky. The woken case below is the
    # discriminator instead, and an exact one: a BLPOP that is not blocking answers nil
    # at once and can never come back with the value that was pushed after it.
    assert r.execute_command("BLPOP", "b:nothing", "1") is None

    p = r.pipeline(transaction=False)
    p.execute_command("KEYS", "s:00001")
    p.execute_command("BLPOP", "b:nothing2", "1")
    mixed = p.execute()
    assert mixed[1] is None, f"the timed out BLPOP should answer nil, got {mixed[1]!r}"

    # and it has to actually wake, not just wait
    def push_later(key, after):
        time.sleep(after)
        w = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
        w.rpush(key, "hello")
        w.close()

    threading.Thread(target=push_later, args=("b:waited", 0.4)).start()
    p = r.pipeline(transaction=False)
    p.execute_command("KEYS", "s:00002")
    p.execute_command("BLPOP", "b:waited", "5")
    woke_start = time.perf_counter()
    woke = p.execute()
    took = time.perf_counter() - woke_start
    assert woke[1] == (b"b:waited", b"hello"), (
        f"the woken BLPOP answered {woke[1]!r} - behind an asynchronous call its blocks "
        f"used to be registered on a copy of the caller and thrown away, so it answered "
        f"nil immediately and never saw the push")
    assert took < 4.0, f"the BLPOP took {took:.2f}s to wake, the push was at 0.4s"

    # the rest of the batch behind the blocking command still runs, and in order
    threading.Thread(target=push_later, args=("b:rest", 0.3)).start()
    p = r.pipeline(transaction=False)
    p.execute_command("KEYS", "s:00003")
    p.execute_command("BLPOP", "b:rest", "5")
    p.get("s:00004")
    p.execute_command("DBSIZE")
    rest = p.execute()
    assert [x for x in rest[0]] == [b"s:00003"], f"first reply {rest[0]!r}"
    assert rest[1] == (b"b:rest", b"hello"), f"blocking reply {rest[1]!r}"
    assert rest[2] == b"v4", f"the reply after the block is out of order: {rest[2]!r}"
    assert isinstance(rest[3], int), f"the last reply of the batch is missing: {rest[3]!r}"

    # the connection is still healthy after all of that
    assert r.get("s:00000") == b"v0"
    assert r.execute_command("VALUES", "*", "COUNT") == r.dbsize()

    r.close()
finally:
    barch.stop()
print("complete async pipeline test")
