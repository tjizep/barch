import random
import threading
import time

import scale
import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

# Guards the KEYS reply shape in src/keys_api.cpp - TODO 491.
#
# KEYS over RESP used to walk the store twice with no lock: once to count, so it could
# send *N first, then again to send. When the second walk found fewer keys - a DEL, an
# expiry, an eviction or a defrag in between - the rest of the *N was padded with nils.
# Redis never answers KEYS with a nil, and `for k in r.keys(): r.get(k)` falls over on
# one a long way from where it came from.
#
# Now it walks once, keeps what it found as encoded bytes, and sends *N with exactly
# those. Checked here: KEYS in a loop while other clients delete and put back keys that
# match, and not one reply has a nil in it, or a key outside the pattern.

PORT = scale.port(default=14000)
KEYS = scale.scaled(20000, floor=4000)
PAD = "p" * 40
SECONDS = scale.scaled_seconds(8.0, floor=3.0)


def name(i: int) -> str:
    return f"k:{i:06d}:{PAD}"


print("start keys nil pad test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
    r.execute_command("CLEARALL")
    r.flushdb()
    for start in range(0, KEYS, 5000):
        r.mset({name(i): "v" for i in range(start, min(start + 5000, KEYS))})

    stop = threading.Event()
    churn_errors = []

    def churn(seed: int):
        w = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
        rnd = random.Random(seed)
        try:
            while not stop.is_set():
                batch = [name(rnd.randrange(KEYS)) for _ in range(200)]
                w.delete(*batch)
                w.mset({k: "v" for k in batch})
        except Exception as e:  # noqa: BLE001 - reported below
            churn_errors.append(repr(e))
        finally:
            w.close()

    churners = [threading.Thread(target=churn, args=(s,)) for s in range(3)]
    for t in churners:
        t.start()

    rounds = nils = dups = strangers = 0
    worst = None
    deadline = time.monotonic() + SECONDS
    try:
        while time.monotonic() < deadline:
            got = r.execute_command("KEYS", "k:*")
            rounds += 1
            n = sum(1 for k in got if k is None)
            nils += n
            if n and worst is None:
                worst = f"round {rounds}: {n} nils in a reply of {len(got)}"
            present = [k for k in got if k is not None]
            dups += len(present) - len(set(present))
            strangers += sum(1 for k in present if not k.startswith(b"k:"))
    finally:
        stop.set()
        for t in churners:
            t.join()

    print(f"{rounds} KEYS replies under churn: {nils} nils, {dups} duplicates, "
          f"{strangers} keys outside the pattern")
    assert not churn_errors, churn_errors[0]
    assert rounds >= 3, f"only {rounds} KEYS ran in {SECONDS}s"
    assert nils == 0, (f"KEYS answered with {nils} nils over {rounds} replies ({worst}) - "
                       f"the reply is being padded again")
    assert strangers == 0, f"{strangers} keys outside the pattern"

    # quiet again: exactly the keys that are there
    everything = r.execute_command("KEYS", "k:*")
    assert len(everything) == KEYS and len(set(everything)) == KEYS, (
        f"KEYS on a quiet store returned {len(everything)} ({len(set(everything))} "
        f"distinct) of {KEYS}")
    r.close()
finally:
    barch.stop()
print("complete keys nil pad test")
