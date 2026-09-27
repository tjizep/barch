import time

import scale
import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

# Guards two STATS fields - TODO 494.
#
# get_statistics filled local_calls twice, the second time from max_spin, and never
# filled max_spin, so STATS (and the Python stats()) showed the spin under local_calls
# and 0 for max_spin.
#
# max_spin is the longest run of leaves a lower bound had to step over. A RANGE that
# starts in front of a run of keys that have just expired steps over them, so it can be
# driven above 0 on purpose. local_calls counts every call through rpc_caller::call, so
# it has to grow by at least the number of commands sent - and not follow max_spin.

PORT = scale.port(default=14000)
EXPIRED = 50


def stat(r, name):
    s = r.execute_command("STATS")
    for i in range(0, len(s) - 1, 2):
        k = s[i].decode() if isinstance(s[i], bytes) else str(s[i])
        if k.lstrip("$") == name:
            return int(s[i + 1])
    raise AssertionError("no such stat: " + name)


print("start stats spin test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
    r.execute_command("CLEARALL")
    r.flushdb()
    p = r.pipeline(transaction=False)
    for i in range(EXPIRED):
        p.set(f"s:{i:04d}", "gone", px=1)
    p.set("s:zzzz", "here")
    p.execute()
    time.sleep(0.1)             # expired, and not yet swept

    got = r.execute_command("RANGE", "s:", "s:~")
    assert got == [b"s:zzzz"], f"RANGE over the expired run answered {got!r}"

    spin, calls = stat(r, "max_spin"), stat(r, "local_calls")
    print(f"STATS max_spin {spin}, local_calls {calls}; stats() max_spin "
          f"{barch.stats().max_spin}")
    assert spin > 0, (f"max_spin is {spin} after a RANGE stepped over {EXPIRED} expired "
                      f"keys (local_calls says {calls})")
    assert barch.stats().max_spin == spin, "the Python stats() disagrees with STATS"

    # local_calls counts calls, whatever max_spin is doing
    for _ in range(100):
        r.ping()
    more = stat(r, "local_calls")
    assert more - calls >= 100, (f"local_calls went from {calls} to {more} over 100 PINGs - "
                                 f"it isn't counting calls")
    assert stat(r, "max_spin") == spin, "max_spin moved with calls that don't spin"
    r.close()
finally:
    barch.stop()
print("complete stats spin test")
