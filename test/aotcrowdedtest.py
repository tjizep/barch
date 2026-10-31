# A native call that reaches its deadline somewhere it can't yield, inside a
# table.sort comparator, on a crowded CPU - TODO 802, made deterministic in TODO 805.
#
# On the pool a call's deadline counts CPU time. When the watch fires at the wall
# clock deadline, the interrupt finds budget left and moves the deadline on, then
# puts the hook back down because it can't yield. The old watch had dropped the call
# at that firing, as its timeout, so nothing put the hook back again and the call
# spun for ever.
#
# This used to need CPU hogs and a pinned CPU, and only went wrong when the machine
# was busy enough. Here BARCH_TEST_CPU_CLOCK_PERCENT makes the CPU clock run at a
# tenth of its speed instead, so every run is the crowded case: the wall clock always
# reaches the deadline long before the CPU budget is used up. Nothing is timed. The
# call has to come back with a timeout, counted once in INFO errors.
import os

# read once, at the first call, so it has to be set before barch starts
os.environ["BARCH_TEST_CPU_CLOCK_PERCENT"] = "10"

import scale
import redis
from redis.backoff import NoBackoff
from redis.retry import Retry
import barch

scale.workdir()
PORT = scale.port(default=14622)

print("start aot crowded test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)


def counter(section, name):
    return int(r.info(section).get(name, 0))


try:
    # the loop runs well past the first slice, so the call yields and goes on the
    # pool, where its CPU time is what's counted
    assert r.execute_command("SETF", "aotsort", '--@barch {"deadline_ms": 300}\n'
        "function call() local s = 0 for i = 1, 200000000 do s = s + i end "
        "table.sort({3, 1, 2}, function(a, b) while true do end end) return s end",
        "AOT") == b"OK"

    timeouts = counter("errors", "function_timeouts")
    slices = counter("memory", "luau_slices")
    # the socket timeout is only so a call that never stops fails the test instead
    # of hanging it. And no retry: redis-py sends a timed out command again on a new
    # connection, where the call starts over
    c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=20,
                    retry=Retry(NoBackoff(), 0), retry_on_timeout=False)
    try:
        try:
            out = str(c.execute_command("aotsort"))
        except redis.exceptions.TimeoutError:
            out = None
        except redis.exceptions.ResponseError as e:
            out = str(e)
    finally:
        c.close()
    if out is None:
        # the call is still spinning on a worker, and stop() would wait for it
        print("a native call stuck in a C frame past its deadline never stopped", flush=True)
        os._exit(1)
    assert counter("memory", "luau_slices") > slices, "the call never reached the pool"
    assert "timeout" in out.lower(), out
    assert counter("errors", "function_timeouts") - timeouts == 1
    print("a native call stuck in a C frame stops at its deadline")

    print("complete aot crowded test")
finally:
    try:
        barch.stop()
    except Exception:
        pass
