"""
A park answers from the space it was made in, and leaves no registration behind - TODO 926.

`space:CMD` sets the space for one call. A blocking pop sent that way registers its
keys in that space, but the wake ran its handler after the space was put back, so
it popped from the connection's own space: a value from a space the client never
named. FLUSHDB and FLUSHALL took every client off every key's wait list, so a pop
parked before the flush never saw the push after it.

Checked:
  - each blocking pop sent as `other:CMD` answers from `other`, and the same key in
    the connection's own space is left alone;
  - a pop parked before FLUSHDB or FLUSHALL is answered by the push after it;
  - INFO clients' blocked_keys and blocked_clients are back to 0 after every way a
    park ends: answered, on one key of several, timed out, woken with nothing left,
    a key named twice, and the client hanging up.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14819)
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


def clients(r):
    i = r.info("clients")
    return int(i.get("blocked_clients", -1)), int(i.get("blocked_keys", -1))


def wait_for(r, blocked, keys=None):
    """until INFO says so. The deadline only keeps a broken run from hanging"""
    end = time.time() + 60
    while time.time() < end:
        b, k = clients(r)
        if b == blocked and (keys is None or k == keys):
            return True
        time.sleep(0.02)
    return False


def parked(space, *args, failsafe=120):
    """a connection that has sent a blocking command; read its reply with read_response"""
    c = redis.Connection(host="127.0.0.1", port=PORT, socket_timeout=failsafe)
    c.connect()
    c.send_command("USE", space)
    c.read_response()
    c.send_command(*args)
    return c


DATA = os.path.join(os.getcwd(), "blockspace_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "blockspace.log"), "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"],
                     stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

tag = str(int(time.time()))
HOME = "bhome_" + tag
OTHER = "bother_" + tag
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=120, single_connection_client=True)
    home = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=120, single_connection_client=True)
    other = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=120, single_connection_client=True)
    home.execute_command("USE", HOME)
    other.execute_command("USE", OTHER)

    print("a pop sent as other:CMD answers from other")
    pops = [
        ("BLPOP", ("BLPOP", "k", "0"), "list"),
        ("BRPOP", ("BRPOP", "k", "0"), "list"),
        ("BLMPOP", ("BLMPOP", "0", "1", "k", "LEFT"), "list"),
        ("BZPOPMIN", ("BZPOPMIN", "k", "0"), "zset"),
        ("BZMPOP", ("BZMPOP", "0", "1", "k", "MIN"), "zset"),
    ]
    for name, args, kind in pops:
        home.delete("k")
        other.delete("k")
        # the same key in the connection's own space, holding something to take
        if kind == "list":
            home.rpush("k", "home")
        else:
            home.zadd("k", {"home": 1})
        c = parked(HOME, OTHER + ":" + args[0], *args[1:])
        check(wait_for(r, 1), "%s parks" % name)
        if kind == "list":
            other.rpush("k", "other")
        else:
            other.zadd("k", {"other": 1})
        got = c.read_response()
        flat = repr(got)
        check("other" in flat and "home" not in flat, "%s answers other's value (%s)" % (name, flat))
        left = home.lrange("k", 0, -1) if kind == "list" else home.zrange("k", 0, -1)
        check(left == [b"home"], "%s leaves the home space's key alone (%r)" % (name, left))
        c.disconnect()
        check(wait_for(r, 0, 0), "%s is off every list after" % name)

    print("a pop parked before a flush is answered by the push after it")
    for flush in ("FLUSHDB", "FLUSHALL"):
        home.execute_command("USE", HOME)
        # the failsafe is how a broken run ends: a pop the push missed waits for ever
        c = parked(HOME, "BLPOP", "fk", "0", failsafe=20)
        check(wait_for(r, 1, 1), "BLPOP parks before %s" % flush)
        home.execute_command(flush)
        home.rpush("fk", "after")
        try:
            got = c.read_response()
        except (redis.TimeoutError, OSError) as e:
            got = repr(e)
        check(got == [b"fk", b"after"], "the push after %s answers it (%r)" % (flush, got))
        c.disconnect()
        check(wait_for(r, 0, 0), "and it is off every list after %s" % flush)
    home.execute_command("USE", HOME)
    other.execute_command("USE", OTHER)

    print("every way a park ends leaves no registration")
    # answered on one key of three: the other two come off too
    c = parked(HOME, "BLPOP", "a", "b", "c", "0")
    check(wait_for(r, 1, 3), "a pop on three keys is on three lists")
    home.rpush("b", "v")
    check(c.read_response() == [b"b", b"v"], "it answers from the key pushed")
    check(wait_for(r, 0, 0), "and is off all three")
    c.disconnect()

    # a key named twice is one registration
    c = parked(HOME, "BZPOPMIN", "z1", "z2", "z2", "z1", "0")
    check(wait_for(r, 1, 2), "a pop naming its keys twice is on two lists")
    home.zadd("z2", {"m": 1})
    got = c.read_response()
    check(isinstance(got, list) and got[:2] == [b"z2", b"m"], "it answers once (%r)" % (got,))
    check(wait_for(r, 0, 0), "and is off both")
    c.disconnect()

    # timed out
    c = parked(HOME, "BLPOP", "t1", "t2", "0.05")
    check(c.read_response() is None, "a pop that times out answers nil")
    check(wait_for(r, 0, 0), "and is off both lists")
    c.disconnect()

    # woken with nothing left: another client took it first. Two waiters, one value
    w1 = parked(HOME, "BLPOP", "n", "0")
    check(wait_for(r, 1, 1), "the first waiter parks")
    w2 = parked(HOME, "BLPOP", "n", "0")
    check(wait_for(r, 2, 2), "the second waiter parks")
    home.rpush("n", "one")
    check(w1.read_response() == [b"n", b"one"], "the first waiter gets the value")
    check(wait_for(r, 1, 1), "the second is still parked, on one list")
    home.rpush("n", "two")
    check(w2.read_response() == [b"n", b"two"], "and the next push answers it")
    check(wait_for(r, 0, 0), "and both are off")
    w1.disconnect()
    w2.disconnect()

    # the client hangs up while parked
    c = parked(HOME, "BLPOP", "h1", "h2", "h3", "0")
    check(wait_for(r, 1, 3), "a parked client on three lists")
    c.disconnect()
    check(wait_for(r, 0, 0), "hangs up, and is off all three")

    # a cross space park that hangs up comes off the other space's lists
    c = parked(HOME, OTHER + ":BLPOP", "x1", "x2", "0")
    check(wait_for(r, 1, 2), "a cross space park on two lists")
    c.disconnect()
    check(wait_for(r, 0, 0), "hangs up, and is off both")

    b, k = clients(r)
    check(b == 0 and k == 0, "at the end, no client parked and no key registered (%d, %d)" % (b, k))
    r.close()
    home.close()
    other.close()
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
