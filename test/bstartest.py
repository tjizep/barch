import scale
import redis
import barch
import threading
import time

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

PORT = scale.port(default=11000)
def btest(num):
    r = redis.Redis("127.0.0.1", PORT, 0, protocol=2)
    time.sleep(.1)
    r.lpush("testkey","l2")
    time.sleep(.1)
    r.lpush("testkey1","l3")
    time.sleep(.1)
    r.lpush("testkey2","l4")
def ctest(num):
    r = redis.Redis("127.0.0.1", PORT, 0, protocol=2)
    popped = r.blpop(["testkey1"],10)
    print ("c",popped)
    assert (popped == None or (popped[0] == b'testkey1' and popped[1] == b'l3'))
def tloss(num):
    r = redis.Redis("127.0.0.1", PORT, 0, protocol=2)
    time.sleep(1)
    for i in range(1,1000):
        r.lpush("testloss",f"l{i}")
        #print(f"Pushed {i}")
barch.clear()
barch.save()
barch.start("0.0.0.0", PORT)

bt = threading.Thread(target=btest, args=(1,))
ct = threading.Thread(target=ctest, args=(1,))

rp = redis.Redis("127.0.0.1", PORT, 0, protocol=2)
bt.start()
ct.start()
popped = rp.blpop(["testkey"],10)
print("a",popped)
assert (popped[0] == b'testkey' and popped[1] == b'l2')
popped = rp.blpop(["testkey1","testkey"],10)
print("b",popped)
assert (popped == None or (popped[0] == b'testkey1' and popped[1] == b'l3'))
assert (rp.blpop(["nonekey"],0.1)== None)
tl = threading.Thread(target=tloss, args=(1,))
tl.start()
i = 0
at = 1

for i in range(1,900):
    print(f"pop {i}")
    pp = None
    while pp == None:
        pp = rp.brpop(["testloss"],2)

    assert(f"l{i}" == pp[1].decode('utf-8'))
tl.join()


# ---------------------------------------------------------------------------
# A parked client that disconnects - TODO 226.
#
# While a connection is parked the read chain is deliberately down, so nothing
# was watching the socket and a client that went away mid-block left its
# registration on the shard for good. `blocked_clients` never came back down,
# and the next wake on that key went to the dead session, which popped the
# value and dropped it while a live waiter carried on waiting. That is what
# broke `BZPOPMIN with same key multiple times` in the differential run.
import socket


def _enc(args):
    out = ("*%d\r\n" % len(args)).encode()
    for a in args:
        a = str(a).encode()
        out += b"$%d\r\n" % len(a) + a + b"\r\n"
    return out


def _blocked(conn):
    info = conn.execute_command("INFO", "clients")
    if isinstance(info, dict):
        return int(info.get("blocked_clients", 0))
    for line in str(info).splitlines():
        if line.startswith("blocked_clients:"):
            return int(line.split(":", 1)[1])
    return 0


def _wait_blocked(conn, count, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if _blocked(conn) >= count:
            return True
        time.sleep(0.01)
    return False


def _park(port, args):
    """a raw connection that sends a blocking command and never reads it"""
    s = socket.create_connection(("127.0.0.1", port), timeout=10)
    s.sendall(_enc(args))
    return s


print("a parked client that drops its socket releases the block", flush=True)
rp.execute_command("DEL", "deadkey")
base = _blocked(rp)
dead = _park(PORT, ["blpop", "deadkey", "0"])
assert _wait_blocked(rp, base + 1), "the client never parked"
dead.close()
end = time.time() + 5
while time.time() < end and _blocked(rp) > base:
    time.sleep(0.01)
assert _blocked(rp) == base, "blocked_clients stayed up after the socket closed"

print("and does not take the wake from a live waiter", flush=True)
dead = _park(PORT, ["blpop", "deadkey", "0"])
assert _wait_blocked(rp, base + 1), "the client never parked"
dead.close()
live = _park(PORT, ["blpop", "deadkey", "0"])
assert _wait_blocked(rp, base + 1), "the live client never parked"
rp.execute_command("RPUSH", "deadkey", "v")
live.settimeout(5.0)
got = b""
try:
    got = live.recv(4096)
except (socket.timeout, TimeoutError):
    pass
live.close()
assert b"deadkey" in got and b"v" in got, \
    "the live waiter got %r - the wake went to the dead registration" % got
assert rp.execute_command("LLEN", "deadkey") == 0, "the value was not consumed by the live waiter"

print("a command pipelined behind a blocking one is still answered, in order", flush=True)
rp.execute_command("DEL", "pipekey")
s = socket.create_connection(("127.0.0.1", PORT), timeout=10)
# one write, so the PING is sitting in the buffer while the BLPOP parks. The
# disconnect watch sees the socket readable then and must neither take those
# bytes nor read them as a disconnect
s.sendall(_enc(["blpop", "pipekey", "0"]) + _enc(["ping"]))
assert _wait_blocked(rp, base + 1), "the client never parked"
time.sleep(0.3)
assert _blocked(rp) == base + 1, "the pipelined bytes were read as a disconnect"
rp.execute_command("RPUSH", "pipekey", "v")
s.settimeout(5.0)
got = b""
end = time.time() + 5
while time.time() < end and b"PONG" not in got:
    try:
        chunk = s.recv(4096)
        if not chunk:
            break
        got += chunk
    except (socket.timeout, TimeoutError):
        break
s.close()
assert got.startswith(b"*2\r\n$7\r\npipekey\r\n$1\r\nv\r\n"), "blpop reply wrong: %r" % got
assert got.endswith(b"+PONG\r\n"), "the pipelined PING was not answered after it: %r" % got
print("parked disconnect tests passed", flush=True)
