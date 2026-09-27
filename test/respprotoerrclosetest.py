# A protocol error lets the session go - TODO 489.
#
# After a protocol error (an argument over redis_max_item_len, TODO 428) the
# session sent its error and then shut down *and closed* the socket. Close sets
# the handle to -1 on the socket's thread while the session collector reads it
# from its own thread with no lock, which TSan reports as a race. Worse, the
# collector frees a session only when a peek on its fd returns 0, and a peek on
# -1 is EBADF, so the session and its slot were never freed. Now it only shuts
# down, the peek sees 0 and the fd closes when the session goes.
#
# This sends a few oversized arguments on their own connections, checks each
# gets the error and is let go, and checks redis_sessions in INFO comes back
# down to where it started. Under TSan it also guards the race.
import socket
import time

import scale

import redis
import barch

scale.workdir()
PORT = scale.port(default=14502)
CONNECTIONS = 5

print("start resp protocol error close test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

failures = []


def check(ok, what):
    if not ok:
        print("  FAIL " + what, flush=True)
        failures.append(what)


def bulk(b):
    return b"$%d\r\n%s\r\n" % (len(b), b)


def sessions(r):
    # INFO only knows this section by its capitals
    return int(r.info("SERVER")["redis_sessions"])


def wait_sessions(r, want, timeout):
    deadline = time.time() + timeout
    n = sessions(r)
    while n != want and time.time() < deadline:
        time.sleep(0.2)
        n = sessions(r)
    return n


# one connection of our own to watch from, open the whole time
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
r.ping()
# anything barch.ping left behind has to be collected first, so the baseline is ours
base = wait_sessions(r, 1, 20)
check(base == 1, "only the watching connection is open at the start: %d" % base)

big = b"x" * 7_000_000                 # over the 6,400,000 limit
for i in range(CONNECTIONS):
    s = socket.create_connection(("127.0.0.1", PORT), timeout=20)
    s.sendall(b"*3\r\n" + bulk(b"SET") + bulk(b"oversized") + bulk(big))
    got = b""
    closed = False
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            chunk = s.recv(65536)
        except socket.timeout:
            break
        except ConnectionResetError:
            closed = True
            break
        if not chunk:
            closed = True
            break
        got += chunk
    s.close()
    check(b"Protocol error" in got, "connection %d gets a protocol error: %r" % (i, got[:120]))
    check(closed, "connection %d is ended by the server" % i)

# the collector runs every maintenance_poll_delay (440 ms), so this is plenty. With
# the close it never came down at all.
after = wait_sessions(r, base, 20)
check(after == base, "the sessions are let go: redis_sessions %d, started at %d" % (after, base))

# and the server keeps serving, on the old connection and on a new one
check(r.ping(), "the watching connection still works")
r2 = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r2.set("after", "yes")
check(r2.get("after") == b"yes", "a new connection reads and writes")
check(r2.get("oversized") is None, "nothing oversized was written")
r2.close()

if failures:
    print("resp protocol error close test FAILED: %d" % len(failures), flush=True)
    raise SystemExit(1)
print("resp protocol error close test ok", flush=True)
r.close()
barch.stop()
