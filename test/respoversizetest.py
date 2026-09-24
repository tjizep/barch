# An argument over redis_max_item_len - TODO 428.
#
# The parser refuses it, and that used to be logged and nothing else: no reply, no
# next read, no close, and the client waited forever. Now the client gets a protocol
# error and the connection is closed, the way redis handles one. What was asked
# before it in the same pipeline is still answered.
import socket
import time

import scale

import redis
import barch

scale.workdir()
PORT = scale.port(default=14498)

print("start resp oversize test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

failures = []


def check(ok, what):
    if not ok:
        print("  FAIL " + what, flush=True)
        failures.append(what)


def bulk(b):
    return b"$%d\r\n%s\r\n" % (len(b), b)


big = b"x" * 7_000_000                 # over the 6,400,000 limit
s = socket.create_connection(("127.0.0.1", PORT), timeout=20)
s.sendall(b"*1\r\n" + bulk(b"PING") + b"*3\r\n" + bulk(b"SET") + bulk(b"oversized") + bulk(big))
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
check(got.startswith(b"+PONG\r\n"), "the PING before it is answered: %r" % got[:60])
check(b"-ERR Protocol error" in got or b"-Protocol error" in got,
      "the oversized argument gets a protocol error: %r" % got[:120])
check(closed, "and the connection is closed rather than left waiting")

# the server is fine, and the oversized key was never written
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
check(r.ping(), "a new connection is answered")
check(r.get("oversized") is None, "nothing was written")
# an argument under the limit still works (a value itself has to fit a 512K page)
r.set("undersized", b"y" * 400_000)
check(len(r.get("undersized")) == 400_000, "an argument under the limit is stored")

if failures:
    print("resp oversize test FAILED: %d" % len(failures), flush=True)
    raise SystemExit(1)
print("resp oversize test ok", flush=True)
barch.stop()
