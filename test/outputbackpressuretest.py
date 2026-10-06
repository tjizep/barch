import os
import select
import socket
import threading
import time

import scale
import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

# Guards the output backpressure in src/rpc/asio_resp_session.h - TODO 490.
#
# A session kept reading and running requests however far its replies had backed up,
# so a client that pipelined GETs faster than it read the answers grew the reply queue
# without limit. That memory isn't counted by max_memory, so nothing was evicted or
# refused on the way to the kernel's OOM killer.
#
# Now a session stops taking requests once its replies back up past a limit, and picks
# up where it left off when they've drained. The requests wait in the kernel and in the
# parser's buffer instead, which are both bounded.
#
# Checked here, once with nothing but GETs and once with a KEYS every 500 requests:
#   1. pipelining without reading, the server stops taking requests well before the end
#   2. and the process doesn't grow by the size of the replies it owes
#   3. reading it all back, every reply is there, whole and in order
#
# Both shapes matter. The GETs alone are what grew without limit. With a KEYS among them
# the old code happened to stop anyway, because an asynchronous batch waits for queued
# replies to go out before it runs - so the mixed run is the one that crosses the new
# pause and resume with the batch path.
#
# The volume isn't scaled: the point is to send more than the socket buffers hold.

PORT = scale.port(default=14000)
VALUE = b"x" * 8192
KEY = "g:" + "k" * 1000             # long, so the requests outrun the kernel buffers too
REQUESTS = 40000


def bulk(b: bytes) -> bytes:
    return b"$%d\r\n%s\r\n" % (len(b), b)


def command(*args: bytes) -> bytes:
    return b"*%d\r\n" % len(args) + b"".join(bulk(a) for a in args)


GET_REQ = command(b"GET", KEY.encode())
KEYS_REQ = command(b"KEYS", KEY.encode())
GET_REPLY = bulk(VALUE)
KEYS_REPLY = b"*1\r\n" + bulk(KEY.encode())


def rss_bytes() -> int:
    return scale.rss_bytes()


def run(keys_every: int) -> None:
    """one connection's worth: 0 means GETs only"""
    def is_keys(i: int) -> bool:
        return keys_every > 0 and i % keys_every == keys_every - 1

    requests = b"".join(KEYS_REQ if is_keys(i) else GET_REQ for i in range(REQUESTS))
    expected_total = sum(len(KEYS_REPLY if is_keys(i) else GET_REPLY) for i in range(REQUESTS))
    owed = REQUESTS * len(GET_REPLY)
    shape = f"a KEYS every {keys_every}" if keys_every else "GETs only"

    client = None
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(("127.0.0.1", PORT))
        client.setblocking(False)

        # --- send without reading, until the server stops taking anything ------------
        before = rss_bytes()
        sent = 0
        while sent < len(requests):
            try:
                sent += client.send(requests[sent:sent + (1 << 20)])
            except BlockingIOError:
                _, writable, _ = select.select([], [client], [], 2.0)
                if not writable:
                    break               # nothing taken for 2 seconds: the server has stopped
        grew = rss_bytes() - before
        print(f"{shape}: sent {sent} of {len(requests)} request bytes before the server stopped; "
              f"the process grew {grew // (1 << 20)} MB, replies owed {owed // (1 << 20)} MB")
        assert sent < len(requests) // 2, (
            f"{shape}: the server took {sent} of {len(requests)} request bytes without a reply being "
            f"read - it isn't stopping when its replies back up")
        assert grew < owed // 3, (
            f"{shape}: the process grew by {grew} bytes while it owed {owed} bytes of replies - the "
            f"replies are being queued in memory")

        # --- read it all back while sending the rest ---------------------------------
        failure = []

        def read_all():
            buf = bytearray()
            at = 0                      # position in buf
            i = 0                       # the reply being checked
            got = 0
            client_r = client
            try:
                while i < REQUESTS:
                    want = KEYS_REPLY if is_keys(i) else GET_REPLY
                    while len(buf) - at < len(want):
                        ready, _, _ = select.select([client_r], [], [], 30.0)
                        if not ready:
                            failure.append(f"no reply for 30s at reply {i} of {REQUESTS}")
                            return
                        try:
                            chunk = client_r.recv(1 << 20)
                        except BlockingIOError:
                            continue
                        if not chunk:
                            failure.append(f"the connection closed at reply {i} of {REQUESTS}")
                            return
                        got += len(chunk)
                        if at > (1 << 22):
                            del buf[:at]
                            at = 0
                        buf += chunk
                    if buf[at:at + len(want)] != want:
                        failure.append(f"reply {i} is wrong: {bytes(buf[at:at + 60])!r}...")
                        return
                    at += len(want)
                    i += 1
                if len(buf) - at:
                    failure.append(f"{len(buf) - at} bytes more than {REQUESTS} replies")
            except Exception as e:  # noqa: BLE001 - reported below
                failure.append(repr(e))

        reader = threading.Thread(target=read_all)
        reader.start()
        while sent < len(requests):
            try:
                sent += client.send(requests[sent:sent + (1 << 20)])
            except BlockingIOError:
                select.select([], [client], [], 1.0)
        reader.join(timeout=120)
        assert not reader.is_alive(), "reading the replies back took more than 120s"
        assert not failure, failure[0]
        print(f"{shape}: all {REQUESTS} replies back, {expected_total} bytes, in order")

        # the requests waited in the parser, and it has to have let go of the ones it
        # ran. tot-mem is the read buffer plus the largest the parser's buffer has been;
        # without compaction that's every byte this connection sent
        o = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=30)
        tot = max(int(c["tot-mem"]) for c in o.client_list())
        o.close()
        print(f"{shape}: largest connection tot-mem {tot} bytes")
        assert tot < len(requests) // 4, (
            f"{shape}: a connection's input buffer reached {tot} bytes of the {len(requests)} "
            f"it was sent - the parser isn't dropping requests it has run")

        # the connection is still good afterwards
        client.setblocking(True)
        client.settimeout(10)
        client.sendall(command(b"PING"))
        assert client.recv(64) == b"+PONG\r\n"
    finally:
        if client is not None:
            client.close()


print("start output backpressure test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
    r.execute_command("CLEARALL")
    r.flushdb()
    r.set(KEY, VALUE)
    r.close()

    for keys_every in (0, 500):
        run(keys_every)
finally:
    barch.stop()
print("complete output backpressure test")
