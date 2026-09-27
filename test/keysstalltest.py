import os
import socket
import time

import scale
import redis
import redis.backoff
import redis.retry
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

# Guards the streamed KEYS reply in src/rpc/asio_resp_session.h - TODO 488.
#
# KEYS runs on the worker pool and writes each key to the socket as it finds it. That
# used to be a plain blocking asio::write, made while art::glob held its process-wide
# glob_queue mutex. A client that sent KEYS * and then stopped reading filled its send
# buffer, and a live peer that isn't reading keeps a zero window open, so the write
# never finished. The worker stayed stuck holding glob_queue, and every other KEYS in
# every space queued behind it for good - no error, no log line, just a hang.
#
# Now the streamed bytes go out through the session's own async writes, and a worker
# only waits on them while there is too much outstanding. If the client takes nothing
# for rpc_client_max_wait_ms, the connection is closed and the worker goes. Since
# TODO 491 KEYS doesn't write until its walk is done, so it doesn't hold glob_queue
# while it waits either, and the second client below isn't held up at all.
#
# Checked here:
#   1. a second client's KEYS answers while the first one is stalled
#   2. the stalled client is disconnected rather than left hanging
#   3. a big KEYS to a client that does read still arrives whole

PORT = scale.port(default=14000)
KEYS = scale.scaled(100000, floor=40000)
PAD = "p" * 150                     # long names so the reply dwarfs the socket buffers
WAIT_MS = 2000

print("start keys stall test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
stalled = None
# set when a worker is known to be stuck: barch.stop() would wait on it for good
wedged = False
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
    r.execute_command("CLEARALL")
    r.flushdb()
    r.execute_command("CONFIG SET rpc_client_max_wait_ms", str(WAIT_MS))
    for start in range(0, KEYS, 5000):
        r.mset({f"k:{i:07d}:{PAD}": "v" for i in range(start, min(start + 5000, KEYS))})
    assert r.dbsize() == KEYS

    # --- a client that asks for everything and then reads nothing ------------------
    stalled = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    stalled.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    stalled.connect(("127.0.0.1", PORT))
    stalled.sendall(b"*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n")
    time.sleep(0.5)                 # let the worker get into the write

    # --- someone else's KEYS still answers -----------------------------------------
    # no retries: redis-py retries a timeout three times by default, which turns one
    # 17 second wait into more than a minute
    other = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2,
                        socket_timeout=WAIT_MS / 1000 + 15,
                        retry=redis.retry.Retry(redis.backoff.NoBackoff(), 0))
    began = time.perf_counter()
    try:
        got = other.execute_command("KEYS", f"k:0000001:{PAD}")
    except redis.exceptions.TimeoutError:
        wedged = True
        raise AssertionError(
            "a KEYS from a second client hung behind a client that stopped reading - "
            "the streamed write is holding glob_queue again")
    took = time.perf_counter() - began
    assert got == [f"k:0000001:{PAD}".encode()], f"the second KEYS answered {got!r}"
    assert took < WAIT_MS / 1000 + 10, f"the second KEYS took {took:.1f}s"
    print(f"second client's KEYS answered in {took:.2f}s")

    # --- the stalled client was let go --------------------------------------------
    # it still holds a worker while it takes nothing, so it has to be cut off. Since
    # TODO 491 the second KEYS no longer waits out the stall, so wait it out here
    # before reading, or the reading is what un-stalls it
    time.sleep(WAIT_MS / 1000 + 2)
    # drain what the server managed to send; it has to end, and short of the whole reply
    stalled.settimeout(10)
    received = 0
    ended = False
    try:
        while True:
            chunk = stalled.recv(1 << 20)
            if not chunk:
                ended = True
                break
            received += len(chunk)
    except ConnectionResetError:
        ended = True
    except socket.timeout:
        pass
    assert ended, (f"the stalled connection is still open after {received} bytes - it "
                   f"should have been closed once it took nothing for {WAIT_MS}ms")
    whole = KEYS * (len(f"k:0000000:{PAD}") + 16)
    assert received < whole, "the stalled client got the whole reply; it never stalled"
    print(f"stalled client closed after {received} bytes")

    # --- a reader still gets the whole of a big streamed reply ---------------------
    everything = other.execute_command("KEYS", "*")
    assert len(everything) == KEYS, f"KEYS * returned {len(everything)} of {KEYS}"
    assert None not in everything, "KEYS * was padded with nils"

    # and a pipeline around a streamed KEYS keeps its order
    p = other.pipeline(transaction=False)
    p.get(f"k:0000002:{PAD}")
    p.execute_command("KEYS", f"k:000000[34]:{PAD}")
    p.execute_command("DBSIZE")
    a, b, c = p.execute()
    assert a == b"v", f"GET before KEYS answered {a!r}"
    assert sorted(b) == [f"k:0000003:{PAD}".encode(), f"k:0000004:{PAD}".encode()], b
    assert c == KEYS, f"DBSIZE after KEYS answered {c!r}"

    other.close()
    r.close()
finally:
    if stalled is not None:
        stalled.close()
    if wedged:
        # the stuck worker never lets stop() finish, so leave without it
        import traceback
        traceback.print_exc()
        os._exit(1)
    barch.stop()
print("complete keys stall test")
