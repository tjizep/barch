import socket
import time

import scale
import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

# Guards connection buffers counting towards max_memory - TODO 493.
#
# Refusing writes and evicting went by the data alone, so replies backed up for clients
# (omem) and requests waiting in a connection's parser could take the server well past
# max_memory with nothing refused, and INFO said mem_clients_normal:0.
#
# Checked here, with no eviction policy so the limit shows up as refused writes:
#   1. clients that asked for KEYS * and read nothing push data + buffers past a limit
#      set just above the data, and a SET is refused
#   2. INFO shows the buffers as mem_clients_normal, and STATS as connection_buffer_bytes
#   3. once those clients are gone, writes work again, and the total is back to exactly 0
#      - read in process, with no connection open, so nothing of the test's own counts

PORT = scale.port(default=14000)
KEYS = scale.scaled(60000, floor=30000)
PAD = "p" * 150
HEADROOM = 4 << 20                  # max_memory = the data + this
STALLED = 3


def stat(r, name):
    s = r.execute_command("STATS")
    for i in range(0, len(s) - 1, 2):
        k = s[i].decode() if isinstance(s[i], bytes) else str(s[i])
        if k.lstrip("$") == name:
            return int(s[i + 1])
    raise AssertionError("no such stat: " + name)


def wait_for(what, value, done, seconds=60.0):
    """generous, because under a sanitizer the stalled KEYS walks are slow"""
    deadline = time.monotonic() + seconds
    seen = None
    while time.monotonic() < deadline:
        seen = value()
        if done(seen):
            return
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for {what}: last saw {seen}")


print("start mem clients test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
stalled = []
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
    r.execute_command("CLEARALL")
    r.flushdb()
    r.execute_command("CONFIG", "SET", "eviction_policy", "none")
    # long enough that the stalled clients below aren't cut off mid test - DONE 455
    r.execute_command("CONFIG", "SET", "rpc_client_max_wait_ms", "120000")
    for start in range(0, KEYS, 5000):
        r.mset({f"k:{i:07d}:{PAD}": "v" for i in range(start, min(start + 5000, KEYS))})

    held = stat(r, "logical_allocated")

    # --- clients that asked for every key and read nothing -------------------------
    # the kernel's socket buffers take a few MB of each reply, so what's left over the
    # limit depends on the key count - it only has to be past the headroom
    for _ in range(STALLED):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        s.connect(("127.0.0.1", PORT))
        s.sendall(b"*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n")
        stalled.append(s)
    wait_for("the stalled replies to back up",
             lambda: stat(r, "connection_buffer_bytes"), lambda v: v > HEADROOM + (1 << 20))
    # the limit only goes on now: KEYS stops building a reply once the process is past
    # max_memory (its own ceiling, keys_api.cpp), so set first it would cut these short
    r.execute_command("CONFIG", "SET", "max_memory_bytes", str(held + HEADROOM))
    buffers = stat(r, "connection_buffer_bytes")
    clients_mem = r.info("memory")["mem_clients_normal"]
    print(f"{STALLED} stalled clients hold {buffers} bytes; INFO mem_clients_normal "
          f"{clients_mem}; data {held}, limit {held + HEADROOM}")
    assert clients_mem > HEADROOM, f"INFO shows mem_clients_normal:{clients_mem}"

    try:
        r.set("after", "x" * 100)
        refused = None
    except redis.exceptions.ResponseError as e:
        refused = str(e)
    assert refused is not None, (
        f"a SET went through with the data at {held} bytes, {buffers} bytes of connection "
        f"buffers and max_memory at {held + HEADROOM} - buffers aren't counted")
    assert "memory" in refused, f"the SET was refused, but for {refused!r}"
    print(f"SET refused: {refused}")

    # --- gone again ----------------------------------------------------------------
    for s in stalled:
        s.close()
    stalled.clear()
    wait_for("writes to be taken again",
             lambda: stat(r, "connection_buffer_bytes"), lambda v: v < 4096)
    assert r.set("after", "x" * 100) is True, "a SET was still refused after the clients went"
    r.close()
    # nothing connected at all: every session's share has to have come back out
    wait_for("the total to reach exactly 0",
             lambda: barch.stats().connection_buffer_bytes, lambda v: v == 0)
    print("connection buffers back to 0")
finally:
    for s in stalled:
        s.close()
    barch.stop()
print("complete mem clients test")
