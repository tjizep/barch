#!/usr/bin/env python3
"""
How much will Windows take from a server before a client that reads nothing
makes its sends stop completing? A diagnostic for TODO 599, not a test.

barch's backpressure and stall handling (rpc_output_high_water,
rpc_client_max_wait_ms) count on a send to a client that isn't reading
eventually not completing. On Linux that's a few MB. On Windows five tests that
rely on it fail and one pipeline deadlocks, so this measures it directly with
overlapped (IOCP) sends - asyncio's Proactor loop uses the same mechanism asio
does - for a few SO_SNDBUF settings on the server's socket.

    python win32/send_probe.py
"""
import asyncio
import socket
import sys
import time

CHUNK = 64 * 1024
SECONDS = 3.0


async def probe(sndbuf):
    loop = asyncio.get_running_loop()
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.setblocking(False)
    port = listener.getsockname()[1]

    client = socket.socket()
    client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    client.setblocking(False)
    connecting = asyncio.ensure_future(loop.sock_connect(client, ("127.0.0.1", port)))
    server, _ = await loop.sock_accept(listener)
    await connecting
    if sndbuf is not None:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, sndbuf)
    actual = server.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)

    # one overlapped send at a time, the way barch's session writes
    sent = 0
    data = b"x" * CHUNK
    stalled_after = None
    deadline = time.perf_counter() + SECONDS
    while time.perf_counter() < deadline:
        try:
            await asyncio.wait_for(loop.sock_sendall(server, data), timeout=0.5)
        except asyncio.TimeoutError:
            stalled_after = sent
            break
        sent += CHUNK
    for s in (server, client, listener):
        s.close()
    label = "default" if sndbuf is None else str(sndbuf)
    if stalled_after is None:
        print(f"SO_SNDBUF={label:>8} (reads back {actual}): {sent:>12,} bytes completed "
              f"in {SECONDS:g}s and still going", flush=True)
    else:
        print(f"SO_SNDBUF={label:>8} (reads back {actual}): sends stopped completing "
              f"after {stalled_after:,} bytes", flush=True)


async def main():
    print(f"{sys.platform}, {type(asyncio.get_running_loop()).__name__}", flush=True)
    for sndbuf in (None, 256 * 1024, 64 * 1024, 0):
        await probe(sndbuf)


if __name__ == "__main__":
    asyncio.run(main())
