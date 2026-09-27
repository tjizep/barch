import select
import socket
import time

import scale
import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

# Guards omem and oll in CLIENT LIST - TODO 492.
#
# They used to be a fixed "obl=0 oll=0 omem=0", so a client whose replies were backing up
# looked exactly like an idle one. Now omem is the reply bytes waiting to go out to that
# client - the send queue and a streamed KEYS reply - and oll is how many buffers they're
# in. obl stays 0: there is no fixed reply buffer here for it to describe.
#
# Checked here: a client pipelining GETs without reading, and a client that asked for
# KEYS * without reading, both show a large omem and a non-zero oll, while an idle client
# shows 0 for both; tot-mem is at least omem; and once the two read everything, they're
# back to 0.

PORT = scale.port(default=14000)
VALUE = b"x" * 8192
KEY = "g:" + "k" * 1000
KEYS = scale.scaled(60000, floor=30000)
PAD = "p" * 150


def bulk(b: bytes) -> bytes:
    return b"$%d\r\n%s\r\n" % (len(b), b)


def command(*args: bytes) -> bytes:
    return b"*%d\r\n" % len(args) + b"".join(bulk(a) for a in args)


def addr(s: socket.socket) -> str:
    host, port = s.getsockname()
    return f"{host}:{port}"


def clients(r) -> dict:
    return {c["addr"]: c for c in r.client_list()}


def drain(s: socket.socket) -> int:
    """read until the server has nothing more to say for a second"""
    s.setblocking(False)
    got = 0
    while True:
        ready, _, _ = select.select([s], [], [], 1.0)
        if not ready:
            return got
        try:
            chunk = s.recv(1 << 20)
        except BlockingIOError:
            continue
        if not chunk:
            return got
        got += len(chunk)


print("start client omem test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
piper = stalled = None
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
    r.execute_command("CLEARALL")
    r.flushdb()
    r.set(KEY, VALUE)
    for start in range(0, KEYS, 5000):
        r.mset({f"k:{i:07d}:{PAD}": "v" for i in range(start, min(start + 5000, KEYS))})
    idle = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
    idle.ping()

    # --- a client pipelining GETs and reading nothing ------------------------------
    piper = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    piper.connect(("127.0.0.1", PORT))
    piper.setblocking(False)
    requests = command(b"GET", KEY.encode()) * 20000
    sent = 0
    while sent < len(requests):
        try:
            sent += piper.send(requests[sent:sent + (1 << 20)])
        except BlockingIOError:
            _, writable, _ = select.select([], [piper], [], 2.0)
            if not writable:
                break

    # --- a client that asked for every key and reads nothing -----------------------
    stalled = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    stalled.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    stalled.connect(("127.0.0.1", PORT))
    stalled.sendall(command(b"KEYS", b"*"))
    time.sleep(1.0)

    seen = clients(r)
    p, k = seen[addr(piper)], seen[addr(stalled)]
    print(f"pipelining client: obl={p['obl']} oll={p['oll']} omem={p['omem']} tot-mem={p['tot-mem']}")
    print(f"stalled KEYS client: obl={k['obl']} oll={k['oll']} omem={k['omem']} tot-mem={k['tot-mem']}")
    assert int(p["omem"]) >= 512 * 1024, f"the pipelining client shows omem={p['omem']}"
    assert int(p["oll"]) > 0, f"the pipelining client shows oll={p['oll']}"
    assert int(k["omem"]) >= 1 << 20, f"the stalled KEYS client shows omem={k['omem']}"
    assert int(k["oll"]) > 0, f"the stalled KEYS client shows oll={k['oll']}"
    for c in (p, k):
        assert int(c["tot-mem"]) >= int(c["omem"]), f"tot-mem {c['tot-mem']} < omem {c['omem']}"
        assert c["obl"] == "0", f"obl={c['obl']}"
    quiet = [c for a, c in seen.items() if a not in (addr(piper), addr(stalled))]
    assert quiet, "no idle connection in CLIENT LIST"
    for c in quiet:
        assert c["omem"] == "0" and c["oll"] == "0", (
            f"an idle connection shows oll={c['oll']} omem={c['omem']}")

    # --- once they've read everything, back to nothing -----------------------------
    got_p = drain(piper)
    got_k = drain(stalled)
    print(f"drained {got_p} and {got_k} bytes")
    seen = clients(r)
    for s, what in ((piper, "pipelining"), (stalled, "KEYS")):
        c = seen[addr(s)]
        assert c["omem"] == "0" and c["oll"] == "0", (
            f"the {what} client still shows oll={c['oll']} omem={c['omem']} after reading "
            f"everything")
    idle.close()
    r.close()
finally:
    for s in (piper, stalled):
        if s is not None:
            s.close()
    barch.stop()
print("complete client omem test")
