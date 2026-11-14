# The websocket transport - TODO 901.
#
# A `kind = "websocket"` route hands each socket event to a stored function as
# call(event, id, data). Here one route is Luau and one is WASM. A string the
# function returns goes back as a frame, an array as a frame per item. A failed
# call closes the socket with 1011, and close reaches the function last.
#
# Nothing here waits on time. A close is seen through BLPOP on a list the close
# event writes, so the test waits for the event itself.
import threading

import scale
import redis
import barch
import wasmmodule as wm
from wsclient import WS as _WS

scale.workdir()
PORT = scale.port(default=14610)
HTTP_PORT = scale.port(1, default=14611)

print("start websocket test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("USE", "wst")

failures = []


def check(ok, what):
    print("  %-70s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures.append(what)


def refused(*args):
    try:
        r.execute_command(*args)
    except redis.ResponseError as e:
        return str(e)
    return None


def WS(path):
    return _WS(HTTP_PORT, path)


LUAU = '''
function call(event, id, data)
    if event == "message" and data == "boom" then error("boom") end
    barch.call('RPUSH', 'ev:' .. id, event .. ':' .. data)
    if event == "close" then barch.call('RPUSH', 'closed', id) return end
    if event == "open" then return {"open:" .. data, id} end
    if data == "two" then return {"a", "b"} end
    if data == "quiet" then return nil end
    return "echo:" .. data
end
function service() return {kind = "websocket", route = "chat", user = "default", max_payload = 1024} end
'''

# a build without ADD_WASM runs the Luau half only
probe = refused("SETF", "WPROBE", wm.echo().build())
HAS_WASM = probe is None
if not HAS_WASM:
    print("no WASM in this build (%s): the Luau route only" % probe)

assert r.execute_command("SETF", "WS", LUAU) == b"OK"
if HAS_WASM:
    assert r.execute_command("SETF", "WSW", wm.transport(wm.ws_logger(), {
        "kind": "websocket", "route": "/wasm", "user": "default"}).build()) == b"OK"
assert r.execute_command("SETF", "CONF", '''
function call() return "conf" end
function service() return {kind = "http", bind = "127.0.0.1", keys = {"WS", "WSW"}} end
''') == b"OK"


def stop_http():
    """HTTP STOP on a connection of its own, for a thread"""
    own = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
    own.execute_command("USE", "wst")
    own.execute_command("HTTP", "STOP")
    own.close()


def wasm_refusals():
    for spec, what in (({"kind": "websocket"}, "needs a route"),
                       ({"kind": "websocket", "route": "/x/{id}"}, "{holes}"),
                       ({"kind": "websocket", "route": "/x", "methods": {"GET": "call"}}, "no methods"),
                       ({"kind": "websocket", "route": "/x", "max_payload": 0}, "max_payload")):
        e = refused("SETF", "WBAD", wm.transport(wm.echo(), spec).build())
        check(e is not None and what in e, "%s: %s" % (spec, e))
    e = refused("SETF", "WBAD", wm.transport(wm.echo(), {"kind": "http"}).build())
    check(e is not None and "resp, cron, queue or websocket" in e, "kind=http is still refused: %s" % e)


if HAS_WASM:
    print("SETF refuses a WASM websocket transport it can't serve")
    wasm_refusals()

try:
    lines = r.execute_command("HTTP", "START", "CONF", str(HTTP_PORT), "127.0.0.1")
    lines = [x.decode() for x in lines]
    check("WS /chat websocket call=WS" in lines, "START lists the Luau route: %s" % lines)
    if HAS_WASM:
        check("WSW /wasm websocket call=WSW" in lines, "START lists the WASM route")
    status = [x.decode() for x in r.execute_command("HTTP", "STATUS")]
    check("WS /chat websocket call=WS" in status, "and so does STATUS")

    print("a Luau route")
    a = WS("/chat")
    check(a.status.startswith(b"HTTP/1.1 101"), "the handshake upgrades: %s" % a.status)
    check(a.recv() == (1, b"open:127.0.0.1"), "open gets the peer's address")
    op, sid = a.recv()
    check(op == 1 and sid.isdigit(), "an array answers a frame per item: id %s" % sid)
    sid = sid.decode()
    a.send("hi")
    check(a.recv() == (1, b"echo:hi"), "a message is answered")
    a.send(b"\x00\x01bin", opcode=2)
    check(a.recv() == (2, b"echo:\x00\x01bin"), "a binary message is answered in binary")
    a.send("quiet")
    a.send("two")
    check(a.recv() == (1, b"a") and a.recv() == (1, b"b"), "nil sends nothing; {a, b} sends two")
    for i in range(50):
        a.send("n%d" % i)
    got = [a.recv() for _ in range(50)]
    check(got == [(1, ("echo:n%d" % i).encode()) for i in range(50)], "fifty in a row come back in order")
    a.close()
    check(a.recv()[0] == 8, "a close is answered with a close")
    a.end()
    closed = r.blpop("closed", 30)
    check(closed == (b"closed", sid.encode()), "close reaches the function: %s" % (closed,))
    log = [x.decode() for x in r.lrange("ev:" + sid, 0, -1)]
    want = (["open:127.0.0.1", "message:hi", "message:\x00\x01bin", "message:quiet", "message:two"] +
            ["message:n%d" % i for i in range(50)])
    check(log[:-1] == want, "the function saw every event, in order")
    check(len(log) == len(want) + 1 and log[-1].startswith("close:"), "and close last: %s" % log[-1:])

    print("a handler that fails")
    b = WS("/chat")
    b.recv()
    op, bid = b.recv()
    b.send("boom")
    check(b.recv() == (8, 1011), "a failed call closes the socket with 1011")
    b.end()
    check(r.blpop("closed", 30) == (b"closed", bid), "and still reaches the function's close")

    print("a frame over max_payload")
    c = WS("/chat")
    c.recv()
    op, cid = c.recv()
    c.send("x" * 2000)
    check(c.recv() == (None, None), "Crow drops the socket, with no close frame")
    c.end()
    check(r.blpop("closed", 30) == (b"closed", cid), "and close reaches the function")

    if HAS_WASM:
        print("a WASM route")
        w = WS("/wasm")
        check(w.status.startswith(b"HTTP/1.1 101"), "the handshake upgrades")
        check(w.recv() == (1, b"127.0.0.1"), "open answers the address it was given")
        w.send("hello wasm")
        check(w.recv() == (1, b"hello wasm"), "a message is answered")
        w.close()
        w.recv()
        w.end()
        check(r.blpop("close", 30) is not None, "close reaches the module")
        check(r.lrange("message", 0, -1) == [b"hello wasm"], "the module saw the message")
        check(r.lrange("open", 0, -1) == [b"127.0.0.1"], "and the open")

    print("STOP with a socket open")
    d = WS("/chat")
    d.recv()
    d.recv()
    # STOP waits for the peer to answer its close frame, so it runs beside us
    stopper = threading.Thread(target=stop_http)
    stopper.start()
    op, code = d.recv()
    check((op, code) == (8, 1001), "STOP sends a close with 1001: %s %s" % (op, code))
    d.close()
    check(d.recv() == (None, None), "and drops the socket once it's answered")
    d.end()
    stopper.join()
    check(r.execute_command("HTTP", "STATUS") == b"stopped", "and the server stops")

    print("START again")
    r.execute_command("HTTP", "START", "CONF", str(HTTP_PORT), "127.0.0.1")
    e2 = WS("/chat")
    check(e2.recv() == (1, b"open:127.0.0.1"), "a new socket opens")
    e2.end()
finally:
    try:
        r.execute_command("HTTP", "STOP")
    except Exception:
        pass
    barch.stop()

print()
if failures:
    print("FAILED: %d" % len(failures))
    for f in failures:
        print("  " + f)
    raise SystemExit(1)
print("websocket test passed")
