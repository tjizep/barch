# resp.connect: the pooled, asynchronous RESP client for stored functions - TODO 379.
#
# Most of it talks to barch's own port, which is a real RESP server for free. A small
# fake server covers what a good server never does - stall, send garbage, hang up,
# send more than was asked for, send something huge, dribble a reply a byte at a time
# - and counts its connections, so pooling can be checked from the server's side.
import socket
import socketserver
import threading
import time

import scale

import redis
import barch

scale.workdir()
PORT = scale.port(default=14260)
FAKE_PORT = scale.port(1, default=18260)
HTTP_PORT = scale.port(2, default=18261)

print("start resp client test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")


def conf(key, value):
    r.execute_command("configuration:SET", key, value)


def pool_lines():
    return [x.decode() for x in r.execute_command("RESP", "POOL")]


def totals():
    line = [l for l in pool_lines() if l.startswith("total ")][0]
    return dict(kv.split("=") for kv in line.split()[1:])


# --- the fake server -----------------------------------------------------------

accepted = [0]
seen = []            # every command the fake server received, as lists of str
seen_lock = threading.Lock()


class Fake(socketserver.StreamRequestHandler):
    def read_command(self):
        line = self.rfile.readline()
        if not line:
            return None
        assert line.startswith(b"*"), line
        words = []
        for _ in range(int(line[1:])):
            n = int(self.rfile.readline()[1:])
            words.append(self.rfile.read(n + 2)[:-2].decode())
        return words

    def send(self, data):
        self.wfile.write(data)
        self.wfile.flush()

    def handle(self):
        with seen_lock:
            accepted[0] += 1
        while True:
            try:
                cmd = self.read_command()
            except (ConnectionError, OSError, ValueError, AssertionError):
                return
            if cmd is None:
                return
            with seen_lock:
                seen.append(cmd)
            w = cmd[0].upper()
            if w in ("AUTH", "SELECT"):
                if w == "AUTH" and cmd[-1] == "wrong":
                    self.send(b"-WRONGPASS invalid username-password pair\r\n")
                else:
                    self.send(b"+OK\r\n")
            elif w == "HELLO":
                self.send(b"%1\r\n+proto\r\n:3\r\n")
            elif w == "PING":
                self.send(b"+PONG\r\n")
            elif w == "SLOW":
                time.sleep(float(cmd[1]))
                self.send(b"+late\r\n")
            elif w == "GARBAGE":
                self.send(b"?this is not resp\r\n")
            elif w == "HANGUP":
                return
            elif w == "HALF":
                self.send(b"$10\r\nhal")
                return
            elif w == "EXTRA":
                self.send(b"+OK\r\n+unasked\r\n")
            elif w == "HUGE":
                self.send(b"$1000\r\n" + b"x" * 1000 + b"\r\n")
            elif w == "DRIBBLE":
                for b in b"*3\r\n$5\r\nhello\r\n:42\r\n%1\r\n+k\r\n,1.5\r\n":
                    self.send(bytes([b]))
                    time.sleep(0.002)
            else:
                self.send(b"-ERR the fake server doesn't know " + w.encode() + b"\r\n")


class FakeServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


fake = FakeServer(("127.0.0.1", FAKE_PORT), Fake)
threading.Thread(target=fake.serve_forever, daemon=True).start()

snips = [0]


def lua(body, conn=None):
    """a function of its own each time - a session keeps what it compiled"""
    snips[0] += 1
    name = "rc%d" % snips[0]
    r.execute_command("SETF", name, "function call()\n%s\nend" % body)
    return (conn or r).execute_command(name.upper())


def fails(body, want, conn=None):
    try:
        got = lua(body, conn)
    except redis.ResponseError as e:
        assert want in str(e), (want, str(e))
        return str(e)
    raise AssertionError("should have failed with %r, got %r" % (want, got))


OWN = 'resp.connect("127.0.0.1", %d)' % PORT
FAKE = 'resp.connect("127.0.0.1", %d, {timeout = 2000})' % FAKE_PORT

try:
    print("talking to a real server", flush=True)
    assert lua('local c = %s; c:call("SET", "k", "v1"); return c:call("GET", "k")' % OWN) == b"v1"
    assert lua('return %s:call("GET", "nothing") == nil' % OWN) == 1
    assert lua('return %s:call("INCRBY", "n", 41)' % OWN) == 41
    assert lua('return %s:call("INCRBY", "n", 1.0)' % OWN) == 42  # numbers as the wire writes them
    r.execute_command("HSET", "h", "a", "1", "b", "2")
    got = lua('return %s:call("HGETALL", "h")' % OWN)
    assert sorted(got) == [b"1", b"2", b"a", b"b"], got
    # the same shapes barch.call gives
    assert lua('return %s:call("HGETALL", "h")' % OWN) == lua('return barch.call("HGETALL", "h")')
    fails('return %s:call("NOSUCHCMD")' % OWN, "unknown command")
    assert lua('local ok, e = pcall(function() return %s:call("NOSUCHCMD") end); '
               'return tostring(ok)' % OWN) == b"false"

    print("a pipeline", flush=True)
    got = lua('''local out = %s:pipeline({{"SET", "p", "1"}, {"INCR", "p"}, {"NOSUCH"}, {"GET", "p"}})
    return {out[1], out[2], out[3].err, out[4], #out}''' % OWN)
    assert got[0] == b"OK" and got[1] == 2 and b"unknown command" in got[2] \
        and got[3] == b"2" and got[4] == 4, got
    assert lua('return #%s:pipeline({})' % OWN) == 0
    # MULTI and EXEC in one pipeline are fine, and leave the connection clean
    got = lua('''local out = %s:pipeline({{"MULTI"}, {"SET", "t", "x"}, {"GET", "t"}, {"EXEC"}})
    return out[4]''' % OWN)
    assert got == [b"OK", b"x"], got

    print("RESP3", flush=True)
    got = lua('return resp.connect("127.0.0.1", %d, {protocol = 3}):call("HGETALL", "h")' % PORT)
    assert sorted(got) == [b"1", b"2", b"a", b"b"], got

    print("the pool: one connection, reused", flush=True)
    with seen_lock:
        before = accepted[0]
    for _ in range(5):
        assert lua('return %s:call("PING")' % FAKE) == b"PONG"
    with seen_lock:
        assert accepted[0] - before == 1, accepted[0] - before
    fake_pool = [l for l in pool_lines() if l.startswith("127.0.0.1:%d " % FAKE_PORT)]
    assert fake_pool and "idle=1" in fake_pool[0] and "busy=0" in fake_pool[0], pool_lines()

    print("a connection that changed its session is not pooled", flush=True)
    t0 = totals()
    with seen_lock:
        before = accepted[0]
    assert lua('return %s:call("SELECT", "3")' % FAKE) == b"OK"
    assert lua('return %s:call("PING")' % FAKE) == b"PONG"
    with seen_lock:
        assert accepted[0] - before == 1, "the SELECTed connection was reused"
    assert int(totals()["discarded"]) > int(t0["discarded"])
    # a MULTI left open is the same
    # (not returned: a function returning a table with `err` in it is an error reply)
    assert lua('local out = %s:pipeline({{"MULTI"}, {"PING"}}); return #out' % FAKE) == 2
    with seen_lock:
        before = accepted[0]
    lua('return %s:call("PING")' % FAKE)
    with seen_lock:
        assert accepted[0] - before == 1, "a connection with MULTI open was reused"

    print("credentials, handed over or read from configuration", flush=True)
    conf("cache.password", "s3cret")
    with seen_lock:
        seen.clear()
    assert lua('return resp.connect("127.0.0.1", %d, {password_from = "cache.password"}):call("PING")'
               % FAKE_PORT) == b"PONG"
    assert lua('return resp.connect("127.0.0.1", %d, {user = "app", password = "pw", db = 2}):call("PING")'
               % FAKE_PORT) == b"PONG"
    with seen_lock:
        assert ["AUTH", "s3cret"] in seen, seen
        assert ["AUTH", "app", "pw"] in seen and ["SELECT", "2"] in seen, seen
    assert lua('return resp.connect("127.0.0.1", %d, {protocol = 3, user = "u", password = "p"}):call("PING")'
               % FAKE_PORT) == b"PONG"
    with seen_lock:
        assert ["HELLO", "3", "AUTH", "u", "p"] in seen, seen
    fails('return resp.connect("127.0.0.1", %d, {password = "wrong"}):call("PING")' % FAKE_PORT,
          "refused the connection setup")
    fails('return resp.connect("127.0.0.1", %d, {password_from = "no.such.key"})' % FAKE_PORT,
          "no password at configuration key")
    # different credentials never share a connection, and no password is ever shown
    labels = "\n".join(pool_lines())
    assert "s3cret" not in labels, labels

    print("what a bad server does", flush=True)
    fails('return %s:call("GARBAGE")' % FAKE, "isn't RESP")
    fails('return %s:call("HANGUP")' % FAKE, "closed the connection")
    fails('return %s:call("HALF")' % FAKE, "closed the connection")
    t0 = totals()
    assert lua('return %s:call("EXTRA")' % FAKE) == b"OK"
    assert int(totals()["discarded"]) > int(t0["discarded"]), "leftover bytes, and pooled anyway"
    got = lua('return %s:call("DRIBBLE")' % FAKE)
    # 1.5 comes back as text only because this test reads the function's result over
    # RESP2, where barch writes a fractional number as a bulk string
    assert got == [b"hello", 42, [b"k", b"1.5"]], got
    fails('return resp.connect("127.0.0.1", 9, {connect_timeout = 500}):call("PING")',
          "could not connect")
    fails('return resp.connect("no-such-host.invalid", 6379, {connect_timeout = 2000}):call("PING")',
          "resp:")

    print("timeouts", flush=True)
    t0 = time.time()
    fails('return resp.connect("127.0.0.1", %d, {timeout = 300}):call("SLOW", "1.5")' % FAKE_PORT,
          "timed out waiting")
    assert time.time() - t0 < 1.4, time.time() - t0
    # and the next call is fine, on a fresh connection
    assert lua('return %s:call("PING")' % FAKE) == b"PONG"

    print("limits", flush=True)
    conf("resp.max_bulk_bytes", "100")
    fails('return %s:call("HUGE")' % FAKE, "larger than the limit")
    conf("resp.max_bulk_bytes", "67108864")
    assert lua('return #%s:call("HUGE")' % FAKE) == 1000

    print("what it refuses", flush=True)
    fails('return %s:call("SUBSCRIBE", "c")' % FAKE, "never stops replying")
    fails('return %s:call("MONITOR")' % FAKE, "never stops replying")
    fails('return %s:call("QUIT")' % FAKE, "pooled")
    fails('return %s:pipeline({{"PING"}, {"SUBSCRIBE", "c"}})' % FAKE, "never stops replying")
    fails('return resp.connect("127.0.0.1", 0)', "port must be")
    fails('return resp.connect("127.0.0.1", 6379, {protocol = 4})', "protocol is 2 or 3")
    fails('return resp.connect("127.0.0.1", 6379, {timeout = -1})', "must be positive")
    fails('''return barch.store.locked("x", function()
        return resp.connect("127.0.0.1", %d)
    end)''' % PORT, "not allowed inside a locked region")
    r.execute_command("ACL", "SETUSER", "noout", "on", ">pw", "+read", "+write", "+keys",
                      "+data", "+function", "+connection")
    noout = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2,
                        username="noout", password="pw")
    fails('return %s:call("PING")' % OWN, "outbound category", noout)

    print("a parked call doesn't hold its worker", flush=True)
    r.execute_command("SETF", "fast", 'function call() return "fast" end')
    slow = []

    def slow_call():
        c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
        t = time.time()
        r.execute_command("SETF", "slowone",
                          'function call() return resp.connect("127.0.0.1", %d, {timeout = 5000}):call("SLOW", "1") end'
                          % FAKE_PORT)
        slow.append((c.execute_command("SLOWONE"), time.time() - t))

    t = threading.Thread(target=slow_call)
    t.start()
    time.sleep(0.3)
    fast = 0
    t1 = time.time()
    while t.is_alive() and time.time() - t1 < 3:
        assert r.execute_command("FAST") == b"fast"
        fast += 1
    t.join(timeout=10)
    assert slow and slow[0][0] == b"late", slow
    assert fast > 5, fast
    print("  %d calls ran while one waited on the remote" % fast, flush=True)

    print("the connection cap", flush=True)
    conf("resp.max_connections", "2")
    results = []

    def capped(i):
        c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
        try:
            results.append(c.execute_command("CAPPED%d" % i))
        except redis.ResponseError as e:
            results.append(str(e))

    # a pool key of its own (db 9), so the connections already idle don't count
    # towards what these three can reuse
    for i in range(3):
        r.execute_command("SETF", "capped%d" % i,
                          'function call() return resp.connect("127.0.0.1", %d, {db = 9, timeout = 5000}):call("SLOW", "0.8") end'
                          % FAKE_PORT)
    # the idle ones from earlier still count towards the cap, so make room first
    conf("resp.idle_ms", "100")
    time.sleep(1.5)
    lua('return %s:call("PING")' % OWN)   # a connect, so the new settings are read
    time.sleep(1.0)
    threads = [threading.Thread(target=capped, args=(i,)) for i in range(3)]
    for x in threads:
        x.start()
        time.sleep(0.05)
    for x in threads:
        x.join(timeout=10)
    assert results.count(b"late") >= 1 and any("too many RESP connections" in str(x) for x in results), results
    conf("resp.max_connections", "256")

    print("idle connections expire", flush=True)
    conf("resp.idle_ms", "200")
    assert lua('return %s:call("PING")' % FAKE) == b"PONG"
    time.sleep(1.5)
    lua('return %s:call("PING")' % OWN)    # anything, to read the settings again
    time.sleep(0.5)
    fake_pool = [l for l in pool_lines() if l.startswith("127.0.0.1:%d " % FAKE_PORT)]
    assert not fake_pool or "idle=0" in fake_pool[0], pool_lines()
    conf("resp.idle_ms", "60000")

    print("from an HTTP route, which waits inline", flush=True)
    r.execute_command("ACL", "SETUSER", "web", "on", "+outbound")
    r.execute_command("SETF", "route", '''function call() return "r" end
function get(req, res)
    res.body = resp.connect("127.0.0.1", %d):call("GET", "k") or "nil"
    res.code = 200
end
function transport()
    return { kind = "resource", route = "/rc", methods = { GET = get } }
end''' % PORT)
    r.execute_command("SETF", "conf", '''function call() return "c" end
function transport()
    return { kind = "http", bind = "127.0.0.1", keys = {"ROUTE"} }
end''')
    r.execute_command("HTTP", "START", "CONF", str(HTTP_PORT), "127.0.0.1")
    try:
        import http.client
        got = None
        for _ in range(30):
            try:
                c = http.client.HTTPConnection("127.0.0.1", HTTP_PORT, timeout=10)
                c.request("GET", "/rc")
                resp = c.getresponse()
                got = (resp.status, resp.read())
                c.close()
                break
            except OSError:
                time.sleep(0.1)
        assert got == (200, b"v1"), got
    finally:
        r.execute_command("HTTP", "STOP")

    print("  " + " | ".join(pool_lines()), flush=True)
    print("resp client test complete", flush=True)
finally:
    try:
        fake.shutdown()
    except Exception:
        pass
    barch.stop()
