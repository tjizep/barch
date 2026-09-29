# service() declares a service, the same as transport() - TODO 557.
#
# The stored function that says what a key is - a RESP command set, a cron
# schedule, a queue, an HTTP server, or a route mounted on one - used to be
# read only from transport(). service() is the name now, and transport() still
# works, since every declaration already stored has it. When a function has
# both, service() is the one read.
import http.client
import time

import scale
import redis
import barch

scale.workdir()
PORT = scale.port(default=14560)
HTTP_PORT = scale.port(1, default=14561)

print("start service test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")

failures = []


def check(ok, what):
    print("  %-70s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures.append(what)


def wait_until(pred, timeout=15.0, step=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(step)
    return pred()


def setf(cmd, key, src):
    """SETF, answering the error rather than raising it, so one kind that
    isn't picked up doesn't hide the rest"""
    try:
        return r.execute_command(cmd, key, src)
    except redis.ResponseError as e:
        return str(e)


def get(path):
    conn = http.client.HTTPConnection("127.0.0.1", HTTP_PORT, timeout=15)
    try:
        conn.request("GET", path, headers={"Connection": "close"})
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


try:
    print("resp")
    got = setf("SETF", "svcresp", '''
        function call() return "svcresp" end
        function hello(k) return "hello:" .. k end
        function service() return {kind = "resp", methods = {SVCHELLO = hello}} end
    ''')
    check(got == b"OK", "SETF svcresp takes it: %r" % got)
    try:
        got = r.execute_command("SVCHELLO", "k")
    except redis.ResponseError as e:
        got = str(e)
    check(got == b"hello:k", "a resp service() exposes its methods: %r" % got)

    print("cron")
    assert r.execute_command("SETF", "SVCTICK", '''
        function call()
            local n = tonumber(barch.store.get("svcticks") or "0") or 0
            barch.store.set("svcticks", tostring(n + 1))
        end
    ''') == b"OK"
    got = setf("configuration:SETF", "cron/jobs/svcticker", '''
        function service()
            return {kind = "cron", space = "default", call = "SVCTICK",
                    every = "200ms", user = "default"}
        end
    ''')
    check(got == b"OK", "SETF cron/jobs/svcticker takes it: %r" % got)
    check("name=svcticker" in r.execute_command("FUNCTIONS", "CRON").decode(),
          "a cron service() is listed")
    check(wait_until(lambda: r.get("svcticks") is not None),
          "and fires")

    print("queue")
    got = setf("configuration:SETF", "queues/svcq", '''
        function service()
            return {kind = "queue", name = "svcq", space = "default",
                    call = "SVCTICK", user = "default", durability = "each",
                    poll = "1h", enabled = false}
        end
    ''')
    check(got == b"OK", "SETF queues/svcq takes it: %r" % got)
    check(" name=svcq " in r.execute_command("QUEUE", "STATUS").decode() + " ",
          "a queue service() is declared")

    print("http, with a resource and a files route")
    assert r.execute_command("SETF", "fsput", '''
        function call(path, ctype, content) return barch.fs.put(path, content, ctype, 65536) end
    ''') == b"OK"
    r.execute_command("fsput", "/svc/a.txt", "text/plain", "from files")
    got = setf("SETF", "svcping", '''
        function call() return "svcping" end
        function hit(req, res) res.body = "pong" res.code = 200 end
        function service()
            return {kind = "resource", route = "/svcping", methods = {GET = hit},
                    send = "text/plain"}
        end
    ''')
    check(got == b"OK", "SETF svcping takes it: %r" % got)
    got = setf("SETF", "svcfiles", '''
        function call() return "svcfiles" end
        function service() return {kind = "files", route = "/svc/*", root = "/svc"} end
    ''')
    check(got == b"OK", "SETF svcfiles takes it: %r" % got)
    got = setf("SETF", "svcconf", '''
        function call() return "svcconf" end
        function service()
            return {kind = "http", bind = "127.0.0.1", keys = {"SVCPING", "SVCFILES"}}
        end
    ''')
    check(got == b"OK", "SETF svcconf takes it: %r" % got)
    try:
        started = r.execute_command("HTTP", "START", "SVCCONF", str(HTTP_PORT), "127.0.0.1")
    except redis.ResponseError as e:
        started = str(e)
    check(isinstance(started, list), "an http service() starts: %r" % (started,))
    if isinstance(started, list):
        check(get("/svcping") == (200, b"pong"), "its resource route answers")
        check(get("/svc/a.txt") == (200, b"from files"), "and its files route")
        r.execute_command("HTTP", "STOP")

    print("both names")
    got = setf("configuration:SETF", "queues/svcboth", '''
        function service()
            return {kind = "queue", name = "fromservice", space = "default",
                    call = "SVCTICK", user = "default", enabled = false}
        end
        function transport()
            return {kind = "queue", name = "fromtransport", space = "default",
                    call = "SVCTICK", user = "default", enabled = false}
        end
    ''')
    check(got == b"OK", "SETF queues/svcboth takes it: %r" % got)
    st = r.execute_command("QUEUE", "STATUS").decode() + " "
    check(" name=fromservice " in st and " name=fromtransport " not in st,
          "with both, service() is the one read")

    print("the old name")
    assert r.execute_command("SETF", "oldresp", '''
        function call() return "oldresp" end
        function hello(k) return "old:" .. k end
        function transport() return {kind = "resp", methods = {OLDHELLO = hello}} end
    ''') == b"OK"
    check(r.execute_command("OLDHELLO", "k") == b"old:k", "transport() still works")
finally:
    try:
        r.execute_command("HTTP", "STOP")
    except Exception:
        pass
    barch.stop()

print()
if failures:
    print("FAILURES: %d" % len(failures))
    for f in failures:
        print("  " + f)
    raise SystemExit(1)
print("all service checks pass")
