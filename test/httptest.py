# Crow HTTP for stored Luau: transport(), GET html, POST json via simdjson.
# Concurrent ingress hammers store.locked against overlapping RESP writes.
import http.client
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

import redis
import barch

PORT = 14088
HTTP_PORT = 18088

print("start http luau test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")


def refused(*args):
    try:
        r.execute_command(*args)
        return None
    except redis.exceptions.ResponseError as e:
        return str(e)


def http_call(method, path, data=None, headers=None, timeout=3):
    hdrs = dict(headers or {})
    body = None
    if data is not None:
        body = data if isinstance(data, bytes) else data.encode()
        hdrs.setdefault("Content-Length", str(len(body)))
    hdrs.setdefault("Connection", "close")
    conn = http.client.HTTPConnection("127.0.0.1", HTTP_PORT, timeout=timeout)
    try:
        conn.request(method, path, body=body, headers=hdrs)
        resp = conn.getresponse()
        data_out = resp.read()
        return resp.status, data_out, dict(resp.getheaders())
    finally:
        conn.close()


ECHO = r'''
function call()
    return "echo"
end

function echo(req, res)
    local j = simdjson.parse(req.body)
    res.body = simdjson.encode({ok = true, a = j.a})
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/echo",
        methods = {POST = echo},
        accept = "application/json",
        send = "application/json",
        cors = "*",
    }
end
'''

PAGE = r'''
function call()
    return "page"
end

function getpage(req, res)
    res.body = "<html><body>hello</body></html>"
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/page",
        methods = {GET = getpage},
        send = "text/html",
        cors = "*",
    }
end
'''

CONF = r'''
function call()
    return "http"
end

function transport()
    return {
        kind = "http",
        port = 18088,
        bind = "127.0.0.1",
        keys = {"ECHO", "PAGE", "SESS", "SLOW"},
    }
end
'''

# a handler that stays busy long enough for STATUS to catch it running - TODO 181.
# No clock in the sandbox, so the wait is a loop; the count is well inside the
# function deadline but takes a few hundred ms.
SLOW = r'''
function call()
    return "slow"
end

function spin(req, res)
    local x = 0.0
    for i = 1, 12000000 do
        x = x + i % 7
    end
    res.body = tostring(x)
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/slow",
        methods = {GET = spin},
        send = "text/plain",
    }
end
'''

PLAIN = r'''
function call()
    return "plain"
end
'''

SESS = r'''
function call()
    return "sess"
end

local function newsid()
    local t = {}
    for i = 1, 16 do
        t[i] = string.format("%02x", math.random(0, 255))
    end
    return table.concat(t)
end

function hits(req, res)
    local id = req:cookie("sid")
    if not id or id == "" then
        id = newsid()
        res:cookie("sid", id, {path = "/", httponly = true})
    end
    local key = "sess:" .. id
    local n
    barch.store.locked(key, function()
        n = tonumber(barch.store.get(key)) or 0
        n = n + 1
        barch.store.set(key, tostring(n))
    end)
    res.body = simdjson.encode({sid = id, n = n})
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/sess",
        methods = {GET = hits},
        send = "application/json",
    }
end
'''



try:
    assert r.execute_command("SETF", "echo", ECHO) == b"OK"
    assert r.execute_command("SETF", "page", PAGE) == b"OK"
    assert r.execute_command("SETF", "httpconf", CONF) == b"OK"
    assert r.execute_command("SETF", "plain", PLAIN) == b"OK"
    assert r.execute_command("SETF", "sess", SESS) == b"OK"
    assert r.execute_command("SETF", "slow", SLOW) == b"OK"
    # no transport: still an ordinary function
    assert r.execute_command("plain") == b"plain"

    e = refused("HTTP", "START", "ECHO")
    assert e and "resource" in e.lower(), e

    started = r.execute_command("HTTP", "START", "HTTPCONF", str(HTTP_PORT), "127.0.0.1")
    text = b" ".join(started).decode()
    assert "port=" + str(HTTP_PORT) in text, started
    assert "ECHO /echo POST" in text, started
    assert "PAGE /page GET" in text, started
    assert "SESS /sess GET" in text, started
    assert "SLOW /slow GET" in text, started
    assert "PLAIN" not in text, started

    # Crow bind can take a beat after START returns
    last = None
    for _ in range(20):
        try:
            last = http_call("GET", "/page")
            if last[0] == 200:
                break
        except (TimeoutError, ConnectionError, OSError):
            time.sleep(0.05)
    assert last and last[0] == 200, last
    assert b"hello" in last[1], last
    assert "text/html" in last[2].get("Content-Type", last[2].get("content-type", "")), last[2]
    assert last[2].get("Access-Control-Allow-Origin", last[2].get("access-control-allow-origin")) == "*", last[2]

    status, body, hdrs = http_call(
        "POST", "/echo",
        data=json.dumps({"a": 7}),
        headers={"Content-Type": "application/json"},
    )
    assert status == 200, (status, body)
    got = json.loads(body)
    assert got["ok"] is True and got["a"] == 7, got
    assert "application/json" in hdrs.get("Content-Type", hdrs.get("content-type", "")), hdrs

    status, _, _ = http_call("GET", "/echo")
    assert status == 405, status

    status, body, hdrs = http_call("GET", "/sess", timeout=10)
    assert status == 200, (status, body)
    first = json.loads(body)
    assert first["n"] == 1, first
    sid = None
    for k, v in hdrs.items():
        if k.lower() == "set-cookie" and v.startswith("sid="):
            sid = v.split(";")[0]
            break
    assert sid, hdrs
    status, body, _ = http_call("GET", "/sess", headers={"Cookie": sid}, timeout=10)
    assert status == 200, (status, body)
    second = json.loads(body)
    assert second["n"] == 2 and second["sid"] == first["sid"], second

    st = r.execute_command("HTTP", "STATUS")
    assert any(b"ECHO" in x for x in st), st

    # TODO 181: the pool counts and the Luau bytes those VMs hold
    def status_fields():
        out = {}
        for line in r.execute_command("HTTP", "STATUS"):
            text = line.decode()
            if "=" in text:
                k, _, v = text.partition("=")
                out[k] = v
        return out

    fields = status_fields()
    vms = int(fields["vms"])
    assert 2 <= vms <= 8, fields
    assert int(fields["idle"]) == vms and int(fields["executing"]) == 0, fields
    # every VM in the pool has a state with the same functions compiled in, so the
    # figure is well past a single state's ~50kB but nothing like a gigabyte
    idle_bytes = int(fields["luau_bytes"])
    assert idle_bytes > 50 * 1024, fields

    # and the executing count moves while a handler is actually in flight
    with ThreadPoolExecutor(max_workers=1) as spinner:
        fut = spinner.submit(http_call, "GET", "/slow", None, None, 20)
        seen = 0
        peak = 0
        for _ in range(400):
            f = status_fields()
            peak = max(peak, int(f["executing"]))
            if int(f["executing"]) >= 1:
                seen += 1
                assert int(f["idle"]) == vms - int(f["executing"]), f
                break
            if fut.done():
                break
            time.sleep(0.005)
        status, body, _ = fut.result()
    assert status == 200, (status, body)
    assert seen == 1, f"never caught the slow handler running, peak={peak}"
    after = status_fields()
    assert int(after["executing"]) == 0 and int(after["idle"]) == vms, after

    workers = 8
    per_worker = 20
    print(f"concurrent ingress {workers} threads x {per_worker}", flush=True)

    def http_worker(i):
        rc = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
        for n in range(per_worker):
            status, page, _ = http_call("GET", "/page", timeout=10)
            assert status == 200 and b"hello" in page, (status, page)
            status, body, _ = http_call(
                "POST", "/echo",
                data=json.dumps({"a": i * 100 + n}),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            assert status == 200, (status, body)
            got = json.loads(body)
            assert got["ok"] is True and got["a"] == i * 100 + n, got
            rc.set(f"noise-{i}-{n}", n)
            assert rc.get(f"noise-{i}-{n}") == str(n).encode()
        return True

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(http_worker, i) for i in range(workers)]
        for f in as_completed(futs):
            f.result()

    assert r.execute_command("HTTP", "STOP") == b"OK"
    assert r.execute_command("HTTP", "STATUS") == b"stopped"
    print("complete http luau test")
finally:
    try:
        r.execute_command("HTTP", "STOP")
    except Exception:
        pass
    try:
        barch.stop()
    except Exception:
        pass
