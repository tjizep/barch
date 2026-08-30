# Crow HTTP for stored Luau: transport(), GET html, POST json via simdjson.
import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import time

import redis
import barch

PORT = 14088
HTTP_PORT = 18088
BASE = f"http://127.0.0.1:{HTTP_PORT}"

print("start http luau test")
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


def http(method, path, data=None, headers=None, timeout=3):
    hdrs = headers or {}
    body = None
    if data is not None:
        body = data if isinstance(data, bytes) else data.encode()
    req = Request(BASE + path, data=body, headers=hdrs, method=method)
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except HTTPError as e:
        return e.code, e.read(), dict(e.headers)


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
        keys = {"ECHO", "PAGE"},
    }
end
'''

PLAIN = r'''
function call()
    return "plain"
end
'''

try:
    assert r.execute_command("SETF", "echo", ECHO) == b"OK"
    assert r.execute_command("SETF", "page", PAGE) == b"OK"
    assert r.execute_command("SETF", "httpconf", CONF) == b"OK"
    assert r.execute_command("SETF", "plain", PLAIN) == b"OK"
    # no transport: still an ordinary function
    assert r.execute_command("plain") == b"plain"

    e = refused("HTTP", "START", "ECHO")
    assert e and "resource" in e.lower(), e

    started = r.execute_command("HTTP", "START", "HTTPCONF", str(HTTP_PORT), "127.0.0.1")
    text = b" ".join(started).decode()
    assert "port=" + str(HTTP_PORT) in text, started
    assert "ECHO /echo POST" in text, started
    assert "PAGE /page GET" in text, started
    assert "PLAIN" not in text, started

    # Crow bind can take a beat after START returns
    last = None
    for _ in range(20):
        try:
            last = http("GET", "/page")
            if last[0] == 200:
                break
        except (URLError, TimeoutError, ConnectionError):
            time.sleep(0.05)
    assert last and last[0] == 200, last
    assert b"hello" in last[1], last
    assert "text/html" in last[2].get("Content-Type", last[2].get("content-type", "")), last[2]
    assert last[2].get("Access-Control-Allow-Origin", last[2].get("access-control-allow-origin")) == "*", last[2]

    status, body, hdrs = http(
        "POST", "/echo",
        data=json.dumps({"a": 7}),
        headers={"Content-Type": "application/json"},
    )
    assert status == 200, (status, body)
    got = json.loads(body)
    assert got["ok"] is True and got["a"] == 7, got
    assert "application/json" in hdrs.get("Content-Type", hdrs.get("content-type", "")), hdrs

    status, _, _ = http("GET", "/echo")
    assert status == 405, status

    st = r.execute_command("HTTP", "STATUS")
    assert any(b"ECHO" in x for x in st), st

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
