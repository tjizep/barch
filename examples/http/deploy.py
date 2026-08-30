#!/usr/bin/env python3
"""Load the HTTP Luau functions and start Crow on this space.

    python3 deploy.py --start --demo

Then, from a browser or curl:

    curl http://127.0.0.1:18088/page
    curl -H 'Content-Type: application/json' -d '{"a":7}' http://127.0.0.1:18088/echo
"""
import argparse
import json
import os
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

HERE = os.path.dirname(os.path.abspath(__file__))
LUAU = os.path.join(HERE, "luau")
PORT = 14000
HTTP_PORT = 18088

ORDER = ("echo.luau", "page.luau", "session.luau", "conf.luau")


def read_luau(name):
    path = os.path.join(LUAU, name)
    with open(path, encoding="utf-8") as f:
        return f.read()


def connect(host, port):
    import redis
    r = redis.Redis(host=host, port=port, db=0, protocol=2)
    r.ping()
    return r


def deploy(r, http_port, bind):
    for fname in ORDER:
        name = os.path.splitext(fname)[0]
        src = read_luau(fname)
        r.execute_command("SETF", name, src)
        print(f"SETF {name.upper()}  ({len(src)} bytes)")
    started = r.execute_command("HTTP", "START", "CONF", str(http_port), bind)
    for line in started:
        if isinstance(line, bytes):
            line = line.decode()
        print(line)
    return started


def http(method, url, data=None, headers=None, timeout=3):
    hdrs = headers or {}
    body = None
    if data is not None:
        body = data if isinstance(data, bytes) else data.encode()
    req = Request(url, data=body, headers=hdrs, method=method)
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except HTTPError as e:
        return e.code, e.read(), dict(e.headers)


def demo(http_host, http_port):
    base = f"http://{http_host}:{http_port}"
    last = None
    for _ in range(20):
        try:
            last = http("GET", base + "/page")
            if last[0] == 200:
                break
        except (URLError, TimeoutError, ConnectionError):
            time.sleep(0.05)
    if not last or last[0] != 200:
        print("GET /page failed:", last)
        return False
    print("GET /page ->", last[0], last[1])
    status, body, _ = http(
        "POST", base + "/echo",
        data=json.dumps({"a": 7}),
        headers={"Content-Type": "application/json"},
    )
    print("POST /echo ->", status, body)
    if status != 200:
        return False
    got = json.loads(body)
    if got.get("ok") is not True or got.get("a") != 7:
        print("echo body was not {ok:true, a:7}:", got)
        return False
    status, _, _ = http("GET", base + "/echo")
    print("GET /echo ->", status, "(want 405)")
    return status == 405


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=PORT)
    p.add_argument("--http-port", type=int, default=HTTP_PORT)
    p.add_argument("--bind", default="127.0.0.1",
                   help="address Crow listens on")
    p.add_argument("--start", action="store_true",
                   help="start an embedded barch server on --port")
    p.add_argument("--demo", action="store_true",
                   help="GET /page and POST /echo after HTTP START")
    args = p.parse_args()

    if args.start:
        import barch
        barch.start("0.0.0.0", args.port)
        barch.ping("127.0.0.1", args.port)

    r = connect(args.host, args.port)
    deploy(r, args.http_port, args.bind)
    if args.demo:
        ok = demo(args.bind if args.bind not in ("0.0.0.0", "*") else "127.0.0.1",
                  args.http_port)
        if not ok:
            print("demo failed")
            sys.exit(1)
        print("demo ok")
    if args.start and not args.demo:
        print(f"HTTP on {args.bind}:{args.http_port}, barch on {args.port}, Ctrl+C to stop")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
