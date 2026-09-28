# An HTTP session can only be made by barch.auth - TODO 542.
#
# barch.auth kept the session as a plain key, `http:sess:<sid>` -> user name, and
# every request carrying that sid cookie ran with that user's ACL. So anyone who
# could write a key in the served space could sign in as anyone: a RESP user with
# data rights only, or a web visitor through any handler that stores a key the
# visitor names. The session is a meta key now, which no command can name.
#
# Checked, against barchd:
#   - a user with data rights only writes http:sess:forged -> default, and a
#     request with that cookie still runs as the route's default user
#   - barch.auth with the right password still signs in, and the cookie it
#     hands out keeps working on the next request
import http.client
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14542)
HTTP = PORT + 1
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "httpsession_data")

CONF = '''function call() return "http" end
function transport()
    return {kind = "http", port = %d, bind = "127.0.0.1", user = "web", keys = {"WHO", "LOGIN"}}
end''' % HTTP

WHO = '''function call() return "who" end
function who(req, res) res.body = simdjson.encode({user = barch.user()}); res.code = 200 end
function transport()
    return {kind = "resource", route = "/who", methods = {GET = who}, send = "application/json"}
end'''

LOGIN = '''function call() return "login" end
function login(req, res)
    local j = simdjson.parse(req.body)
    if barch.auth(j.user, j.pass) then
        res.body = simdjson.encode({ok = true, user = barch.user()}); res.code = 200
    else
        res.body = simdjson.encode({ok = false}); res.code = 401
    end
end
function transport()
    return {kind = "resource", route = "/login", methods = {POST = login},
            accept = "application/json", send = "application/json"}
end'''

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def start():
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA,
                          "--no-save-on-exit"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            raise AssertionError("barchd exited with %s" % p.returncode)
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % PORT)


def call(method, path, body=None, cookie=None):
    """status, parsed body, and the sid a Set-Cookie handed back"""
    headers = {"Content-Type": "application/json"}
    if cookie:
        headers["Cookie"] = cookie
    for _ in range(50):
        try:
            c = http.client.HTTPConnection("127.0.0.1", HTTP, timeout=10)
            c.request(method, path, body=body, headers=headers)
            r = c.getresponse()
            raw = r.read()
            sid = None
            for k, v in r.getheaders():
                if k.lower() == "set-cookie" and v.startswith("sid="):
                    sid = v.split(";")[0]
            return r.status, json.loads(raw) if raw else None, sid
        except OSError:
            time.sleep(0.1)
    raise AssertionError("no answer on the HTTP port")


shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)
server = start()
try:
    admin = redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=30)
    for name, src in (("who", WHO), ("login", LOGIN), ("httpconf", CONF)):
        admin.execute_command("SETF", name, src)
    try:
        admin.execute_command("HTTP", "START", "HTTPCONF", str(HTTP), "127.0.0.1")
    except redis.exceptions.ResponseError as e:
        print("SKIP: no HTTP server in this build (%s)" % e)
        sys.exit(0)
    admin.execute_command("ACL", "SETUSER", "writer", "on", ">pw", "+read", "+write", "+keys", "+data")
    admin.execute_command("ACL", "SETUSER", "alice", "on", ">alicepw", "+read")

    print("a key a client writes is not a session", flush=True)
    status, body, _ = call("GET", "/who")
    check(status == 200 and body["user"] == "web", "a request with no cookie runs as web (%s)" % body)
    writer = redis.Redis(host="127.0.0.1", port=PORT, protocol=2, username="writer", password="pw")
    try:
        writer.execute_command("ACL", "LIST")
        admin_rights = True
    except redis.exceptions.ResponseError:
        admin_rights = False
    check(not admin_rights, "the writer may not run ACL LIST")
    check(writer.set("http:sess:forged", "default") is True, "the writer can SET http:sess:forged")
    status, body, _ = call("GET", "/who", cookie="sid=forged")
    check(status == 200 and body["user"] == "web",
          "a request with that cookie still runs as web, not default (%s)" % body)

    print("barch.auth still signs in", flush=True)
    status, body, sid = call("POST", "/login", body=json.dumps({"user": "alice", "pass": "wrong"}))
    check(status == 401, "a wrong password is refused (%s)" % status)
    status, body, sid = call("POST", "/login", body=json.dumps({"user": "alice", "pass": "alicepw"}))
    check(status == 200 and body["user"] == "alice" and sid, "the right one signs in as alice (%s, %s)" % (body, sid))
    status, body, _ = call("GET", "/who", cookie=sid)
    check(status == 200 and body["user"] == "alice", "and its cookie is alice on the next request (%s)" % body)
    keys = [k.decode(errors="replace") for k in admin.execute_command("KEYS", "*")]
    check(not any(k.startswith("http:sess:") and k != "http:sess:forged" for k in keys),
          "the session isn't a key a client can see (%s)" % [k for k in keys if "sess" in k])
finally:
    server.send_signal(signal.SIGKILL)
    server.wait(timeout=30)

print("\n%s" % ("http session checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
