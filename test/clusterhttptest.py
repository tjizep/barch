#!/usr/bin/env python3
"""
HTTP on a replicated space - TODO 687.

HTTP START, STOP and STATUS were refused in a replicated space, though they only run
this node's web server. And a handler on a follower failed its first write with a
500. Now HTTP is each node's own; on a follower, a request that isn't GET or HEAD,
or a GET whose handler tried to write, is answered 307 to the same url on the
leader's web server, which the follower asks the leader for. Reads answer from the
follower's copy.

What has to hold, on a three-node cluster, each node serving the replicated space on
a port of its own:
  - HTTP START and STATUS answer in the replicated space on a follower;
  - a POST to a follower is answered 307 to the leader's web server, and followed,
    its write is on every member;
  - a GET whose handler writes is redirected the same way, and nothing was written
    on the follower;
  - a GET that only reads answers on the follower, from its copy;
  - a POST to the leader is answered there.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import http.client
import os
import sys
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=34500)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def web_port(n):
    return BASE + 16 + n.i


def request(port, method, path, body=None):
    """(status, body, location), without following a redirect"""
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    try:
        c.request(method, path, body=body, headers={"Content-Type": "text/plain"} if body else {})
        r = c.getresponse()
        return r.status, r.read().decode(), r.getheader("Location")
    finally:
        c.close()


def local(n):
    c = n.client(SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c


def settled(members):
    l = leader_of(members, SPACE)
    if l is None:
        return False
    top = l.field(SPACE, "applied")
    return wait_for(60, lambda: all(n.field(SPACE, "applied") >= top for n in members))


ROUTES = {
    "PUT": '''
function call() return "r" end
function put(req, res) barch.store.set("posted", req.body) res.body = "stored" res.code = 200 end
function service() return {kind = "resource", route = "/put", methods = {POST = put}, send = "text/plain"} end
''',
    "BUMP": '''
function call() return "r" end
function bump(req, res) barch.store.set("bumped", "1") res.body = "bumped" res.code = 200 end
function service() return {kind = "resource", route = "/bump", methods = {GET = bump}, send = "text/plain"} end
''',
    "READ": '''
function call() return "r" end
function read(req, res) res.body = barch.store.get("posted") or "none" res.code = 200 end
function service() return {kind = "resource", route = "/read", methods = {GET = read}, send = "text/plain"} end
''',
    "CONF": '''
function call() return "conf" end
function service() return {kind = "http", bind = "127.0.0.1", keys = {"PUT", "BUMP", "READ"}} end
''',
}

nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]
try:
    print("a replicated space on three nodes, each serving it")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE)
        and "learners 0" in n.group_line("cluster") for n in nodes)), "three voters and a leader")
    leader = leader_of(nodes, SPACE)
    follower = next(n for n in nodes if n is not leader)
    for name, src in ROUTES.items():
        leader.client(SPACE).execute_command("SETF", name, src)
    settled(nodes)
    started = []
    for n in nodes:
        try:
            started.append(n.client(SPACE).execute_command("HTTP", "START", "CONF", str(web_port(n)), "127.0.0.1"))
        except redis.ResponseError as e:
            started.append("ERR " + str(e))
    check(all(not str(s).startswith("ERR") for s in started), "HTTP START on every node (%s)" % [str(s)[:40] for s in started])
    status = follower.client(SPACE).execute_command("HTTP", "STATUS")
    check("port=%d" % web_port(follower) in status, "HTTP STATUS on a follower (%s)" % status[:2])

    print("a POST to a follower")
    code, body, where = request(web_port(follower), "POST", "/put?x=1", "hello")
    check(code == 307 and where == "http://127.0.0.1:%d/put?x=1" % web_port(leader),
          "307 to the leader's web server (%s %s)" % (code, where))
    u = urllib.parse.urlsplit(where or "http://127.0.0.1:1/")
    code, body, _ = request(u.port, "POST", u.path + ("?" + u.query if u.query else ""), "hello")
    check(code == 200 and body == "stored", "followed, the leader stores it (%s %r)" % (code, body))
    check(settled(nodes) and all(local(n).execute_command("GET", "posted") == "hello" for n in nodes),
          "and every member has it")

    print("a GET whose handler writes, on a follower")
    code, body, where = request(web_port(follower), "GET", "/bump")
    check(code == 307 and where == "http://127.0.0.1:%d/bump" % web_port(leader),
          "307 to the leader's web server (%s %s)" % (code, where))
    check(local(follower).execute_command("GET", "bumped") is None, "nothing was written")
    code, body, _ = request(web_port(leader), "GET", "/bump")
    check(code == 200 and settled(nodes) and all(local(n).execute_command("GET", "bumped") == "1" for n in nodes),
          "on the leader it writes, to every member")

    print("a GET that only reads, on a follower")
    code, body, _ = request(web_port(follower), "GET", "/read")
    check(code == 200 and body == "hello", "answers from the follower's copy (%s %r)" % (code, body))

    print("a POST to the leader")
    code, body, _ = request(web_port(leader), "POST", "/put", "again")
    check(code == 200 and settled(nodes) and all(local(n).execute_command("GET", "posted") == "again" for n in nodes),
          "is answered there, and reaches every member (%s)" % code)
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
