#!/usr/bin/env python3
"""
Websocket routes on a replicated space - TODO 902.

What has to hold, on a three-node cluster, each node serving the space on a port of
its own:
  - HTTP START takes a websocket route in a replicated space;
  - on the leader, a writing route's events run from the log, so each event's write
    is on every member, and the leader's run answers the socket;
  - on a follower, the same route's socket is closed at once with 1013, the reason
    naming the leader's socket, and the function sees nothing of it;
  - a read-only route answers on a follower, from its copy.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402
from wsclient import WS  # noqa: E402

scale.workdir()
BASE = scale.port(default=18820)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "chat"
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def web_port(n):
    return BASE + 16 + n.i


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
    # may write, so it runs from the log on every member
    "LOGGED": '''
function call(event, id, data)
    barch.call('RPUSH', 'events', event .. ':' .. data)
    barch.call('SET', 'last', event .. ':' .. data)
    if event == "message" then return "ok:" .. data end
end
function service() return {kind = "websocket", route = "/log", user = "default"} end
''',
    "READER": '''--@barch {"write": false}
function call(event, id, data)
    -- barch.call reads are the leader's unless the caller asks for follower reads;
    -- barch.store reads this member's copy
    if event == "message" then return barch.store.get('last') end
end
function service() return {kind = "websocket", route = "/read", user = "default"} end
''',
    "CONF": '''
function call() return "conf" end
function service() return {kind = "http", bind = "127.0.0.1", keys = {"LOGGED", "READER"}} end
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
    check(all(not str(s).startswith("ERR") for s in started),
          "HTTP START on every node (%s)" % [str(s)[:60] for s in started])

    print("a writing route on the leader")
    a = WS(web_port(leader), "/log")
    check(a.status.startswith(b"HTTP/1.1 101"), "the handshake upgrades (%s)" % a.status)
    a.send("one")
    check(a.recv() == (1, b"ok:one"), "a message is answered by the leader's run")
    a.send("two")
    check(a.recv() == (1, b"ok:two"), "and the next")
    a.close()
    check(a.recv()[0] == 8, "a close is answered")
    a.end()
    # close is the last event, so once it's on the leader every event is in the log
    check(wait_for(60, lambda: local(leader).execute_command("LRANGE", "events", -1, -1) == ["close:bye"]),
          "close reaches the function")
    want = ["open:127.0.0.1", "message:one", "message:two", "close:bye"]
    check(settled(nodes) and all(local(n).execute_command("LRANGE", "events", 0, -1) == want for n in nodes),
          "every member ran each event once, in order")

    print("the same route on a follower")
    f = WS(web_port(follower), "/log")
    check(f.status.startswith(b"HTTP/1.1 101"), "the handshake upgrades")
    check(f.recv() == (8, 1013), "and the socket closes with 1013")
    check(f.reason == "leader at ws://127.0.0.1:%d/log" % web_port(leader), "naming the leader (%r)" % f.reason)
    f.end()
    check(settled(nodes) and local(leader).execute_command("LLEN", "events") == len(want),
          "the function saw nothing of it")

    print("a read-only route on a follower")
    r = WS(web_port(follower), "/read")
    r.send("list")
    got = r.recv()
    check(got == (1, b"close:bye"), "answers from the follower's copy (%s %r)" % (got, r.reason))
    if got[0] != 8:
        r.close()
        r.recv()
    r.end()
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
