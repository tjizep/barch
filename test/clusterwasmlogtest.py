#!/usr/bin/env python3
"""
A stored WASM function run from a space's Raft log, on every member - TODO 823.

The same as clusterlogfunctiontest.py does for Luau. A call to a function that may
write is one log entry, and every member runs it when the entry commits, on a fresh
instance. So every member has to get the same result, and what follows has to hold
on a three-node cluster:
  - 400 naive read, add one, write calls, from clients on the leader and on a
    follower, leave 400 on every member;
  - a function that writes its loop count on every turn, until function_log_insns
    stops it, leaves the same count on every member. The cap counts instructions, not
    time, so every member stops at the same one;
  - barch_time and barch_uuid answer the same on every member;
  - a locked region run from the log does the same on every member;
  - http, another space and a refused command are all refused from the log, and
    nothing is written; with {"replicate": "effects"}, the other space is fine.

The modules come from wasmmodule.py. Ports are BARCH_TEST_PORT and the 19 after it.
"""
import json
import os
import struct
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402
from wasmmodule import Module, BUF, i32, i64, lget, lset, DROP, ADD, EXTEND, IF, END, LOOP  # noqa: E402
from wasmmodule import locked_region, bumps  # noqa: E402

scale.workdir()
BASE = scale.port(default=32300)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
failures = 0

I64_LOAD = b"\x29\x03\x00"
I64_STORE = b"\x37\x03\x00"
I64_ADD = b"\x7c"


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def local_get(n, key):
    """the value as this member holds it, as bytes"""
    c = redis.Redis(host="127.0.0.1", port=n.port, single_connection_client=True)
    c.execute_command("USE", SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c.execute_command("GET", key)


def as_int(v):
    return struct.unpack("<q", v)[0] if v is not None and len(v) == 8 else None


def settled(nodes):
    l = leader_of(nodes, SPACE)
    top = l.field(SPACE, "applied")
    return wait_for(30, lambda: all(n.field(SPACE, "applied") >= top for n in nodes))


def effects(m):
    m.customs.append(("barch", json.dumps({"replicate": "effects"}).encode()))
    return m


def inc():
    """arity 1: reads key arg0 as an 8 byte count, adds one, writes it back, answers it"""
    m = Module(["arg_read", "store_get", "store_set", "val_kind", "val_read", "reply_int"]).arity(1)
    m.locals = 2
    m.body = (i32(0) + i32(2000) + m.call("arg_read") + lset(0) +
              i32(2000) + lget(0) + m.call("store_get") + lset(1) +
              i32(BUF) + i64(0) + I64_STORE +
              lget(1) + m.call("val_kind") + IF + lget(1) + i32(BUF) + m.call("val_read") + DROP + END +
              i32(BUF) + i32(BUF) + I64_LOAD + i64(1) + I64_ADD + I64_STORE +
              i32(2000) + lget(0) + i32(BUF) + i32(8) + m.call("store_set") + DROP +
              i32(BUF) + I64_LOAD + m.call("reply_int") + i32(0))
    return m


def steps():
    """writes how many times round it has been to `steps` on every turn, forever"""
    m = Module(["store_set"])
    m.locals = 1
    m.body = (LOOP + lget(0) + i32(1) + ADD + lset(0) +
              i32(BUF) + lget(0) + EXTEND + I64_STORE +
              m.put("steps") + i32(BUF) + i32(8) + m.call("store_set") + DROP +
              b"\x0c\x00" + END + i32(0))
    return m


def clock_keys():
    """writes barch_time to `tm` and a uuid to `id`"""
    m = Module(["time", "uuid", "store_set", "reply_int"])
    m.body = (i32(BUF) + m.call("time") + I64_STORE +
              m.put("tm") + i32(BUF) + i32(8) + m.call("store_set") + DROP +
              i32(BUF + 64) + m.call("uuid") + DROP +
              m.put("id") + i32(BUF + 64) + i32(36) + m.call("store_set") + DROP +
              i64(1) + m.call("reply_int") + i32(0))
    return m


def then_writes(m, key, before):
    """`before`, then a write of `key`, answering 1"""
    m.body = before + m.put(key) + m.put("x") + m.call("store_set") + DROP + i64(1) + m.call("reply_int") + i32(0)
    return m


def http_then(key):
    m = Module(["http_request", "store_set", "reply_int"])
    return then_writes(m, key, i32(0) + m.put("http://127.0.0.1:1/") + m.put("") + m.put("") + i32(1000) +
                       m.call("http_request") + DROP)


def other_then(key):
    m = Module(["space_open", "store_set", "reply_int"])
    return then_writes(m, key, m.put("other") + m.call("space_open") + DROP)


def save_then(key):
    m = Module(["call", "store_set", "reply_int"])
    return then_writes(m, key, m.argv("SAVE") + m.call("call") + DROP)


FUNCTIONS = {
    "winc": inc(),
    "wsteps": steps(),
    "wclock": clock_keys(),
    "whttp": http_then("h1"),
    "wother": other_then("o1"),
    "wotherfx": effects(other_then("o2")),
    "wsave": save_then("v1"),
    "wlockinc": locked_region(bumps, region_locals=1),
}

nodes = [clusternodes.Node(i, BASE, BINARY, ["raft_snapshot_entries=200"]) for i in range(3)]
by_port = {n.port: n for n in nodes}
try:
    print("a replicated space on three nodes")
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
    c = leader.client(SPACE)
    leader.client("other").execute_command("SET", "there", "1")
    for fname, m in FUNCTIONS.items():
        c.execute_command("SETF", fname, m.build())

    print("400 naive increments from the leader and a follower")
    errors = []

    def run(node):
        target = node.client(SPACE)
        for _ in range(50):
            while True:
                try:
                    target.execute_command("winc", "n")
                    break
                except redis.exceptions.TryAgainError:
                    continue
                except redis.RedisError as e:
                    msg = str(e)
                    if msg.startswith("NOTLEADER"):
                        target = by_port[int(msg.split()[1].split(":")[1])].client(SPACE)
                        continue
                    errors.append(msg[:80])
                    break

    ts = [threading.Thread(target=run, args=(n,)) for n in [leader] * 4 + [follower] * 4]
    [t.start() for t in ts]
    [t.join() for t in ts]
    check(not errors, "every call answered (%s)" % sorted(set(errors))[:3])
    settled(nodes)
    got = [as_int(local_get(n, "n")) for n in nodes]
    check(got == [400] * 3, "every member holds 400 (%s)" % got)

    print("the cap stops every member at the same instruction")
    for n in nodes:
        n.client().execute_command("CONFIG", "SET", "function_log_insns", "200000")
    got = answer(c, "wsteps")
    check(str(got).startswith("ERR") and "function_log_insns" in str(got),
          "stops at function_log_insns (%r)" % str(got)[:100])
    settled(nodes)
    got = [as_int(local_get(n, "steps")) for n in nodes]
    check(got[0] is not None and got[0] > 1000 and len(set(got)) == 1,
          "the same count of turns on every member (%s)" % got)
    check(answer(c, "winc", "after") == 1, "and the group still takes writes")
    settled(nodes)
    check(len(set(n.digest(SPACE)[0] for n in nodes)) == 1, "every member the same")

    print("a locked region from the log")
    for _ in range(20):
        c.execute_command("wlockinc", "ln")
    settled(nodes)
    got = [as_int(local_get(n, "ln")) for n in nodes]
    check(got == [20] * 3, "every member holds 20 (%s)" % got)

    print("the clock and ids come from the log entry")
    c.execute_command("wclock")
    settled(nodes)
    got = [(local_get(n, "tm"), local_get(n, "id")) for n in nodes]
    check(len(set(got)) == 1 and as_int(got[0][0]) and len(got[0][1]) == 36,
          "the same time and uuid on every member (%s)" % [(as_int(t), i) for t, i in got[:1]])

    print("what reaches outside the data")
    for fname, what in (("whttp", "http"), ("wother", "another space"), ("wsave", "SAVE")):
        got = answer(c, fname)
        check(str(got).startswith("ERR") and "replicate" in str(got),
              "%s is refused from the log (%r)" % (what, str(got)[:90]))
    settled(nodes)
    check(all(local_get(n, k) is None for n in nodes for k in ("h1", "o1", "v1")), "and nothing was written")
    got = answer(c, "wotherfx")
    check(got == 1, "with \"replicate\": \"effects\" another space is fine (%r)" % str(got)[:100])
    settled(nodes)
    check(all(local_get(n, "o2") == b"x" for n in nodes), "and its write reaches every member")
    check(len(set(n.digest(SPACE)[0] for n in nodes)) == 1, "every member the same")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
