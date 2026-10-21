#!/usr/bin/env python3
"""
A script's barch.store goes through the session's transaction - TODO 707.

barch.store reached the space through an access built once per connection, so a
script in a transaction wrote straight past it: its writes landed at once and its reads
weren't checked. Now, while the session's transaction is open, barch.store's reads and
writes go to the transaction's overlay, its reads logged for COMMITTX to check, and
what can't be taken there is refused, saying so.

Checked against one barchd, a space without Raft:
  - outside a transaction barch.store works as it did;
  - a store write inside a transaction isn't seen outside it until COMMITTX, and is
    made then;
  - a store write inside one that's aborted is never made;
  - a store read inside one sees the transaction's own writes and the keys beneath,
    and nil for one it deleted; a key read through
    barch.store that another client changes makes COMMITTX answer TXCONFLICT;
  - a container's kind and its members read through the overlay to the space beneath
    (TODO 713), a container the transaction deleted reads as gone, and a change beneath
    to the container a kind or member read looked at makes COMMITTX answer TXCONFLICT,
    one made since that kind read nil included;
  - barch.store.locked is refused inside a transaction, saying why, and works outside
    one (barch.store.range is taken now, TODO 701: TestTxStoreRange);
  - barch.space for another space is refused inside a transaction;
  - CALLF inside a client's transaction joins it (TODO 697), which is how all of the
    above is reached.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14710)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


DATA = os.path.join(os.getcwd(), "txstore_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txstore.log"), "a")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

TAG = str(int(time.time()))
SPACE, OTHER = "txst" + TAG, "txso" + TAG


def client(space=SPACE):
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=30, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", space)
    return c


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


FUNCTIONS = {
    "sset": "function call(k, v) barch.store.set(k, v) return 'set' end\n",
    "sget": "function call(k) local v = barch.store.get(k) return v == nil and 'nil' or v end\n",
    "srange": "function call() local r = barch.store.range('a', 'z', 10) return #r end\n",
    "slocked": "function call(k) return barch.store.locked(k, function() return 'in' end) end\n",
    "sother": "function call(name) return tostring(barch.space[name]:get('x')) end\n",
    # TODO 713: a container's kind and one member, through barch.space for this space
    "skind": "function call(n) return barch.space['%s']:kind(n) or 'nil' end\n" % SPACE,
    "smember": "function call(n, m) local v = barch.space['%s']:container(n)[m] "
               "return v == nil and 'nil' or tostring(v) end\n" % SPACE,
}

try:
    c = client()
    client(OTHER).execute_command("SET", "x", "there")
    for name, body in FUNCTIONS.items():
        check(answer(c, "SETF", name, body) == "OK", "SETF %s" % name)

    print("outside a transaction")
    check(answer(c, "CALLF", "sset", "k0", "plain") == "set" and c.get("k0") == "plain",
          "barch.store.set writes at once")
    check(answer(c, "CALLF", "srange") >= 1 and answer(c, "CALLF", "slocked", "k0") == "in",
          "range and locked work")

    print("a store write in a transaction")
    c.execute_command("BEGINTX")
    check(answer(c, "CALLF", "sset", "k1", "in-tx") == "set", "the script answers")
    check(client().get("k1") is None, "another client doesn't see it before COMMITTX")
    check(answer(c, "CALLF", "sget", "k1") == "in-tx", "a store read in the transaction does")
    check(answer(c, "COMMITTX") == "OK" and client().get("k1") == "in-tx", "COMMITTX makes it")

    print("a store read of a key only beneath the overlay")
    client().execute_command("SET", "b1", "beneath")
    c.execute_command("BEGINTX")
    check(answer(c, "CALLF", "sget", "b1") == "beneath", "reads it through")
    c.execute_command("DEL", "b1")
    check(answer(c, "CALLF", "sget", "b1") == "nil", "and after the transaction's DEL, reads nil, not a tomb")
    c.execute_command("ABORTTX")

    print("a store write in an aborted transaction")
    c.execute_command("BEGINTX")
    answer(c, "CALLF", "sset", "k2", "never")
    check(answer(c, "ABORTTX") == "OK" and client().get("k2") is None, "is never made")

    print("a store read checked at COMMITTX")
    client().execute_command("SET", "k3", "before")
    c.execute_command("BEGINTX")
    seen = answer(c, "CALLF", "sget", "k3")
    client().execute_command("SET", "k3", "changed")
    c.execute_command("SET", "k4", "x")
    got = answer(c, "COMMITTX")
    check(seen == "before" and "TXCONFLICT" in str(got), "COMMITTX answers TXCONFLICT (%s)" % str(got)[:60])
    check(client().get("k4") is None, "and makes nothing")

    print("a container made before the transaction (TODO 713)")
    client().execute_command("HSET", "h1", "a", "1", "b", "2")
    client().execute_command("RPUSH", "l1", "x")
    client().execute_command("ZADD", "z1", "3", "m")
    outside = [answer(c, "CALLF", "skind", n) for n in ("h1", "l1", "z1")]
    check(outside == ["hash", "list", "orderedset"], "outside one the kinds read (%s)" % outside)
    c.execute_command("BEGINTX")
    inside = [answer(c, "CALLF", "skind", n) for n in ("h1", "l1", "z1", "nope")]
    members = [answer(c, "CALLF", "smember", "h1", "a"), answer(c, "CALLF", "smember", "h1", "zz"),
               answer(c, "CALLF", "smember", "z1", "m")]
    c.execute_command("DEL", "h1")
    gone = [answer(c, "CALLF", "skind", "h1"), answer(c, "CALLF", "smember", "h1", "a")]
    c.execute_command("ABORTTX")
    check(inside == ["hash", "list", "orderedset", "nil"], "inside one they read the same (%s)" % inside)
    check(members == ["1", "nil", "3"], "and so do its members (%s)" % [str(m)[:60] for m in members])
    check(gone[0] == "nil" and str(gone[1]).startswith("ERR"),
          "after the transaction's DEL it's gone (%s)" % [str(g)[:60] for g in gone])
    check(answer(c, "CALLF", "skind", "h1") == "hash", "and still there outside it after ABORTTX")

    print("container reads checked at COMMITTX (TODO 713)")
    c.execute_command("BEGINTX")
    seen = answer(c, "CALLF", "smember", "h1", "a")
    client().execute_command("HSET", "h1", "a", "changed")
    c.execute_command("SET", "k5", "x")
    got = answer(c, "COMMITTX")
    check(seen == "1" and "TXCONFLICT" in str(got), "a member changed since it was read (%s)" % str(got)[:60])
    c.execute_command("BEGINTX")
    seen = answer(c, "CALLF", "skind", "h1")
    client().execute_command("DEL", "h1")
    c.execute_command("SET", "k5", "x")
    got = answer(c, "COMMITTX")
    check(seen == "hash" and "TXCONFLICT" in str(got), "a container deleted since its kind was read (%s)"
          % str(got)[:60])
    c.execute_command("BEGINTX")
    seen = answer(c, "CALLF", "skind", "h2")
    client().execute_command("HSET", "h2", "a", "1")
    c.execute_command("SET", "k5", "x")
    got = answer(c, "COMMITTX")
    check(seen == "nil" and "TXCONFLICT" in str(got), "one made since its kind read nil (%s)" % str(got)[:60])
    check(client().get("k5") is None, "and none of them made anything")
    c.execute_command("BEGINTX")
    seen = [answer(c, "CALLF", "skind", "l1"), answer(c, "CALLF", "smember", "z1", "m")]
    client().execute_command("HSET", "h3", "a", "1")
    c.execute_command("SET", "k5", "x")
    got = answer(c, "COMMITTX")
    check(seen == ["list", "3"] and got == "OK" and client().get("k5") == "x",
          "a change to another container doesn't refuse it (%s)" % str(got)[:60])

    print("what a transaction doesn't take")
    c.execute_command("BEGINTX")
    got = [answer(c, "CALLF", "slocked", "k0"), answer(c, "CALLF", "sother", OTHER)]
    c.execute_command("ABORTTX")
    check(all(str(g).startswith("ERR") for g in got), "locked and another space are refused (%s)"
          % [str(g)[:60] for g in got])
    check("transaction" in str(got[0]) and "transaction" in str(got[1]), "each saying it's the transaction")
    check(answer(c, "CALLF", "sother", OTHER) == "there", "another space works outside one")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
