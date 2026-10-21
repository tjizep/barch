#!/usr/bin/env python3
"""
A queue on a replicated space - TODO 682.

A queue kept its messages in a ring file on the node it was pushed to, and that node's
consumer handed them out, so a replicated space refused QUEUE. Now a queue whose space
is replicated keeps its messages as meta keys in the space, and every change to them
(a push, a delivery started, done, failed) is an entry in the space's log that every
member applies. Only the member leading the space hands messages out, and a handler
that runs from the log is called through it. A queue on a space that isn't replicated
keeps its ring file, on each node.

What has to hold, on a three-node cluster:
  - a push on a follower answers NOTLEADER, and a client can't send the entry the
    log uses for a push itself;
  - a push on the leader answers its sequence, and with the consumer off for that
    queue every member holds the message;
  - ten pushes are each handled once, on the leader, and the handler's writes reach
    every member, whose digests match;
  - a handler that always fails is dead lettered after max_attempts, on every member;
  - a function run from the log can push to a queue on its own space;
  - a leader killed with messages waiting: the new leader hands out the rest, each at
    least once, and the old one catches up to the same space;
  - a queue on a space that isn't replicated still takes a push on any node, which
    that node handles.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=33700)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
OWN = "mine"
failures = 0


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


def on(members, space, *cmd):
    """cmd on the space's leader, following it through an election"""
    for _ in range(600):
        l = leader_of(members, space)
        if l is not None:
            try:
                return l.client(SPACE if space == SPACE else "configuration").execute_command(*cmd)
            except redis.exceptions.TryAgainError:
                pass
            except redis.ResponseError as e:
                if not str(e).startswith(("NOTLEADER", "TRYAGAIN")):
                    raise
            except redis.ConnectionError:
                pass
        time.sleep(0.1)
    raise RuntimeError("no leader took %s" % (cmd,))


def declare(members, name, call, extra=""):
    on(members, "cluster", "SETF", "queues/" + name, '''
        function transport()
            return {kind = "queue", name = "%s", space = "%s", call = "%s", user = "default",
                    max_attempts = 2, poll = "1s" %s}
        end
    ''' % (name, SPACE, call, extra))


def local(n, space=SPACE):
    c = n.client(space)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c


def status(n, name):
    for line in n.client().execute_command("QUEUE", "STATUS").splitlines():
        if (" name=%s " % name) in line + " ":
            return dict(kv.split("=", 1) for kv in line.split(" ") if "=" in kv)
    return {}


def seen(n):
    # a member that has just come back can answer TryAgain while a new leader applies
    # what came before it: not yet, so the waits around this ask again
    try:
        return len(local(n).execute_command("KEYS", "seen:*"))
    except redis.exceptions.TryAgainError:
        return -1


def settled(members):
    l = leader_of(members, SPACE)
    if l is None:
        return False
    top = l.field(SPACE, "applied")
    return wait_for(60, lambda: all(n.field(SPACE, "applied") >= top for n in members))


def same(members):
    return len(set(n.digest(SPACE)[0] for n in members)) == 1


nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]
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

    # the handler counts its calls and marks each sequence it was given
    on(nodes, SPACE, "SETF", "TAKE", '''
        function call(message, sequence, attempts)
            barch.store.set("taken", tostring(tonumber(barch.store.get("taken") or "0") + 1))
            barch.store.set("seen:" .. sequence, message)
            return "ok"
        end''')
    on(nodes, SPACE, "SETF", "REFUSE", 'function call(message) error("no thanks") end')
    on(nodes, SPACE, "SETF", "FORWARD", 'function call(message) return barch.queue("work", message) end')
    declare(nodes, "work", "TAKE")
    declare(nodes, "bad", "REFUSE")
    declare(nodes, "held", "TAKE", ", enabled = false")
    settled(nodes)

    print("a push on a follower")
    leader = leader_of(nodes, SPACE)
    follower = next(n for n in nodes if n is not leader)
    got = answer(follower.client(SPACE), "QUEUE", "PUSH", "work", "x")
    check(str(got).startswith("ERR NOTLEADER"), "answers NOTLEADER (%r)" % str(got)[:70])
    got = answer(leader.client(SPACE), "\x01QUEUE", "PUSH", "work", "forged")
    check(str(got).startswith("ERR unknown command"), "a client can't send the log's own entry (%r)" % str(got)[:60])

    print("a push on the leader, its consumer off")
    got = [on(nodes, SPACE, "QUEUE", "PUSH", "held", "h%d" % i) for i in range(3)]
    check(got == [1, 2, 3], "answers the sequences 1, 2, 3 (%r)" % got)
    settled(nodes)
    check(all(status(n, "held").get("waiting") == "3" for n in nodes),
          "every member holds the three (%s)" % [status(n, "held").get("waiting") for n in nodes])

    print("ten pushes, handled on the leader")
    for i in range(10):
        on(nodes, SPACE, "QUEUE", "PUSH", "work", "w%d" % i)
    check(wait_for(60, lambda: settled(nodes) and all(seen(n) == 10 for n in nodes)),
          "every member holds what the handler wrote for each (%s)" % [seen(n) for n in nodes])
    check(all(local(n).execute_command("GET", "taken") == "10" for n in nodes),
          "the handler ran once for each (%s)" % [local(n).execute_command("GET", "taken") for n in nodes])
    check(wait_for(30, lambda: all(status(n, "work").get("waiting") == "0" for n in nodes)),
          "nothing waits on any member")
    leader = leader_of(nodes, SPACE)
    check(status(leader, "work").get("delivered") == "10", "the leader delivered them (%s)"
          % status(leader, "work").get("delivered"))
    check(all(status(n, "work").get("delivered") == "0" for n in nodes if n is not leader),
          "no follower delivered any")
    check(settled(nodes) and same(nodes), "every member's digest matches")

    print("a handler that always fails")
    on(nodes, SPACE, "QUEUE", "PUSH", "bad", "poison")
    check(wait_for(60, lambda: all(status(n, "bad").get("dead") == "1"
                                   and status(n, "bad").get("waiting") == "0" for n in nodes)),
          "dead lettered after two attempts, on every member (%s)"
          % [(status(n, "bad").get("waiting"), status(n, "bad").get("dead")) for n in nodes])
    check(settled(nodes) and same(nodes), "every member's digest matches")

    print("a function run from the log pushes to a queue on its own space")
    got = answer(leader_of(nodes, SPACE).client(SPACE), "FORWARD", "fwd")
    check(got == 11, "it answers the sequence (%r)" % (got,))
    check(wait_for(60, lambda: settled(nodes) and all(seen(n) == 11 for n in nodes)),
          "and the message is handled everywhere (%s)" % [seen(n) for n in nodes])

    print("the leader killed with messages waiting")
    for i in range(40):
        on(nodes, SPACE, "QUEUE", "PUSH", "work", "f%d" % i)
    leader = leader_of(nodes, SPACE)
    leader.kill()
    rest = [n for n in nodes if n is not leader]
    check(wait_for(60, lambda: leader_of(rest, SPACE) is not None), "the other two elect a leader")
    check(wait_for(120, lambda: settled(rest) and all(seen(n) == 51 for n in rest)
                   and all(status(n, "work").get("waiting") == "0" for n in rest)),
          "the new leader hands out the rest (%s)" % [seen(n) for n in rest])
    taken = [int(local(n).execute_command("GET", "taken")) for n in rest]
    check(all(t >= 51 for t in taken), "each at least once (%s)" % taken)
    leader.start()
    check(wait_for(120, lambda: settled(nodes) and same(nodes)), "the old leader catches up")
    check(wait_for(30, lambda: seen(leader) == 51), "and holds them all")

    print("a queue on a space that isn't replicated")
    for n in nodes:
        n.client().execute_command("CONFIG", "SET", "queue_dir", os.path.join(n.dir, "queues"))
        n.client(OWN).execute_command("SETF", "OWN", '''
            function call(message) barch.store.set("got", message) return "ok" end''')
    on(nodes, "cluster", "SETF", "queues/own", '''
        function transport()
            return {kind = "queue", name = "own", space = "%s", call = "OWN", user = "default",
                    durability = "each", poll = "1s"}
        end''' % OWN)
    follower = next(n for n in nodes if n is not leader_of(nodes, SPACE))
    got = answer(follower.client(SPACE), "QUEUE", "PUSH", "own", "mine")
    check(got == 1, "a follower takes the push (%r)" % (got,))
    check(wait_for(30, lambda: follower.client(OWN).execute_command("GET", "got") == "mine"),
          "and handles it itself")
    check(all(n.client(OWN).execute_command("GET", "got") is None for n in nodes if n is not follower),
          "the other nodes don't")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
