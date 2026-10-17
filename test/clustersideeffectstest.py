#!/usr/bin/env python3
"""
What an install does besides writing the function's key happens on every member, and
a run from the log is the same run everywhere - TODO 667.

An install clears the exposed-command cache, tells cron and the queues to look again,
and with RELOAD publishes the compiled code. A member that only applied the key did
none of it. A run from the log used the commit thread's cached compiled copy, so a
member just started could run a newer version than one that had been running. It ran
with the server's rights. And cron fired on every node.

What has to hold, on a three-node cluster:
  - a follower that had already read the space's exposed commands answers one a SETF
    on the leader exposes afterwards, and one a LOADKEYS brings;
  - a read-only function a follower session compiled is the new code there after a
    SETF ... RELOAD on the leader, and after a LOADKEYS ... RELOAD;
  - after a quiet SETF (no RELOAD), a member restarted since runs the same code as
    the others at the next call from the log;
  - a user who may call functions but not write the space can't write it through
    one that runs from the log;
  - a cron job on a replicated space writes once a tick, and no follower's scheduler
    reports an error running it.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import re
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=32700)
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


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def exposing(prefix):
    """a function that exposes <prefix>ADD as a read-only command"""
    return ('local function ADD(a, b) return tonumber(a) + tonumber(b) end\n'
            'function call() return "usage" end\n'
            'function transport()\n'
            '    return {kind = "resp", methods = {%sADD = ADD}, categories = {%sADD = {"read"}},\n'
            '            arity = {%sADD = 2}}\n'
            'end\n' % (prefix, prefix, prefix))


def version(v):
    return '--@barch {"write": false}\nfunction call() return "%s" end\n' % v


def settled(nodes):
    l = leader_of(nodes, SPACE)
    top = l.field(SPACE, "applied")
    return wait_for(30, lambda: all(n.field(SPACE, "applied") >= top for n in nodes))


def follower_session(n):
    c = n.client(SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c


def errors_in(n, word):
    text = re.sub(r"\x1b\[[0-9;]*m", "", open(n.dir + ".log").read())
    return [l for l in text.splitlines() if word in l and (" E " in l or ":E " in l)]


IMPORT_DIR = tempfile.mkdtemp(prefix="sideeffects")
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
    leader = leader_of(nodes, SPACE)
    follower = next(n for n in nodes if n is not leader)
    c = leader.client(SPACE)

    print("exposed commands on a follower")
    c.execute_command("SETF", "ex1", exposing("EXA"))
    settled(nodes)
    f = follower_session(follower)
    check(answer(f, "EXAADD", "1", "2") == 3, "the follower answers EXAADD, and has read the index")
    c.execute_command("SETF", "ex2", exposing("EXB"))
    settled(nodes)
    got = answer(f, "EXBADD", "2", "3")
    check(got == 5, "and one a later SETF exposes (%r)" % got)
    open(os.path.join(IMPORT_DIR, "ex3.luau"), "w").write(exposing("EXC"))
    c.execute_command("LOADKEYS", IMPORT_DIR)
    settled(nodes)
    got = answer(f, "EXCADD", "3", "4")
    check(got == 7, "and one a LOADKEYS brings (%r)" % got)

    print("RELOAD reaches a follower's session")
    c.execute_command("SETF", "rv", version("v1"))
    settled(nodes)
    check(answer(f, "rv") == "v1", "the follower's session runs v1")
    c.execute_command("SETF", "rv", version("v2"), "RELOAD")
    settled(nodes)
    got = answer(f, "rv")
    check(got == "v2", "a SETF ... RELOAD on the leader: v2 there (%r)" % got)
    os.remove(os.path.join(IMPORT_DIR, "ex3.luau"))
    open(os.path.join(IMPORT_DIR, "lk.luau"), "w").write(version("k1"))
    c.execute_command("LOADKEYS", IMPORT_DIR)
    settled(nodes)
    check(answer(f, "lk") == "k1", "the follower's session runs k1 from LOADKEYS")
    open(os.path.join(IMPORT_DIR, "lk.luau"), "w").write(version("k2"))
    c.execute_command("LOADKEYS", IMPORT_DIR, "", "RELOAD")
    settled(nodes)
    got = answer(f, "lk")
    check(got == "k2", "a LOADKEYS ... RELOAD on the leader: k2 there (%r)" % got)

    print("a quiet SETF, then a member restarted")
    c.execute_command("SETF", "stamp", "function call() barch.store.set('stamped', 'A') return 1 end")
    c.execute_command("stamp")
    c.execute_command("SETF", "stamp", "function call() barch.store.set('stamped', 'B') return 1 end")
    settled(nodes)
    follower.stop()
    follower.start()
    check(wait_for(90, lambda: settled(nodes)), "it catches up")
    leader = leader_of(nodes, SPACE)
    c = leader.client(SPACE)
    c.execute_command("stamp")
    settled(nodes)
    got = [follower_session(n).execute_command("GET", "stamped") for n in nodes]
    check(got == ["B"] * 3, "every member ran the code the store holds (%s)" % got)

    print("a user who can't write the space")
    # once, on the cluster group's leader: the users are the cluster's - TODO 676
    (leader_of(nodes, "cluster") or nodes[0]).client().execute_command(
        "ACL", "SETUSER", "limited", "on", ">pw", "+read", "+function", "+data")
    c.execute_command("SETF", "writes", "function call() barch.store.set('by_limited', 'x') return 1 end")
    settled(nodes)
    lim = leader.client(SPACE)
    lim.execute_command("AUTH", "limited", "pw")
    got = answer(lim, "writes")
    check(str(got).startswith("ERR") and "authorized" in str(got), "its call is refused (%r)" % str(got)[:90])
    settled(nodes)
    check(all(follower_session(n).execute_command("GET", "by_limited") is None for n in nodes),
          "and nothing was written anywhere")

    print("a cron job on the replicated space")
    c.execute_command("SETF", "tick", "function call() barch.store.set('ticks', tostring("
                                      "tonumber(barch.store.get('ticks') or '0') + 1)) return 1 end")
    conf_leader = leader_of(nodes, "cluster") or nodes[0]
    conf_leader.client("configuration").execute_command("SETF", "cron/jobs/ticker", '''
        function transport()
            return {kind = "cron", space = "%s", call = "TICK", every = "500ms", user = "default"}
        end
    ''' % SPACE)
    started = time.time()
    check(wait_for(30, lambda: int(follower_session(leader).execute_command("GET", "ticks") or "0") >= 4),
          "it ticks")
    conf_leader.client("configuration").execute_command("REMF", "cron/jobs/ticker")
    took = time.time() - started
    settled(nodes)
    time.sleep(1.5)
    settled(nodes)
    got = [follower_session(n).execute_command("GET", "ticks") for n in nodes]
    ticks = int(got[0] or 0)
    check(len(set(got)) == 1, "every member holds the same count (%s)" % got)
    check(ticks <= took / 0.5 + 4, "about one a tick, not one per node (%d in %.1fs)" % (ticks, took))
    bad = [e for n in nodes if n is not leader for e in errors_in(n, "cron")]
    check(not bad, "no follower's scheduler failed it (%s)" % [b[-120:] for b in bad[:2]])
finally:
    for n in nodes:
        n.stop()
    shutil.rmtree(IMPORT_DIR, ignore_errors=True)

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
