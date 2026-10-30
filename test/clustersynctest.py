#!/usr/bin/env python3
"""
FUNCTIONS in a cluster - TODO 683.

FUNCTIONS was class local, so a replicated space refused even FUNCTIONS STATUS. And a
repository sync runs on every node's timer, while only the member leading a replicated
space can write it, so every sync failed in part on each follower. Now the read
subcommands answer on any node, and a sync writes a replicated space only where this
node leads it, leaving it to that member everywhere else. What a sync wrote to a space
is kept in the space too, so a member that leads it later removes what the repository
dropped.

What has to hold, on a three-node cluster with a repository filling a replicated space:
  - FUNCTIONS STATUS, COMMANDS, CRON, QUEUES and LIBRARIES answer in the replicated
    space on a follower;
  - the repository, synced on each node's timer, lands on every member: the function
    and the keys, matching digests, no sync failed anywhere, and the followers say
    they left the space to the leader;
  - a change upstream reaches every member;
  - FUNCTIONS SYNC on a follower answers NOTLEADER, and OK on the leader;
  - the leader killed, then a key dropped upstream: the new leader removes it from
    every member, and the old one catches up to the same space.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=18660)
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


ORIGIN = tempfile.mkdtemp(prefix="clustersync")


def git(*args):
    subprocess.check_call(["git", "-C", ORIGIN, *args], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def write(rel, body):
    with open(os.path.join(ORIGIN, rel), "w", encoding="utf-8") as f:
        f.write(body)


def commit(message):
    git("add", "-A")
    git("commit", "-q", "-m", message)


def conf(members, setting, value):
    for _ in range(600):
        l = leader_of(members, "cluster")
        if l is not None:
            try:
                return l.client("configuration").execute_command(
                    "SET", "git/repositories/app/" + setting, value)
            except (redis.ResponseError, redis.ConnectionError):
                pass
        time.sleep(0.1)
    raise RuntimeError("no cluster leader took the setting")


def local(n):
    c = n.client(SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c


def status(n):
    line = n.client(SPACE).execute_command("FUNCTIONS", "STATUS")
    return dict(kv.split("=", 1) for kv in line.split(" ") if "=" in kv)


def holds(n, key, value):
    return local(n).execute_command("GET", key) == value


def same(members):
    return len(set(n.digest(SPACE)[0] for n in members)) == 1


git("init", "-q", "-b", "main")
git("config", "user.email", "t@t")
git("config", "user.name", "t")
write("ver.luau", 'function call() return "v1" end\n')
write("data.txt", "one")
write("old.txt", "going")
commit("v1")

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
    for n in nodes:
        n.client().execute_command("CONFIG", "SET", "functions_dir", os.path.join(n.dir, "checkouts"))

    print("the read subcommands, in the replicated space on a follower")
    follower = next(n for n in nodes if n is not leader_of(nodes, SPACE))
    for sub in ("STATUS", "COMMANDS", "CRON", "QUEUES", "LIBRARIES"):
        got = answer(follower.client(SPACE), "FUNCTIONS", sub)
        check(not str(got).startswith("ERR"), "FUNCTIONS %s answers (%r)" % (sub, str(got)[:50]))

    print("a repository into the replicated space, synced on every node's timer")
    conf(nodes, "url", ORIGIN)
    conf(nodes, "space", SPACE)
    conf(nodes, "pull", "on")
    conf(nodes, "ms", "1000")
    check(wait_for(90, lambda: all(holds(n, "data.txt", "one") and holds(n, "old.txt", "going")
                                   and "VER" in local(n).execute_command("KEYSF") for n in nodes)),
          "every member holds the function and the keys")
    check(wait_for(30, lambda: same(nodes)), "their digests match")
    time.sleep(3)       # a few timer syncs each
    states = [status(n) for n in nodes]
    check(all(s.get("state") == "ok" for s in states),
          "no sync failed on any node (%s)" % [(s.get("state"), s.get("last", "")[:60]) for s in states])
    leader = leader_of(nodes, SPACE)
    check(all(status(n).get("leader_elsewhere") == SPACE for n in nodes if n is not leader)
          and "leader_elsewhere" not in status(leader),
          "the followers left the space to the leader (%s)"
          % [status(n).get("leader_elsewhere") for n in nodes])

    print("a change upstream")
    write("data.txt", "two")
    commit("v2")
    check(wait_for(60, lambda: all(holds(n, "data.txt", "two") for n in nodes)), "reaches every member")

    print("FUNCTIONS SYNC by hand")
    leader = leader_of(nodes, SPACE)
    follower = next(n for n in nodes if n is not leader)
    got = answer(follower.client(SPACE), "FUNCTIONS", "SYNC")
    check(str(got).startswith("ERR NOTLEADER"), "on a follower: NOTLEADER (%r)" % str(got)[:70])
    got = answer(leader.client(SPACE), "FUNCTIONS", "SYNC")
    check(got == "OK", "on the leader: OK (%r)" % (got,))

    print("the leader killed, then a key dropped upstream")
    leader.kill()
    rest = [n for n in nodes if n is not leader]
    check(wait_for(60, lambda: leader_of(rest, SPACE) is not None), "the other two elect a leader")
    os.remove(os.path.join(ORIGIN, "old.txt"))
    commit("v3")
    check(wait_for(60, lambda: all(local(n).execute_command("GET", "old.txt") is None for n in rest)),
          "the new leader removes it from every member")
    check(all(holds(n, "data.txt", "two") for n in rest), "and keeps the rest")
    leader.start()
    check(wait_for(120, lambda: same(nodes)), "the old leader catches up")
    # the digest can match before the restarted member has joined the group again,
    # and a follower read answers TRYAGAIN until it has
    check(wait_for(60, lambda: local(leader).execute_command("GET", "old.txt") is None
                   and local(leader).execute_command("GET", "data.txt") == "two"),
          "and doesn't hold the dropped key")
finally:
    for n in nodes:
        n.stop()
    shutil.rmtree(ORIGIN, ignore_errors=True)

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
