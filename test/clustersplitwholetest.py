#!/usr/bin/env python3
"""
Whole-space commands in a space split over several groups - TODO 690.

BEGIN, COMMIT and ROLLBACK (a mark on the whole space), LOAD and RETRIEVE (the whole
space replaced) and queues act on the whole space at one point in its log, which a split
space doesn't have, so TODO 689 refused them there. Now they go group by group: each goes
in every group's log (through that group's leader when it's led elsewhere), and each
group's run acts on its own shards at its own point. A queue's keys are all kept in one
shard, and its entries go in that shard's group's log.

What has to hold, on a three-node cluster with a space split over two groups (16 keys,
over both):
  - BEGIN, writes, ROLLBACK: every member holds the keys as they were before BEGIN,
    and a BEGIN sent again in between doesn't move the mark;
  - BEGIN, writes, COMMIT: every member keeps the writes, and a COMMIT sent again
    answers OK;
  - a queue on the split space: ten pushes are handled, and every member holds what
    the handler wrote;
  - SAVE on every node, more writes, LOAD: every member is back to what was saved;
  - RETRIEVE from a standalone server: every member holds the server's keys;
  - after each, every member's digest is the same.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import re
import shutil
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for  # noqa: E402

scale.workdir()
BASE = scale.port(default=18940)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "whole"
LABELS = [SPACE, SPACE + "/1"]
KEYS = ["k%d" % i for i in range(16)]
SHARDS = os.environ.get("BARCH_TEST_RAFT_SHARDS", "32")
STANDALONE = BASE + 19
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]
by_port = {n.port: n for n in nodes}


def line_of(n, label):
    for l in n.info():
        if (" label %s" % label) in l:
            return l
    return ""


def leader_of_group(label):
    for n in nodes:
        if n.proc:
            try:
                if "(this node)" in line_of(n, label):
                    return n
            except redis.RedisError:
                pass
    return None


def following(n, *cmd, space=SPACE, again=False):
    """cmd through redirects; `again`: an UNKNOWN (a leader change mid commit) is sent
    again, for a command that's the same twice"""
    target = n
    for _ in range(20):
        try:
            return target.client(space).execute_command(*cmd)
        except redis.exceptions.TryAgainError:
            time.sleep(0.1)         # redis-py takes the TRYAGAIN off the message
        except redis.ResponseError as e:
            m = re.match(r"NOTLEADER 127\.0\.0\.1:(\d+)", str(e))
            if m and int(m.group(1)) in by_port:
                target = by_port[int(m.group(1))]
            elif str(e).startswith(("TRYAGAIN", "NOTLEADER")) or (again and str(e).startswith("UNKNOWN")):
                time.sleep(0.1)
            else:
                raise
    raise RuntimeError("no answer for %s" % (cmd,))


def settled():
    def done():
        for l in LABELS:
            tops = [int(m.group(1)) for n in nodes for m in [re.search(r"\bapplied (\d+)", line_of(n, l))] if m]
            if len(tops) != len(nodes) or len(set(tops)) != 1:
                return False
        return True
    return wait_for(60, done)


def local(n):
    c = n.client(SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c


def values(n):
    return [local(n).execute_command("GET", k) for k in KEYS]


def same():
    return settled() and len(set(n.digest(SPACE)[0] for n in nodes)) == 1


def set_all(value):
    for k in KEYS:
        following(nodes[0], "SET", k, value, again=True)


standalone = None
try:
    print("a space split over two groups")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    conf = nodes[0].client("configuration")
    conf.execute_command("SET", SPACE + ".raft_groups", "2")
    conf.execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: all("members 3" in line_of(n, l) and "learners 0" in line_of(n, l)
                                   for n in nodes for l in LABELS)
                   and all(leader_of_group(l) for l in LABELS)), "two groups, three voters each")

    print("BEGIN, writes, ROLLBACK")
    set_all("before")
    # sent again on an UNKNOWN: a group already in a transaction keeps its mark, so a
    # second BEGIN finishes the groups the first didn't reach - TODO 798
    check(following(nodes[1], "BEGIN", again=True) == "OK", "BEGIN")
    set_all("during")
    following(nodes[0], "SET", "extra", "x", again=True)
    # what that rests on: one sent after the writes, and the ROLLBACK below still
    # goes back to before them
    check(following(nodes[2], "BEGIN", again=True) == "OK", "a BEGIN sent again answers OK")
    # sent again on an UNKNOWN too: with no mark left a ROLLBACK answers OK and does nothing
    check(following(nodes[2], "ROLLBACK", again=True) == "OK", "ROLLBACK")
    # a group can be between leaders for a moment after the save ROLLBACK makes
    check(wait_for(60, lambda: same() and all(values(n) == ["before"] * 16 for n in nodes)),
          "every member holds the keys as before BEGIN")
    check(wait_for(30, lambda: all(local(n).execute_command("GET", "extra") is None for n in nodes)),
          "and none written since")

    print("BEGIN, writes, COMMIT")
    check(following(nodes[0], "BEGIN", again=True) == "OK", "BEGIN")
    set_all("after")
    # and a COMMIT with no transaction left answers OK and does nothing
    check(following(nodes[1], "COMMIT", again=True) == "OK", "COMMIT")
    check(following(nodes[2], "COMMIT", again=True) == "OK", "a COMMIT sent again answers OK")
    check(wait_for(60, lambda: same() and all(values(n) == ["after"] * 16 for n in nodes)),
          "every member keeps the writes")

    print("a queue on the split space")
    following(nodes[0], "SETF", "TAKE", '''
        function call(message, sequence)
            barch.store.set("q" .. sequence, message)
            return "ok"
        end''')
    (leader_of_group("cluster") or nodes[0]).client("configuration").execute_command("SETF", "queues/work", '''
        function transport()
            return {kind = "queue", name = "work", space = "%s", call = "TAKE", user = "default", poll = "1s"}
        end''' % SPACE)
    # the cluster group commits on a quorum, and a push that lands on a member that
    # hasn't applied the declaration yet is refused - TODO 798
    check(wait_for(30, lambda: all(" name=work " in n.client().execute_command("QUEUE", "STATUS") + " "
                                   for n in nodes)), "every member knows the queue")
    got = [following(nodes[i % 3], "QUEUE", "PUSH", "work", "m%d" % i) for i in range(10)]
    check(got == list(range(1, 11)), "ten pushes answer 1..10 (%s)" % got)
    check(wait_for(60, lambda: all(local(n).execute_command("GET", "q10") == "m9" for n in nodes)),
          "every member holds what the handler wrote")
    check(same(), "every member's digest is the same")

    print("SAVE on every node, more writes, LOAD")
    set_all("saved")
    settled()
    for n in nodes:
        n.client(SPACE).execute_command("SAVE")
    set_all("unsaved")
    check(following(nodes[0], "LOAD") in ("OK", True), "LOAD")
    check(wait_for(60, lambda: same() and all(values(n) == ["saved"] * 16 for n in nodes)),
          "every member is back to what was saved (%s)" % [values(n)[:2] for n in nodes])

    print("RETRIEVE from a standalone server")
    sdir = os.path.join(os.getcwd(), "standalone")
    shutil.rmtree(sdir, ignore_errors=True)
    os.makedirs(sdir)
    standalone = subprocess.Popen([BINARY, "--port", str(STANDALONE), "--bind", "127.0.0.1", "--dir", sdir],
                                  stdout=open(sdir + ".log", "a"), stderr=subprocess.STDOUT)
    s = redis.Redis(port=STANDALONE, decode_responses=True)
    check(wait_for(30, lambda: s.ping()), "it's up")
    s.execute_command("configuration:SET", SPACE + ".shards", SHARDS)
    s.execute_command("USE", SPACE)
    for k in KEYS:
        s.set(k, "retrieved")
    check(following(nodes[1], "RETRIEVE", "127.0.0.1", str(STANDALONE)) in ("OK", True), "RETRIEVE")
    ok = wait_for(60, lambda: same() and all(values(n) == ["retrieved"] * 16 for n in nodes))
    check(ok, "every member holds the server's keys (%s; %s; %s)" % (
        [values(n)[:2] for n in nodes], [n.digest(SPACE)[0] for n in nodes],
        [[re.search(r"\bapplied (\d+)", line_of(n, l)).group(1) for l in LABELS] for n in nodes]))
    if not ok:
        for n in nodes:
            print("    node %d keys: %s" % (n.i + 1, sorted(local(n).execute_command("KEYS", "*"))))
finally:
    for n in nodes:
        n.stop()
    if standalone:
        standalone.terminate()
        standalone.wait(30)

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
