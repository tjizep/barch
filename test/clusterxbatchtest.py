#!/usr/bin/env python3
"""
A batch across the groups of a split space - TODO 666.

A space split over several Raft groups put a batch (an FS PUT's chunks and inode, say)
in each group's log in turn, so a writer that died between two left the first groups'
part in and the rest out. Now every group's log gets the whole batch as one entry, each
member applying its group's records; once the first group has it, it's decided, and if
the writer goes before the rest have it, a leader that finds a group without it sends it
there. A group led by another node gets its part through that leader.

What has to hold, on a three-node cluster with a space split over two groups and the
writer pausing three seconds after the first group has the batch:
  - an FS PUT whose chunks span both groups reads back whole on every member (the
    groups are led by different nodes, so the writer sends one group its part through
    that group's leader);
  - a new file's FS PUT whose writer is killed in that pause reads back whole on the
    other two members, once the batch has been rolled forward; the killed node comes
    back to the same copy.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import re
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for  # noqa: E402

scale.workdir()
BASE = scale.port(default=34900)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "files"
LABELS = [SPACE, SPACE + "/1"]
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


nodes = [clusternodes.Node(i, BASE, BINARY, env={"BARCH_TEST_XBATCH_PAUSE_MS": "3000"}) for i in range(3)]
by_port = {n.port: n for n in nodes}


def line_of(n, label):
    for l in n.info():
        if (" label %s" % label) in l:
            return l
    return ""


def leader_of_group(label, among=None):
    for n in (among or nodes):
        if n.proc:
            try:
                if "(this node)" in line_of(n, label):
                    return n
            except redis.RedisError:
                pass
    return None


def fs_get(n, path):
    """FS GET through redirects, from `n`"""
    target = n
    for _ in range(10):
        try:
            return target.client(SPACE).execute_command("FS", "GET", path)
        except redis.ResponseError as e:
            m = re.match(r"NOTLEADER 127\.0\.0\.1:(\d+)", str(e))
            if m and int(m.group(1)) in by_port and by_port[int(m.group(1))].proc:
                target = by_port[int(m.group(1))]
            else:
                time.sleep(0.2)
        except redis.ConnectionError:
            time.sleep(0.2)
    return None


def same(members):
    return len(set(n.digest(SPACE)[0] for n in members)) == 1


def settled(members):
    def done():
        for l in LABELS:
            tops = [int(m.group(1)) for n in members for m in [re.search(r"\bapplied (\d+)", line_of(n, l))] if m]
            if len(tops) != len(members) or len(set(tops)) != 1:
                return False
        return True
    return wait_for(60, done)


def put(n, path, body, out, took=None):
    """FS PUT, following a redirect: a new file's id is a write to the shard holding the
    space's id counter, which only that shard's group's leader makes. `took` gets the
    node that took it"""
    target = n
    for _ in range(5):
        if took is not None:
            took[:] = [target]
        try:
            out.append(target.client(SPACE).execute_command("FS", "PUT", path, body, "CHUNK", "1024"))
            return
        except redis.ResponseError as e:
            m = re.match(r"NOTLEADER 127\.0\.0\.1:(\d+)", str(e))
            if not m:
                out.append("ERR " + str(e))
                return
            target = by_port[int(m.group(1))]
        except redis.RedisError as e:
            out.append("ERR " + str(e))
            return
    out.append("ERR too many redirects")


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

    print("an FS PUT across both groups")
    body1 = "".join("%04d" % i for i in range(2000))          # 8000 bytes, 8 chunks
    out = []
    put(nodes[0], "/a.txt", body1, out)
    check(out and not str(out[0]).startswith("ERR"), "it answers (%s)" % str(out[0])[:60])
    check(settled(nodes) and same(nodes), "every member's copy is the same")
    check(all(fs_get(n, "/a.txt") == body1 for n in nodes), "and the file reads back whole")

    print("the writer killed after the first group has it")
    body2 = "".join("%04d" % (i * 7 % 10000) for i in range(2000))
    out, took = [], []
    t = threading.Thread(target=put, args=(nodes[0], "/b.txt", body2, out, took))
    t.start()
    time.sleep(1.5)                     # the first group has it; the writer is pausing
    writer = took[0]
    writer.kill()
    t.join(30)
    rest = [n for n in nodes if n is not writer]
    check(wait_for(60, lambda: all(leader_of_group(l, rest) for l in LABELS)), "both groups have a leader again")
    check(wait_for(90, lambda: all(fs_get(n, "/b.txt") == body2 for n in rest)),
          "the other two read the new file, whole")
    check(settled(rest) and same(rest), "and their copies are the same")
    writer.start()
    check(wait_for(120, lambda: settled(nodes) and same(nodes)), "the killed writer comes back to the same copy")
    check(fs_get(writer, "/b.txt") == body2, "and reads the new file")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
