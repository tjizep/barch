#!/usr/bin/env python3
"""
The bulk loads in a replicated space - TODO 681.

LOAD puts back a node's own files, and RETRIEVE copies the space from another server,
so run on every member they'd differ. Each goes in the log, the member leading
the group when it applies the entry runs it, and every other member is rebuilt from
that one. IMPORT replays an export's commands, each through Raft's gate. UNLOAD is
refused, since the space's group can open it again before it's saved. A batch over a
size bound (BARCH_TEST_BATCH_BYTES here) lands as several entries, each whole.

What has to hold, on a three-node cluster with a standalone server beside it:
  - LOAD on the leader, after keys saved and more written since: every member holds
    the saved keys and none of the later ones, and their digests match the leader's;
  - RELOAD, which saves each shard and loads it straight back, changes nothing: on a
    follower it answers OK and every member still holds the same keys;
  - RETRIEVE from the standalone server: every member holds what that server holds;
  - IMPORT of an export: refused with NOTLEADER on a follower, and on the leader every
    member gets the keys;
  - UNLOAD is refused, saying why;
  - a LOADFS bigger than the batch bound: every member holds every file.

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
BASE = scale.port(default=33500)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
SHARDS = os.environ.get("BARCH_TEST_RAFT_SHARDS", "32")
STANDALONE = BASE + 19
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


def on(nodes, *cmd):
    """cmd on the space's leader, following it through an election or a rebuild"""
    for _ in range(600):
        l = leader_of(nodes, SPACE)
        if l is not None:
            try:
                return l.client(SPACE).execute_command(*cmd)
            except redis.exceptions.TryAgainError:
                pass
            except redis.ResponseError as e:
                if not str(e).startswith(("NOTLEADER", "TRYAGAIN")):
                    raise
            except redis.ConnectionError:
                pass
        time.sleep(0.1)
    raise RuntimeError("no leader took %s" % (cmd,))


def digests(nodes):
    return [n.digest(SPACE)[0] for n in nodes]


def all_match(nodes, want=None, seconds=120):
    """every member's digest the same (and `want`'s key count, if given), and still so a
    second later: a member going round a rebuild can pass through the right answer"""
    def ok():
        try:
            d = digests(nodes)
        except redis.RedisError:
            return False
        return len(set(d)) == 1 and (want is None or d[0].startswith("keys %d " % want))
    def steady():
        if not ok():
            return False
        time.sleep(1)
        return ok()
    return wait_for(seconds, steady)


IMPORT_DIR = tempfile.mkdtemp(prefix="clusterbulk")
nodes = [clusternodes.Node(i, BASE, BINARY, env={"BARCH_TEST_BATCH_BYTES": "4096"}) for i in range(3)]
standalone = None
try:
    print("a replicated space on three nodes, batches cut at 4096 bytes")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE)
        and "learners 0" in n.group_line("cluster") for n in nodes)), "three voters and a leader")

    print("LOAD on the leader")
    on(nodes, "FLUSHDB")
    for i in range(20):
        on(nodes, "SET", "saved%d" % i, "v")
    leader = leader_of(nodes, SPACE)
    leader.client(SPACE).execute_command("SAVE")
    for i in range(10):
        on(nodes, "SET", "later%d" % i, "v")
    got = on(nodes, "LOAD")
    check(got in ("OK", True), "LOAD answers OK (%r)" % (got,))
    check(all_match(nodes, want=20), "every member holds the 20 saved keys and nothing later (%s)" % digests(nodes))

    print("RELOAD, on a follower: its own storage, the data as it was")
    for i in range(10):
        on(nodes, "SET", "later%d" % i, "v")
    follower = next(n for n in nodes if n is not leader_of(nodes, SPACE))
    got = answer(follower.client(SPACE), "RELOAD")
    check(got in ("OK", True), "RELOAD answers OK (%r)" % (got,))
    check(all_match(nodes, want=30), "every member still holds all 30 (%s)" % digests(nodes))

    print("RETRIEVE from a standalone server")
    sdir = os.path.join(os.getcwd(), "standalone")
    shutil.rmtree(sdir, ignore_errors=True)
    os.makedirs(sdir)
    standalone = subprocess.Popen([BINARY, "--port", str(STANDALONE), "--bind", "127.0.0.1", "--dir", sdir],
                                  stdout=open(sdir + ".log", "a"), stderr=subprocess.STDOUT)
    s = redis.Redis(port=STANDALONE, decode_responses=True)
    check(wait_for(30, lambda: s.ping()), "it's up")
    s.execute_command("USE", "configuration")
    s.execute_command("SET", SPACE + ".shards", SHARDS)
    s.execute_command("USE", SPACE)
    for i in range(15):
        s.execute_command("SET", "retrieved%d" % i, "v")
    got = on(nodes, "RETRIEVE", "127.0.0.1", str(STANDALONE))
    check(got in ("OK", True), "RETRIEVE answers OK (%r)" % (got,))
    check(all_match(nodes, want=15), "every member holds the server's 15 keys (%s)" % digests(nodes))

    print("IMPORT of an export")
    s.execute_command("FLUSHDB")
    for i in range(12):
        s.execute_command("SET", "imported%d" % i, "v")
    path = os.path.join(IMPORT_DIR, "export.resp")
    s.execute_command("EXPORT", path)
    leader = leader_of(nodes, SPACE)
    follower = next(n for n in nodes if n is not leader)
    got = answer(follower.client(SPACE), "IMPORT", path)
    check(str(got).startswith("ERR NOTLEADER"), "on a follower: NOTLEADER (%r)" % str(got)[:60])
    got = on(nodes, "IMPORT", path)
    check(got == 12, "on the leader: 12 commands (%r)" % (got,))
    check(all_match(nodes, want=15 + 12), "every member has them (%s)" % digests(nodes))

    print("UNLOAD")
    got = answer(leader_of(nodes, SPACE).client(SPACE), "UNLOAD", SPACE)
    check(str(got).startswith("ERR") and "replicated space refuses" in str(got), "refused (%r)" % str(got)[:90])

    print("a LOADFS bigger than the batch bound")
    files = os.path.join(IMPORT_DIR, "files")
    os.makedirs(files)
    for i in range(20):
        open(os.path.join(files, "f%02d.txt" % i), "w").write(("%02d" % i) * 600)
    got = on(nodes, "LOADFS", files, "/big")
    check(isinstance(got, list) and "files=20" in got, "LOADFS answers files=20 (%r)" % (got,))
    check(all_match(nodes), "every member's digest matches (%s)" % digests(nodes))
    got = [on(nodes, "FS", "GET", "/big/f%02d.txt" % i) for i in (0, 19)]
    check(got == ["00" * 600, "19" * 600], "the first and last file read back whole")
finally:
    for n in nodes:
        n.stop()
    if standalone:
        standalone.terminate()
        standalone.wait(30)
    shutil.rmtree(IMPORT_DIR, ignore_errors=True)

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
