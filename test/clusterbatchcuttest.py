#!/usr/bin/env python3
"""
A file write cut by a leader change is in whole or not at all - TODO 665. So are a
stored function's writes - TODO 673.

FS, GRAPH and DIR write through `staged`, which used to reach Raft one key at a time.
An FS overwrite keeps the file's id, so its chunks are overwritten in place, then the
inode and the name. A leader killed part way left every member with some new chunks
under the old inode: a file that reads as neither version. Now a replicated space
commits the whole batch as one entry.

Every node runs with BARCH_TEST_COMMIT_DELAY_MS, so each commit waits before it goes
in the log. The file is overwritten on the leader, and the leader is killed while the
overwrite is under way: once early, while the one entry is still waiting, and once
late, when one key at a time would have committed some chunks and not the rest.

Then a stored function that writes six keys is cut the same way, late: before TODO
673 each of its writes committed on its own.

What has to hold, each time:
  - the surviving members elect a leader, which reads the file as either the old
    version or the new one, whole;
  - the surviving members' digests match, so the other one holds the same.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=30700)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "files"
DELAY_MS = 300
CHUNK = 1024
CHUNKS = 6
KEYS = 6
# writes its argument to KEYS keys
PUTALL = "function call(v) for i = 0, %d do barch.store.set('k' .. i, v) end return %d end" % (KEYS - 1, KEYS)
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def body(ch):
    return ch * (CHUNK * CHUNKS)


def steady_leader(nodes):
    """the leader, once it has taken writes for 3s running: a node that rejoined may
    take the lead from the one leader_of saw, and the overwrite has to reach the node
    that gets killed"""
    end = time.time() + 120
    held, since = None, 0
    while time.time() < end:
        l = leader_of(nodes, SPACE)
        try:
            if l is None:
                raise redis.RedisError("no leader")
            l.client(SPACE).execute_command("SET", "probe", "1")
        except redis.RedisError:
            held = None
            time.sleep(0.1)
            continue
        if l is not held:
            held, since = l, time.time()
        elif time.time() - since >= 3:
            return l
        time.sleep(0.2)
    raise RuntimeError("no node led for 3s running")


def all_voting(nodes):
    return leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE) for n in nodes)


# a function's writes take a held commit each on a build before TODO 673, so its
# deadline has room for all of them
nodes = [clusternodes.Node(i, BASE, BINARY, ["function_deadline_ms=10000"],
                           env={"BARCH_TEST_COMMIT_DELAY_MS": str(DELAY_MS)})
         for i in range(3)]
try:
    print("a replicated space on three nodes, each commit held %dms" % DELAY_MS)
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: all_voting(nodes)), "three voters and a leader")

    current = None
    # early: the one entry is still waiting. late: some chunks would be in
    for round_, (kill_after, ch) in enumerate(((DELAY_MS * 0.5 / 1000, "b"), (DELAY_MS * 3.5 / 1000, "c"))):
        leader = steady_leader(nodes)
        c = leader.client(SPACE)
        if current is None:
            c.execute_command("FS", "PUT", "/f.bin", body("a"), "CHUNK", str(CHUNK))
            current = "a"
        print("overwrite with %r on node %d, which is killed after %.2fs" % (ch, leader.i + 1, kill_after))

        answered = []

        def overwrite():
            try:
                answered.append(leader.client(SPACE).execute_command(
                    "FS", "PUT", "/f.bin", body(ch), "CHUNK", str(CHUNK)))
            except redis.RedisError as e:
                answered.append(e)

        t = threading.Thread(target=overwrite)
        t.start()
        time.sleep(kill_after)
        leader.kill()
        t.join(30)
        # cut by the kill, or done before it: one entry commits well inside the late
        # kill's wait. Any other answer means the overwrite never reached the
        # leader, and the round shows nothing
        check(answered and (isinstance(answered[0], redis.ConnectionError) or answered[0] == CHUNKS),
              "the overwrite reached the leader (%s)" % (answered[0] if answered else "no answer"))
        rest = [n for n in nodes if n is not leader]
        check(wait_for(60, lambda: leader_of(rest, SPACE) is not None), "the other two elect a leader")
        top = leader_of(rest, SPACE).field(SPACE, "applied")
        check(wait_for(30, lambda: all(n.field(SPACE, "applied") >= top for n in rest)),
              "both have applied the new leader's log")
        # FS is a write command, so only the leader answers it; the digests say the
        # other member holds the same
        got = leader_of(rest, SPACE).client(SPACE).execute_command("FS", "GET", "/f.bin")
        seen = "nothing" if got is None else "".join(sorted(set(got)))
        check(got in (body(current), body(ch)), "the file reads as one version, whole (holds %s)" % seen)
        check(wait_for(30, lambda: len(set(n.digest(SPACE)[0] for n in rest)) == 1), "both members' digests match")
        if got == body(ch):
            current = ch
        print("node %d back" % (leader.i + 1))
        leader.start()
        check(wait_for(120, lambda: all_voting(nodes)), "three voters again")

    # a stored function's writes, the same way - TODO 673. It runs from the log as one
    # entry; before, each of its writes committed on its own, one held commit at a time
    print("a function that writes %d keys, its leader killed after %.2fs" % (KEYS, DELAY_MS * 3.5 / 1000))
    # a slow runner can move the lead between choosing the leader and the call; then
    # the call answers NOTLEADER without reaching it, and the round is tried again
    answered = []
    for attempt in range(5):
        leader = steady_leader(nodes)
        c = leader.client(SPACE)
        try:
            c.execute_command("SETF", "putall", PUTALL)
            c.execute_command("putall", "a")
        except (redis.exceptions.TryAgainError, redis.ResponseError) as e:
            # a leader whose lease no quorum has confirmed yet, under a slow runner
            if not isinstance(e, redis.exceptions.TryAgainError) and not str(e).startswith("NOTLEADER"):
                raise
            time.sleep(0.5)
            continue
        answered = []

        def put():
            try:
                answered.append(leader.client(SPACE).execute_command("putall", "b"))
            except redis.RedisError as e:
                answered.append(e)

        t = threading.Thread(target=put)
        t.start()
        time.sleep(DELAY_MS * 3.5 / 1000)
        # or one whose lease no quorum had confirmed yet, which is refused the same
        # way without reaching the log - TODO 735
        if answered and (isinstance(answered[0], redis.exceptions.TryAgainError)
                         or (isinstance(answered[0], redis.ResponseError)
                             and str(answered[0]).startswith("NOTLEADER"))):
            t.join(30)
            print("    the call went to a node that had lost the lead; again", flush=True)
            continue
        leader.kill()
        t.join(30)
        break
    check(answered and (isinstance(answered[0], redis.ConnectionError) or answered[0] == KEYS),
          "the call reached the leader (%s)" % (answered[0] if answered else "no answer"))
    rest = [n for n in nodes if n is not leader]
    check(wait_for(60, lambda: leader_of(rest, SPACE) is not None), "the other two elect a leader")
    top = leader_of(rest, SPACE).field(SPACE, "applied")
    check(wait_for(30, lambda: all(n.field(SPACE, "applied") >= top for n in rest)),
          "both have applied the new leader's log")
    got = [leader_of(rest, SPACE).client(SPACE).execute_command("GET", "k%d" % i) for i in range(KEYS)]
    check(len(set(got)) == 1, "its keys hold one call's values, all of them (%s)" % "".join(got))
    check(wait_for(30, lambda: len(set(n.digest(SPACE)[0] for n in rest)) == 1), "both members' digests match")
    leader.start()
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
