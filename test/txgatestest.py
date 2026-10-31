#!/usr/bin/env python3
"""
Commits on different shards at once - TODO 775.

COMMITTX in a space without Raft held the space's commit gate alone, from the conflict
check through the sync of its commit marker. So every commit in a space ran one after
another, and with a change log that waited on the disk each time: about 215 commits a
second on this machine, against about 30k without a log.

Now a commit that touches only some shards holds the gate in commit mode, which other
commits share, and the write latches of its shards, so commits on different shards run
at once and commits on the same shard still go one after another. A commit that walked
a range, or one in a range sharded space, still holds the gate alone. Read commands and
BEGINTX hold the gate shared, which no commit shares, so they still see a commit whole
or not at all. And commits that overlap share one sync of the change log: the first to
get there syncs for everyone who has written by then.

Checked against one barchd with a change log (timer durability, so each commit's marker
is the sync), in a space of 8 shards, with BARCH_TEST_TX_HALF_PAUSE_MS pausing each
commit halfway through its writes:
  - two commits on different shards finish together, not one after the other;
  - two on the same shard still take twice as long;
  - one that read a range keeps the other one out;
  - an MGET while two are paused sees each whole;
  - one that read a range gets in while two clients keep committing on other shards,
    their commits overlapping so one of them always has its shard. It waited for them
    to stop;
  - kill -9 while one commit is paused, after another that started before it and
    finished: a restart has the finished one and none of the paused one, though their
    records are mixed in the log.
Then, with BARCH_TEST_AOF_SYNC_DELAY_MS making each sync slow, several clients on
different shards: more than one commit for each sync (change_log_syncs in INFO
transactions). It was one each.
"""
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=19300)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SANITIZED = bool(os.environ.get("BARCH_SANITIZE"))
DATA = os.path.join(os.getcwd(), "txgates_data")
LOGS = os.path.join(os.getcwd(), "txgates_logs")
SPACE = "g"
SHARDS = 8
PAUSE = 0.8         # seconds, each commit's pause halfway
ARGS = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000",
        "-c", "aof_dir=" + LOGS, "-c", "aof_durability=timer"]
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def start(env=None):
    log = open(os.path.join(os.getcwd(), "txgates.log"), "a")
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA,
                          "--no-save-on-exit"] + ARGS, stdout=log, stderr=log,
                         env=dict(os.environ, **(env or {})))
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            raise AssertionError("barchd exited with %s" % p.returncode)
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % PORT)


def kill(p):
    p.send_signal(signal.SIGKILL)
    p.wait(timeout=30)


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", SPACE)
    return c


def prepare(ks, value, range_first=False):
    # a client of its own, with its transaction begun and written, not yet committed
    # on the client itself: a pipeline takes another connection from the pool
    c = client()
    c.execute_command("BEGINTX")
    if range_first:
        c.execute_command("RANGE", "r0", "r9")
    for k in ks:
        c.set(k, value)
    return c


def finish(c):
    try:
        return c.execute_command("COMMITTX")
    except redis.ResponseError as e:
        try:
            c.execute_command("ABORTTX")
        except redis.ResponseError:
            pass
        return "ERR " + str(e)


def commit(ks, value, range_first=False):
    return finish(prepare(ks, value, range_first))


def listed(c):
    # INFO transactions' row for each transaction: what it holds and waits on
    return [line for line in c.execute_command("INFO", "transactions").splitlines()
            if line.startswith("tx") and line[2:3].isdigit()]


def at_once(*jobs, look=None):
    # each job a commit's arguments, all begun and written first, since a BEGINTX waits
    # for a commit partway: how long until the COMMITTXs, sent 50 ms apart, are done.
    # With look, each one's own time, and what INFO transactions lists halfway through
    # the pause, go in it
    answers = [None] * len(jobs)
    ready = [prepare(*j) for j in jobs]
    own = [None] * len(jobs)
    # opened after the others, so they get the I/O threads they would without it
    watcher = None
    if look is not None:
        watcher = client()
        watcher.response_callbacks.pop("INFO", None)

    def run(i):
        t0 = time.time()
        answers[i] = finish(ready[i])
        own[i] = time.time() - t0
    ts = [threading.Thread(target=run, args=(i,), daemon=True) for i in range(len(jobs))]
    began = time.time()
    for t in ts:
        t.start()
        time.sleep(0.05)
    if watcher is not None:
        time.sleep(max(0.0, began + PAUSE / 2 - time.time()))
        try:
            look["halfway"] = listed(watcher)
        except redis.RedisError as e:
            look["halfway"] = ["ERR " + str(e)]
    for t in ts:
        t.join(timeout=60)
    took = time.time() - began
    if look is not None:
        look["own"] = own
    return took, answers


def keys_by_shard(c, want=4):
    # a few keys in each shard, found by asking: INFO SHARD says where a key lands
    found = {n: [] for n in range(SHARDS)}
    i = 0
    while any(len(v) < want for v in found.values()) and i < 10000:
        k = "k%d" % i
        n = int(c.execute_command("INFO", "SHARD", k)["number"])
        if len(found[n]) < want:
            found[n].append(k)
        i += 1
    return found


def stats(c):
    got = c.execute_command("INFO", "transactions")
    return {k: v for k, v in got.items() if isinstance(v, int)}


for d in (DATA, LOGS):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
p = start({"BARCH_TEST_TX_HALF_PAUSE_MS": str(int(PAUSE * 1000))})
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60)
    r.execute_command("configuration:SET", SPACE + ".shards", str(SHARDS))
    r.execute_command("configuration:SET", SPACE + ".aof", "on")
    r.execute_command("configuration:SAVE")     # the settings themselves, across a kill
    c = client()
    by = keys_by_shard(c)
    check(all(len(v) == 4 for v in by.values()), "four keys found in each of the %d shards" % SHARDS)
    a, b, d = by[0][:2], by[1][:2], by[0][2:4]
    every = a + b + d
    check(commit(every, "old") == "OK", "a first commit, of all of them")

    print("two commits on different shards")
    look = {}
    took, answers = at_once((a, "a1"), (b, "b1"), look=look)
    check(answers == ["OK", "OK"], "both commit (%s)" % answers)
    # TODO 800: on Windows this pair has taken about three pauses, where the pairs below
    # take their two, and not under wine. What each was doing halfway, to see why
    print("    each one's own time: %s" % ", ".join("%.2f s" % s for s in look["own"] if s is not None))
    for row in look["halfway"]:
        print("    halfway: " + row[:300])
    # one after the other is two pauses and the 50 ms between them; at once, one pause
    check(took < PAUSE * 1.6, "together, in %.2f s, not one after the other" % took)
    check(c.execute_command("MGET", *(a + b)) == ["a1"] * 2 + ["b1"] * 2, "and both are there")

    print("two commits on the same shard")
    took, answers = at_once((a, "a2"), (d, "d2"))
    check(answers == ["OK", "OK"], "both commit (%s)" % answers)
    check(took >= PAUSE * 1.9, "one after the other, in %.2f s" % took)

    print("a commit that walked a range")
    took, answers = at_once((a, "a3", True), (b, "b3"))
    check(answers == ["OK", "OK"], "both commit (%s)" % answers)
    check(took >= PAUSE * 1.9, "keeps the other out, %.2f s for both" % took)

    print("an MGET while two are paused")
    seen = []

    def read_while():
        time.sleep(PAUSE / 2)
        seen.append(client().execute_command("MGET", *(a + b)))
    reader = threading.Thread(target=read_while, daemon=True)
    reader.start()
    took, answers = at_once((a, "a4"), (b, "b4"))
    reader.join(timeout=60)
    check(answers == ["OK", "OK"], "both commit (%s)" % answers)
    # each one whole or not at all; at once, both are paused when it reads
    check(seen and seen[0][:2] in (["a3"] * 2, ["a4"] * 2) and seen[0][2:] in (["b3"] * 2, ["b4"] * 2),
          "sees each whole or not at all (%s)" % (seen[0] if seen else None))

    print("a commit that walked a range, while others keep committing")
    # two clients, each committing on its own shard again and again, half a pause apart,
    # so a shard is always claimed. One that needs every shard has them stand back
    keep_going = time.time() + PAUSE * 10
    looped = {"a": 0, "b": 0, "other": []}

    def loop(name, ks):
        while time.time() < keep_going:
            got = commit(ks, name + str(looped[name]))
            if got == "OK":
                looped[name] += 1
            else:
                looped["other"].append(got)
    la = threading.Thread(target=loop, args=("a", a), daemon=True)
    lb = threading.Thread(target=loop, args=("b", b), daemon=True)
    la.start()
    time.sleep(PAUSE / 2)
    lb.start()
    time.sleep(PAUSE)
    # its RANGE waits for the latches of the shards being committed, so only the COMMITTX
    rc = prepare(by[2][:1], "ranged", True)
    began = time.time()
    ranged = finish(rc)
    ranged_took = time.time() - began
    la.join(timeout=60)
    lb.join(timeout=60)
    check(ranged == "OK" and not looped["other"], "all commit (%s, %s)" % (ranged, looped["other"][:3]))
    # in: the two finish what they have, then its own pause, so about two pauses.
    # Waiting for them to stop is more like five
    check(ranged_took < PAUSE * 3, "among them, in %.2f s" % ranged_took)
    check(c.execute_command("MGET", by[2][0]) == ["ranged"], "and it's there")

    print("kill -9 with one commit paused, after another that finished")
    b_was = c.execute_command("MGET", *b)
    done = {}
    ca, cb = prepare(a, "a5"), prepare(b, "b5")

    def first():
        done["a"] = finish(ca)
    ta = threading.Thread(target=first, daemon=True)
    tb = threading.Thread(target=lambda: finish(cb), daemon=True)
    ta.start()
    time.sleep(PAUSE / 4)
    tb.start()
    ta.join(timeout=60)
    # the second is still in its pause, a quarter of one behind the first
    kill(p)
    check(done.get("a") == "OK", "the first one said OK before the kill (%s)" % done.get("a"))
    p = start()
    c = client()
    got = c.execute_command("MGET", *(a + b))
    check(got == ["a5"] * 2 + b_was, "a restart has the first and none of the second (%s)" % got)
    check(commit(b, "b6") == "OK" and c.execute_command("MGET", *b) == ["b6"] * 2, "and a new one commits")
    kill(p)

    print("commits on different shards share a sync")
    # each sync takes at least this long, so commits that come in meanwhile wait for the next
    p = start({"BARCH_TEST_AOF_SYNC_DELAY_MS": "20"})
    c = client()
    before = stats(c)
    counts = [0] * SHARDS
    other = []
    run_for = 6.0 if SANITIZED else 2.0
    stop = time.time() + run_for

    def work(n):
        while time.time() < stop:
            got = commit(by[n][:1], "w%d" % counts[n])
            if got == "OK":
                counts[n] += 1
            else:
                other.append(got)
    ts = [threading.Thread(target=work, args=(n,), daemon=True) for n in range(SHARDS)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=60)
    after = stats(c)
    commits = after.get("tx_commits", 0) - before.get("tx_commits", 0)
    syncs = after.get("change_log_syncs", 0) - before.get("change_log_syncs", 0)
    print("    %d commits, %d syncs" % (commits, syncs))
    check(not other, "every one commits (%s)" % other[:3])
    check(commits == sum(counts) and commits > 0, "every commit counted (%d of %d)" % (commits, sum(counts)))
    check(syncs > 0 and commits / syncs > 1.5, "more than one commit a sync (%.2f)" % (commits / max(syncs, 1)))
    got = c.execute_command("MGET", *[by[n][0] for n in range(SHARDS)])
    check(got == ["w%d" % (counts[n] - 1) for n in range(SHARDS)], "each client's last commit is there")
    kill(p)
    p = start()
    got = client().execute_command("MGET", *[by[n][0] for n in range(SHARDS)])
    check(got == ["w%d" % (counts[n] - 1) for n in range(SHARDS)], "and after kill -9 too")
finally:
    if p.poll() is None:
        kill(p)

print("%s" % ("FAILED %d" % failures if failures else "all passed"))
sys.exit(1 if failures else 0)
