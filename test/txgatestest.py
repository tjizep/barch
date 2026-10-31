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
is the sync), in a space of 8 shards. While the file BARCH_TEST_TX_HALF_HOLD_FILE names
is there, commits stop halfway through their writes, and INFO transactions lists them
as waiting on it - TODO 800. Nothing is timed: who is held and who waits at the gate
says it all.
  - two commits on different shards are both halfway at once;
  - two on the same shard aren't: the second waits at the gate for the first;
  - one that read a range keeps the other one out;
  - an MGET waits while two are halfway, then sees each whole;
  - one that read a range, waiting for a commit on another shard, gets in before one
    that came after it on a third shard;
  - kill -9 while one commit is halfway, after another that started after it and
    finished: a restart has the finished one and none of the held one, though their
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
HOLD = os.path.join(os.getcwd(), "txgates_hold")   # commits stop halfway while it's there
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


def prepare(ks, value, range_first=False, c=None):
    # a client of its own unless given one, with its transaction begun and written, not
    # yet committed on the client itself: a pipeline takes another connection from the pool
    c = c or client()
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
    # INFO transactions' row for each transaction, as a dict of its fields
    rows = []
    for line in c.execute_command("INFO", "transactions").splitlines():
        if line.startswith("tx") and line[2:3].isdigit():
            rows.append(dict(f.split("=", 1) for f in line.split(":", 1)[1].split(",") if "=" in f))
    return rows


def gauge(c, name):
    for line in c.execute_command("INFO", "transactions").splitlines():
        if line.startswith(name + ":"):
            return int(line.split(":", 1)[1])
    return 0


def hold(*keys):
    # commits stop halfway while the file's there: every one, or the ones writing keys
    with open(HOLD, "w") as f:
        f.write("".join(k + "\n" for k in keys))


def let_go():
    os.remove(HOLD)


def held(row):
    return row.get("waiting") == "the test hold file"


def stopped(row):
    # waiting for another commit, at the gate or at its shards, not at the hold file
    return row.get("waiting", "") != "" and not held(row)


def until(watcher, ready, what):
    # polls INFO for an event; the minute is only so a broken run fails instead of hanging
    end = time.time() + 60
    while time.time() < end:
        rows = listed(watcher)
        if ready(rows):
            return rows
        time.sleep(0.01)
    raise AssertionError("never saw %s: %s" % (what, listed(watcher)))


def go(*ready):
    # COMMITTX on each prepared client, a thread each; the answers come in `answers`.
    # Prepare them all before a hold: a BEGINTX waits for a commit that's partway
    answers = [None] * len(ready)

    def run(i):
        answers[i] = finish(ready[i])
    ts = [threading.Thread(target=run, args=(i,), daemon=True) for i in range(len(ready))]
    for t in ts:
        t.start()
    return ts, answers


def join(ts):
    for t in ts:
        t.join(timeout=60)
    return not any(t.is_alive() for t in ts)


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
if os.path.exists(HOLD):
    os.remove(HOLD)
p = start({"BARCH_TEST_TX_HALF_HOLD_FILE": HOLD})
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

    # A COMMITTX held halfway blocks its connection's I/O thread, so does one waiting
    # for another commit, and barchd hands new connections its I/O threads in turn, at
    # least 4 of them. So the watcher and three more are opened here, one after another,
    # each on a thread of its own, and every commit that's held or waits uses one of the
    # three. A new connection per commit came round to the watcher's thread, and its
    # INFO waited for the hold
    w = client()                        # watches, with INFO's text as it comes
    w.response_callbacks.pop("INFO", None)
    x, y, z = client(), client(), client()

    print("two commits on different shards")
    ready = prepare(a, "a1", c=x), prepare(b, "b1", c=y)
    hold()
    ts, answers = go(*ready)
    # one after the other, the second waits for good; a wait on the way that passes
    # can show up in the listing, so only both held settles it
    try:
        rows = until(w, lambda rs: sum(held(x) for x in rs) == 2, "both commits held")
        check(True, "both halfway at once, not one after the other")
    except AssertionError as e:
        check(False, "both halfway at once, not one after the other (%s)" % e)
    let_go()
    check(join(ts) and answers == ["OK", "OK"], "both commit (%s)" % answers)
    check(c.execute_command("MGET", *(a + b)) == ["a1"] * 2 + ["b1"] * 2, "and both are there")

    print("two commits on the same shard")
    ca, cd = prepare(a, "a2", c=x), prepare(d, "d2", c=y)
    hold()
    ts, answers = go(ca)
    until(w, lambda rs: sum(held(x) for x in rs) == 1, "the first commit held")
    t2, a2 = go(cd)
    rows = until(w, lambda rs: any(stopped(x) for x in rs) or sum(held(x) for x in rs) == 2,
                 "the second one stop")
    check(sum(held(x) for x in rows) == 1 and sum(stopped(x) for x in rows) == 1,
          "the second waits for the first (%s)" % [x.get("waiting") for x in rows])
    let_go()
    check(join(ts + t2) and answers + a2 == ["OK", "OK"], "both commit (%s)" % (answers + a2))
    check(c.execute_command("MGET", *(a + d)) == ["a2"] * 2 + ["d2"] * 2, "and both are there")

    print("a commit that walked a range")
    ca, cb = prepare(a, "a3", True, c=x), prepare(b, "b3", c=y)
    hold()
    ts, answers = go(ca)
    until(w, lambda rs: sum(held(x) for x in rs) == 1, "the ranged commit held")
    t2, a2 = go(cb)
    rows = until(w, lambda rs: any(stopped(x) for x in rs) or sum(held(x) for x in rs) == 2,
                 "the other one stop")
    check(sum(held(x) for x in rows) == 1 and sum(stopped(x) for x in rows) == 1,
          "keeps the other out (%s)" % [x.get("waiting") for x in rows])
    let_go()
    check(join(ts + t2) and answers + a2 == ["OK", "OK"], "both commit (%s)" % (answers + a2))

    print("an MGET while two are halfway")
    ready = prepare(a, "a4", c=x), prepare(b, "b4", c=y)
    hold()
    ts, answers = go(*ready)
    until(w, lambda rs: sum(held(x) for x in rs) == 2, "both commits held")
    seen = []
    reader = threading.Thread(target=lambda: seen.append(z.execute_command("MGET", *(a + b))),
                              daemon=True)
    waiting_reads = gauge(w, "tx_gate_reads_waiting")
    reader.start()
    # a read waits at the gate while a commit is in it
    end = time.time() + 60
    while gauge(w, "tx_gate_reads_waiting") == waiting_reads and reader.is_alive() and time.time() < end:
        time.sleep(0.01)
    check(not seen, "it waits for them (%s)" % (seen[0] if seen else None))
    let_go()
    reader.join(timeout=60)
    check(join(ts) and answers == ["OK", "OK"], "both commit (%s)" % answers)
    check(seen == [["a4"] * 2 + ["b4"] * 2], "and then sees both whole (%s)" % (seen[0] if seen else None))

    print("a commit that walked a range, while others keep committing")
    # a's commit holds its shard; one that needs every shard waits for it, and b's,
    # on a shard nobody holds, comes after and has to wait behind the ranged one
    ca, cr, cb = prepare(a, "a5", c=x), prepare(by[2][:1], "ranged", True, c=y), prepare(b, "b5", c=z)
    hold(a[0])
    ts, answers = go(ca)
    until(w, lambda rs: sum(held(x) for x in rs) == 1, "a's commit held")
    tr, ranged = go(cr)
    until(w, lambda rs: sum(stopped(x) for x in rs) == 1, "the ranged one waiting")
    tb, bs = go(cb)
    rows = until(w, lambda rs: sum(stopped(x) for x in rs) == 2 or bs[0] is not None, "b's commit stop")
    check(bs[0] is None and sum(stopped(x) for x in rows) == 2, "a later one on another shard waits too (%s)" % bs)
    # now the ranged one holds halfway instead of a: if b went ahead of it, b is done
    hold(by[2][0])
    rows = until(w, lambda rs: sum(held(x) for x in rs) == 1 and answers[0] is not None, "the ranged one held")
    check(answers == ["OK"], "a's commit goes (%s)" % answers)
    check(bs[0] is None and sum(stopped(x) for x in rows) == 1, "the ranged one gets in before b (%s, %s)"
          % (bs, [x.get("waiting") for x in rows]))
    let_go()
    check(join(ts + tr + tb) and ranged + bs == ["OK", "OK"], "all commit (%s)" % (ranged + bs))
    check(c.execute_command("MGET", by[2][0]) == ["ranged"], "and it's there")

    print("kill -9 with one commit halfway, after another that finished")
    # b's commit stops halfway, then a's starts after it and finishes: their records
    # are mixed in the log, and a's marker comes after b's first writes
    b_was = c.execute_command("MGET", *b)
    cb, ca = prepare(b, "b6", c=x), prepare(a, "a6", c=y)
    hold(b[0])
    tb, bs = go(cb)
    until(w, lambda rs: sum(held(x) for x in rs) == 1, "b's commit held")
    check(finish(ca) == "OK", "the other one commits meanwhile")
    kill(p)
    let_go()
    p = start()
    c = client()
    got = c.execute_command("MGET", *(a + b))
    check(got == ["a6"] * 2 + b_was, "a restart has the first and none of the second (%s)" % got)
    check(commit(b, "b7") == "OK" and c.execute_command("MGET", *b) == ["b7"] * 2, "and a new one commits")
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
