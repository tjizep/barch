#!/usr/bin/env python3
"""
Transactions on a range sharded space - TODO 771.

BEGINTX used to refuse one: a transaction's overlay reads the space shard by shard, so
both have to put a key in the same shard, and the rebalancer keeps moving the spans.
Now the overlay takes the space's boundaries, as a range sharded dependant does (TODO
716), and the space doesn't rebalance while a transaction is open.

One barchd, a space over 8 shards with its keys spread over all of them:
  - BEGINTX answers OK;
  - while another client changes, deletes and adds keys after BEGINTX, and writes a
    lopsided batch into the top shard: the transaction reads what it had at BEGINTX,
    with its own writes over it, through GET, MGET, RANGE, COUNT, a script's count and
    every offset tried;
  - the space doesn't rebalance while the transaction is open, and does again after
    ABORTTX;
  - one that commits: its writes, deletes and adds are in the space, every key still
    found after the space rebalances;
  - one whose read key another client changed, and one with a key written into a range
    it walked, are each a TXCONFLICT;
  - a savepoint whose function raises takes back what it wrote;
  - a pessimistic one: another client's write to a key it read, or into a range it
    walked, waits for COMMITTX and then goes in;
  - fifty in a row, each a commit, with the space rebalancing between them: all of it
    reads right, so an overlay used again follows the boundaries as they are now;
  - in a Raft space (one node, range sharded), one commits and one conflicts. A build
    without the cluster skips this;
  - over 20,000 keys and 17 shards, an offset near the end and a count over all of them
    cost about what they do outside a transaction. The walk goes shard by shard through
    each shard's chain now, as it does outside one, instead of merging every chain.
"""
import os
import socket
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
PORT = scale.port(default=14831)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-74s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


TAG = str(int(time.time()))
SPACE = "txranged_" + TAG
KEYS = ["k%05d" % i for i in range(4000)]
WAIT = scale.scaled_seconds(30.0, floor=10.0)
FUNCTIONS = {
    "sn": "function call(k) return barch.store.shardNumber(k) end\n",
    "cnt": "function call(lo, hi) return barch.store.count(lo, hi) end\n",
    "page": """
function call(lo, hi, n, offset)
    return table.concat(barch.store.range(lo, hi, tonumber(n), tonumber(offset)), ",")
end""",
    "sp": """
function call(k)
    barch.store.set(k, 'kept')
    pcall(function()
        barch.tx.savepoint(function()
            barch.store.set(k, 'taken back')
            barch.store.set(k .. 'x', 'taken back')
            error('no')
        end)
    end)
    return barch.store.get(k)
end""",
}

DATA = os.path.join(os.getcwd(), "txranged_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txranged.log"), "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
node = None


def ranged(conf, name, shards):
    for k, v in ((".ordered", "1"), (".shards", str(shards)), (".range_sharded", "1")):
        conf.execute_command("configuration:SET", name + k, v)


def put(c, keys, value):
    for i in range(0, len(keys), 500):
        c.execute_command("MSET", *[x for k in keys[i:i + 500] for x in (k, value)])


def gets(c, keys):
    out = []
    for i in range(0, len(keys), 500):
        out += c.execute_command("MGET", *keys[i:i + 500])
    return out


def run_tests(port):
    def client():
        c = redis.Redis(host="127.0.0.1", port=port, socket_timeout=60, decode_responses=True,
                        single_connection_client=True)
        c.execute_command("USE", SPACE)
        return c

    c, other = client(), client()
    for name, body in FUNCTIONS.items():
        check(answer(c, "SETF", name, body) == "OK", "SETF %s" % name)

    def shard(k):
        return int(other.execute_command("CALLF", "sn", k))

    def layout(keys):
        return [shard(k) for k in keys]

    def spread(lo, hi):
        deadline = time.time() + WAIT
        while time.time() < deadline:
            if shard(lo) == 0 and shard(hi) == 7:
                return True
            time.sleep(0.2)
        return False

    put(c, KEYS, "v")
    check(spread(KEYS[0], KEYS[-1]), "the space spread its keys over its 8 shards")
    model = {k: "v" for k in KEYS}

    print("reading")
    got = answer(c, "BEGINTX")
    check(got in ("OK", True), "BEGINTX answers OK (%s)" % str(got)[:90])
    if got not in ("OK", True):
        return
    other.set(KEYS[5], "changed")
    other.delete(KEYS[6])
    other.set("k02000x", "added")
    other.delete(KEYS[3000])
    before = layout(KEYS[::250])
    TOP = ["z%05d" % i for i in range(8000)]           # all in the top shard
    put(other, TOP, "top")
    seen = dict(model)
    for cmd in [("SET", KEYS[10], "mine"), ("SET", "k00010a", "new"), ("SET", "k03456b", "new"),
                ("DEL", KEYS[11]), ("DEL", "k00011q"), ("SET", "k00500t", "tmp"), ("DEL", "k00500t"),
                ("DEL", KEYS[600]), ("SET", KEYS[600], "again"), ("DEL", KEYS[-1]), ("SET", "k99999", "past")]:
        c.execute_command(*cmd)
    seen.update({KEYS[10]: "mine", "k00010a": "new", "k03456b": "new", KEYS[600]: "again", "k99999": "past"})
    del seen[KEYS[11]], seen[KEYS[-1]]
    order = sorted(seen)
    asked = order + ["k02000x", KEYS[11], KEYS[-1], "k00500t", "z00000"]
    got = gets(c, asked)
    wrong = [k for k, v in zip(asked, got) if v != seen.get(k)]
    check(not wrong, "MGET of %d keys sees BEGINTX's space and its own writes (%d wrong, %s)"
          % (len(asked), len(wrong), wrong[:3]))
    check(c.get(KEYS[5]) == "v" and c.get(KEYS[6]) == "v", "GET doesn't see the other client's writes")
    got = c.execute_command("RANGE", "k", "l", 100000)
    check(got == order, "RANGE k l is every key it should see (%d of %d)" % (len(got), len(order)))
    for lo, hi in [("k", "l"), ("k00000", "k00012"), ("k005", "k007"), ("k02000", "k02001"),
                   ("k03456", "k03457"), ("k03990", "k99999"), ("k03990", "k999999"), ("k1", "k2"), ("a", "{")]:
        want = sum(1 for k in order if lo <= k < hi)
        got_c = answer(c, "CALLF", "cnt", lo, hi)
        got_n = answer(c, "COUNT", lo, hi)
        check(got_c == want and got_n == want, "count [%s, %s) is %d (%s, COUNT %s)" % (lo, hi, want, got_c, got_n))
    bad = []
    near = []
    for k in ["k00010", "k00010a", "k00011", "k00500t", "k00600", "k02000", "k02000x", "k03000",
              "k03456b", "k03998", "k99999"]:
        at = sum(1 for o in order if o < k)
        near += [at + d for d in range(-3, 4)]
    tried = sorted(set(o for o in list(range(0, 25)) + near + list(range(250, 262))
                       + list(range(len(order) - 5, len(order) + 1)) + list(range(0, len(order), 97))
                       if o >= 0))
    for off in tried:
        got = answer(c, "CALLF", "page", "k", "l", 3, off)
        want = ",".join(order[off:off + 3])
        if got != want:
            bad.append((off, want, got))
    check(not bad, "%d offsets from k agree with the walk %s" % (len(tried), bad[:2]))
    sub = [k for k in order if "k00300" <= k < "k00900"]
    bad = [o for o in range(0, len(sub) + 1)
           if answer(c, "CALLF", "page", "k00300", "k00900", 2, o) != ",".join(sub[o:o + 2])]
    check(not bad, "every offset in [k00300, k00900) agrees %s" % bad[:3])
    time.sleep(scale.scaled_seconds(4.0, floor=3.0))
    check(layout(KEYS[::250]) == before and shard(TOP[-1]) == 7,
          "the space doesn't rebalance while the transaction is open")
    got = gets(c, asked)
    check(got == [seen.get(k) for k in asked], "and the transaction still reads what it did")
    check(answer(c, "ABORTTX") in ("OK", True), "ABORTTX")
    model.update({KEYS[5]: "changed", "k02000x": "added"})
    del model[KEYS[6]], model[KEYS[3000]]
    model.update({k: "top" for k in TOP})
    deadline = time.time() + WAIT
    while time.time() < deadline and layout(KEYS[::250]) == before:
        time.sleep(0.2)
    check(layout(KEYS[::250]) != before, "the space rebalances again after ABORTTX")
    every = sorted(model)
    got = gets(other, every + ["k00010a", "k99999"])
    check(got == [model.get(k) for k in every] + [None, None], "the space has the other client's writes only")

    print("committing")
    c.execute_command("BEGINTX")
    writes = {KEYS[20]: "tx", "k00020a": "tx", "a0": "tx", "zz": "tx", "k01500": "tx"}
    for k, v in writes.items():
        c.set(k, v)
    c.delete(KEYS[21], KEYS[3500])
    other.set("y0", "other")                           # a key the transaction never saw
    check(answer(c, "COMMITTX") in ("OK", True), "COMMITTX answers OK")
    model.update(writes)
    model["y0"] = "other"
    del model[KEYS[21]], model[KEYS[3500]]
    every = sorted(model)
    got = gets(other, every + [KEYS[21], KEYS[3500]])
    wrong = [k for k, v in zip(every + [KEYS[21], KEYS[3500]], got) if v != model.get(k)]
    check(not wrong, "every key is as the commit left it (%d wrong, %s)" % (len(wrong), wrong[:3]))
    before = layout(every[::300])
    put(other, ["a%05d" % i for i in range(6000)], "low")   # lopsided the other way
    model.update({"a%05d" % i: "low" for i in range(6000)})
    deadline = time.time() + WAIT
    while time.time() < deadline and layout(every[::300]) == before:
        time.sleep(0.2)
    time.sleep(1)
    every = sorted(model)
    got = gets(other, every)
    wrong = [k for k, v in zip(every, got) if v != model.get(k)]
    check(not wrong, "and still found after the space rebalanced (%d wrong, %s)" % (len(wrong), wrong[:3]))

    print("conflicts")
    c.execute_command("BEGINTX")
    c.get(KEYS[100])
    other.set(KEYS[100], "first")
    c.set(KEYS[100], "second")
    got = answer(c, "COMMITTX")
    check("TXCONFLICT" in str(got), "a key it read and another client changed (%s)" % str(got)[:60])
    c.execute_command("BEGINTX")
    c.execute_command("RANGE", "k01000", "k01010", 100)
    other.set("k01005x", "phantom")
    c.set("y1", "mine")
    got = answer(c, "COMMITTX")
    check("TXCONFLICT" in str(got), "a key written into a range it walked (%s)" % str(got)[:60])
    model.update({KEYS[100]: "first", "k01005x": "phantom"})
    check(other.get("y1") is None, "and neither wrote anything")

    print("a savepoint")
    c.execute_command("BEGINTX")
    got = answer(c, "CALLF", "sp", "s0")
    check(got == "kept", "the function's writes after the savepoint are taken back (%s)" % got)
    check(answer(c, "COMMITTX") in ("OK", True), "COMMITTX")
    check(other.get("s0") == "kept" and other.get("s0x") is None, "only the write before it is in")
    model["s0"] = "kept"

    print("pessimistic")
    for what, read, write in (("a key it read", ("GET", KEYS[200]), ("SET", KEYS[200], "waited")),
                              ("a range it walked", ("RANGE", "k01200", "k01210", 100),
                               ("SET", "k01205x", "waited"))):
        c.execute_command("BEGINTX", "PESSIMISTIC")
        c.execute_command(*read)
        done = {}

        def write_it():
            w = client()
            t = time.time()
            done["got"] = answer(w, *write)
            done["at"] = time.time() - t

        th = threading.Thread(target=write_it)
        th.start()
        time.sleep(0.5)
        waiting = th.is_alive()
        check(answer(c, "COMMITTX") in ("OK", True), "COMMITTX")
        th.join(10)
        check(waiting and done.get("got") in ("OK", True) and other.get(write[1]) == "waited",
              "a write to %s waits for COMMITTX, then goes in (%s after %.2fs)"
              % (what, done.get("got"), done.get("at", -1)))
        model[write[1]] = "waited"

    print("one after another")
    lots = ["b%05d" % i for i in range(3000)]
    bad = []
    for i in range(50):
        c.execute_command("BEGINTX")
        k = KEYS[(i * 79) % len(KEYS)]
        got = c.get(k)
        if got != model.get(k):
            bad.append((i, k, got, model.get(k)))
        c.set("t%03d" % i, k)
        c.execute_command("COMMITTX")
        model["t%03d" % i] = k
        if i % 10 == 0:                                 # something to move each time
            batch = lots[i * 60:(i + 10) * 60]
            put(other, batch, "b")
            model.update({b: "b" for b in batch})
            time.sleep(0.3)
    check(not bad, "fifty transactions each read the right value %s" % bad[:2])
    every = sorted(model)
    got = gets(other, every)
    wrong = [k for k, v in zip(every, got) if v != model.get(k)]
    check(not wrong, "and the space has every key right after them (%d wrong, %s)" % (len(wrong), wrong[:3]))


def median_ms(c, *cmd, runs=31):
    took = []
    for _ in range(runs):
        t = time.perf_counter()
        c.execute_command(*cmd)
        took.append((time.perf_counter() - t) * 1000)
    took.sort()
    return took[len(took) // 2]


def timing(port, conf):
    """a far offset and a count in a transaction cost about what they do outside one"""
    print("timing")
    space = SPACE + "_t"
    ranged(conf, space, 17)

    def client():
        c = redis.Redis(host="127.0.0.1", port=port, socket_timeout=60, decode_responses=True,
                        single_connection_client=True)
        c.execute_command("USE", space)
        return c

    c, other = client(), client()
    for name, body in FUNCTIONS.items():
        check(answer(c, "SETF", name, body) == "OK", "SETF %s" % name)
    n = 20000
    keys = ["c%05d" % i for i in range(n)]
    put(c, keys, "v")
    # until the rebalancer has spread them and stopped moving them
    sample = keys[::250]
    last, still, deadline = None, 0, time.time() + WAIT
    while time.time() < deadline and still < 6:
        now = [int(other.execute_command("CALLF", "sn", k)) for k in sample]
        still = still + 1 if now == last else 0
        last = now
        time.sleep(0.5)
    check(last is not None and last[0] == 0 and last[-1] == 16, "the space spread its keys over its 17 shards")
    plain_far = median_ms(other, "CALLF", "page", "c", "d", 3, n - 100)
    plain_count = median_ms(other, "CALLF", "cnt", "c", "d")
    c.execute_command("BEGINTX")
    for cmd in [("SET", "c00010a", "new"), ("DEL", keys[11]), ("SET", "c12345b", "new"), ("DEL", keys[-1])]:
        c.execute_command(*cmd)
    other.set("c05000x", "added")
    tx_far = median_ms(c, "CALLF", "page", "c", "d", 3, n - 100)
    tx_count = median_ms(c, "CALLF", "cnt", "c", "d")
    want = sorted(set(keys) - {keys[11], keys[-1]} | {"c00010a", "c12345b"})
    check(answer(c, "CALLF", "page", "c", "d", 3, n - 100) == ",".join(want[n - 100:n - 97]),
          "the far offset is right")
    c.execute_command("ABORTTX")
    # 0.5 ms against 0.07 before the walk went shard by shard through the chains
    check(tx_far < plain_far * 3 + 0.15, "an offset near the end costs about what it does outside (%.3f ms against %.3f)"
          % (tx_far, plain_far))
    check(tx_count < plain_count * 3 + 0.15, "a count over all of them too (%.3f ms against %.3f)"
          % (tx_count, plain_count))


try:
    end = time.time() + 60
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            break
        except OSError:
            time.sleep(0.1)
    conf = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True)
    ranged(conf, SPACE, 8)
    print("a plain space")
    run_tests(PORT)
    timing(PORT, conf)

    node = clusternodes.Node(0, PORT + 1, BINARY)
    node.start()
    init = answer(node.client(), "CLUSTER", "INIT")
    if "no cluster" in str(init):
        print("a Raft space: SKIP, this build has no cluster")
    else:
        print("a Raft space")
        rconf = node.client("configuration")
        for k, v in ((".ordered", "1"), (".shards", "4"), (".range_sharded", "1"), (".raft", "on")):
            rconf.execute_command("SET", SPACE + k, v)
        ok = wait_for(60, lambda: leader_of([node], SPACE) is not None)
        check(ok, "the Raft space has a leader")
        if ok:
            c, other = node.client(SPACE), node.client(SPACE)
            put(c, KEYS[:1000], "v")
            got = answer(c, "BEGINTX")
            check(got in ("OK", True), "BEGINTX answers OK (%s)" % str(got)[:90])
            if got in ("OK", True):
                c.set("k00001", "tx")
                c.delete("k00002")
                c.set("a0", "tx")
                check(c.execute_command("COUNT", "a", "l") == 1000, "COUNT sees its own writes")
                check(answer(c, "COMMITTX") in ("OK", True), "COMMITTX answers OK")
                check(other.mget("k00001", "k00002", "a0", "k00003") == ["tx", None, "tx", "v"],
                      "the space has its writes")
                c.execute_command("BEGINTX")
                c.get("k00010")
                other.set("k00010", "first")
                c.set("k00010", "second")
                got = answer(c, "COMMITTX")
                check("TXCONFLICT" in str(got), "a key it read and another client changed (%s)" % str(got)[:60])
finally:
    p.kill()
    p.wait()
    if node:
        node.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
