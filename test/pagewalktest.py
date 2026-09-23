# The page walk for backups - TODO 416 - and ROLLBACK - TODO 417.
#
# Every page of a space's leaf or node arena, as (shard, page, bytes): barch.Pages
# from here, barch.store.leaves / barch.store.nodes from Luau. Inside BEGIN ...
# COMMIT the walk sees the pages as they stood at BEGIN while writes carry on, which
# is what lets a backup copy a space without stopping it.
#
# One shard, so the space has several leaf pages in the same shard - a COMMIT in the
# middle of a walk only shows when the walk has more of that shard still to read.
import struct
import threading

import scale

import redis
import barch

scale.workdir()
PORT = scale.port(default=14470)

print("start page walk test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

conf = barch.KeyValue("configuration")
conf.set("pw.shards", "1")


def conn():
    c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
    c.execute_command("USE", "pw")
    return c


r = conn()
r.execute_command("FLUSHDB")
kv = barch.KeyValue("pw")

failures = []


def check(ok, what):
    if not ok:
        print("  FAIL " + what, flush=True)
        failures.append(what)


def walk(nodes):
    p = barch.Pages("pw", nodes)
    out = []
    while p.next():
        out.append((p.shard(), p.page(), p.data()))
    return out, p.error()


def page_table(buf):
    """page -> write position, off the end of a freeList buffer: a count, then a
    32 byte record per page (page, fragmentation, 0, size, ticker, write position)"""
    for n in range(0, 100000):
        at = len(buf) - 8 - 32 * n
        if at < 0:
            return None
        if struct.unpack_from("<Q", buf, at)[0] == n:
            out = {}
            for k in range(n):
                page, frag, zero, size, ticker, wp = struct.unpack_from("<QIIIQI", buf, at + 8 + 32 * k)
                out[page] = wp
            return out
    return None


N = 1000
PAD = "x" * 2000
for i in range(N):
    r.set("pw:%05d" % i, "value-%d-%s" % (i, PAD))

# --- outside a transaction: the pages are the live ones --------------------------
leaves, err = walk(False)
check(err == "", "leaf walk error %r" % err)
check(len(leaves) > 1, "a 2MB space in one shard should have several leaf pages, got %d" % len(leaves))
check(all(isinstance(d, bytes) and len(d) > 0 for _, _, d in leaves), "pages are non-empty bytes")
check(all(s == 0 for s, _, _ in leaves), "one shard, so every page is shard 0")
check([pg for _, pg, _ in leaves] == sorted(pg for _, pg, _ in leaves), "pages come in page order")
blob = b"".join(d for _, _, d in leaves)
check(all(("value-%d-" % i).encode() in blob for i in range(0, N, 37)), "every value is on some leaf page")

nodes, err = walk(True)
check(err == "" and len(nodes) > 0, "node walk: %d pages, error %r" % (len(nodes), err))

# --- inside a transaction: BEGIN-time pages, whatever is written since ----------
check(kv.begin(), "BEGIN")
at_begin_l, _ = walk(False)
at_begin_n, _ = walk(True)
fl_begin = (barch.freeList("pw", 0, False), barch.freeList("pw", 0, True))
st_begin = barch.shardStats("pw", 0)
check(isinstance(fl_begin[0], bytes) and len(fl_begin[0]) > 0 and len(fl_begin[1]) > 0,
      "freeList gives bytes for both arenas")
check(isinstance(st_begin, bytes) and len(st_begin) > 0, "shardStats gives bytes")
table = page_table(fl_begin[0])
check(table is not None, "the leaf free list ends in a page table")
if table is not None:
    check(all(table.get(pg) == len(d) for _, pg, d in at_begin_l),
          "every walked leaf page is in the table with its write position")
check(barch.freeList("pw", 99, False) == b"" and barch.shardStats("pw", 99) == b"",
      "no such shard is empty bytes")
check(at_begin_l == leaves, "BEGIN changes nothing the walk sees")

for i in range(0, N, 3):
    r.set("pw:%05d" % i, "changed-%d-%s" % (i, PAD))
for i in range(1, N, 3):
    r.delete("pw:%05d" % i)
for i in range(N, N + 500):
    r.set("pw:%05d" % i, "new-%d-%s" % (i, PAD))

# the live store has the changes...
check(r.get("pw:00000") == ("changed-0-" + PAD).encode(), "a write inside the transaction reads back")
check(r.get("pw:00001") is None, "a delete inside the transaction reads back")
check(r.get("pw:%05d" % N) == ("new-%d-%s" % (N, PAD)).encode(), "a new key inside the transaction reads back")

# ...and the walk still has the pages as they were
during_l, err_l = walk(False)
during_n, err_n = walk(True)
check(err_l == "" and err_n == "", "walks inside the transaction: %r %r" % (err_l, err_n))
check(during_l == at_begin_l, "leaf pages inside the transaction are the BEGIN-time ones")
check(during_n == at_begin_n, "node pages inside the transaction are the BEGIN-time ones")
during_blob = b"".join(d for _, _, d in during_l)
check(b"changed-" not in during_blob and b"new-" not in during_blob,
      "nothing written after BEGIN shows in the walk")
check((barch.freeList("pw", 0, False), barch.freeList("pw", 0, True)) == fl_begin,
      "the free lists inside the transaction are the BEGIN-time ones")
check(barch.shardStats("pw", 0) == st_begin, "the stats inside the transaction are the BEGIN-time ones")

# concurrent readers during the transaction: each first read of a page used to be
# able to see it half copied. Reads while a writer copies pages all round them
bad = []


def reader(seed):
    c = conn()
    for n in range(1500):
        i = (seed * 7919 + n * 13) % N
        want = None
        if i % 3 == 0:
            want = ("changed-%d-%s" % (i, PAD)).encode()
        elif i % 3 == 2:
            want = ("value-%d-%s" % (i, PAD)).encode()
        got = c.get("pw:%05d" % i)
        if got != want:
            bad.append((i, got[:20] if got else got))
            return


def writer():
    c = conn()
    for i in range(N + 500, N + 1500):
        c.set("pw:%05d" % i, "late-%d-%s" % (i, PAD))


threads = [threading.Thread(target=reader, args=(k,)) for k in range(4)] + [threading.Thread(target=writer)]
for t in threads:
    t.start()
for t in threads:
    t.join()
check(not bad, "reads during the transaction came back wrong: %r" % bad[:3])

check(kv.commit(), "COMMIT")
after_l, err = walk(False)
after_blob = b"".join(d for _, _, d in after_l)
check(err == "" and b"changed-0-" in after_blob and b"new-" in after_blob and b"late-" in after_blob,
      "after COMMIT the walk has what was written in the transaction")
check(r.get("pw:00000") == ("changed-0-" + PAD).encode(), "COMMIT keeps the writes")
check(r.get("pw:00001") is None, "COMMIT keeps the deletes")
check(barch.shardStats("pw", 0) != st_begin, "after COMMIT the stats are live again")
check(barch.freeList("pw", 0, False) != fl_begin[0], "after COMMIT the leaf free list is live again")

# --- a COMMIT part way through ends the walk rather than mixing two states -------
check(kv.begin(), "second BEGIN")
p = barch.Pages("pw", False)
check(p.next(), "first page of the second walk")
check(kv.commit(), "COMMIT during the walk")
while p.next():
    pass
check("transaction ended" in p.error(), "the walk says the transaction ended: %r" % p.error())

# --- Luau: barch.store.leaves / nodes, and the same on a space handle ------------
# SETF stores the function in this space, which changes its pages, so the Python
# side of each comparison is walked after every SETF below

assert r.execute_command("SETF", "pwwalk", """
    function call(which)
        local n, bytes = 0, 0
        local function f(page, buf, shard)
            n += 1
            bytes += buffer.len(buf)
        end
        local got
        if which == "nodes" then got = barch.store.nodes(f)
        elseif which == "handle" then got = barch.current():leaves(f)
        else got = barch.store.leaves(f) end
        return { got, n, bytes }
    end
""") == b"OK"
leaves, _ = walk(False)
nodes, _ = walk(True)
want_l = [len(leaves), len(leaves), sum(len(d) for _, _, d in leaves)]
want_n = [len(nodes), len(nodes), sum(len(d) for _, _, d in nodes)]
got = r.execute_command("pwwalk", "leaves")
check(got == want_l, "barch.store.leaves: %r, want %r" % (got, want_l))
got = r.execute_command("pwwalk", "nodes")
check(got == want_n, "barch.store.nodes: %r, want %r" % (got, want_n))
check(r.execute_command("pwwalk", "handle") == want_l, "sp:leaves matches barch.store.leaves")

assert r.execute_command("SETF", "pwstop", """
    function call()
        local seen = 0
        local got = barch.store.leaves(function(page, buf, shard)
            seen += 1
            return false
        end)
        return { got, seen }
    end
""") == b"OK"
check(r.execute_command("pwstop") == [1, 1], "returning false stops the walk after one page")

assert r.execute_command("SETF", "pwraise", """
    function call()
        local ok, err = pcall(function()
            barch.store.leaves(function() error("boom") end)
        end)
        return ok and "ran" or tostring(err)
    end
""") == b"OK"
got = r.execute_command("pwraise")
check(b"boom" in got, "an error in the callback comes back out: %r" % got)

assert r.execute_command("SETF", "pwstate", """
    function call(which)
        local out = {}
        local got
        if which == "stats" then
            got = barch.store.stats(function(st, shard)
                table.insert(out, buffer.tostring(st))
                table.insert(out, shard)
            end)
        else
            got = barch.store.freeList(function(leaves, nodes, shard)
                table.insert(out, buffer.tostring(leaves))
                table.insert(out, buffer.tostring(nodes))
                table.insert(out, shard)
            end)
        end
        table.insert(out, 1, got)
        return out
    end
""") == b"OK"
got = r.execute_command("pwstate", "freeList")
check(got == [1, barch.freeList("pw", 0, False), barch.freeList("pw", 0, True), 0],
      "barch.store.freeList gives the same bytes as barch.freeList")
got = r.execute_command("pwstate", "stats")
check(got == [1, barch.shardStats("pw", 0), 0], "barch.store.stats gives the same bytes as barch.shardStats")

# the page bytes a script sees are the page bytes Python sees
assert r.execute_command("SETF", "pwfirst", """
    function call()
        local out
        barch.store.leaves(function(page, buf, shard)
            out = buffer.tostring(buf)
            return false
        end)
        return out
    end
""") == b"OK"
leaves, _ = walk(False)
check(r.execute_command("pwfirst") == leaves[0][2], "the first page is the same bytes from Luau as from Pages")

# the configuration space has secrets on its pages, and no way to hide them there
assert r.execute_command("SETF", "pwconf", """
    function call()
        local ok, err = pcall(function()
            barch.space.configuration:leaves(function() end)
        end)
        return ok and "walked" or tostring(err)
    end
""") == b"OK"
got = r.execute_command("pwconf")
check(got != b"walked", "the configuration space refuses a page walk, got %r" % got)

# --- ROLLBACK puts the whole space back, allocator included - TODO 417 ----------
#
# It used to put back the tree and not the free lists, so space freed inside the
# transaction was handed out again after it while the restored tree still used it.
# The checks that catch that are the writes *after* the rollback: same sizes as
# the ones freed inside, so they land in exactly that space if it is still free.
def rollback_round(space, shards):
    conf.set("%s.shards" % space, str(shards))
    c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
    c.execute_command("USE", space)
    c.execute_command("FLUSHDB")
    k = barch.KeyValue(space)
    want = {}
    for i in range(1500):
        key = "rb:%05d" % i
        want[key] = ("orig-%d-" % i + "o" * (100 + i % 400)).encode()
        c.set(key, want[key])

    def pages():
        out = []
        for nodes_ in (False, True):
            p = barch.Pages(space, nodes_)
            while p.next():
                out.append((nodes_, p.shard(), p.page(), p.data()))
        return out

    before = pages()
    check(k.begin(), "%s: BEGIN" % space)
    for i in range(0, 1500, 3):
        c.set("rb:%05d" % i, "overwritten-%d" % i)
    for i in range(1, 1500, 3):
        c.delete("rb:%05d" % i)
    for i in range(1500, 2500):
        c.set("rb:%05d" % i, "added-%d-%s" % (i, "a" * (i % 300)))
    # freed and reused inside the transaction as well
    for i in range(1, 300, 3):
        c.set("rb:%05d" % i, "reborn-%d-%s" % (i, "r" * (100 + i % 400)))
    check(k.rollback(), "%s: ROLLBACK" % space)

    check(c.dbsize() == 1500, "%s: size after ROLLBACK is %d, want 1500" % (space, c.dbsize()))
    wrong = [key for key, v in want.items() if c.get(key) != v]
    check(not wrong, "%s: %d keys differ after ROLLBACK, e.g. %r" % (space, len(wrong), wrong[:3]))
    check(c.get("rb:01500") is None, "%s: a key added in the transaction is gone" % space)
    check(pages() == before, "%s: every page is back to how it was at BEGIN" % space)

    # and the space keeps working: the writes below reuse whatever is free
    for i in range(1500, 3000):
        c.set("rb:%05d" % i, "after-%d-%s" % (i, "f" * (100 + i % 400)))
    for i in range(1500, 3000, 2):
        c.delete("rb:%05d" % i)
    wrong = [key for key, v in want.items() if c.get(key) != v]
    check(not wrong, "%s: %d BEGIN-time keys were written over after ROLLBACK, e.g. %r"
          % (space, len(wrong), wrong[:3]))
    check(c.get("rb:01501") == ("after-1501-" + "f" * (100 + 1501 % 400)).encode(),
          "%s: a write after ROLLBACK reads back" % space)

    # a second transaction on the same space, committed this time
    check(k.begin(), "%s: BEGIN again" % space)
    c.set("rb:00000", "committed")
    check(k.commit(), "%s: COMMIT" % space)
    check(c.get("rb:00000") == b"committed", "%s: COMMIT after a ROLLBACK keeps its write" % space)


rollback_round("rb1", 1)
rollback_round("rb4", 4)

# a COMMIT between two shards of a walk ends it too - the walk used to check only
# the shard it was reading, so the shards after the COMMIT came back live
k4 = barch.KeyValue("rb4")
check(k4.begin(), "rb4: BEGIN for the between-shards walk")
p = barch.Pages("rb4", False)
check(p.next(), "rb4: first page")
first_shard = p.shard()
while p.next() and p.shard() == first_shard:
    pass
check(k4.commit(), "rb4: COMMIT during the walk")
while p.next():
    pass
check("transaction ended" in p.error(), "a COMMIT between shards ends the walk: %r" % p.error())

# over RESP as well as through the binding
c = conn()
c.set("rb:resp", "before")
check(c.execute_command("BEGIN") == b"OK", "BEGIN over RESP")
c.set("rb:resp", "during")
check(c.execute_command("ROLLBACK") == b"OK", "ROLLBACK over RESP")
check(c.get("rb:resp") == b"before", "ROLLBACK over RESP puts the value back")

if failures:
    print("page walk test FAILED: %d" % len(failures), flush=True)
    raise SystemExit(1)
print("page walk test ok", flush=True)
barch.stop()
