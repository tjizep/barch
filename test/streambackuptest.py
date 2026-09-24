# Streaming save and load - TODO 418.
#
# A whole space out as numbered blocks per shard and back in again: StreamSave and
# StreamLoad from here, barch.store.save and barch.store.load (and sp:save/sp:load)
# from Luau. A save is the space as it stood at BEGIN, and BEGIN and COMMIT are the
# caller's, so metadata can be taken from the same moment afterwards (TODO 424).
import threading
import time

import scale

import redis
import barch

scale.workdir()
PORT = scale.port(default=14480)

print("start stream backup test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

conf = barch.KeyValue("configuration")
for name, shards in (("sa", 4), ("sb", 4), ("sc", 4), ("sd", 4), ("s2", 2), ("bk", 1), ("fn", 1)):
    conf.set("%s.shards" % name, str(shards))

failures = []


def check(ok, what):
    if not ok:
        print("  FAIL " + what, flush=True)
        failures.append(what)


def conn(space):
    c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
    c.execute_command("USE", space)
    return c


def everything(c):
    return {k: c.get(k) for k in c.keys("*")}


def save(space, pause=0.0):
    s = barch.StreamSave(space)
    out = []
    while s.next():
        out.append((s.shard(), s.block(), s.data()))
        if pause:
            time.sleep(pause)       # the cursor makes a shard at a time, so this
                                    # holds the save open across shards
    return out, s.error()


def load(space, blocks):
    l = barch.StreamLoad(space)
    for shard, block, data in blocks:
        if not l.add(shard, block, data):
            return l.error()
    if not l.finish():
        return l.error()
    return ""


sa = conn("sa")
sa.execute_command("FLUSHDB")
want = {}
for i in range(3000):
    k = ("k:%05d" % i).encode()
    want[k] = ("v%d-" % i + "x" * (50 + (i * 37) % 1000)).encode()
    sa.set(k, want[k])

# --- a save needs a transaction, and leaves it to the caller --------------------
blocks, err = save("sa")
check(blocks == [] and "BEGIN" in err, "a save with no transaction open is refused: %r" % err)

# --- a round trip -------------------------------------------------------------
kv = barch.KeyValue("sa")
check(kv.begin(), "BEGIN for the round trip")
blocks, err = save("sa")
check(err == "", "save error %r" % err)
# metadata from the same moment, after the save, before the COMMIT
meta = [(barch.freeList("sa", i, False), barch.shardStats("sa", i)) for i in range(4)]
check(all(len(f) > 0 and len(st) > 0 for f, st in meta), "free lists and stats read after the save")
check(kv.commit(), "COMMIT after the round trip save")
check(sorted({s for s, _, _ in blocks}) == [0, 1, 2, 3], "every shard is in the stream")
check(all(isinstance(d, bytes) and 0 < len(d) <= 65536 for _, _, d in blocks), "blocks are bytes of up to 64K")
per = {}
for s, b, _ in blocks:
    per.setdefault(s, []).append(b)
check(all(v == list(range(len(v))) for v in per.values()), "block numbers run from 0 in every shard")
check([s for s, _, _ in blocks] == sorted(s for s, _, _ in blocks), "shards come in order")
total = sum(len(d) for _, _, d in blocks)
check(total < 3000 * 1100 * 2, "the stream carries what was written, not whole pages: %d bytes" % total)

sb = conn("sb")
sb.execute_command("FLUSHDB")
sb.set("stale", "gone after the load")
err = load("sb", blocks)
check(err == "", "load error %r" % err)
check(everything(sb) == want, "the loaded space has exactly what was saved")
check(sb.dbsize() == 3000, "and the same size")
sb.set("k:99999", "after")
check(sb.get("k:99999") == b"after" and sb.get("k:00000") == want[b"k:00000"], "the loaded space takes writes")

# --- inside a transaction the save is the BEGIN-time space ---------------------
check(kv.begin(), "BEGIN")
for i in range(0, 3000, 2):
    sa.set("k:%05d" % i, "changed")
for i in range(1, 3000, 4):
    sa.delete("k:%05d" % i)
sa.set("k:new", "new")
blocks, err = save("sa")
check(err == "", "save in a transaction: %r" % err)
check(sa.get("k:00000") == b"changed", "the transaction's writes are live while the save runs")
check(kv.commit(), "COMMIT after the save")
sc = conn("sc")
sc.execute_command("FLUSHDB")
check(load("sc", blocks) == "", "load of the BEGIN-time stream")
check(everything(sc) == want, "a save inside a transaction is the space as it was at BEGIN")
check(sa.get("k:00000") == b"changed" and sa.get("k:new") == b"new", "and the COMMIT kept the writes")

# --- one moment, while writes carry on -------------------------------------------
# a writer adds w:0, w:1, ... in order the whole time; a copy of one moment has
# exactly the ones before that moment, never a later one without an earlier one
stop = threading.Event()
written = [0]


def writer():
    c = conn("sa")
    i = 0
    while not stop.is_set():
        c.set("w:%06d" % i, "w")
        i += 1
    written[0] = i


t = threading.Thread(target=writer)
t.start()
while written[0] == 0 and sa.exists("w:000050") == 0:
    pass
check(kv.begin(), "BEGIN with the writer running")
blocks, err = save("sa", pause=0.05)
stop.set()
t.join()
check(err == "", "save with writes going on: %r" % err)
sd = conn("sd")
sd.execute_command("FLUSHDB")
check(load("sd", blocks) == "", "load of the concurrent save")
ws = sorted(k for k in sd.keys("w:*"))
print("concurrent save: %d blocks, %d bytes, kept %d of %d writes"
      % (len(blocks), sum(len(d) for _, _, d in blocks), len(ws), written[0]), flush=True)
check(len(ws) > 0 and ws == [("w:%06d" % i).encode() for i in range(len(ws))],
      "the concurrent save holds a prefix of the writes: %d of %d" % (len(ws), written[0]))
check(written[0] > len(ws) + 20, "writes carried on while the save ran, and it left them out")
# the save left the transaction open: a ROLLBACK now still undoes a write
sa.set("k:after-save", "rolled back")
check(sa.execute_command("ROLLBACK") == b"OK", "ROLLBACK after the save")
check(sa.get("k:after-save") is None, "the save left the transaction to the caller")

# --- refusals ------------------------------------------------------------------
before = everything(sd)
# cut short: the last block of shard 0 missing, so shard 0 is refused untouched
cut = [b for b in blocks if not (b[0] == 0 and b[1] == max(x[1] for x in blocks if x[0] == 0))]
err = load("sd", cut)
check("cut short" in err, "a stream cut short is refused: %r" % err)
check(everything(sd) == before, "and the space is left as it was")

s2 = conn("s2")
s2.execute_command("FLUSHDB")
s2.set("mine", "1")
err = load("s2", blocks)
check("of 4" in err and "of 2" in err, "a stream for another shard count is refused: %r" % err)
check(s2.get("mine") == b"1", "and the space is left as it was")

k_sd = barch.KeyValue("sd")
check(k_sd.begin(), "BEGIN on the load target")
err = load("sd", blocks)
check("transaction" in err, "a load inside a transaction is refused: %r" % err)
check(k_sd.commit(), "COMMIT")

# --- Luau: save into a backup space, load it back into another -----------------
fn = conn("fn")
fn.execute_command("FLUSHDB")
bk = conn("bk")
bk.execute_command("FLUSHDB")
assert fn.execute_command("SETF", "bksave", """
    function call(from)
        local bk = barch.space.bk
        local sp = barch.space[from]
        sp:call("BEGIN")
        local n = sp:save(function(buf, block, shard)
            bk:setBufferAt("b:" .. shard .. ":" .. block, buf)
        end)
        sp:call("COMMIT")
        return n
    end
""") == b"OK"
assert fn.execute_command("SETF", "bkload", """
    function call(into)
        local bk = barch.space.bk
        return barch.space[into]:load(function(block, shard)
            return bk:getBufferAt("b:" .. shard .. ":" .. block)
        end)
    end
""") == b"OK"
now_sa = everything(sa)
got = fn.execute_command("bksave", "sa")
check(isinstance(got, int) and got == bk.dbsize(), "sp:save handed over %r blocks, %d stored" % (got, bk.dbsize()))
sc.execute_command("FLUSHDB")
got = fn.execute_command("bkload", "sc")
check(isinstance(got, int) and got == bk.dbsize(), "sp:load read %r blocks" % got)
check(everything(sc) == now_sa, "a Luau save and load round trip gives the space back")

assert fn.execute_command("SETF", "bkbad", """
    function call()
        local ok, err = pcall(function()
            barch.space.sc:load(function(block, shard) return 42 end)
        end)
        return ok and "loaded" or tostring(err)
    end
""") == b"OK"
got = fn.execute_command("bkbad")
check(b"wants a buffer" in got, "a load callback answering a number is refused: %r" % got)
check(everything(sc) == now_sa, "and the space is left as it was")

assert fn.execute_command("SETF", "bknotx", """
    function call()
        local ok, err = pcall(function()
            barch.space.sa:save(function() end)
        end)
        return ok and "saved" or tostring(err)
    end
""") == b"OK"
got = fn.execute_command("bknotx")
check(b"BEGIN" in got, "sp:save with no transaction open is refused: %r" % got)

assert fn.execute_command("SETF", "bkconf", """
    function call()
        local ok, err = pcall(function()
            barch.space.configuration:save(function() end)
        end)
        return ok and "saved" or tostring(err)
    end
""") == b"OK"
check(fn.execute_command("bkconf") != b"saved", "the configuration space refuses a streaming save")

# --- the function deadline stops a save - TODO 429 ---------------------------------
# The deadline is the space the call runs in, read when that space is first opened -
# a setting made after it's open isn't seen until it is opened again. It fires in the
# save's callback, which is Luau, so a save that takes too long stops with a timeout
# and the transaction the caller opened is left open for the caller to end.
conf.set("dl.function_deadline_ms", "50")
dl = conn("dl")
check(dl.execute_command("KSPACE", "OPTION", "GET", "FUNCTION_DEADLINE") in (50, b"50", "50"),
      "the space took the 50ms deadline: %r" % dl.execute_command("KSPACE", "OPTION", "GET", "FUNCTION_DEADLINE"))
slow_save = """
    function call(from)
        local sp = barch.space[from]
        sp:call("BEGIN")
        local ok, err = pcall(function()
            return sp:save(function(buf, block, shard)
                local n = 0
                for i = 1, 2000000 do n = n + i end    -- a slow sink, some ms a block
            end)
        end)
        return ok and ("saved " .. tostring(err)) or tostring(err)
    end
"""
assert dl.execute_command("SETF", "SLOWSAVE", slow_save) == b"OK"
# the timeout ends the whole call - pcall inside the function doesn't catch it, so a
# runaway can't swallow its own deadline
try:
    got = dl.execute_command("SLOWSAVE", "sa")
    check(False, "a save past the deadline should have been stopped, got %r" % got)
except redis.ResponseError as e:
    check("timeout" in str(e).lower(), "a save past the deadline stops with a timeout: %r" % str(e))
# the save didn't end the transaction: it is the caller's, and still open
sa.set("k:after-timeout", "rolled back")
check(sa.execute_command("ROLLBACK") == b"OK" and sa.get("k:after-timeout") is None,
      "the timed out save left the caller's transaction open")
# and given the time it needs, the same save finishes. Its own header, not the default
# 1000ms: 2M iterations a block is about 130ms on a slow CI runner, and 11 blocks is
# past a second there - TODO 444
assert fn.execute_command("SETF", "SLOWSAVE", '--@barch {"deadline_ms": 30000}\n' + slow_save) == b"OK"
got = fn.execute_command("SLOWSAVE", "sa")
check(got.startswith(b"saved "), "with a deadline it can meet, the save finishes: %r" % got[:40])
sa.execute_command("COMMIT")

if failures:
    print("stream backup test FAILED: %d" % len(failures), flush=True)
    raise SystemExit(1)
print("stream backup test ok", flush=True)
barch.stop()
