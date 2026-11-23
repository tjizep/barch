#!/usr/bin/env python3
"""
barch.store.getBuffers - TODO 910.

getBuffers(keys [, offset]) reads many keys' bytes in one call. It answers one slot
per key, so the array keeps #keys: a buffer, barch.tomb, or false where getBufferAt
would answer nil. In a transaction it is one MGET: one routing step and one lock
request for the batch, and each read is logged as GET's is.

Checked against one barchd, a space without Raft, tx_lock_wait_ms 0 so a held lock
is refused at once rather than waited for:
  - outside a transaction each slot is what getBufferAt answers, an offset included;
  - in the configuration space a key ending in "password" reads as false, as get
    hides it there, and the ones around it still read;
  - a space handle has it too;
  - in a transaction it sees the transaction's own writes, and not after an abort;
  - a key changed beneath since getBuffers read it makes COMMITTX answer TXCONFLICT;
  - in a pessimistic transaction it takes a shared lock on every key, so another
    transaction's write to any of them is refused;
  - a key that isn't a string is refused.

TODO 960 reads a batch a shard at a time, straight into the buffers. So also:
  - more keys than one run of 32 locks, repeated, missing, empty and large, in a
    hashed space and a range sharded one;
  - a compressed value is decompressed, in a transaction too;
  - a cached source miss is barch.tomb;
  - inside barch.store.locked the held shard reads, a second one is refused, and
    both take writes afterwards.

TODO 961 gives getBufferAt the same copy, so it can't vouch for getBuffers alone.
It is checked against the bytes written: every size, offsets, a secret, a handle,
compressed values, a tomb, a locked region, and a transaction's writes, read log
and locks.
"""
import os
import shutil
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14931)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


DATA = os.path.join(os.getcwd(), "getbuffers_data")
shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)
log = open(os.path.join(os.getcwd(), "getbuffers.log"), "a")
exec(open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_data.py")).read())
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000", "-c", "tx_lock_wait_ms=0",
                      "-c", "compression=zstd"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "gbs" + str(int(time.time()))


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=30, single_connection_client=True)
    c.execute_command("USE", SPACE)
    return c


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


# each slot as a string: its bytes, "tomb" or "false", so a reply shows the array's shape
SHOW = """
    local function show(got)
        local out = {}
        for i = 1, #got do
            local v = got[i]
            if v == false then out[i] = "false"
            elseif v == barch.tomb then out[i] = "tomb"
            else out[i] = buffer.tostring(v) end
        end
        return out
    end
"""

FUNCTIONS = {
    "many": SHOW + """
        function call(off, ...)
            return show(barch.store.getBuffers({...}, tonumber(off)))
        end
    """,
    "one": """
        function call(off, ...)
            local out = {}
            for i, k in {...} do
                local v = barch.store.getBufferAt(k, tonumber(off))
                out[i] = v == nil and "false" or v == barch.tomb and "tomb" or buffer.tostring(v)
            end
            return out
        end
    """,
    "handle": SHOW + """
        function call(...)
            return show(barch.current():getBuffers({...}))
        end
    """,
    "secrets": SHOW + """
        function call(...)
            return show(barch.space.configuration:getBuffers({...}))
        end
    """,
    "one_handle": """
        function call(...)
            local out, sp = {}, barch.current()
            for i, k in {...} do
                local v = sp:getBufferAt(k)
                out[i] = v == nil and "false" or v == barch.tomb and "tomb" or buffer.tostring(v)
            end
            return out
        end
    """,
    "one_secret": """
        function call(...)
            local out = {}
            for i, k in {...} do
                local v = barch.space.configuration:getBufferAt(k)
                out[i] = v == nil and "false" or buffer.tostring(v)
            end
            return out
        end
    """,
    "one_put_read": """
        function call(k, s)
            return barch.tx.run(function()
                barch.store.setBufferAt(k, buffer.fromstring(s), 0)
                return buffer.tostring(barch.store.getBufferAt(k))
            end)
        end
    """,
    "one_locked": """
        function call(k)
            local held
            barch.store.locked(k, function() held = buffer.tostring(barch.store.getBufferAt(k)) end)
            return held
        end
    """,
    "put": "function call(k, s) barch.store.setBufferAt(k, buffer.fromstring(s), 0) return 'ok' end\n",
    "put_read": SHOW + """
        function call(k, s, ...)
            local keys = {...}
            return barch.tx.run(function()
                barch.store.setBufferAt(k, buffer.fromstring(s), 0)
                return show(barch.store.getBuffers(keys))
            end)
        end
    """,
    "same": """
        function call(off, n)
            local keys = {}
            for i = 1, tonumber(n) do
                -- every third is asked for twice
                keys[i] = "batch:" .. (i % 3 == 0 and i - 1 or i)
            end
            local got = barch.store.getBuffers(keys, tonumber(off))
            local same, read = 0, 0
            for i, k in keys do
                local want = barch.store.getBufferAt(k, tonumber(off))
                local v = got[i]
                if want == nil then
                    if v == false then same += 1 end
                elseif v and v ~= barch.tomb then
                    read += 1
                    if buffer.tostring(v) == buffer.tostring(want) then same += 1 end
                end
            end
            -- a write to one slot's buffer is seen in no other's
            local apart = true
            local one, two = got[2], got[3]
            if one and two and buffer.len(one) > 0 then
                local b = buffer.readu8(two, 0)
                buffer.writeu8(one, 0, (b + 1) % 256)
                apart = buffer.readu8(two, 0) == b
            end
            return { same, read, #got, apart and 1 or 0 }
        end
    """,
    "locked": SHOW + """
        function call(k)
            local other
            for i = 1, 10000 do
                other = "other:" .. i
                if barch.store.shardNumber(other) ~= barch.store.shardNumber(k) then break end
            end
            local held
            barch.store.locked(k, function() held = show(barch.store.getBuffers({k, k, k})) end)
            local ok = pcall(function()
                barch.store.locked(k, function() barch.store.getBuffers({k, other}) end)
            end)
            barch.store.set(other, "free")
            held[#held + 1] = ok and "crossed" or "refused"
            held[#held + 1] = barch.store.get(other)
            return held
        end
    """,
    "notstring": """
        function call()
            local ok, err = pcall(function() return barch.store.getBuffers({"a", {}}) end)
            return ok and "accepted" or "refused"
        end
    """,
}

try:
    c = client()
    for name, body in FUNCTIONS.items():
        got = answer(c, "SETF", name, body)
        check(got == b"OK", "SETF %s (%s)" % (name, str(got)[:60]))
    c.set("a", "alpha")
    c.set("b", "be")
    c.delete("none")

    print("outside a transaction")
    keys = ["a", "none", "b", "a"]
    for off in (0, 2, 3):
        got = answer(c, "CALLF", "many", off, *keys)
        want = answer(c, "CALLF", "one", off, *keys)
        check(isinstance(got, list) and got == want, "offset %d is getBufferAt's answer per key (%s)" % (off, got))
    got = answer(c, "CALLF", "many", 0, *keys)
    check(isinstance(got, list) and len(got) == len(keys), "one slot per key, a missing one included")
    c.execute_command("configuration:SET", "gbs.endpoint", "http://here")
    c.execute_command("configuration:SET", "gbs.password", "hunter2")
    got = answer(c, "CALLF", "secrets", "gbs.endpoint", "gbs.password", "gbs.endpoint")
    check(got == [b"http://here", b"false", b"http://here"], "a configuration secret reads as false (%s)" % got)
    check(answer(c, "CALLF", "many", 0) == [], "no keys, an empty array")
    got = answer(c, "CALLF", "handle", "b", "none", "a")
    check(got == [b"be", b"false", b"alpha"], "a space handle has it (%s)" % got)
    check(answer(c, "CALLF", "notstring") == b"refused", "a key that isn't a string is refused")

    print("in a transaction")
    got = answer(c, "CALLF", "put_read", "b", "new", "a", "b")
    check(got == [b"alpha", b"new"], "it sees the transaction's own write (%s)" % got)
    c.set("b", "be")
    c.execute_command("BEGINTX")
    answer(c, "CALLF", "put", "a", "inside")
    got = answer(c, "CALLF", "many", 0, "a", "b")
    check(got == [b"inside", b"be"], "a client's transaction too (%s)" % got)
    check(client().get("a") == b"alpha", "another client doesn't see it")
    answer(c, "ABORTTX")
    check(answer(c, "CALLF", "many", 0, "a") == [b"alpha"], "and after ABORTTX it's gone")

    c.execute_command("BEGINTX")
    got = answer(c, "CALLF", "many", 0, "a", "b")
    answer(c, "CALLF", "put", "x", "written")
    client().set("b", "beneath")
    got = answer(c, "COMMITTX")
    check("TXCONFLICT" in str(got), "a key changed beneath its read: TXCONFLICT (%s)" % str(got)[:60])
    check(client().get("b") == b"beneath" and client().get("x") is None, "and the other client's write stands")

    print("in a pessimistic transaction")
    holder, other = client(), client()
    holder.execute_command("BEGINTX", "PESSIMISTIC")
    got = answer(holder, "CALLF", "many", 0, "a", "b")
    check(got == [b"alpha", b"beneath"], "it reads (%s)" % got)
    other.execute_command("BEGINTX", "PESSIMISTIC")
    got = answer(other, "SET", "b", "taken")
    check(str(got).startswith("ERR"), "another transaction's write to its last key is refused (%s)" % str(got)[:60])
    answer(other, "ABORTTX")
    other.execute_command("BEGINTX", "PESSIMISTIC")
    got = answer(other, "GET", "a")
    check(got == b"alpha", "a read of one of its keys isn't (%s)" % got)
    answer(other, "ABORTTX")
    check(answer(holder, "COMMITTX") is not None, "COMMITTX")
    check(client().get("b") == b"beneath", "the refused write never landed")
    print("getBufferAt")
    c.set("b", "be")
    got = answer(c, "CALLF", "one", 0, "a", "none", "b")
    check(got == [b"alpha", b"false", b"be"], "its bytes, and nil for none (%s)" % got)
    got = answer(c, "CALLF", "one", 2, "a", "b")
    check(got == [b"pha", b""], "from an offset, the value's end included (%s)" % got)
    check(answer(c, "CALLF", "one", 3, "b") == [b"false"], "past the end is nil")
    got = answer(c, "CALLF", "one_handle", "b", "none", "a")
    check(got == [b"be", b"false", b"alpha"], "a space handle has it (%s)" % got)
    got = answer(c, "CALLF", "one_secret", "gbs.endpoint", "gbs.password")
    check(got == [b"http://here", b"false"], "a configuration secret reads as nil (%s)" % got)
    check(answer(c, "CALLF", "one_locked", "a") == b"alpha", "its held shard reads")
    got = answer(c, "CALLF", "one_put_read", "b", "new")
    check(got == b"new", "it sees the transaction's own write (%s)" % got)
    c.set("b", "be")
    c.execute_command("BEGINTX")
    check(answer(c, "CALLF", "one", 0, "b") == [b"be"], "it reads in a client's transaction")
    answer(c, "CALLF", "put", "x", "written")
    client().set("b", "beneath")
    got = answer(c, "COMMITTX")
    check("TXCONFLICT" in str(got), "a key changed beneath its read: TXCONFLICT (%s)" % str(got)[:60])
    holder, other = client(), client()
    holder.execute_command("BEGINTX", "PESSIMISTIC")
    check(answer(holder, "CALLF", "one", 0, "b") == [b"beneath"], "it reads in a pessimistic one")
    other.execute_command("BEGINTX", "PESSIMISTIC")
    got = answer(other, "SET", "b", "taken")
    check(str(got).startswith("ERR"), "another transaction's write to its key is refused (%s)" % str(got)[:60])
    answer(other, "ABORTTX")
    answer(holder, "ABORTTX")

    print("whole batches")
    ranged = SPACE + "r"
    c.execute_command("configuration:SET", ranged + ".ordered", "1")
    c.execute_command("configuration:SET", ranged + ".shards", "7")
    c.execute_command("configuration:SET", ranged + ".range_sharded", "1")
    for space in (SPACE, ranged):
        b = client()
        b.execute_command("USE", space)
        check(answer(b, "SETF", "same", FUNCTIONS["same"]) == b"OK", "SETF same in %s" % space)
        sizes = {}
        for i in range(1, 301):
            if i % 5:
                sizes[i] = 0 if i % 11 == 0 else 7090 if i % 2 == 0 else 100000 if i == 41 else i
                b.set("batch:%d" % i, bytes((i + j) % 251 for j in range(sizes[i])))
        for n in (1, 2, 16, 31, 32, 33, 64, 65, 300):
            for off in (0, 7, 7090):
                asked = [i - 1 if i % 3 == 0 else i for i in range(1, n + 1)]
                read = sum(1 for i in asked if i in sizes and sizes[i] >= off)
                got = answer(b, "CALLF", "same", off, n)
                check(got == [n, read, n, 1], "%s: %d keys at %d (%s)" % (space, n, off, got))
        check(answer(b, "SETF", "one", FUNCTIONS["one"]) == b"OK", "SETF one in %s" % space)
        known = {i: bytes((i + j) % 251 for j in range(n)) for i, n in sizes.items()}
        for tx in (False, True):
            if tx:
                b.execute_command("BEGINTX")
            for off in (0, 7, 7090):
                want = [known[i][off:] if i in known and sizes[i] >= off else b"false" for i in range(1, 66)]
                got = answer(b, "CALLF", "one", off, *["batch:%d" % i for i in range(1, 66)])
                check(got == want, "%s: getBufferAt's bytes at %d%s" % (space, off, ", in a transaction" if tx else ""))
        answer(b, "ABORTTX")
        b.execute_command("BEGINTX")
        got = answer(b, "CALLF", "same", 0, 65)
        check(isinstance(got, list) and got[0] == 65 and got[2:] == [65, 1], "and 65 in a transaction (%s)" % got)
        answer(b, "ABORTTX")

    print("compressed")
    zto = client()
    zto.execute_command("USE", SPACE + "z")
    for w in words:
        zto.execute_command("TRAIN", train_set[w])
    check(zto.execute_command("TRAIN") == 0, "a dictionary is trained")
    for w in words:
        zto.set(w, test_set[w])
    done = [answer(zto, "COMPRESS", w) for w in words]
    check(1 in done, "values are compressed (%d of %d)" % (done.count(1), len(words)))
    check(answer(zto, "SETF", "many", FUNCTIONS["many"]) == b"OK", "SETF many")
    check(answer(zto, "SETF", "one", FUNCTIONS["one"]) == b"OK", "SETF one")
    plain = [test_set[w] if isinstance(test_set[w], bytes) else test_set[w].encode() for w in words]
    for tx in (False, True):
        if tx:
            zto.execute_command("BEGINTX")
        for off in (0, 3):
            got = answer(zto, "CALLF", "many", off, *words)
            one = answer(zto, "CALLF", "one", off, *words)
            check(one == [v[off:] for v in plain], "getBufferAt's decompressed at %d%s" % (off, ", in a transaction" if tx else ""))
            check(got == [v[off:] for v in plain], "they read decompressed at %d%s" % (off, ", in a transaction" if tx else ""))
    answer(zto, "ABORTTX")

    print("a tomb")
    c.execute_command("configuration:SET", SPACE + "f.foreign", "fake")
    c.execute_command("configuration:SET", SPACE + "f.missing_ttl", "30")
    f = client()
    f.execute_command("USE", SPACE + "f")
    f.execute_command("FOREIGN", "FAKE", "SET", "there", "from-source")
    check(f.get("there") == b"from-source" and f.get("gone") is None, "a hit and a miss are cached")
    check(answer(f, "SETF", "many", FUNCTIONS["many"]) == b"OK", "SETF many")
    got = answer(f, "CALLF", "many", 0, "there", "gone", "unasked", "gone")
    check(got == [b"from-source", b"tomb", b"false", b"tomb"], "a cached miss is barch.tomb (%s)" % got)
    check(answer(f, "SETF", "one", FUNCTIONS["one"]) == b"OK", "SETF one")
    got = answer(f, "CALLF", "one", 0, "there", "gone", "unasked")
    check(got == [b"from-source", b"tomb", b"false"], "from getBufferAt too (%s)" % got)

    print("in a locked region")
    got = answer(c, "CALLF", "locked", "a")
    check(got == [b"alpha", b"alpha", b"alpha", b"refused", b"free"], "its shard reads, a second is refused (%s)" % got)
    check(client().set("a", "alpha") is True, "and the held shard takes a write after")
finally:
    p.terminate()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
