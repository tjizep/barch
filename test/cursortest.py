"""barch.store.cursor: range reads into the caller's buffer - TODO 962.

A cursor records [lo, hi) once and reads each key with a point seek, into a buffer the
caller keeps. What has to hold, on hash shards, range shards and one shard:

  - a read is getBufferAt's answer: present, absent, empty, large, expired, both key
    regions, a cached miss, a deleted key over a source, a compressed value;
  - partial copies write the bytes asked for and no byte beside them, and a copy that
    doesn't fit writes nothing;
  - each error's text, with no position before it;
  - in a transaction a read is the snapshot's, or the transaction's own write;
  - a change inside the range is TXCONFLICT for a writer, one outside isn't, and a
    read only commit without Raft succeeds;
  - reads add nothing to what tx_max_keys and tx_max_bytes count;
  - a pessimistic cursor holds the range's span;
  - a cursor ends with its transaction, or with the call outside one;
  - it reads where rangeValues may, and no secret;
  - in a Raft space the prepare checks the range, for writers and readers.

A build without the cluster skips the Raft half.
"""
import os
import shutil
import subprocess
import sys
import threading
import time

import scale
import redis

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
PORT = scale.port(default=19760)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-70s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1
    return ok


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.exceptions.TryAgainError as e:
        return "ERR TRYAGAIN " + str(e)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def short(v):
    return str(v)[:90]


READ = """
local function bytes(cur, key)
    local size = cur:read(key)
    if size == nil then return "false" end
    if size == barch.tomb then return "tomb" end
    local out = buffer.create(size)
    assert(cur:read(key, out) == size, "the size moved")
    return buffer.tostring(out)
end
local function point(v)
    if v == nil then return "false" end
    if v == barch.tomb then return "tomb" end
    return buffer.tostring(v)
end
local function why(fn, ...)
    local ok, err = pcall(fn, ...)
    if ok then return "no error" end
    return tostring(err)
end
"""

FUNCTIONS = {
    "read": READ + """
        function call(lo, hi, ...)
            local cur, out = barch.store.cursor(lo, hi), {}
            for i, k in {...} do out[i] = bytes(cur, k) end
            return out
        end
    """,
    "handle": READ + """
        function call(lo, hi, ...)
            local cur, out = barch.current():cursor(lo, hi), {}
            for i, k in {...} do out[i] = bytes(cur, k) end
            return out
        end
    """,
    "elsewhere": READ + """
        function call(space, lo, hi, ...)
            local cur, out = barch.space[space]:cursor(lo, hi), {}
            for i, k in {...} do out[i] = bytes(cur, k) end
            return out
        end
    """,
    "point": READ + """
        function call(lo, hi, ...)
            local out = {}
            for i, k in {...} do out[i] = point(barch.store.getBufferAt(k)) end
            return out
        end
    """,
    # out is all '#', with a byte each side of what may be written
    "part": READ + """
        function call(lo, hi, key, len, outat, from, n)
            local cur = barch.store.cursor(lo, hi)
            local out = buffer.fromstring(string.rep("#", tonumber(len)))
            local size
            if n == "" then
                size = cur:read(key, out, tonumber(outat), tonumber(from))
            else
                size = cur:read(key, out, tonumber(outat), tonumber(from), tonumber(n))
            end
            return { tostring(size), buffer.tostring(out) }
        end
    """,
    "defaults": READ + """
        function call(lo, hi, key)
            local cur, out = barch.store.cursor(lo, hi), buffer.fromstring("#####")
            return { tostring(cur:read(key)), tostring(cur:read(key, nil)),
                     tostring(cur:read(key, out)), buffer.tostring(out) }
        end
    """,
    "errors": READ + """
        function call(lo, hi, key, outside)
            local cur, out = barch.store.cursor(lo, hi), buffer.create(8)
            local none = barch.store.cursor(lo, "")
            local back = barch.store.cursor(hi, lo)
            return {
                why(function() cur:read(outside) end),
                why(function() cur:read(hi) end),
                why(function() none:read(key) end),
                why(function() back:read(key) end),
                why(function() cur:read(key, "no buffer") end),
                why(function() cur:read(key, out, -1) end),
                why(function() cur:read(key, out, 1.5) end),
                why(function() cur:read(key, out, 0, -1) end),
                why(function() cur:read(key, out, 0, 0.25) end),
                why(function() cur:read(key, out, 0, 0, -2) end),
                why(function() cur:read(key, out, 0, 0, 0/0) end),
                why(function() barch.store.cursor("a\\0b", hi) end),
            }
        end
    """,
    "put_read": READ + """
        function call(lo, hi, k, v)
            return barch.tx.run(function()
                local cur = barch.store.cursor(lo, hi)
                local out = { bytes(cur, k) }
                barch.store.set(k, v)
                out[2] = bytes(cur, k)
                barch.store.remove(k)
                out[3] = bytes(cur, k)
                out[4] = point(barch.store.getBufferAt(k))
                barch.store.set(k, v)
                pcall(barch.tx.savepoint, function()
                    barch.store.set(k, "inner")
                    out[5] = bytes(cur, k)
                    error("take it back")
                end)
                out[6] = bytes(cur, k)
                return out
            end)
        end
    """,
    "around": READ + """
        function call(lo, hi, k)
            local a = point(barch.store.getBufferAt(k))
            local cur = barch.store.cursor(lo, hi)
            local b = bytes(cur, k)
            return { a, b, point(barch.store.getBufferAt(k)) }
        end
    """,
    "open_undone": READ + """
        function call(lo, hi, k)
            local got
            pcall(barch.tx.savepoint, function()
                got = bytes(barch.store.cursor(lo, hi), k)
                error("take it back")
            end)
            return got
        end
    """,
    "lifetime": READ + """
        function call(lo, hi, k)
            local out, v = {}, bytes(barch.store.cursor(lo, hi), k)
            out[1] = v
            local cur = barch.store.cursor(lo, hi)
            barch.tx.begin()
            out[2] = why(function() cur:read(k) end)
            local inner = barch.store.cursor(lo, hi)
            out[3] = bytes(inner, k)
            barch.tx.commit()
            out[4] = why(function() inner:read(k) end)
            -- one that began and ended with no read between
            cur = barch.store.cursor(lo, hi)
            barch.tx.begin()
            barch.tx.abort()
            out[5] = why(function() cur:read(k) end)
            barch.tx.begin()
            inner = barch.store.cursor(lo, hi)
            barch.tx.abort()
            out[6] = why(function() inner:read(k) end)
            barch.tx.begin()
            out[7] = why(function() inner:read(k) end)
            barch.tx.abort()
            return out
        end
    """,
    "kept": READ + """
        local kept
        function call(op, a, b)
            if op == "open" then
                kept = barch.store.cursor(a, b)
                return "open"
            elseif op == "both" then
                local ok, v = pcall(bytes, kept, a)
                local fine, p = pcall(function() return point(barch.store.getBufferAt(a)) end)
                return { tostring(v), tostring(p) }
            end
            local ok, v = pcall(bytes, kept, a)
            return tostring(v)
        end
    """,
    "many": READ + """
        function call(lo, hi, n)
            local cur, out = barch.store.cursor(lo, hi), buffer.create(8192)
            local read, total = 0, 0
            for i = 1, tonumber(n) do
                local size = cur:read("m:" .. i, out)
                if type(size) == "number" then
                    read += 1
                    total += size
                end
            end
            return { read, total }
        end
    """,
    "yields": READ + """
        function call(lo, hi, a, b)
            local cur = barch.store.cursor(lo, hi)
            local step = coroutine.wrap(function()
                coroutine.yield(bytes(cur, a))
                coroutine.yield(bytes(cur, b))
            end)
            local first = step()
            barch.store.set("z:between", "written")
            return { first, step() }
        end
    """,
    "dropped": READ + """
        function call(lo, hi, k)
            local last
            for i = 1, 20000 do
                local cur = barch.store.cursor(lo, hi)
                last = cur:read(k)
                local garbage = { buffer.create(512) }
            end
            barch.store.set("z:after", "written")
            return { tostring(last), barch.store.get("z:after") }
        end
    """,
    "locked": READ + """
        function call(lo, hi, k)
            local cur = barch.store.cursor(lo, hi)
            local held
            barch.store.locked(k, function() held = bytes(cur, k) end)
            barch.store.set(k, barch.store.get(k))
            return held
        end
    """,
    "untorn": READ + """
        function call(lo, hi, k, rounds)
            local cur, out = barch.store.cursor(lo, hi), buffer.create(20000)
            local torn = 0
            for i = 1, tonumber(rounds) do
                local size = cur:read(k, out)
                if type(size) == "number" and size > 0 then
                    local first = buffer.readu8(out, 0)
                    if buffer.readu8(out, size - 1) ~= first
                        or buffer.readu8(out, math.floor(size / 2)) ~= first
                        or size ~= (first + 1) * 64 then
                        torn += 1
                    end
                end
            end
            return torn
        end
    """,
    # reads till one is refused. The limit is age, so the cap is only for a build without it
    "outgrows": READ + """
        function call(lo, hi, k)
            barch.tx.begin()
            local cur = barch.store.cursor(lo, hi)
            cur:read(k)
            pcall(barch.store.set, "q:big", string.rep("b", 300000))
            local err = why(function() cur:read(k) end)
            pcall(barch.tx.abort)
            return err
        end
    """,
    "outlives": READ + """
        function call(lo, hi, k)
            barch.tx.begin()
            local cur = barch.store.cursor(lo, hi)
            local ok, err = true, nil
            for i = 1, 10000000 do
                ok, err = pcall(function() cur:read(k) end)
                if not ok then break end
            end
            barch.tx.abort()
            return tostring(err)
        end
    """,
    "refused": READ + """
        function call(space)
            if space == nil then
                return why(function() barch.store.cursor("a", "z") end)
            end
            return why(function() barch.space[space]:cursor("a", "z") end)
        end
    """,
}

ENDED = b"FUNCTION cursor has ended"
OUTSIDE = b"FUNCTION cursor key is outside its range"
OFFSETS = b"FUNCTION cursor read offsets must be whole numbers from 0"

DATA = os.path.join(os.getcwd(), "cursor_data")
shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)
log = open(os.path.join(os.getcwd(), "cursor.log"), "a")
exec(open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_data.py")).read())
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000", "-c", "tx_lock_wait_ms=600000",
                      "-c", "compression=zstd"], stdout=log, stderr=log)
BASE = "cursor" + str(int(time.time()))


def client(space, **who):
    c = redis.Redis(host="127.0.0.1", port=PORT, single_connection_client=True,
                    socket_timeout=None, retry=None, **who)
    c.response_callbacks.pop("INFO", None)
    c.execute_command("USE", space)
    return c


def define(c, space, head=""):
    ok = True
    for name, body in FUNCTIONS.items():
        got = answer(c, "SETF", name, head + body)
        if got not in (b"OK", "OK"):
            check(False, "%s: SETF %s (%s)" % (space, name, short(got)))
            ok = False
    return ok


def same_as_point(c, where, keys, want=None):
    got = answer(c, "CALLF", "read", "k", "l", *keys)
    was = answer(c, "CALLF", "point", "k", "l", *keys)
    ok = isinstance(got, list) and got == was and (want is None or got == want)
    check(ok, "%s (%s)" % (where, "same" if ok else short(got)))


def equality(space):
    c = client(space)
    other = client(space)
    print("%s: a read is getBufferAt's" % space)
    known = {}
    for i in range(150):
        # plain keys and composite ones
        key = "k%03d" % i if i % 2 else "k:%03d" % i
        size = 0 if i % 13 == 0 else 100000 if i == 41 else 7424 if i % 3 == 0 else i
        known[key] = bytes((i + j) % 251 for j in range(size))
        c.set(key, known[key])
    lapsed = ["k:x%d" % i for i in range(10)] + ["kx%d" % i for i in range(10)]
    for key in lapsed:
        c.psetex(key, 1, "lapsed")
    gone = wait_for(30, lambda: answer(c, "CALLF", "point", "k", "l", *lapsed) == [b"false"] * len(lapsed))
    check(gone, "the short lived keys lapsed")
    absent = ["k", "k:none", "knone", "k:0000"]
    keys = sorted(known) + absent + lapsed
    want = [known.get(k, b"false") for k in keys]
    same_as_point(c, "outside a transaction", keys, want)
    same_as_point(c, "in any order", keys[::-1], want[::-1])
    check(answer(c, "CALLF", "handle", "k", "l", *keys) == want, "a space handle has it")
    got = answer(other, "CALLF", "elsewhere", space, "k", "l", "k:002", "k:none")
    check(got == [known["k:002"], b"false"], "another space's store has it (%s)" % short(got))
    for mode in ((), ("PESSIMISTIC",)):
        c.execute_command("BEGINTX", *mode)
        got = answer(c, "CALLF", "read", "k", "l", *keys)
        check(got == want, "in a transaction %s (%s)" % ("".join(mode).lower(), "same" if got == want else short(got)))
        got = answer(c, "COMMITTX")
        check(not str(got).startswith("ERR"), "and it commits (%s)" % short(got))


def arguments(space):
    c = client(space)
    print("%s: arguments" % space)
    c.set("k:arg", "abcdef")
    c.set("k:nil", "")
    cases = [
        # key, len, outat, from, n: the size and out
        ("k:arg", 8, 1, 0, "", b"6", b"#abcdef#"),
        ("k:arg", 5, 1, 2, 3, b"6", b"#cde#"),
        ("k:arg", 6, 1, 2, 99, b"6", b"#cdef#"),
        ("k:arg", 3, 1, 5, "", b"6", b"#f#"),
        ("k:arg", 3, 1, 6, "", b"6", b"###"),
        ("k:arg", 3, 1, 900, 4, b"6", b"###"),
        ("k:arg", 3, 1, 0, 0, b"6", b"###"),
        ("k:arg", 5, 0, 0, "", b"6", b"#####"),
        ("k:arg", 6, 1, 0, "", b"6", b"######"),
        ("k:arg", 6, 1, 2, 4, b"6", b"#cdef#"),
        ("k:arg", 4, 4, 0, 1, b"6", b"####"),
        ("k:arg", 4, 900, 0, 1, b"6", b"####"),
        ("k:arg", 4, 900, 6, "", b"6", b"####"),
        ("k:nil", 2, 1, 0, "", b"0", b"##"),
        ("k:none", 2, 0, 0, "", b"nil", b"##"),
    ]
    for tx in (False, True):
        if tx:
            c.execute_command("BEGINTX")
        for key, length, outat, start, n, size, out in cases:
            got = answer(c, "CALLF", "part", "k", "l", key, length, outat, start, n)
            check(got == [size, out], "%s len %s at %s from %s n %s%s (%s)"
                  % (key, length, outat, start, n, ", tx" if tx else "", short(got)))
        got = answer(c, "CALLF", "defaults", "k", "l", "k:arg")
        check(got == [b"6", b"6", b"6", b"#####"], "the defaults (%s)" % short(got))
        got = answer(c, "CALLF", "errors", "k", "l", "k:arg", "m:arg")
        want = [OUTSIDE] * 4 + [b"FUNCTION cursor read wants a buffer"] + [OFFSETS] * 6
        check(isinstance(got, list) and got[:11] == want, "each error's text (%s)" % short(got))
        check(isinstance(got, list) and b"null interior byte" in got[11], "a bound rangeValues refuses")
        if tx:
            answer(c, "ABORTTX")


def snapshot(space):
    c, other = client(space), client(space)
    print("%s: the snapshot and own writes" % space)
    other.set("k:changed", "old")
    other.set("k:deleted", "there")
    other.set("kchanged", "plain old")
    other.delete("k:added")
    c.execute_command("BEGINTX")
    other.set("k:changed", "new")
    other.set("kchanged", "plain new")
    other.delete("k:deleted")
    other.set("k:added", "late")
    keys = ["k:changed", "kchanged", "k:deleted", "k:added"]
    got = answer(c, "CALLF", "read", "k", "l", *keys)
    check(got == [b"old", b"plain old", b"there", b"false"], "another client's later changes aren't seen (%s)" % short(got))
    answer(c, "ABORTTX")

    c.set("k:own", "before")
    got = answer(c, "CALLF", "put_read", "k", "l", "k:own", "mine")
    check(got == [b"before", b"mine", b"false", b"false", b"inner", b"mine"],
          "own writes, a delete and a savepoint's rollback (%s)" % short(got))
    c.set("k:own", "before")
    c.execute_command("BEGINTX")
    got = answer(c, "CALLF", "around", "k", "l", "k:own")
    check(got == [b"before"] * 3, "a point read each side of a cursor's (%s)" % short(got))
    c.execute_command("SET", "k:own", "by command")
    got = answer(c, "CALLF", "read", "k", "l", "k:own")
    check(got == [b"by command"], "a client's write shows in the next read (%s)" % short(got))
    answer(c, "ABORTTX")

    c.execute_command("BEGINTX", "PESSIMISTIC")
    other.set("k:own", "latest")
    got = answer(c, "CALLF", "read", "k", "l", "k:own")
    check(got == [b"latest"], "a pessimistic one reads the latest (%s)" % short(got))
    answer(c, "ABORTTX")


def commits(space):
    c, other = client(space), client(space)
    print("%s: commit" % space)
    other.set("k:base", "one")
    other.set("k:skipped", "one")
    c.execute_command("BEGINTX")
    answer(c, "CALLF", "read", "k", "l", "k:base")
    other.set("k:skipped", "two")
    got = answer(c, "COMMITTX")
    check(not str(got).startswith("ERR"), "a read only commit succeeds (%s)" % short(got))

    def conflict(what, change, conflicts, fn="read"):
        other.set("k:skipped", "one")
        other.delete("k:new")
        c.execute_command("BEGINTX")
        got = answer(c, "CALLF", fn, "k", "l", "k:base")
        # writing what's there already writes nothing, and that commit checks no range
        c.execute_command("SET", "q:written", what)
        change()
        ended = answer(c, "COMMITTX")
        ok = "TXCONFLICT" in str(ended) if conflicts else not str(ended).startswith("ERR")
        check(not str(got).startswith("ERR") and ok, "%s (%s, %s)" % (what, short(got)[:30], short(ended)[:50]))

    conflict("a changed key it skipped conflicts", lambda: other.set("k:skipped", "three"), True)
    conflict("an added key conflicts", lambda: other.set("k:new", "x"), True)
    conflict("one in the plain region conflicts", lambda: other.set("knew", str(time.time())), True)
    conflict("a deleted key conflicts", lambda: other.delete("k:skipped"), True)
    conflict("one before the range commits", lambda: other.set("j:outside", str(time.time())), False)
    conflict("one at hi commits", lambda: other.set("l", str(time.time())), False)
    conflict("after a savepoint's rollback too", lambda: other.set("k:skipped", "four"), True, "open_undone")

    gone = client(space + "c")
    late = client(space + "c")
    define(gone, space + "c")
    for how in ("FLUSHDB", "LOAD"):
        gone.set("k:base", "one")
        if how == "LOAD":
            late.execute_command("SAVE")
        gone.execute_command("BEGINTX")
        answer(gone, "CALLF", "read", "k", "l", "k:base")
        did = answer(late, how)
        check(not str(did).startswith("ERR"), "%s runs (%s)" % (how, short(did)))
        got = answer(gone, "COMMITTX")
        check("TXCONFLICT" in str(got), "a %s after BEGINTX conflicts (%s)" % (how, short(got)))
        define(gone, space + "c")


def lifetime(space):
    c = client(space)
    print("%s: lifetime" % space)
    c.set("k:life", "v")
    got = answer(c, "CALLF", "lifetime", "k", "l", "k:life")
    check(got == [b"v", ENDED, b"v", ENDED, ENDED, ENDED, ENDED], "with the transaction (%s)" % short(got))
    check(answer(c, "CALLF", "kept", "open", "k", "l") == b"open", "one opens outside")
    got = answer(c, "CALLF", "kept", "read", "k:life")
    check(got == ENDED, "and ends when the call returns (%s)" % short(got))
    for mode in ((), ("PESSIMISTIC",)):
        c.execute_command("BEGINTX", *mode)
        answer(c, "CALLF", "kept", "open", "k", "l")
        got = answer(c, "CALLF", "kept", "read", "k:life")
        check(got == b"v", "it lasts across calls in a transaction (%s)" % short(got))
        answer(c, "COMMITTX")
        got = answer(c, "CALLF", "kept", "read", "k:life")
        check(got == ENDED, "and ends at COMMITTX (%s)" % short(got))
        c.execute_command("BEGINTX", *mode)
        got = answer(c, "CALLF", "kept", "read", "k:life")
        check(got == ENDED, "and stays ended in the next (%s)" % short(got))
        answer(c, "ABORTTX")
    c.set("k:two", "w")
    got = answer(c, "CALLF", "yields", "k", "l", "k:life", "k:two")
    check(got == [b"v", b"w"], "a yield between reads (%s)" % short(got))
    got = answer(c, "CALLF", "dropped", "k", "l", "k:life")
    check(got == [b"1", b"written"], "open cursors are collected (%s)" % short(got))
    got = answer(c, "CALLF", "locked", "k", "l", "k:life")
    check(got == b"v", "it reads in a locked region (%s)" % short(got))

    # a transaction keeps a dropped space, and a clear empties what it reads
    for how, want in (("FLUSHDB", b"false"), ("DROP", b"v")):
        name = space + how.lower()
        inside = client(name)
        define(inside, name)
        inside.set("k:life", "v")
        inside.execute_command("BEGINTX")
        answer(inside, "CALLF", "kept", "open", "k", "l")
        if how == "FLUSHDB":
            answer(client(name), "FLUSHDB")
        else:
            answer(client(space), "KSPACE", "DROP", name)
        got = answer(inside, "CALLF", "kept", "both", "k:life")
        check(got == [want, want], "a space after %s reads as a point read does (%s)" % (how, short(got)))
        answer(inside, "ABORTTX")
        check(p.poll() is None, "and the server is up")


def concurrent(space):
    c, other = client(space), client(space)
    print("%s: beside a writer" % space)
    rounds = scale.scaled(3000, 200)
    values = [bytes([i]) * ((i + 1) * 64) for i in (0, 3, 40, 115, 250)]
    other.set("k:torn", values[0])
    stop, wrote = threading.Event(), []

    def write():
        i = 0
        while not stop.is_set():
            i += 1
            other.set("k:torn", values[i % len(values)])
        wrote.append(i)

    t = threading.Thread(target=write)
    t.start()
    try:
        got = answer(c, "CALLF", "untorn", "k", "l", "k:torn", rounds)
        check(got == 0, "no torn value in %d reads (%s)" % (rounds, short(got)))
        c.execute_command("BEGINTX")
        got = answer(c, "CALLF", "untorn", "k", "l", "k:torn", rounds)
        check(got == 0, "nor in a transaction (%s)" % short(got))
        answer(c, "ABORTTX")
    finally:
        stop.set()
        t.join()


def waits(space):
    print("%s: the span lock" % space)
    holder, writer, free, watch = client(space), client(space), client(space), client(space)
    holder.set("k:held", "v")

    def waiting():
        rows = watch.execute_command("INFO", "transactions", "waiting")
        if isinstance(rows, bytes):
            rows = rows.decode()
        return "waiting=a lock on" in str(rows)

    def later(out, c, *cmd):
        t = threading.Thread(target=lambda: out.append(answer(c, *cmd)))
        t.start()
        return t

    holder.execute_command("BEGINTX", "PESSIMISTIC")
    if not check(answer(holder, "CALLF", "read", "k", "l", "k:held") == [b"v"], "the cursor reads"):
        answer(holder, "ABORTTX")
        return
    free.execute_command("BEGINTX", "PESSIMISTIC")
    got = answer(free, "SET", "q:free", "w")
    check(not str(got).startswith("ERR"), "a writer outside doesn't wait")
    answer(free, "ABORTTX")

    out = []
    writer.execute_command("BEGINTX", "PESSIMISTIC")
    t = later(out, writer, "SET", "k:skipped", "w")
    check(wait_for(60, waiting), "a writer inside waits for it")
    check(not out, "and hasn't written")
    answer(holder, "COMMITTX")
    t.join()
    check(len(out) == 1 and not str(out[0]).startswith("ERR"), "and writes once it's let go (%s)" % short(out))

    out = []
    holder.execute_command("BEGINTX", "PESSIMISTIC")
    t = later(out, holder, "CALLF", "read", "k", "l", "k:skipped")
    check(wait_for(60, waiting), "opening waits for a writer inside")
    answer(writer, "COMMITTX")
    t.join()
    check(out == [[b"w"]], "then reads what it wrote (%s)" % short(out))
    answer(holder, "ABORTTX")


def limits(space):
    print("%s: limits" % space)
    c = client(space)
    value = b"v" * 7424
    # no pipeline: it takes a connection of its own, which isn't in this space
    for at in range(1, 3001, 100):
        c.execute_command("MSET", *[x for i in range(at, at + 100) for x in ("m:%d" % i, value)])
    was = {}
    try:
        for name, v in (("tx_max_bytes", 8 * 1024 * 1024), ("tx_max_keys", 10)):
            got = c.execute_command("CONFIG", "GET", name)
            was[name] = list(got.values())[0] if isinstance(got, dict) else got[1]
            c.execute_command("CONFIG", "SET", name, v)
        c.execute_command("BEGINTX")
        got = answer(c, "CALLF", "many", "m:", "m;", 3000)
        check(got == [3000, 3000 * 7424], "22 MB read past 8 MiB and 10 keys (%s)" % short(got))
        got = answer(c, "COMMITTX")
        check(not str(got).startswith("ERR"), "and committed (%s)" % short(got))

        c.execute_command("CONFIG", "SET", "tx_max_keys", was["tx_max_keys"])
        c.execute_command("CONFIG", "SET", "tx_max_bytes", 100000)
        got = answer(c, "CALLF", "outgrows", "m:", "m;", "m:1")
        check("tx_max_bytes" in str(got) and "cursor" in str(got), "a read past the size limit fails (%s)" % short(got))
        answer(c, "ABORTTX")
    finally:
        answer(c, "ABORTTX")
        for name, v in was.items():
            c.execute_command("CONFIG", "SET", name, v)

    watch = client(space)
    age = watch.execute_command("CONFIG", "GET", "tx_max_age_ms")
    age = list(age.values())[0] if isinstance(age, dict) else age[1]

    def expired():
        info = watch.execute_command("INFO", "transactions")
        info = info.decode() if isinstance(info, bytes) else str(info)
        for line in info.splitlines():
            if line.startswith("tx_expired:"):
                return int(line.split(":")[1])
        return 0

    try:
        c.execute_command("BEGINTX")
        answer(c, "CALLF", "kept", "open", "m:", "m;")
        before = expired()
        watch.execute_command("CONFIG", "SET", "tx_max_age_ms", 1)
        check(wait_for(60, lambda: expired() > before), "the transaction expires")
        got = answer(c, "CALLF", "kept", "read", "m:1")
        check("tx_max_age_ms" in str(got), "and its next call is refused (%s)" % short(got))
        answer(c, "ABORTTX")
        watch.execute_command("CONFIG", "SET", "tx_max_age_ms", 200)
        got = answer(c, "CALLF", "outlives", "m:", "m;", "m:1")
        check("tx_max_age_ms" in str(got) and "cursor" in str(got),
              "a read past the time limit fails (%s)" % short(got))
    finally:
        watch.execute_command("CONFIG", "SET", "tx_max_age_ms", age)
        answer(c, "ABORTTX")


def permissions(space):
    print("%s: what it may read" % space)
    c = client(space)
    c.execute_command("configuration:SET", space + ".endpoint", "http://here")
    c.execute_command("configuration:SET", space + ".password", "test-value")
    asked = (space + ".endpoint", space + ".password")
    got = answer(c, "CALLF", "elsewhere", "configuration", space, space + "~", *asked)
    check(got == [b"http://here", b"false"], "a secret reads as nil (%s)" % short(got))
    own = client("configuration")
    if define(own, "configuration"):
        own.execute_command("BEGINTX")
        got = answer(own, "CALLF", "read", space, space + "~", *asked)
        check(got == [b"http://here", b"false"], "in a transaction there too (%s)" % short(got))
        answer(own, "ABORTTX")
    got = answer(c, "CALLF", "read", "r", "s", "read")
    check(got == [b"false"], "a function isn't a key (%s)" % short(got))

    there = space + "o"
    client(there).set("a:key", "v")
    c.execute_command("BEGINTX")
    got = answer(c, "CALLF", "refused", there)
    check(b"a transaction is open on another space" in (got if isinstance(got, bytes) else b""),
          "another space's refuses in a transaction (%s)" % short(got))
    answer(c, "ABORTTX")

    word = "cursor-test-only"
    c.execute_command("ACL", "SETUSER", "cursor_blind", "on", ">" + word, "+read", "+write", "+data", "+function")
    got = answer(client(space, username="cursor_blind", password=word), "CALLF", "refused")
    check(got == b"FUNCTION not authorized to read there", "no read permission (%s)" % short(got))
    c.execute_command("ACL", "SETUSER", "cursor_here", "on", ">" + word, "+read", "+write", "+keys", "+data", "+function")
    c.execute_command("KSPACE", "ACL", there, "SETUSER", "cursor_here", "on", "-read")
    got = answer(client(space, username="cursor_here", password=word), "CALLF", "refused", there)
    check(b"not authorized" in (got if isinstance(got, bytes) else b""), "none on that space (%s)" % short(got))


def sources(space):
    print("a tomb, a source, compression")
    c = client(space)
    f = space + "f"
    c.execute_command("configuration:SET", f + ".foreign", "fake")
    c.execute_command("configuration:SET", f + ".missing_ttl", "30")
    fc = client(f)
    fc.execute_command("FOREIGN", "FAKE", "SET", "k:there", "from-source")
    check(fc.get("k:there") == b"from-source" and fc.get("k:gone") is None, "a hit and a miss are cached")
    define(fc, f)
    keys = ["k:there", "k:gone", "k:unasked"]
    same_as_point(fc, "a cached miss is barch.tomb", keys, [b"from-source", b"tomb", b"false"])
    fc.execute_command("BEGINTX")
    same_as_point(fc, "and nil in a transaction", keys, [b"from-source", b"false", b"false"])
    answer(fc, "ABORTTX")

    below, above = client(space + "below"), client(space + "above")
    for i in range(20):
        below.set("k:%d" % i, "below %d" % i)
    c.execute_command("KSPACE", "DEPENDS", space + "above", "ON", space + "below")
    define(above, space + "above")
    above.set("k:1", "above")
    above.delete("k:2")
    keys = ["k:%d" % i for i in range(22)]
    want = [b"below %d" % i for i in range(20)] + [b"false"] * 2
    want[1], want[2] = b"above", b"false"
    same_as_point(above, "over a source", keys, want)
    above.execute_command("BEGINTX")
    check(answer(above, "CALLF", "read", "k", "l", *keys) == want, "over a source in a transaction")
    answer(above, "ABORTTX")

    zto = client(space + "z")
    define(zto, space + "z")
    for w in words:
        zto.execute_command("TRAIN", train_set[w])
    check(zto.execute_command("TRAIN") == 0, "a dictionary is trained")
    names = ["k:" + w for w in words]
    for w, name in zip(words, names):
        zto.set(name, test_set[w])
    done = [answer(zto, "COMPRESS", name) for name in names]
    check(1 in done, "values are compressed (%d of %d)" % (done.count(1), len(words)))
    plain = [test_set[w] if isinstance(test_set[w], bytes) else test_set[w].encode() for w in words]
    same_as_point(zto, "they read decompressed", names, plain)
    got = answer(zto, "CALLF", "part", "k", "l", names[0], 6, 1, 3, 4)
    check(got == [b"%d" % len(plain[0]), b"#" + plain[0][3:7] + b"#"], "and in part (%s)" % short(got))
    zto.execute_command("BEGINTX")
    same_as_point(zto, "and in a transaction", names, plain)
    answer(zto, "ABORTTX")


def in_raft():
    print("a Raft space")
    nodes = [clusternodes.Node(0, PORT + 2, BINARY), clusternodes.Node(1, PORT + 10, BINARY,
                                                                        env={"BARCH_TEST_TX_NO_LOCAL_CHECK": "1"})]
    try:
        nodes[0].start()
        init = answer(nodes[0].client(), "CLUSTER", "INIT")
        if "no cluster" in str(init):
            print("  SKIP, this build has no cluster")
            return
        # each is a cluster of its own: the second skips the local check, so its prepare decides
        for node, what in zip(nodes, ("the local check on", "the local check off")):
            if node.proc is None:
                node.start()
                answer(node.client(), "CLUSTER", "INIT")
            space = BASE + "raft"
            node.client("configuration").execute_command("SET", space + ".raft", "on")
            if not check(wait_for(60, lambda: leader_of([node], space) is not None), "it has a leader"):
                return
            c, other = node.client(space), node.client(space)
            define(c, space, '--@barch {"replicate": "effects"}\n')
            c.set("k:base", "one")
            for writes in (False, True):
                for key, conflicts in (("k:skipped", True), ("j:outside", False)):
                    c.execute_command("BEGINTX")
                    got = answer(c, "CALLF", "read", "k", "l", "k:base")
                    if writes:
                        c.execute_command("SET", "q:written", str(time.time()))
                    other.set(key, str(time.time()))
                    ended = answer(c, "COMMITTX")
                    ok = "TXCONFLICT" in str(ended) if conflicts else not str(ended).startswith("ERR")
                    check("one" in str(got) and ok, "%s, a %s, %s changed (%s)"
                          % (what, "writer" if writes else "reader", key, short(ended)))
            node.stop()
    finally:
        for node in nodes:
            node.stop()


try:
    scale.wait_for_port(PORT, proc=p)
    shapes = [BASE + "h", BASE + "r", BASE + "one"]
    setup = client(BASE + "setup")
    setup.execute_command("configuration:SET", shapes[1] + ".ordered", "1")
    setup.execute_command("configuration:SET", shapes[1] + ".shards", "7")
    setup.execute_command("configuration:SET", shapes[1] + ".range_sharded", "1")
    setup.execute_command("configuration:SET", shapes[2] + ".shards", "1")
    for s in shapes:
        check(define(client(s), s), "%s: the functions are set" % s)
        equality(s)
        arguments(s)
        snapshot(s)
        commits(s)
        lifetime(s)
        concurrent(s)
        waits(s)
        limits(s)
        permissions(s)
    sources(shapes[0])
    in_raft()
finally:
    p.terminate()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
