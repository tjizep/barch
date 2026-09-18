# OFFSET on directory listings - FS LS, DIR LS and Luau fs.list - TODO 373.
#
# Every page is checked against a slice of the same listing read whole, so the
# test doesn't depend on knowing the order. The offset steps over entries rather
# than keys, because a directory's key range also holds its subdirectories'
# contents; directories with subdirectories are here to catch that.
import time

import scale

import redis
import barch

scale.workdir()
PORT = scale.port(default=14180)

print("start fs offset test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)

# the space reads its configuration when it's built, so this comes first
conf = barch.KeyValue("configuration")
conf.set("fso.fs_source_list", "lister")

fs = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
fs.execute_command("USE", "fso")
fs.execute_command("FLUSHDB")

failures = []


def check(ok, what):
    if not ok:
        print("  FAIL " + what, flush=True)
        failures.append(what)


def names(lines):
    # `kind size version name`, the name last because it can hold a space
    return [l.decode().split(" ", 3)[3] for l in lines]


def put_all(paths):
    p = fs.pipeline(transaction=False)
    for i, path in enumerate(paths):
        p.execute_command("FS", "PUT", path, "x" * (i % 50 + 1))
        if i % 500 == 499:
            p.execute()
    p.execute()


# /d: files, subdirectories with their own files and a nested level, and a name with
# a space in it - so skipping keys and skipping entries would give different answers
paths = ["/d/f%03d.txt" % i for i in range(300)]
for s in range(20):
    paths += ["/d/sub%02d/g%d.txt" % (s, j) for j in range(5)]
    paths += ["/d/sub%02d/deeper/h%d.txt" % (s, j) for j in range(3)]
paths.append("/d/with space.txt")
flat = scale.scaled(20000, 4000)
paths += ["/flat/n%06d" % i for i in range(flat)]
put_all(paths)
fs.execute_command("FS", "MKDIR", "/d/emptydir")

# the source knows some names that are stored and some that aren't
assert fs.execute_command("SETF", "lister", """function call(dir)
    if dir ~= "/d" then return {} end
    local out = {}
    for i = 0, 39 do out[#out + 1] = string.format("remote%02d.bin", i) end
    out[#out + 1] = "f001.txt"
    return out
end""") == b"OK"

try:
    def check_listing(label, base, full, tail=()):
        # SOURCE, when there is one, has to be last on the line
        n = len(full)
        offsets = sorted(set([0, 1, 2, 19, 20, 21, n // 2, n - 1, n, n + 7]
                             + list(range(290, min(n, 330)))))
        for off in offsets:
            for lim in (1, 7, 50):
                got = names(fs.execute_command(*base, "OFFSET", off, "LIMIT", lim, *tail))
                if got != full[off:off + lim]:
                    check(False, "%s OFFSET %d LIMIT %d gave %r, wanted %r"
                          % (label, off, lim, got[:3], full[off:off + 3]))
                    return
        # no LIMIT: everything after the offset
        check(names(fs.execute_command(*base, "OFFSET", 5, *tail)) == full[5:],
              "%s OFFSET 5" % label)
        # the old forms are unchanged
        check(names(fs.execute_command(*base, "LIMIT", 10, *tail)) == full[:10],
              "%s LIMIT 10" % label)
        print("  %s: %d entries, %d offsets" % (label, n, len(offsets)), flush=True)

    print("FS LS with subdirectories", flush=True)
    dfull = names(fs.execute_command("FS", "LS", "/d"))
    check(len(dfull) == 300 + 20 + 1 + 1, "every /d entry is listed (%d)" % len(dfull))
    check("sub00" in dfull and "emptydir" in dfull and "with space.txt" in dfull,
          "subdirectories, an empty one and a spaced name are entries")
    check_listing("FS LS /d", ("FS", "LS", "/d"), dfull)
    # AFTER and OFFSET together: the offset counts from the cursor
    after = dfull[100]
    rest = dfull[101:]
    for off in (0, 3, 150, len(rest)):
        got = names(fs.execute_command("FS", "LS", "/d", "AFTER", after, "OFFSET", off, "LIMIT", 9))
        check(got == rest[off:off + 9], "FS LS /d AFTER %s OFFSET %d" % (after, off))

    print("FS LS SOURCE merges before it counts", flush=True)
    sfull = names(fs.execute_command("FS", "LS", "/d", "SOURCE"))
    check(len(sfull) == len(dfull) + 40, "SOURCE adds the 40 names only the source knows")
    check_listing("FS LS /d SOURCE", ("FS", "LS", "/d"), sfull, ("SOURCE",))
    # SOURCE goes last on the line, so check that spelling too
    got = names(fs.execute_command("FS", "LS", "/d", "OFFSET", 300, "LIMIT", 30, "SOURCE"))
    check(got == sfull[300:330], "FS LS OFFSET 300 LIMIT 30 SOURCE")

    print("a big flat directory", flush=True)
    ffull = names(fs.execute_command("FS", "LS", "/flat"))
    check(len(ffull) == flat, "every /flat entry is listed")
    for off in (0, 1, 999, flat // 2, flat - 10, flat):
        got = names(fs.execute_command("FS", "LS", "/flat", "OFFSET", off, "LIMIT", 10))
        check(got == ffull[off:off + 10], "FS LS /flat OFFSET %d" % off)
    t0 = time.perf_counter()
    fs.execute_command("FS", "LS", "/flat", "OFFSET", flat - 10, "LIMIT", 10)
    t_skip = time.perf_counter() - t0
    t0 = time.perf_counter()
    fs.execute_command("FS", "LS", "/flat")
    t_full = time.perf_counter() - t0
    print("  /flat: the last page by OFFSET %.1f ms; the whole listing %.1f ms"
          % (t_skip * 1000, t_full * 1000), flush=True)
    # a skipped file's record isn't read, so reaching the last page is well under
    # listing the lot, where every record is read and sent
    check(t_skip * 3 < t_full, "skipping entries is cheaper than listing them")

    print("bad OFFSET values", flush=True)
    for args in (("OFFSET", -1), ("OFFSET", "x")):
        try:
            fs.execute_command("FS", "LS", "/d", *args)
            check(False, "FS LS %r should be refused" % (args,))
        except redis.ResponseError as e:
            check("OFFSET" in str(e), "FS LS %r refused with %r" % (args, str(e)))

    print("DIR LS over plain keys", flush=True)
    kv = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
    kv.execute_command("USE", "dirs")
    kv.execute_command("FLUSHDB")
    p = kv.pipeline(transaction=False)
    for i in range(250):
        p.set("a:k%03d" % i, "v" * (i % 9))
    for s in range(15):
        for j in range(4):
            p.set("a:node%02d:x%d" % (s, j), "v")
    p.set("a:node03", "both a key and a node")
    p.execute()
    lfull = [l.decode().split(" ", 2)[2] for l in kv.execute_command("DIR", "LS", "a")]
    check(len(lfull) == 250 + 15, "every child of a is listed (%d)" % len(lfull))
    for off in (0, 1, 14, 15, 16, 200, 249, 250, 264, 265, 300):
        for lim in (1, 10):
            got = [l.decode().split(" ", 2)[2] for l in
                   kv.execute_command("DIR", "LS", "a", "OFFSET", off, "LIMIT", lim)]
            if got != lfull[off:off + lim]:
                check(False, "DIR LS a OFFSET %d LIMIT %d" % (off, lim))
    # the kind and size fields survive a skip in front of them
    whole = kv.execute_command("DIR", "LS", "a")
    check(kv.execute_command("DIR", "LS", "a", "OFFSET", 3, "LIMIT", 20) == whole[3:23],
          "DIR LS lines are unchanged by the offset")
    for sub in ("RM", "COUNT"):
        try:
            kv.execute_command("DIR", sub, "a", "OFFSET", 5)
            check(False, "DIR %s OFFSET should be refused" % sub)
        except redis.ResponseError as e:
            check("only for DIR LS" in str(e), "DIR %s OFFSET refused with %r" % (sub, str(e)))
    check(kv.exists("a:k000") == 1, "and nothing was removed")
    print("  DIR LS a: %d children" % len(lfull), flush=True)

    print("Luau fs.list", flush=True)
    assert fs.execute_command("SETF", "lsl", """function call(path, limit, offset, source)
    local got = barch.fs.list(path, nil, tonumber(limit), source == "1", tonumber(offset))
    local out = {}
    for i, e in ipairs(got) do out[i] = e.name end
    return out
end""") == b"OK"
    for path, full, src in (("/d", dfull, "0"), ("/d", sfull, "1"), ("/flat", ffull, "0")):
        n = len(full)
        for off in (0, 1, 20, 21, n // 2, n - 1, n):
            got = [x.decode() for x in fs.execute_command("LSL", path, 12, off, src)]
            check(got == full[off:off + 12], "Luau fs.list %s offset %d source %s" % (path, off, src))
    # the old four-argument call, with no offset at all
    assert fs.execute_command("SETF", "lsl0", """function call()
    local got = barch.fs.list("/d", nil, 5)
    local out = {}
    for i, e in ipairs(got) do out[i] = e.name end
    return out
end""") == b"OK"
    check([x.decode() for x in fs.execute_command("LSL0")] == dfull[:5],
          "Luau fs.list without an offset")
    try:
        fs.execute_command("LSL", "/d", 5, -1, "0")
        check(False, "a negative Luau offset should be refused")
    except redis.ResponseError as e:
        check("negative" in str(e), "negative Luau offset refused with %r" % str(e))

    if failures:
        raise AssertionError("%d checks failed, first: %s" % (len(failures), failures[0]))
    print("fs offset test complete", flush=True)
finally:
    barch.stop()
