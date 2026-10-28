#!/usr/bin/env python3
"""
barch.store.rangeValues - a range with each key's value, from one walk - TODO 764.

A scan through lent's primary key or a unique index read the keys with
barch.store.range and then did a barch.store.get for each one. rangeValues answers two
tables, the keys and the values at the same positions, so the gets go away.

One barchd:
  - the pairs match what range and get answer, with an offset and a limit too, and
    through a space handle (sp:rangeValues);
  - a key with a space in it (a composite, kept in a region of its own) comes back with
    its value, in the place range puts it;
  - values the background pass has compressed come back as GET answers them, and the
    functions stored in that space still run (TODO 767);
  - in a transaction: its own writes and deletes, the snapshot (not another client's
    commits since), and a value another client changes inside the range makes
    COMMITTX answer TXCONFLICT;
  - the keys ending in "password" in the configuration space don't come back.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14826)
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


DATA = os.path.join(os.getcwd(), "rangevalues_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "rangevalues.log"), "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "rangevalues_" + str(int(time.time()))


def client(space=SPACE):
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", space)
    return c


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


# each answers "k=v,k=v" so a pair that lines up wrong shows
FUNCTIONS = {
    "pairs": """
function call(lo, hi, limit, offset)
    local ks, vs = barch.store.rangeValues(lo, hi, tonumber(limit), tonumber(offset or 0))
    local out = {}
    for i, k in ks do out[i] = k .. "=" .. tostring(vs[i]) end
    if #ks ~= #vs then return "keys and values differ in length" end
    return table.concat(out, ",")
end""",
    "gets": """
function call(lo, hi, limit, offset)
    local out = {}
    for i, k in barch.store.range(lo, hi, tonumber(limit), tonumber(offset or 0)) do
        out[i] = k .. "=" .. tostring(barch.store.get(k))
    end
    return table.concat(out, ",")
end""",
    "handle": """
function call(space, lo, hi)
    local ks, vs = barch.space[space]:rangeValues(lo, hi, 100)
    local out = {}
    for i, k in ks do out[i] = k .. "=" .. tostring(vs[i]) end
    return table.concat(out, ",")
end""",
    "secrets": """
function call(lo, hi)
    local ks, vs = barch.space["configuration"]:rangeValues(lo, hi, 100)
    return table.concat(ks, ",") .. "|" .. table.concat(vs, ",")
end""",
}

try:
    c = client()
    for name, body in FUNCTIONS.items():
        check(answer(c, "SETF", name, body) == "OK", "SETF %s" % name)

    print("the pairs are what range and get answer")
    for i in range(50):
        c.set("r%02d" % i, "value of %d" % i)
    c.set("q", "below")
    c.set("s", "above")
    whole = answer(c, "CALLF", "pairs", "r", "s", 100)
    check(whole == answer(c, "CALLF", "gets", "r", "s", 100), "the whole range (%s...)" % str(whole)[:40])
    check(str(whole).count("=") == 50 and str(whole).startswith("r00=value of 0"), "50 pairs, in order")
    check(answer(c, "CALLF", "pairs", "r", "s", 7) == answer(c, "CALLF", "gets", "r", "s", 7), "with a limit")
    got = answer(c, "CALLF", "pairs", "r", "s", 5, 31)
    check(got == "r31=value of 31,r32=value of 32,r33=value of 33,r34=value of 34,r35=value of 35",
          "with an offset (%s)" % got)
    check(answer(c, "CALLF", "pairs", "r", "s", 5, 60) == "", "an offset past the end answers nothing")
    check(answer(c, "CALLF", "handle", SPACE, "r4", "r42") == "r40=value of 40,r41=value of 41",
          "through a space handle")

    print("a key with a space in it")
    c.set("r20 b", "composite")
    got = answer(c, "CALLF", "pairs", "r2", "r21", 100)
    check(got == answer(c, "CALLF", "gets", "r2", "r21", 100) and "r20 b=composite" in str(got),
          "comes back with its value where range puts it (%s)" % got)
    c.delete("r20 b")

    print("compressed values")
    # the server switch reaches the default space's shards and the spaces made after
    # it, not ones already open, so it goes on before this space is made
    answer(c, "CONFIG", "SET", "compression", "zstd")
    z = client("rangevalues_z_" + str(int(time.time())))
    check(answer(z, "SETF", "pairs", FUNCTIONS["pairs"]) == "OK", "SETF pairs there")
    check(answer(z, "SETF", "gets", FUNCTIONS["gets"]) == "OK", "SETF gets there")
    left, n = 1, 0
    while left and n < 60:
        chunk = ("sample %d " % n) + "".join(chr((n * 7 + i) % 90 + 33) for i in range(30000))
        left = answer(z, "TRAIN", chunk)
        n += 1
    words = ["alpha", "bravo", "charlie", "delta", "echo"]
    for i in range(200):
        z.set("z%03d" % i, " ".join(words[(i + j) % 5] for j in range(60)))

    def compressed():
        # STATS answers a flat list of name, value, name, value...
        s = answer(z, "STATS")
        return int(dict(zip(s[::2], s[1::2])).get("value_bytes_compressed", 0)) if isinstance(s, list) else 0
    # wait for the pass to stop finding more, so the function sources had their turn too
    deadline, last, still = time.time() + 30, -1, 0
    while time.time() < deadline and still < 8:
        now = compressed()
        still = still + 1 if now == last and now > 0 else 0
        last = now
        time.sleep(0.25)
    check(compressed() > 0, "the background pass compressed some (%d bytes)" % compressed())
    # the sources SETF stored are keys in the space too - TODO 767
    got = answer(z, "CALLF", "pairs", "z000", "z002", 10)
    check(not str(got).startswith("ERR"), "a function stored there still runs (%s)" % str(got)[:60])
    want = ",".join("z%03d=%s" % (i, z.get("z%03d" % i)) for i in range(200))
    check(answer(z, "CALLF", "pairs", "z", "{", 1000) == want, "they come back as GET answers them")
    check(answer(z, "CALLF", "gets", "z", "{", 1000) == want, "the same as range and get")
    answer(c, "CONFIG", "SET", "compression", "none")

    print("in a transaction")
    for k in c.execute_command("KEYS", "k*"):
        c.delete(k)
    c.set("k1", "a")
    c.set("k2", "b")
    c.set("k3", "c")
    c.execute_command("BEGINTX")
    other = client()
    other.set("k2", "changed")
    other.set("k25", "new")
    c.execute_command("SET", "k3", "mine")
    c.execute_command("SET", "k4", "added")
    c.execute_command("DEL", "k1")
    got = answer(c, "CALLF", "pairs", "k", "l", 100)
    check(got == "k2=b,k3=mine,k4=added", "the snapshot with its own writes and deletes (%s)" % got)
    check(answer(c, "CALLF", "pairs", "k", "l", 1, 1) == "k3=mine", "and an offset there (%s)"
          % answer(c, "CALLF", "pairs", "k", "l", 1, 1))
    got = answer(c, "COMMITTX")
    check("TXCONFLICT" in str(got), "another client's writes in the range make it a conflict (%s)" % str(got)[:40])
    check(client().get("k4") is None, "and nothing of it is made")

    c.delete("k25")
    c.set("k2", "b")
    c.execute_command("BEGINTX")
    answer(c, "CALLF", "pairs", "k", "l", 100)
    client().set("k2", "changed again")     # only a value: no key comes or goes
    c.execute_command("SET", "zz", "mark")
    got = answer(c, "COMMITTX")
    check("TXCONFLICT" in str(got) and client().get("zz") is None,
          "a value changed in the range is a conflict too (%s)" % str(got)[:40])

    c.execute_command("BEGINTX")
    answer(c, "CALLF", "pairs", "k", "l", 100)
    client().set("m1", "outside")
    c.execute_command("SET", "zz", "mark")
    got = answer(c, "COMMITTX")
    check(got == "OK" and client().get("zz") == "mark", "a write outside it isn't (%s)" % str(got)[:40])

    print("the configuration space")
    conf = client("configuration")
    conf.set("rvtest.user", "someone")
    conf.set("rvtest.password", "hunter2")
    got = answer(c, "CALLF", "secrets", "rvtest.", "rvtest/")
    check(got == "rvtest.user|someone", "a key ending in password doesn't come back (%s)" % got)
    conf.delete("rvtest.user", "rvtest.password")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
