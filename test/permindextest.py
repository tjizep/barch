# Secondary indexes over the fields of composite keys - TODO 422, phase 1.
#
# An index over keys with `fields` components and `pinned` more that stay last (an
# n-gram's doc id) keeps C(n, n/2) chains, orderings of the fields in which every set
# of fields is a prefix of exactly one. INDEX FIND answers equality on any set of
# fields from the chain that has exactly them in front. Checked against brute force.
import itertools
import math
import random

import scale

import redis
import barch

scale.workdir()
PORT = scale.port(default=14490)

print("start perm index test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

conf = barch.KeyValue("configuration")
conf.set("pi.key_split", "|")

failures = []


def check(ok, what):
    if not ok:
        print("  FAIL " + what, flush=True)
        failures.append(what)


r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, decode_responses=True)
r.execute_command("USE", "pi")
r.execute_command("FLUSHDB")


def refused(want, *cmd):
    try:
        got = r.execute_command(*cmd)
    except redis.ResponseError as e:
        check(want in str(e), "%s: %r should say %r" % (" ".join(map(str, cmd)), str(e), want))
        return
    check(False, "%s should be refused, got %r" % (" ".join(map(str, cmd)), got))


# --- the chains: C(n, n/2) of them, and every subset a prefix of exactly one ---------
for n in range(1, 9):
    name = "c%d" % n
    check(r.execute_command("INDEX", "CREATE", name, "COMPOSITE", n) == "OK", "CREATE %s" % name)
    lines = r.execute_command("INDEX", "CHAINS", name)
    check(len(lines) == math.comb(n, n // 2), "%d fields: %d chains, want %d" % (n, len(lines), math.comb(n, n // 2)))
    orders = [[int(x) for x in line.split()[2:]] for line in lines]
    check(all(sorted(o) == list(range(n)) for o in orders), "%d fields: every chain orders all the fields" % n)
    covered = {}
    for c, o in enumerate(orders):
        for k in range(n + 1):
            covered.setdefault(frozenset(o[:k]), set()).add(c)
    check(len(covered) == 2 ** n, "%d fields: every subset is a prefix of some chain" % n)
    check(all(line.split()[1] == "not-built" for line in lines), "%d fields: nothing built yet" % n)
    r.execute_command("INDEX", "DROP", name)

# --- n-grams: three words and the doc id, pinned last --------------------------------
rnd = random.Random(422)
vocab = ["the", "quick", "brown", "fox", "lazy", "dog", "red", "7", "12"]
keys = set()
for doc in range(40):
    words = [rnd.choice(vocab) for _ in range(30)]
    for i in range(len(words) - 2):
        keys.add((words[i], words[i + 1], words[i + 2], doc * 1000 + i))
for k in keys:
    r.set("%s|%s|%s|%d" % k, "v")
r.set("not|enough|1", "v")          # 3 components, not 4: skipped
r.set("plain", "v")                  # not a composite at all: not looked at

check(r.execute_command("INDEX", "CREATE", "tri", "COMPOSITE", 3, "PINNED", 1) == "OK", "CREATE tri")
refused("already", "INDEX", "CREATE", "tri", "COMPOSITE", 3)
refused("isn't built: INDEX BUILD tri", "INDEX", "FIND", "tri", "1=quick")
got = r.execute_command("INDEX", "BUILD", "tri", "ALL")
check(got == "records=%d skipped=1" % (3 * len(keys)), "BUILD ALL: %r, want %d records and 1 skipped" % (got, 3 * len(keys)))
check(r.execute_command("INDEX", "LIST") == ["tri composite fields=3 pinned=1 chains=3 built=0,1,2"],
      "LIST: %r" % r.execute_command("INDEX", "LIST"))


def as_key(k):
    return "%s|%s|%s|%d" % k


def brute(eq):
    return sorted(as_key(k) for k in keys if all(str(k[f]) == v for f, v in eq))


# every set of fields, with values taken from real keys so there's something to find
sample = rnd.sample(sorted(keys), 25)
tried = 0
for k in sample:
    for size in range(0, 4):
        for fields in itertools.combinations(range(3), size):
            eq = [(f, str(k[f])) for f in fields]
            got = r.execute_command("INDEX", "FIND", "tri", *["%d=%s" % (f, v) for f, v in eq])
            want = brute(eq)
            if sorted(got) != want:
                check(False, "FIND %r: %d keys, want %d" % (eq, len(got), len(want)))
            tried += 1
check(r.execute_command("INDEX", "FIND", "tri", "0=nothing") == [], "a value that isn't there finds nothing")
got = r.execute_command("INDEX", "FIND", "tri", "0=the", "LIMIT", 3)
check(len(got) == 3 and set(got) <= set(brute([(0, "the")])), "LIMIT: %r" % got)
# numbers are fields too: "7" and "12" are stored as numbers, and found as numbers
check(sorted(r.execute_command("INDEX", "FIND", "tri", "1=7")) == brute([(1, "7")]), "a numeric field")
print("perm index: %d finds against brute force over %d keys" % (tried, len(keys)), flush=True)

refused("has fields 0 to 2", "INDEX", "FIND", "tri", "3=x")
refused("given twice", "INDEX", "FIND", "tri", "0=the", "0=fox")
refused("1 to 8", "INDEX", "CREATE", "nine", "COMPOSITE", 9)
refused("1 to 8", "INDEX", "CREATE", "zero", "COMPOSITE", 0)
refused("letters, digits", "INDEX", "CREATE", "bad name!", "COMPOSITE", 2)
refused("no index called", "INDEX", "FIND", "nosuch")

# one chain only: the others stay unbuilt, and a FIND that needs them says which
check(r.execute_command("INDEX", "CREATE", "one", "COMPOSITE", 3, "PINNED", 1) == "OK", "CREATE one")
r.execute_command("INDEX", "BUILD", "one", 1)
chains = r.execute_command("INDEX", "CHAINS", "one")
check([c.split()[1] for c in chains] == ["not-built", "built", "not-built"], "only chain 1 built: %r" % chains)
first = int(chains[1].split()[2])      # chain 1's first field
check(sorted(r.execute_command("INDEX", "FIND", "one", "%d=fox" % first)) == brute([(first, "fox")]),
      "a FIND through the one built chain")

# phase 1 doesn't follow writes after a build: a new key isn't found until a rebuild
r.set("fox|fox|fox|999999", "v")
check("fox|fox|fox|999999" not in r.execute_command("INDEX", "FIND", "tri", "0=fox", "1=fox", "2=fox"),
      "phase 1: a key written after BUILD isn't in the index yet")
r.execute_command("INDEX", "BUILD", "tri", "ALL")
check("fox|fox|fox|999999" in r.execute_command("INDEX", "FIND", "tri", "0=fox", "1=fox", "2=fox"),
      "and it is after a rebuild")

# DROP takes the paths and the definition, and leaves other indexes alone
ix = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, decode_responses=True)
ix.execute_command("USE", "pi_ix")
before = ix.dbsize()
check(r.execute_command("INDEX", "DROP", "one") == "OK", "DROP one")
# `one` was built before the fox key was written, so it holds exactly len(keys)
check(ix.dbsize() == before - len(keys), "DROP one took exactly its chain's paths: %d -> %d" % (before, ix.dbsize()))
check([l.split()[0] for l in r.execute_command("INDEX", "LIST")] == ["tri"], "LIST after DROP")
check(len(r.execute_command("INDEX", "FIND", "tri", "0=the")) > 0, "the other index still answers")
check(r.execute_command("INDEX", "DROP", "tri") == "OK", "DROP tri")
check(ix.dbsize() == 0, "no paths left: %d" % ix.dbsize())
check(r.execute_command("INDEX", "LIST") == [], "no indexes left")

if failures:
    print("perm index test FAILED: %d" % len(failures), flush=True)
    raise SystemExit(1)
print("perm index test ok", flush=True)
barch.stop()
