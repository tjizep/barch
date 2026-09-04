import os

import scale
import random
import time

import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

# Measures what the optimised glob path in src/glob.cpp is actually worth, through
# the VALUES command, over two corpora chosen to sit at opposite ends of what the
# optimisation depends on.
#
# VALUES globs over the stored values, so a corpus of large values puts nearly all
# the work inside the matcher. glob::stringmatchlen sends a pattern built only from
# literals, '*', '?' and '\' to the optimised asterisk_impl, and falls back to the
# reference redis matcher the moment a '[' appears. Wrapping one literal in a single
# element character class - '*zqxjw*' becomes '*zqxj[w]*' - means exactly the same
# thing but forces the reference path, so the two can be timed against identical data
# and identical semantics.
#
# What the optimisation actually turns on is character distribution. After a '*' the
# fast path either runs memmem over the next four literal bytes, or memchr over the
# single byte at pattern[1]. Both are SIMD scans, and both only earn anything by
# skipping ground the reference matcher would walk a position at a time - which they
# can only do when what they are looking for is rare. Point memchr at a byte that
# occurs in every document and it returns almost at once, over and over, and the
# advantage collapses towards nothing.
#
# So there are two phases. The first is the friendly case: filler drawn from an
# alphabet that excludes the letters being searched for. The second is JSON, where a
# realistic query starts with a quote or a colon and those occur dozens of times in
# every value.
#
# COUNT is used throughout so the reply is a single integer and reply building never
# enters the measurement.

PORT = scale.port(default=14000)

VALUE_LEN = 1000
# about 110 MiB of values per phase by default, which is enough that the matcher rather
# than the surrounding machinery decides the timings. A smaller corpus can be asked for
# where the machine cannot spare that much - a shared CI runner that has already run the
# rest of the suite in the same directory, for instance - at the cost of a noisier
# measurement.
ENTRIES = scale.env_int("BARCH_PERF_ENTRIES", 115000, floor=2000)

# a single VALUES over this corpus is milliseconds on an idle machine. The timeout is
# far above anything healthy, and exists so that a server which stops answering gives a
# clear error with the memory numbers beside it, rather than hanging until whatever is
# watching gives up and the process is torn down mid-scan.
COMMAND_TIMEOUT_S = float(os.environ.get("BARCH_PERF_TIMEOUT", "120"))

# which corpus to measure, one per run. They are deliberately not both loaded in the
# same process: writing to a space that has already been globbed over is presently
# thousands of times slower per key than writing to a fresh one, so a second phase in
# the same server would spend minutes loading and measure nothing useful. See TODO 13.
#   filler - letters the searched-for bytes never appear in, the best case
#   json   - documents where a query starts with a quote or a colon, the worst case
CORPUS = os.environ.get("BARCH_PERF_CORPUS", "filler")

NEEDLE = "zqxjw"           # absent from both corpora except where planted
NEEDLE_HITS = 500
COMMON = "commontoken"     # planted in half the first corpus
# the filler deliberately leaves out j, q, w, x and z so the needle can only appear
# where it is planted
FILLER = "abcdefghiklmnoprstuvy"

REPEATS = 3
MIB = 1024 * 1024


def reference_pattern(pattern):
    """Wrap one plain literal in a single element character class. Same meaning, but
    it forces glob::stringmatchlen onto the reference matcher."""
    for i in range(len(pattern) - 1, -1, -1):
        c = pattern[i]
        if not c.isalnum():
            continue
        if i > 0 and pattern[i - 1] == '\\':
            continue
        return pattern[:i] + '[' + c + ']' + pattern[i + 1:]
    raise ValueError(f"no literal to wrap in {pattern!r}")


def memory_line(r):
    """used_memory and rss, so a run that fails under memory pressure says so in its
    own output instead of leaving the reader to guess."""
    try:
        # sent as one token so redis-py hands back the raw section rather than
        # applying its own INFO parser to it
        raw = r.execute_command("INFO MEMORY")
        if isinstance(raw, dict):
            fields = {k.decode() if isinstance(k, bytes) else str(k):
                      v.decode() if isinstance(v, bytes) else str(v) for k, v in raw.items()}
        else:
            if isinstance(raw, bytes):
                raw = raw.decode()
            fields = {}
            for line in raw.splitlines():
                name, sep, value = line.partition(":")
                if sep:
                    fields[name.strip()] = value.strip()
        return (f"used={fields.get('used_memory_human', '?')} "
                f"rss={fields.get('used_memory_rss_human', '?')} "
                f"peak={fields.get('used_memory_peak_human', '?')}")
    except Exception as e:
        return f"unavailable ({type(e).__name__})"


def time_values(r, pattern):
    """Best of REPEATS runs of VALUES <pattern> COUNT, in milliseconds, with the
    match count so the two paths can be checked against each other."""
    r.execute_command("VALUES", pattern, "COUNT")  # warm up, not measured
    best = None
    count = None
    for _ in range(REPEATS):
        start = time.perf_counter()
        count = r.execute_command("VALUES", pattern, "COUNT")
        elapsed = (time.perf_counter() - start) * 1000.0
        best = elapsed if best is None else min(best, elapsed)
    return best, count


def load(r, prefix, value_for):
    """Fill the space with ENTRIES values and say how many bytes went in."""
    started = time.perf_counter()
    key_bytes = value_bytes = 0
    batch = {}
    for i in range(ENTRIES):
        k = f"{prefix}{i:07d}"
        v = value_for(i)
        batch[k] = v
        key_bytes += len(k)
        value_bytes += len(v)
        if len(batch) == 100:
            r.mset(batch)
            batch = {}
    if batch:
        r.mset(batch)
    total = key_bytes + value_bytes
    print(f"  loaded {total / MIB:.1f} MiB ({value_bytes / MIB:.1f} MiB of values) "
          f"in {time.perf_counter() - started:.1f}s")
    print(f"  server memory: {memory_line(r)}")
    return total


def measure(r, patterns):
    """Time every pattern against its reference twin and print a table. Returns the
    totals so the caller can compare one phase against the other."""
    # '*' returns from the matcher on its first character, so this is the page
    # iteration and callback cost that both paths pay and neither can avoid
    baseline, baseline_count = time_values(r, "*")
    assert baseline_count == ENTRIES, f"'*' should match everything, matched {baseline_count}"
    print(f"  scan overhead, matcher does nothing: {baseline:8.1f} ms")
    print(f"  server memory: {memory_line(r)}\n")

    print(f"  {'pattern':<22} {'optimised':>11} {'reference':>11} {'saved':>9} "
          f"{'matcher only':>14} {'matches':>9}")
    print(f"  {'-' * 22} {'-' * 11} {'-' * 11} {'-' * 9} {'-' * 14} {'-' * 9}")

    fast_total = ref_total = fast_net_total = ref_net_total = 0.0
    for pattern, note in patterns:
        ref = reference_pattern(pattern)
        fast_ms, fast_count = time_values(r, pattern)
        ref_ms, ref_count = time_values(r, ref)

        assert fast_count == ref_count, (
            f"{pattern!r} matched {fast_count} but {ref!r} matched {ref_count} - "
            f"the two matchers disagree, the timings below are meaningless")

        # subtracting the scan overhead leaves the part either matcher can influence
        fast_net = max(fast_ms - baseline, 0.0)
        ref_net = max(ref_ms - baseline, 0.0)
        saved = (ref_ms - fast_ms) / ref_ms * 100.0 if ref_ms > 0 else 0.0
        net_saved = (ref_net - fast_net) / ref_net * 100.0 if ref_net > 0 else 0.0

        fast_total += fast_ms
        ref_total += ref_ms
        fast_net_total += fast_net
        ref_net_total += ref_net

        print(f"  {pattern:<22} {fast_ms:8.1f} ms {ref_ms:8.1f} ms {saved:8.1f}% "
              f"{net_saved:13.1f}% {fast_count:9d}")
        print(f"  {'':<22} {note}")

    overall = (ref_total - fast_total) / ref_total * 100.0 if ref_total > 0 else 0.0
    overall_net = ((ref_net_total - fast_net_total) / ref_net_total * 100.0
                   if ref_net_total > 0 else 0.0)
    print()
    print(f"  end to end : {fast_total:8.1f} ms against {ref_total:8.1f} ms, "
          f"{overall:.1f}% less time")
    print(f"  matcher    : {fast_net_total:8.1f} ms against {ref_net_total:8.1f} ms, "
          f"{overall_net:.1f}% less time")
    return fast_total, ref_total, overall, overall_net


# ------------------------------------------------------------------ the two corpora
random.seed(20260726)
_pool = ''.join(random.choice(FILLER) for _ in range(1 << 20))
_pool += _pool[:VALUE_LEN]
_needle_at = set(random.sample(range(ENTRIES), min(NEEDLE_HITS, ENTRIES)))

_JSON_WORDS = ["alpha", "bravo", "delta", "echo", "india", "kilo", "lima", "mike",
               "november", "oscar", "papa", "romeo", "sierra", "tango", "victor"]


def filler_value(i):
    """Random letters from an alphabet that excludes the letters searched for, so the
    scans almost always run the whole length of the value and reject it in one pass.
    The best case for the optimisation."""
    off = (i * 7919) % (1 << 20)
    v = _pool[off:off + VALUE_LEN]
    if i in _needle_at:
        v = v[:400] + NEEDLE + v[400 + len(NEEDLE):]
    if i % 2 == 0:
        v = v[:100] + COMMON + v[100 + len(COMMON):]
    return v


def _make_json_doc(seed, marked):
    """One document shaped like JSON. Padded with more fields rather than filler, so
    quotes and colons keep their natural density all the way through."""
    rnd = random.Random(seed)
    fields = [f'"id":{seed}',
              f'"user":"{rnd.choice(_JSON_WORDS)}_{seed % 997}"',
              f'"status":"{rnd.choice(["active", "pending", "closed"])}"',
              f'"score":{rnd.randrange(1000)}']
    if marked:
        fields.append(f'"marker":"{NEEDLE}"')
    n = 0
    while sum(len(f) for f in fields) + len(fields) + 2 < VALUE_LEN:
        fields.append(f'"f{n}":"{rnd.choice(_JSON_WORDS)}{rnd.randrange(10000)}"')
        n += 1
    return ("{" + ",".join(fields) + "}")[:VALUE_LEN]


# rendered once and reused. Building a fresh document per entry costs over two minutes
# of python for a corpus this size, which would dwarf what is being measured and make
# the test far heavier than it needs to be on a shared runner. The matcher does not
# care that documents repeat, only what they are made of.
_JSON_VARIANTS = 128
_JSON_DOCS = [_make_json_doc(s, False) for s in range(_JSON_VARIANTS)]
_JSON_MARKED = [_make_json_doc(s, True) for s in range(_JSON_VARIANTS)]


def json_value(i):
    """A JSON document, so a realistic query starts with a quote or a colon -
    characters that occur dozens of times in every value. That is the case the
    shortcuts cannot skip ground in, because the byte they scan for is everywhere."""
    docs = _JSON_MARKED if i in _needle_at else _JSON_DOCS
    return docs[i % _JSON_VARIANTS]


# Phase one: the byte the shortcut scans for is absent from the corpus.
FILLER_PATTERNS = [
    ("*zqxjw*", "four literals after the star, memmem shortcut, rare hit"),
    # the planted needle is always followed by filler, and q is not in the filler,
    # so this run cannot occur anywhere in the corpus
    ("*zqxjwq*", "same shortcut on a run that never occurs, so every value misses"),
    ("*zqx*", "star close behind the run, memchr shortcut instead of memmem"),
    ("*?zqxjw*", "question mark between the star and the run, the '*?' rewrite"),
    ("*" + COMMON + "*", "run planted in half the corpus, so most values hit"),
]

# Phase two: the byte the shortcut scans for is a quote or a colon, which every
# document is full of. The '?' in the memchr rows is what forces that branch - with
# four plain literals after the star the pattern would take memmem instead.
JSON_PATTERNS = [
    ('*"marker":"zqxjw"*', "memmem on a quote, but the four byte needle is still rare"),
    ('*"?zqxjw*', "memchr on a quote, which occurs dozens of times per value"),
    ('*:?zqxjw*', "memchr on a colon, just as common"),
    ('*"status":"active"*', "memmem on a run a third of the corpus contains"),
    ('*"?ctive"*', "memchr on a quote again, and most values match"),
]


print(f"start glob performance test [{CORPUS}]")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2,
                    socket_timeout=COMMAND_TIMEOUT_S)
    r.execute_command("CLEARALL")
    r.flushdb()
    r.execute_command("CONFIG SET rpc_max_buffer 1m")

    if CORPUS == "json":
        heading = "JSON, where a query starts with a quote or a colon"
        value_for, patterns, prefix = json_value, JSON_PATTERNS, "json:"
    else:
        heading = "filler without the letters being searched for"
        value_for, patterns, prefix = filler_value, FILLER_PATTERNS, "perf:"

    print(f"\n=== {heading} ===")
    print(f"  loading {ENTRIES} values of about {VALUE_LEN} bytes ...")
    total = load(r, prefix, value_for)
    assert r.dbsize() == ENTRIES, f"expected {ENTRIES} keys, got {r.dbsize()}"
    if ENTRIES == 115000:
        assert total >= 100 * MIB, f"corpus is only {total / MIB:.1f} MiB, wanted at least 100 MiB"

    fast_total, ref_total, overall, overall_net = measure(r, patterns)

    print()
    if CORPUS == "json":
        print("  A quote or a colon appears dozens of times in every document, so the")
        print("  scan the shortcut relies on stops almost at once and has skipped almost")
        print("  nothing. What is left is the ordinary matching loop, which is why the")
        print("  memchr rows here save little or nothing at all - one of them is slower")
        print("  than the reference, the shortcut having cost more than it found.")
        print("  Run the same file with BARCH_PERF_CORPUS=filler for the other end.")
    else:
        print("  The bytes being searched for do not occur in this corpus, so memchr and")
        print("  memmem cross a whole value and reject it in a single pass where the")
        print("  reference matcher walks it a position at a time. This is the case the")
        print("  shortcuts were built for and they are worth a great deal here.")
        print("  Run the same file with BARCH_PERF_CORPUS=json for the other end.")
    print()
    print("  'saved' is the whole VALUES call, which both paths pay the scan overhead")
    print("  for. 'matcher only' has that overhead subtracted, so it is the part the")
    print("  optimised path can actually affect. The reference column carries one")
    print("  character class, which costs it a little more than a plain literal - the")
    print("  matcher figure is therefore a slight overstatement, not a clean lower bound.")

    # a regression guard rather than a target: the optimised path must not end up
    # meaningfully slower overall. The json corpus is allowed less headroom because it
    # is expected to come out close to even
    ceiling = 1.15 if CORPUS == "json" else 1.05
    assert fast_total <= ref_total * ceiling, (
        f"the optimised path took {fast_total:.1f} ms against the reference's "
        f"{ref_total:.1f} ms on the {CORPUS} corpus - it is no longer paying for itself")

    r.close()
finally:
    barch.stop()
print(f"complete glob performance test [{CORPUS}]")
