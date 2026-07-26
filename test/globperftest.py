import os
import random
import time

import redis
import barch

# Measures what the optimised glob path in src/glob.cpp is actually worth, through
# the VALUES command.
#
# VALUES globs over the stored values, so a corpus of large values puts nearly all
# the work inside the matcher. glob::stringmatchlen sends a pattern built only from
# literals, '*', '?' and '\' to the optimised asterisk_impl, and falls back to the
# reference redis matcher the moment a '[' appears. Wrapping one literal in a single
# element character class - '*zqxjw*' becomes '*zqxj[w]*' - means exactly the same
# thing but forces the reference path, so the two can be timed against identical
# data and identical semantics.
#
# COUNT is used throughout so the reply is a single integer and reply building never
# enters the measurement.

PORT = 14000

VALUE_LEN = 1000
# about 110 MiB of values by default, which is enough that the matcher rather than the
# surrounding machinery decides the timings. A smaller corpus can be asked for where the
# machine cannot spare that much - a shared CI runner that has already run the rest of
# the suite in the same directory, for instance - at the cost of a noisier measurement.
ENTRIES = int(os.environ.get("BARCH_PERF_ENTRIES", "115000"))

# a single VALUES over this corpus is milliseconds on an idle machine. The timeout is
# far above anything healthy, and exists so that a server which stops answering gives a
# clear error with the memory numbers beside it, rather than hanging until whatever is
# watching gives up and the process is torn down mid-scan.
COMMAND_TIMEOUT_S = float(os.environ.get("BARCH_PERF_TIMEOUT", "120"))
NEEDLE = "zqxjw"           # absent from the filler alphabet below
NEEDLE_HITS = 500
COMMON = "commontoken"     # planted in half the corpus
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


print("start glob performance test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
try:


    r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2,
                    socket_timeout=COMMAND_TIMEOUT_S)
    r.execute_command("CLEARALL")
    r.flushdb()
    r.execute_command("CONFIG SET rpc_max_buffer 1m")

    # ---------------------------------------------------------------- build the corpus
    random.seed(20260726)
    pool = ''.join(random.choice(FILLER) for _ in range(1 << 20))
    pool += pool[:VALUE_LEN]

    needle_at = set(random.sample(range(ENTRIES), NEEDLE_HITS))

    print(f"  loading {ENTRIES} values of {VALUE_LEN} bytes ...")
    load_start = time.perf_counter()
    value_bytes = 0
    key_bytes = 0
    batch = {}
    for i in range(ENTRIES):
        off = (i * 7919) % (1 << 20)
        v = pool[off:off + VALUE_LEN]
        if i in needle_at:
            cut = 400
            v = v[:cut] + NEEDLE + v[cut + len(NEEDLE):]
        if i % 2 == 0:
            v = v[:100] + COMMON + v[100 + len(COMMON):]
        k = f"perf:{i:07d}"
        batch[k] = v
        key_bytes += len(k)
        value_bytes += len(v)
        if len(batch) == 100:
            r.mset(batch)
            batch = {}
    if batch:
        r.mset(batch)

    total = key_bytes + value_bytes
    print(f"  loaded {total / MIB:.1f} MiB "
          f"({value_bytes / MIB:.1f} MiB of values) in {time.perf_counter() - load_start:.1f}s")
    assert r.dbsize() == ENTRIES, f"expected {ENTRIES} keys, got {r.dbsize()}"
    if ENTRIES == 115000:
        assert total >= 100 * MIB, f"corpus is only {total / MIB:.1f} MiB, wanted at least 100 MiB"
    print(f"  server memory: {memory_line(r)}")

    # ------------------------------------------------------- the fixed cost of a scan
    # '*' returns from the matcher on its first character, so this is the page iteration
    # and callback cost that both paths pay and neither can avoid
    baseline, baseline_count = time_values(r, "*")
    assert baseline_count == ENTRIES, f"'*' should match everything, matched {baseline_count}"
    print(f"  scan overhead, matcher does nothing: {baseline:8.1f} ms")
    print(f"  server memory: {memory_line(r)}\n")

    # ------------------------------------------------------------------- the patterns
    PATTERNS = [
        ("*zqxjw*",    "four literals after the star, memmem shortcut, rare hit"),
        # the planted needle is always followed by filler, and q is not in the filler,
        # so this run cannot occur anywhere in the corpus
        ("*zqxjwq*",   "same shortcut on a run that never occurs, so every value misses"),
        ("*zqx*",      "star close behind the run, memchr shortcut instead of memmem"),
        ("*?zqxjw*",   "question mark between the star and the run, the '*?' rewrite"),
        ("*" + COMMON + "*", "run planted in half the corpus, so most values hit"),
    ]

    print(f"  {'pattern':<16} {'optimised':>11} {'reference':>11} {'saved':>9} {'matcher only':>14} {'matches':>9}")
    print(f"  {'-' * 16} {'-' * 11} {'-' * 11} {'-' * 9} {'-' * 14} {'-' * 9}")

    fast_total = 0.0
    ref_total = 0.0
    fast_net_total = 0.0
    ref_net_total = 0.0

    for pattern, note in PATTERNS:
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

        print(f"  {pattern:<16} {fast_ms:8.1f} ms {ref_ms:8.1f} ms {saved:8.1f}% "
              f"{net_saved:13.1f}% {fast_count:9d}")
        print(f"  {'':<16} {note}")

    overall = (ref_total - fast_total) / ref_total * 100.0
    overall_net = ((ref_net_total - fast_net_total) / ref_net_total * 100.0
                   if ref_net_total > 0 else 0.0)

    print()
    print(f"  end to end : {fast_total:8.1f} ms against {ref_total:8.1f} ms, "
          f"{overall:.1f}% less time")
    print(f"  matcher    : {fast_net_total:8.1f} ms against {ref_net_total:8.1f} ms, "
          f"{overall_net:.1f}% less time")
    print()
    print("  'saved' is the whole VALUES call, which both paths pay the scan overhead for.")
    print("  'matcher only' has that overhead subtracted, so it is the part the optimised")
    print("  path can actually affect. The reference column carries one character class,")
    print("  which costs it a little more than a plain literal - the matcher figure is")
    print("  therefore a slight overstatement, not a clean lower bound.")

    # a regression guard rather than a target: the optimised path must not end up slower
    assert fast_total <= ref_total * 1.05, (
        f"the optimised path took {fast_total:.1f} ms against the reference's "
        f"{ref_total:.1f} ms - it is no longer paying for itself")

    r.close()
finally:
    barch.stop()
print("complete glob performance test")
