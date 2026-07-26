import redis
import barch

# Deep tests for SCAN, concentrating on the MATCH handling and therefore on the
# glob matcher in src/glob.cpp.
#
# glob::stringmatchlen picks one of two implementations. A pattern built only from
# literals, '*', '?' and '\' goes to the optimised asterisk_impl; the moment a '['
# shows up it falls back to stringmatchlen_impl, the untouched redis matcher. That
# hands us a free oracle: a pattern holding at least one plain literal can have that
# literal wrapped in a character class - '*abcd' becomes '*abc[d]' - which means
# exactly the same thing but is routed to the reference matcher. Both sides see an
# identical key, so a disagreement can only come from the optimised path.
#
# globdifftest.cpp drives the same comparison against the matcher directly and over
# a far larger corpus; this covers the route a real client takes.

PORT = 14000


def reference_pattern(pattern):
    """Rewrite one plain literal as a single element character class. Same meaning,
    but it forces glob::stringmatchlen onto the reference matcher. Returns None when
    there is no literal that can safely be wrapped."""
    for i in range(len(pattern) - 1, -1, -1):
        c = pattern[i]
        if not c.isalnum():
            continue
        if i > 0 and pattern[i - 1] == '\\':
            continue  # part of an escape, wrapping it would change the meaning
        return pattern[:i] + '[' + c + ']' + pattern[i + 1:]
    return None


def scan_all(r, match=None, count=None):
    """Run a SCAN to completion and hand back the keys and the number of round trips."""
    keys = []
    cursor = 0
    trips = 0
    while True:
        cursor, batch = r.scan(cursor=cursor, match=match, count=count)
        keys.extend(k.decode() for k in batch)
        trips += 1
        assert trips < 5000, f"SCAN did not terminate for match={match!r}"
        if cursor == 0:
            break
    return keys, trips


# subjects sitting on the seams of the optimised path - repeated leading characters,
# the literal run turning up more than once, and strings deliberately shorter than
# the pattern's minimum length. all of them carry a non digit so they stay string
# keys; a purely numeric key is stored as a number and comes back as an integer
SUBJECTS = [
    "a", "ab", "aa", "aaa", "abc", "abcd", "abcde", "abcdef",
    "xabcd", "xxabcd", "xxxabcd", "yabcde",
    "aabcd", "aaabcd", "aaaabcd", "abcabcd", "abcdabcd", "abdabcd",
    "zzz", "zzzz", "zzzzz",
    "aabbb", "aaabbb", "aXaabbb", "abbb",
    "key:1", "key:12", "key:123", "key:1234",
    "foo:bar:baz", "foo:bar:qux", "foobar", "foobarbaz", "foobarbazqux",
    "mississippi", "missing", "misp", "miss",
    "hello world", "hello.world", "hello*world", "hello?world",
    "UPPER", "Upper", "upper",
    "pre_abcd_post", "v12345", "v1234", "v123",
    "banana", "bandana", "bananana",
]

PATTERNS = [
    # nothing special
    "*", "**", "***", "?", "??", "a", "abc", "abcd", "abcde",
    # a star followed by a literal run of at least four - the memmem shortcut
    "*abcd", "*abcde", "*bcde", "*abcdef", "*bbb", "*cabcd",
    "*ello world", "*o:bar:baz", "*ssissippi", "*anana",
    # a star with a shorter literal run - the memchr shortcut and its retry
    "*ab", "*abc", "*aab", "*aabbb", "*abbb", "*bcd", "*a", "*z",
    # stars mixed with question marks, where the optimised path rewrites the pattern
    "*?abcd", "*??abcd", "*???abcd", "*????abcd",
    "*?bcd", "*??bcd", "*?abc", "*??abc", "*???abc",
    "*?a", "*?ab", "*?z", "*??z",
    # the same shapes closed off with a star. an anchored pattern currently matches
    # nothing at all (see the end of this file), which would leave both sides of the
    # comparison empty and prove nothing, so these keep the check honest
    "*?abcd*", "*??abcd*", "*???abcd*", "*????abcd*",
    "*?abc*", "*??abc*", "*???abc*", "*?bbb*", "*??bbb*",
    "*?anana*", "*??anana*", "*?issippi*", "*??issippi*",
    # question marks before the star as well
    "?*abcd", "??*abcd", "?*?abcd", "a?*bcd",
    # several stars
    "*abc*", "*a*b*c*", "*abcd*", "a*bcde", "a*b*cde", "*ab*cd", "*abcd*efgh",
    "**abcd", "*abcd**", "**abcd**",
    # a trailing literal run
    "abc*", "abcd*", "a*", "*d", "*abcd*d",
    # escapes, which switch the optimised path's backslash guard on
    r"*\*world", r"hello\*world", r"hello\?world", r"*\?world", r"*a\bcd",
    # anchored, no star at all
    "key:1", "key:1?", "key:1??", "key:???", "?????",
    "hello?world", "hello*world", "UPPER", "upper",
    # long literal runs, past the four byte lookahead
    "*bananana", "*bandana", "*foobarbazqux", "*mississippi",
    # nothing in the corpus can match these
    "*qqqq", "*zzzzzz", "?qqqq*", "*abcd?????????",
]


print("start scan glob test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("CLEARALL")
r.flushdb()

r.mset({s: f"v:{s}" for s in SUBJECTS})
assert r.dbsize() == len(SUBJECTS), f"expected {len(SUBJECTS)} keys, got {r.dbsize()}"

# an unfiltered scan has to hand back every key exactly once
everything, _ = scan_all(r)
assert sorted(everything) == sorted(SUBJECTS), "an unfiltered SCAN did not return the whole keyspace"
assert len(everything) == len(set(everything)), "an unfiltered SCAN returned duplicates"

# COUNT only changes how many round trips it takes, never the result
for count in (1, 2, 7, 1000):
    keys, trips = scan_all(r, count=count)
    assert sorted(keys) == sorted(SUBJECTS), f"SCAN COUNT {count} lost keys"
    assert len(keys) == len(set(keys)), f"SCAN COUNT {count} returned duplicates"
    if count == 1:
        assert trips > 1, "SCAN COUNT 1 should need more than one round trip"

# a pattern nothing can match still has to terminate and hand back an empty result
assert scan_all(r, match="*qqqqqqqq*")[0] == []

mismatches = []
unchecked = []

for pattern in PATTERNS:
    ref = reference_pattern(pattern)
    if ref is None:
        unchecked.append(pattern)
        continue
    optimised = set(scan_all(r, match=pattern)[0])
    expected = set(scan_all(r, match=ref)[0])
    if optimised != expected:
        mismatches.append((pattern, ref, expected, optimised))

if mismatches:
    print(f"\n{len(mismatches)} pattern(s) where the optimised glob path disagrees "
          f"with the reference matcher")
    for pattern, ref, expected, got in mismatches:
        print(f"  pattern {pattern!r} checked against {ref!r}")
        for s in sorted(expected ^ got):
            side = "missing" if s in expected else "returned but should not be"
            print(f"    {s!r} {side}")

if unchecked:
    print(f"\n{len(unchecked)} pattern(s) had no literal to wrap and were skipped "
          f"here; globdifftest.cpp covers them: {unchecked}")

assert not mismatches, "the optimised glob matcher disagrees with the reference matcher"

# Anchored patterns, which is to say anything not ending in '*' or '?'. This is a
# separate concern from the matcher: both implementations are handed the key with a
# trailing byte still attached, so a pattern anchored at the end can never match.
anchored = []
for pattern, should_match in [("abcd", "abcd"), ("*abcd", "abcd"), ("*abcd", "xabcd"),
                              ("*bcde", "abcde"), ("abc", "abc"), ("*d", "abcd"),
                              ("*abc[d]", "abcd")]:
    got = set(scan_all(r, match=pattern)[0])
    if should_match not in got:
        anchored.append((pattern, should_match, sorted(got)))

if anchored:
    print(f"\n{len(anchored)} anchored pattern(s) matched nothing they should have")
    for pattern, wanted, got in anchored:
        print(f"  SCAN MATCH {pattern!r} should include {wanted!r}, returned {got}")

assert not anchored, "SCAN MATCH does not honour patterns anchored at the end of the key"

r.close()
barch.stop()
print("complete scan glob test")