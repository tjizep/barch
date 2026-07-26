# Done

Finished entries from `TODO.md`, newest last. Each records what was actually found,
which is not always what the original entry predicted.

## 1. Out of bounds read in both glob matchers [26-07-2026]

*Was `TODO.md` entry 5.*

`glob.cpp` read one byte past the end of the pattern in four places, not the two the
original entry described. The star run collapse and the trailing star loop each
appear in both `asterisk_impl` and `stringmatchlen_impl`:

```c
while (patternLen && pattern[1] == '*')   // guarded patternLen, not index 1
while (*pattern == '*')                   // no guard at all
```

Now `patternLen > 1 && pattern[1] == '*'` and `patternLen && *pattern == '*'`.

The read was not the whole problem. When the byte past the end happened to be `*`,
the loop consumed it and drove `patternLen` to 0 or -1. A negative length reads as
truthy in `while (patternLen && stringLen)`, and `if (patternLen == 0 && stringLen ==
0) return 1` then never fires, so matches silently became misses and a negative
length propagated into the recursive call. Unfixed, 12 of 16 probes gave the wrong
answer.

Covered by stage 5 of `test/globdifftest.cpp`. It carries expected answers rather
than comparing the two implementations, because both matchers shared the bug - stages
1 to 4 report zero disagreements on the unfixed code, so a differential could never
have found it. Two passes are needed:

- string literals with a hostile trailing `*`, which reproduce the wrong answer
  deterministically in any build but stay invisible to a sanitizer, since a literal
  `"**"` is three bytes and index 1 is still owned memory
- exactly sized `new char[len]` buffers, where index `len` lands in a redzone. This
  pass asserts nothing about the result, because the neighbouring byte is arbitrary;
  it exists only so a sanitizer build has something to trap

Verified both ways against a copy of the unfixed file: unfixed gives 12 wrong answers
and an ASAN `heap-buffer-overflow` in `asterisk_impl`, fixed gives neither.

One expectation in the original probe set was wrong and was corrected rather than
"fixed" in the code: `*` does not match an empty subject. The main loop is `while
(patternLen && stringLen)`, so with an empty subject it never runs and only an empty
pattern returns 1. That is stock redis behaviour.

## 2. VALUES globs over values and answers with keys [26-07-2026]

*Was `TODO.md` entry 6.*

Confirmed intended, no code change. `VALUES 3` matches the value `"3"` and replies
with `solo:only`, the key holding it - it is the inverse lookup `KEYS` cannot do.
`COUNT` replaces the reply with the number of matches.

The question was raised because `replyshapetest.py` had been written against observed
behaviour, so the assertions could equally have been recording a bug. They are now a
specification, and the test comment says so.

The behaviour was also missing from the command's own documentation: the block above
`VALUES` in `barch.cpp` said "match against all values using a glob pattern" without
mentioning that the reply is keys, which is the surprising half. That is now written
down where someone reading the implementation will see it.
