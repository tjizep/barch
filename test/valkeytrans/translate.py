# Turn valkey's tcl tests into cases barch can be run against.
#
# See TODO 40. The point of translating rather than writing fresh assertions is that
# valkey's suite is a specification of the behaviour we decided to aim for, written by the
# people who define it. The point of doing it with a script rather than by hand is that a
# hand translation of 665 tests is unreviewable, and cannot be re-run when valkey is
# bumped.
#
# What this understands is the regular part of the tcl, which is most of it:
#
#     test {name} { body } {expected}      the value of the last command, glob matched
#     assert_equal {expected} [r cmd ...]
#     assert_error {pattern} {r cmd ...}   the command must fail, matching pattern
#     catch {r cmd ...} err ; format $err  the error text, glob matched
#     list [r cmd ...] [r cmd ...]         several results as one list
#     set res {} ; append res [r cmd ...]  several results concatenated with no separator
#     roundFloat [r cmd ...]               the reply rounded, as valkey's helper does
#
# Anything it cannot read becomes a skipped case carrying the original tcl, so the output
# says what was not translated instead of quietly covering less than it appears to. Those
# are for a human to finish, and `translate.py --report` counts them.
#
# The output is data - a json file of cases - rather than generated python. Generated
# python that asserts is harder to review than a list of commands and expectations, and
# the same data drives both sides of the differential run in differential.py.
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def find_brace(text, start):
    """index just past the '}' matching the '{' at `start`, honouring backslash escapes"""
    assert text[start] == "{", text[start:start + 20]
    depth = 0
    i = start
    while i < len(text):
        c = text[i]
        if c == "\\":
            i += 2
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    raise ValueError("unbalanced brace from offset %d" % start)


def find_quote(text, start):
    """index just past the '\"' matching the one at `start`"""
    assert text[start] == '"'
    i = start + 1
    while i < len(text):
        if text[i] == "\\":
            i += 2
            continue
        if text[i] == '"':
            return i + 1
        i += 1
    raise ValueError("unterminated quote from offset %d" % start)


def split_words(line):
    """tcl words: bare, {braced}, "quoted", [bracketed]. Nesting is preserved."""
    words = []
    i = 0
    n = len(line)
    while i < n:
        while i < n and line[i] in " \t":
            i += 1
        if i >= n:
            break
        if line[i] == "{":
            j = find_brace(line, i)
            words.append(("brace", line[i + 1:j - 1]))
            i = j
        elif line[i] == '"':
            j = find_quote(line, i)
            words.append(("quote", line[i + 1:j - 1]))
            i = j
        elif line[i] == "[":
            depth = 0
            j = i
            while j < n:
                if line[j] == "[":
                    depth += 1
                elif line[j] == "]":
                    depth -= 1
                    if depth == 0:
                        j += 1
                        break
                j += 1
            words.append(("bracket", line[i + 1:j - 1]))
            i = j
        else:
            j = i
            while j < n and line[j] not in " \t":
                j += 1
            words.append(("bare", line[i:j]))
            i = j
    return words


def find_tests(text):
    """every `test <name> <body> [expected] [tags]` in the file, as raw pieces"""
    out = []
    for m in re.finditer(r'(?<![\w-])test\s+', text):
        i = m.end()
        if i >= len(text) or text[i] not in "{\"":
            continue
        try:
            if text[i] == "{":
                j = find_brace(text, i)
                name = text[i + 1:j - 1]
            else:
                j = find_quote(text, i)
                name = text[i + 1:j - 1]
            k = j
            while k < len(text) and text[k] in " \t\n":
                k += 1
            if k >= len(text) or text[k] != "{":
                continue
            e = find_brace(text, k)
            body = text[k + 1:e - 1]
            rest = text[e:e + 200]
            mexp = re.match(r'\s*\{([^{}]*)\}', rest)
            expected = mexp.group(1) if mexp else None
        except ValueError:
            continue
        out.append({"name": name.strip(), "body": body, "expected": expected})
    return out


# helpers whose meaning is valkey's internals or its tcl runtime, not a command we can send
UNTRANSLATABLE = re.compile(
    r'\b(assert_encoding|assert_refcount|debug\s+object|debug\s+jmap|debug\s+sleep|'
    r'populate|wait_for_condition|wait_for_ofs_sync|assert_replication_stream|'
    r'foreach|while|proc|for\s*\{|if\s*\{|lsort|lappend|llength|lindex|expr|'
    r'assert_morethan|assert_lessthan|assert_range|debug_sleep|reconnect|'
    r'wait_for_sync|assert_type|memory_usage)\b')


def parse_body(body):
    """
    the commands a body runs and what it is checked against.

    Returns (steps, mode) or None when something in the body is outside what this
    understands. `mode` says how the expectation is applied:
      last    - the last command's reply is compared
      err     - the text of an error raised by a caught command is compared
      asserts - the body carries its own assert_equal lines and there is nothing else
      list    - several replies are compared as one space separated list
    """
    body = re.sub(r'\\\s*\n\s*', ' ', body)   # tcl line continuations
    lines = [l.strip() for l in body.split("\n")]
    lines = [l for l in lines if l and not l.startswith("#")]
    if not lines:
        return None
    if any(UNTRANSLATABLE.search(l) for l in lines):
        return None

    steps = []
    mode = "last"
    for line in lines:
        # `set res {}` and friends only initialise an accumulator; the value is built by
        # the `append` lines that follow
        if re.match(r'^set\s+\w+\s*\{\}$', line) or line.startswith("set err"):
            continue
        m = re.match(r'^catch\s*\{\s*r\s+(.*?)\s*\}\s*(\w+)?$', line)
        if m:
            steps.append({"op": "catch", "args": tcl_args(m.group(1))})
            mode = "err"
            continue
        if re.match(r'^format\s+\$\w+$', line) or re.match(r'^set\s+\w+$', line):
            mode = "err"
            continue
        m = re.match(r'^append\s+\w+\s+\[\s*r\s+(.*?)\s*\]$', line)
        if m:
            steps.append({"op": "cmd", "args": tcl_args(m.group(1)), "collect": True})
            mode = "concat"
            continue
        m = re.match(r'^assert_error\s+(.*)$', line)
        if m:
            parsed = parse_assert_error(m.group(1))
            if parsed is None:
                return None
            steps.append(parsed)
            if mode == "last":
                mode = "asserts"
            continue
        m = re.match(r'^roundFloat\s+\[\s*r\s+(.*?)\s*\]$', line)
        if m:
            steps.append({"op": "cmd", "args": tcl_args(m.group(1)), "round": True})
            continue
        m = re.match(r'^assert_equal\s+(.*)$', line)
        if m:
            parsed = parse_assert_equal(m.group(1))
            if parsed is None:
                return None
            steps.append(parsed)
            mode = "asserts" if mode != "err" else mode
            continue
        m = re.match(r'^assert\s*\{(.*)\}$', line)
        if m:
            return None  # a tcl expression, not a command
        m = re.match(r'^r\s+(.*)$', line)
        if m:
            steps.append({"op": "cmd", "args": tcl_args(m.group(1))})
            continue
        m = re.match(r'^list\s+(.*)$', line)
        if m:
            rounded = re.findall(r'\[\s*roundFloat\s+\[\s*r\s+([^\]]*?)\s*\]\s*\]', m.group(1))
            plain = re.findall(r'(?<!roundFloat )\[\s*r\s+([^\]]*?)\s*\]', m.group(1))
            # only the commands inside the `list` make up the value. A plain `r ...`
            # line before it is setup - folding it in made `r del k` count as a result
            # and the case compare one element too long, which then failed on valkey and
            # was dropped as an unfaithful translation
            if rounded:
                for one in rounded:
                    steps.append({"op": "cmd", "args": tcl_args(one),
                                  "round": True, "collect": True})
            elif plain:
                for one in plain:
                    steps.append({"op": "cmd", "args": tcl_args(one), "collect": True})
            else:
                return None
            mode = "list"
            continue
        return None
    if not steps:
        return None
    return steps, mode


def parse_assert_equal(rest):
    """assert_equal {expected} [r cmd ...] -> a step that checks one reply"""
    words = split_words(rest)
    if len(words) != 2:
        return None
    (k1, w1), (k2, w2) = words
    if k2 != "bracket":
        return None
    m = re.match(r'^\s*r\s+(.*)$', w2)
    if not m:
        return None
    return {"op": "expect", "args": tcl_args(m.group(1)), "value": w1.strip()}


def parse_assert_error(rest):
    """assert_error {pattern} {r cmd ...} -> a step that requires the command to fail"""
    words = split_words(rest)
    if len(words) != 2:
        return None
    (_, pattern), (kind, body) = words
    m = re.match(r'^\s*r\s+(.*)$', body)
    if not m:
        return None
    return {"op": "expect_error", "args": tcl_args(m.group(1)), "value": pattern.strip()}


def tcl_args(text):
    """a command's arguments, with braces and quotes removed but content kept"""
    out = []
    for kind, w in split_words(text):
        if kind == "bracket":
            raise Unsupported("nested command substitution: [%s]" % w)
        if "$" in w:
            raise Unsupported("variable reference: %s" % w)
        out.append(w)
    return out


class Unsupported(Exception):
    pass


def translate_file(path):
    text = open(path, encoding="utf-8", errors="replace").read()
    cases = []
    for raw in find_tests(text):
        entry = {"name": raw["name"], "expected": raw["expected"]}
        try:
            parsed = parse_body(raw["body"])
        except Unsupported as e:
            parsed = None
            entry["why"] = str(e)
        if parsed is None:
            entry["skipped"] = True
            entry.setdefault("why", "body uses tcl this translator does not read")
            entry["tcl"] = raw["body"].strip()
        else:
            steps, mode = parsed
            entry["steps"] = steps
            entry["mode"] = mode
        cases.append(entry)
    return cases


def main(argv):
    if len(argv) < 2:
        print("usage: translate.py <file.tcl> [more.tcl ...]")
        return 2
    outdir = os.path.join(HERE, "cases")
    os.makedirs(outdir, exist_ok=True)
    total = translated = 0
    for path in argv[1:]:
        cases = translate_file(path)
        name = os.path.splitext(os.path.basename(path))[0]
        out = os.path.join(outdir, name + ".json")
        with open(out, "w") as f:
            json.dump({"source": os.path.basename(path), "cases": cases}, f, indent=1)
        ok = sum(1 for c in cases if not c.get("skipped"))
        total += len(cases)
        translated += ok
        print("%-14s %3d tests  %3d translated  %3d left as stubs -> %s"
              % (name + ".tcl", len(cases), ok, len(cases) - ok, os.path.relpath(out, HERE)))
    if total:
        print("total: %d of %d translated (%.0f%%)" % (translated, total, 100.0 * translated / total))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
