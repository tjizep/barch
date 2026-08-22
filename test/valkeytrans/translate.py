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
#     assert_equal [r cmd ...] expected    either argument order
#     assert {[r cmd] eq ...} / == ...     a single equality
#     assert_error {pattern} {r cmd ...}   the command must fail, matching pattern
#     catch {r cmd ...} err ; format $err  the error text, glob matched
#     set v [r cmd ...] ; list $v1 $v2     captured replies, optionally mixed with [r cmd]
#     list [r cmd ...] [r cmd ...]         several results as one list
#     set res {} ; append res [r cmd ...]  several results concatenated with no separator
#     roundFloat [r cmd ...]               the reply rounded, as valkey's helper does
#     create_default_zset / create_zset    the zset.tcl helpers, expanded to ZADD
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
            t = e
            while t < len(text) and text[t] in " \t\n":
                t += 1
            # `{OK {} 1}` is a real expectation; a regex that stops at the first
            # nested brace used to drop those tests' expected value entirely
            expected = None
            if t < len(text) and text[t] == "{":
                u = find_brace(text, t)
                expected = text[t + 1:u - 1]
            elif t < len(text) and text[t] == '"':
                u = find_quote(text, t)
                expected = tcl_unescape(text[t + 1:u - 1])
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
    r'binary\s+format|binary\s+scan|string\s+repeat|randomInt|randstring|'
    r'wait_for_sync|assert_type|memory_usage|LPOS)\b')


def expand_known_helpers(line):
    """
    zset.tcl's create_zset helpers are a DEL plus a ZADD. Expanding the call
    keeps the test and leaves the foreach inside the proc untranslated.
    """
    try:
        words = split_words(line)
    except ValueError:
        return None
    if not words:
        return None
    name = words[0][1]
    if name == "create_default_zset" and len(words) == 1:
        return [
            "r del zset",
            "r zadd zset -inf a 1 b 2 c 3 d 4 e 5 f +inf g",
        ]
    if name == "create_default_lex_zset" and len(words) == 1:
        return [
            "r del zset",
            "r zadd zset 0 alpha 0 bar 0 cool 0 down 0 elephant 0 foo "
            "0 great 0 hill 0 omega",
        ]
    if name == "create_long_lex_zset" and len(words) == 1:
        return [
            "r del zset",
            "r zadd zset 0 alpha 0 bar 0 cool 0 down 0 elephant 0 foo "
            "0 great 0 hill 0 island 0 jacket 0 key 0 lip 0 max 0 null "
            "0 omega 0 point 0 query 0 result 0 sea 0 tree",
        ]
    if name == "create_zset" and len(words) == 3:
        key, items = words[1][1], words[2][1]
        return [
            "r del %s" % key,
            "r zadd %s %s" % (key, " ".join(items.split())),
        ]
    if name == "create_long_zset" and len(words) == 3:
        key, nword = words[1][1], words[2][1]
        try:
            n = int(nword)
        except ValueError:
            return None
        if n < 0 or n > 64:
            return None
        pairs = []
        for i in range(n):
            pairs.extend((str(i), "i%d" % i))
        return [
            "r del %s" % key,
            "r zadd %s %s" % (key, " ".join(pairs)),
        ]
    return None


def parse_simple_assert(expr):
    """[r cmd] eq value, or [r cmd] == value. Anything else stays a stub."""
    try:
        words = split_words(expr.strip())
    except ValueError:
        return None
    if len(words) != 3 or words[0][0] != "bracket":
        return None
    if words[1] != ("bare", "eq") and words[1] != ("bare", "=="):
        return None
    m = re.match(r'^\s*r\s+(.*)$', words[0][1])
    if not m:
        return None
    return {"op": "expect", "args": tcl_args(m.group(1)),
            "value": expected_value(words[2][0], words[2][1])}


def parse_assert_var_eq(expr, steps):
    """assert {$retval == 2} against a reply stored by set retval [r cmd]."""
    m = re.match(r'^\$([A-Za-z_][A-Za-z0-9_]*)\s+==\s+(.+)$', expr.strip())
    if not m:
        return None
    var, rhs = m.group(1), m.group(2).strip()
    if not re.match(r'^-?\d+(\.\d+)?$', rhs):
        return None
    for s in reversed(steps):
        if s.get("as") == var:
            s["op"] = "expect"
            s["value"] = rhs
            return True
    return None


def parse_assert_match_caught(rest, steps):
    """assert_match {*ERR*} $e after catch {r cmd} e -> expect_error."""
    try:
        words = split_words(rest)
    except ValueError:
        return None
    if len(words) != 2:
        return None
    (k1, w1), (k2, w2) = words
    if k2 != "bare" or not re.match(r'^\$[A-Za-z_]', w2):
        return None
    pattern = expected_value(k1, w1)
    for s in reversed(steps):
        if s["op"] == "catch":
            s["op"] = "expect_error"
            s["value"] = pattern
            return True
    return None


def parse_body(body):
    """
    the commands a body runs and what it is checked against.

    Returns a dict {steps, mode, vars?, bind?} or None when something in the
    body is outside what this understands. `mode` says how the expectation is
    applied:
      last    - the last command's reply is compared
      err     - the text of an error raised by a caught command is compared
      asserts - the body carries its own assert_equal lines and there is nothing else
      list    - several replies are compared as one space separated list
      bind    - the reply stored in `bind` is compared, after later cleanup ran
    """
    body = re.sub(r'\\\s*\n\s*', ' ', body)   # tcl line continuations
    raw = [l.strip() for l in body.split("\n")]
    lines = []
    for l in raw:
        if not l or l.startswith("#"):
            continue
        expanded = expand_known_helpers(l)
        if expanded:
            lines.extend(expanded)
        else:
            lines.append(l)
    if not lines:
        return None
    if any(UNTRANSLATABLE.search(l) for l in lines):
        return None

    steps = []
    mode = "last"
    bind = None
    list_vars = None
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
        m = re.match(r'^set\s+_\s+\$(\w+)$', line)
        if m:
            if mode != "err":
                bind = m.group(1)
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
            parsed = parse_assert_equal_or_bound(m.group(1), steps)
            if parsed is None:
                return None
            if parsed is not True:
                steps.append(parsed)
            if mode == "last":
                mode = "asserts"
            continue
        m = re.match(r'^assert_match\s+(.*)$', line)
        if m:
            parsed = parse_assert_match_caught(m.group(1), steps)
            if parsed is not True:
                return None
            if mode == "err":
                mode = "asserts"
            elif mode == "last":
                mode = "asserts"
            continue
        m = re.match(r'^assert\s*\{(.*)\}$', line)
        if m:
            bound = parse_assert_var_eq(m.group(1), steps)
            if bound is True:
                if mode == "last":
                    mode = "asserts"
                continue
            parsed = parse_simple_assert(m.group(1))
            if parsed is None:
                return None
            steps.append(parsed)
            if mode == "last":
                mode = "asserts"
            continue
        m = re.match(r'^r\s+(.*)$', line)
        if m:
            steps.append({"op": "cmd", "args": tcl_args(m.group(1))})
            continue
        m = re.match(r'^set\s+([A-Za-z_][A-Za-z0-9_]*)\s+\[\s*r\s+(.*?)\s*\]$', line)
        if m:
            steps.append({"op": "cmd", "args": tcl_args(m.group(2)),
                          "as": m.group(1), "collect": True})
            continue
        m = re.match(r'^list\s+(.*)$', line)
        if m:
            parsed_list = parse_list_line(m.group(1), steps)
            if parsed_list is None:
                return None
            extra, vars_ = parsed_list
            steps.extend(extra)
            if vars_:
                list_vars = vars_
            mode = "list"
            continue
        return None
    if not steps:
        return None
    # assert_equal inside a body that still has a last command (the `{expected}`
    # clause) must not swallow that last reply
    if mode == "asserts" and steps[-1]["op"] in ("cmd", "catch"):
        mode = "last"
    if bind and mode != "err":
        mode = "bind"
    out = {"steps": steps, "mode": mode}
    if list_vars:
        out["vars"] = list_vars
    if bind and mode == "bind":
        out["bind"] = bind
    return out



# tcl substitutes backslash escapes inside "quotes" and leaves them alone inside {braces}.
# Ignoring that made every expectation containing a byte written as an escape unfaithful:
# `"\x00foo"` was compared as the five characters \x00 rather than as a null and foo, and
# valkey rejected the translation - correctly - before barch ever saw it.
_TCL_SIMPLE = {"n": "\n", "t": "\t", "r": "\r", "a": "\a", "b": "\b", "f": "\f",
               "v": "\v", "\\": "\\", '"': '"', "{": "{", "}": "}", "[": "[", "]": "]",
               "$": "$", " ": " "}


def tcl_unescape(text):
    """what tcl makes of a double quoted word - escapes resolved, everything else kept"""
    out = []
    i = 0
    n = len(text)
    while i < n:
        c = text[i]
        if c != "\\" or i + 1 >= n:
            out.append(c)
            i += 1
            continue
        nxt = text[i + 1]
        if nxt == "x":
            j = i + 2
            hexd = ""
            while j < n and len(hexd) < 2 and text[j] in "0123456789abcdefABCDEF":
                hexd += text[j]
                j += 1
            if hexd:
                out.append(chr(int(hexd, 16)))
                i = j
                continue
        if nxt in "01234567":
            j = i + 1
            octd = ""
            while j < n and len(octd) < 3 and text[j] in "01234567":
                octd += text[j]
                j += 1
            out.append(chr(int(octd, 8) & 0xFF))
            i = j
            continue
        if nxt in _TCL_SIMPLE:
            out.append(_TCL_SIMPLE[nxt])
            i += 2
            continue
        out.append(nxt)
        i += 2
    return out and "".join(out) or ""


def expected_value(kind, word):
    """
    the string an expectation compares against.

    Only a bare word may be stripped. A quoted or braced one carries its whitespace on
    purpose - `getrange` of "Hello World" from 5 answers " World", and stripping that
    turned a faithful case into one valkey rejected.
    """
    if kind == "quote":
        return tcl_unescape(word)
    if kind == "brace":
        return word
    return word.strip()


def parse_assert_equal(rest):
    """
    assert_equal {expected} [r cmd ...] or assert_equal [r cmd ...] expected.
    valkey uses both orders.
    """
    words = split_words(rest)
    if len(words) != 2:
        return None
    (k1, w1), (k2, w2) = words
    if k2 == "bracket":
        m = re.match(r'^\s*r\s+(.*)$', w2)
        if not m:
            return None
        return {"op": "expect", "args": tcl_args(m.group(1)),
                "value": expected_value(k1, w1)}
    if k1 == "bracket":
        m = re.match(r'^\s*r\s+(.*)$', w1)
        if not m:
            return None
        return {"op": "expect", "args": tcl_args(m.group(1)),
                "value": expected_value(k2, w2)}
    return None


def parse_assert_equal_or_bound(rest, steps):
    """
    assert_equal as a command, or assert_equal $var expected against a reply
    already stored by `set var [r cmd]`. Returns True when the existing step
    was rewritten, a new step, or None.
    """
    try:
        words = split_words(rest)
    except ValueError:
        return None
    if len(words) == 2 and words[0][0] == "bare" and re.match(
            r'^\$[A-Za-z_][A-Za-z0-9_]*$', words[0][1]):
        var = words[0][1][1:]
        value = expected_value(words[1][0], words[1][1])
        for s in reversed(steps):
            if s.get("as") == var:
                s["op"] = "expect"
                s["value"] = value
                return True
        return None
    return parse_assert_equal(rest)


def parse_list_line(rest, steps):
    """
    list [r cmd] [r cmd], or list $v1 $v2 [[r cmd] ...].
    Returns (extra_steps, vars_or_None), or None if the line is out of reach.
    """
    try:
        words = split_words(rest)
    except ValueError:
        words = None
    pieces = []
    if words:
        ok = True
        for kind, w in words:
            if kind == "bare" and re.match(r'^\$[A-Za-z_][A-Za-z0-9_]*$', w):
                pieces.append(("var", w[1:]))
            elif kind == "bracket":
                pieces.append(("cmd", w))
            else:
                ok = False
                break
        if ok and pieces and any(t == "var" for t, _ in pieces):
            extra = []
            vars_ = []
            for typ, val in pieces:
                if typ == "var":
                    vars_.append(val)
                    continue
                m = re.match(r'^\s*r\s+(.*)$', val)
                if not m:
                    return None
                name = "_l%d" % len(vars_)
                extra.append({"op": "cmd", "args": tcl_args(m.group(1)),
                              "as": name, "collect": True})
                vars_.append(name)
            return extra, vars_

    rounded = re.findall(r'\[\s*roundFloat\s+\[\s*r\s+([^\]]*?)\s*\]\s*\]', rest)
    plain = re.findall(r'(?<!roundFloat )\[\s*r\s+([^\]]*?)\s*\]', rest)
    # only the commands inside the `list` make up the value. A plain `r ...`
    # line before it is setup - folding it in made `r del k` count as a result
    # and the case compare one element too long, which then failed on valkey and
    # was dropped as an unfaithful translation
    extra = []
    if rounded:
        for one in rounded:
            extra.append({"op": "cmd", "args": tcl_args(one),
                          "round": True, "collect": True})
    elif plain:
        for one in plain:
            extra.append({"op": "cmd", "args": tcl_args(one), "collect": True})
    else:
        return None
    return extra, None


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
        # quoted and bare words resolve backslashes; braces do not. `\[b` is the
        # lex bound `[b`, and sending the backslash made valkey refuse ZRANGE BYLEX
        if kind in ("quote", "bare"):
            w = tcl_unescape(w)
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
        except ValueError as e:
            # the brace and quote scanners refuse what they cannot read, and a body they
            # cannot read is a stub like any other. zset.tcl has one, and it used to take
            # the whole run down with it - a translator that cannot skip a case it does not
            # understand is a translator that stops at the first surprise
            parsed = None
            entry["why"] = "tcl this translator cannot scan: %s" % e
        if parsed is None:
            entry["skipped"] = True
            entry.setdefault("why", "body uses tcl this translator does not read")
            entry["tcl"] = raw["body"].strip()
        else:
            entry["steps"] = parsed["steps"]
            entry["mode"] = parsed["mode"]
            if parsed.get("vars"):
                entry["vars"] = parsed["vars"]
            if parsed.get("bind"):
                entry["bind"] = parsed["bind"]
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
