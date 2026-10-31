#!/usr/bin/env python3
"""
Lists every place a python test depends on how fast the machine is - TODO 805.

It reads the tests, it doesn't run them. Four kinds:

  pause   a time.sleep() that isn't in a loop: the test waits a while and hopes
          something has happened by then
  poll    a time.sleep() in a loop: it waits for something, a bit at a time. Fine as
          long as the loop ends on an event and its deadline is only a safety net
  limit   a duration compared to a number outside a loop condition: the test fails
          when the machine is slow (or passes when the code is slow)
  wait    a loop or a wait helper with a deadline: fails if the event takes longer

pause and limit are the ones to get rid of; poll and wait are worth a look when a
test is slow on CI. Usage:

  ci/timing_points.py                 every test, one line per point
  ci/timing_points.py --summary       counts per test, worst first
  ci/timing_points.py --focus         only the tests in ci/focus_tests.txt
  ci/timing_points.py TestRangeOffset only those tests (ctest names or file names)
"""
import argparse
import ast
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = os.path.join(ROOT, "test")

CLOCKS = {"time", "perf_counter", "monotonic", "perf_counter_ns", "monotonic_ns", "time_ns"}
# helpers from test/ that wait with a deadline: their first argument is seconds
# calls that hand a duration straight back
PASS_THROUGH = {"min", "max", "abs", "round", "float", "int", "sum", "sorted"}
WAITERS = {"wait_for", "wait_until", "wait_leader", "wait_port", "wait_for_port"}


def ctest_names():
    """script file -> the ctest names that run it, from CMakeLists.txt"""
    names = {}
    text = open(os.path.join(ROOT, "CMakeLists.txt")).read()
    for m in re.finditer(r"add_test\s*\(\s*NAME\s+(\w+)\s+COMMAND[^)]*?/(\w+\.py)", text):
        names.setdefault(m.group(2), []).append(m.group(1))
    return names


def is_clock_call(node):
    if not isinstance(node, ast.Call):
        return False
    f = node.func
    return (isinstance(f, ast.Attribute) and f.attr in CLOCKS
            and isinstance(f.value, ast.Name) and f.value.id == "time")


class Finder(ast.NodeVisitor):
    def __init__(self, lines):
        self.lines = lines
        self.points = []
        self.loops = 0
        self.in_while_test = False
        self.in_value = False  # inside the value of an assignment: worked out, not checked
        self.clocky = set()   # names that hold a time or a duration
        # functions here that return one, like a timed() helper: name -> which items
        # of the tuple it returns are clock values, or None for the whole value
        self.clock_fns = {}

    def uses_clock(self, node):
        """does this expression hold a time or a duration? It follows arithmetic,
        but not the arguments of ordinary calls: random.Random(time.time()) isn't one"""
        if is_clock_call(node):
            return True
        if isinstance(node, ast.Name):
            return node.id in self.clocky
        if isinstance(node, ast.Call):
            f = node.func
            name = f.id if isinstance(f, ast.Name) else None
            if name in self.clock_fns:
                return True
            if name in PASS_THROUGH:
                return any(self.uses_clock(a) for a in node.args)
            return False
        if isinstance(node, (ast.Lambda, ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)):
            # min(timed(...)[1] for _ in ...) still counts
            return isinstance(node, ast.GeneratorExp) and self.uses_clock(node.elt)
        return any(self.uses_clock(c) for c in ast.iter_child_nodes(node))

    def add(self, kind, node):
        line = self.lines[node.lineno - 1].strip()
        self.points.append((node.lineno, kind, line))

    def visit_Assign(self, node):
        targets = getattr(node, "targets", None) or [node.target]
        v = node.value
        if (isinstance(v, ast.Call) and isinstance(v.func, ast.Name) and self.clock_fns.get(v.func.id)
                and len(targets) == 1 and isinstance(targets[0], ast.Tuple)):
            # got, took = timed(...): only the items that come from the clock
            for i, t in enumerate(targets[0].elts):
                if i in self.clock_fns[v.func.id] and isinstance(t, ast.Name):
                    self.clocky.add(t.id)
        elif self.uses_clock(v):
            for t in targets:
                for n in ast.walk(t):
                    if isinstance(n, ast.Name):
                        self.clocky.add(n.id)
        was, self.in_value = self.in_value, True
        self.generic_visit(node)
        self.in_value = was

    visit_AugAssign = visit_Assign

    def visit_AnnAssign(self, node):
        if node.value is not None:
            self.visit_Assign(ast.Assign(targets=[node.target], value=node.value))

    def loop(self, node, test=None):
        if test is not None:
            if self.uses_clock(test):
                self.add("wait", node)
            self.in_while_test = True
            self.visit(test)
            self.in_while_test = False
        self.loops += 1
        for part in node.body + node.orelse:
            self.visit(part)
        self.loops -= 1

    def visit_While(self, node):
        self.loop(node, node.test)

    def visit_For(self, node):
        self.visit(node.iter)
        self.loop(node)

    visit_AsyncFor = visit_For

    def visit_FunctionDef(self, node):
        # a function body runs when called, not inside the loop it sits in
        loops, self.loops = self.loops, 0
        self.generic_visit(node)
        self.loops = loops
        if isinstance(node, ast.Lambda):
            return
        for n in ast.walk(node):
            if isinstance(n, ast.Return) and n.value is not None and self.uses_clock(n.value):
                if isinstance(n.value, ast.Tuple):
                    items = {i for i, e in enumerate(n.value.elts) if self.uses_clock(e)}
                else:
                    items = None
                old = self.clock_fns.get(node.name, set())
                self.clock_fns[node.name] = None if items is None or old is None else old | items

    visit_AsyncFunctionDef = visit_FunctionDef
    visit_Lambda = visit_FunctionDef

    def visit_Call(self, node):
        f = node.func
        name = f.attr if isinstance(f, ast.Attribute) else f.id if isinstance(f, ast.Name) else None
        if name == "sleep":
            self.add("poll" if self.loops else "pause", node)
        elif name in WAITERS:
            self.add("wait", node)
        self.generic_visit(node)

    def visit_Compare(self, node):
        membership = all(isinstance(op, (ast.Is, ast.IsNot, ast.In, ast.NotIn)) for op in node.ops)
        if not self.in_while_test and not self.in_value and not membership and self.uses_clock(node):
            sides = [node.left] + node.comparators
            # one side is the clock or a duration, the other a limit
            if any(self.uses_clock(s) for s in sides) and not all(self.uses_clock(s) for s in sides):
                self.add("limit", node)
            elif any(is_clock_call(n) for s in sides for n in ast.walk(s)):
                # now against a deadline: a wait written out by hand
                self.add("wait", node)
            else:
                # one duration against another, like a skip against a walk
                self.add("limit", node)
        self.generic_visit(node)


def scan(path):
    src = open(path).read()
    try:
        tree = ast.parse(src)
    except SyntaxError as e:
        return [(e.lineno or 0, "error", "can't parse: %s" % e.msg)]
    f = Finder(src.splitlines())
    # two passes, so a name assigned from the clock further down still counts
    f.visit(tree)
    f.points, f.loops = [], 0
    f.visit(tree)
    return sorted(set(f.points))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("tests", nargs="*")
    ap.add_argument("--summary", action="store_true")
    ap.add_argument("--focus", action="store_true")
    ap.add_argument("--kinds", default="pause,poll,limit,wait")
    a = ap.parse_args()

    names = ctest_names()
    want = set(a.tests)
    if a.focus:
        for line in open(os.path.join(ROOT, "ci", "focus_tests.txt")):
            line = line.split("#")[0].strip()
            if line:
                want.add(line)
    kinds = set(a.kinds.split(","))

    rows = []
    for fn in sorted(os.listdir(TEST_DIR)):
        if not fn.endswith(".py"):
            continue
        tests = names.get(fn, [])
        if want and not (want & set(tests) or fn in want or fn[:-3] in want):
            continue
        points = [p for p in scan(os.path.join(TEST_DIR, fn)) if p[1] in kinds or p[1] == "error"]
        if points:
            rows.append((fn, tests, points))

    if a.summary:
        def count(points, k):
            return sum(1 for p in points if p[1] == k)
        rows.sort(key=lambda r: (-(count(r[2], "pause") + count(r[2], "limit")), r[0]))
        print("%-34s %5s %5s %5s %5s  %s" % ("file", "pause", "limit", "poll", "wait", "ctest"))
        for fn, tests, points in rows:
            print("%-34s %5d %5d %5d %5d  %s" % (fn, count(points, "pause"), count(points, "limit"),
                                                 count(points, "poll"), count(points, "wait"),
                                                 " ".join(tests) or "-"))
        return
    for fn, tests, points in rows:
        print("%s (%s)" % (fn, " ".join(tests) or "no ctest"))
        for lineno, kind, line in points:
            print("  %5d %-5s %s" % (lineno, kind, line[:110]))


if __name__ == "__main__":
    sys.exit(main())
