#!/usr/bin/env python3
"""
Runs barch's Python tests against a Windows build (TODO 599).

The tests themselves aren't listed here. They're read out of the top-level
CMakeLists.txt - every add_test whose command is ${PYTHON3_EXEC} running a
script, with the ENVIRONMENT, LABELS, DEPENDS, RESOURCE_LOCK and TIMEOUT that
set_tests_properties gives them - so a test added for Linux is picked up here
too. Each one gets what ctest gives it on Linux: its own BARCH_TEST_PORT block,
BARCH_TEST_NAME, and BARCHD pointing at the barchd under test.

Tests that can't run on Windows are listed in win32/test_skips.txt with the
reason. Tests labelled cluster are skipped too, unless the build's CMakeCache.txt
has BARCH_CLUSTER on, which the Windows build never does.

On Windows, from the repository root:

    python win32/run_tests.py --build build-win -j 4

On Linux under wine, with a python.org python.exe:

    python3 win32/run_tests.py --build build-win -j 4 \\
        --python "path/to/wine-launcher path/to/python.exe" --path-prefix Z:

It runs on Linux against a Linux build as well, which is how to check the
runner itself: --build <linux build dir> --python python3.
"""
import argparse
import os
import re
import shlex
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def cmake_calls(text, name):
    """The argument text of each `name(...)` call, comments stripped."""
    text = re.sub(r"#[^\n]*", "", text)
    out = []
    for m in re.finditer(r"\b%s\s*\(" % name, text):
        depth, i = 1, m.end()
        while depth and i < len(text):
            depth += {"(": 1, ")": -1}.get(text[i], 0)
            i += 1
        out.append(text[m.end():i - 1])
    return out


def cmake_words(args):
    """Split CMake arguments: quoted strings stay whole."""
    return [w[1:-1] if w.startswith('"') else w
            for w in re.findall(r'"[^"]*"|\S+', args)]


def read_tests(cmakelists):
    with open(cmakelists) as f:
        text = f.read()
    tests = {}
    for call in cmake_calls(text, "add_test"):
        words = cmake_words(call)
        if words[:1] != ["NAME"] or "COMMAND" not in words:
            continue
        name = words[1]
        cmd = words[words.index("COMMAND") + 1:]
        if "WORKING_DIRECTORY" in cmd:
            cmd = cmd[:cmd.index("WORKING_DIRECTORY")]
        if cmd[:1] != ["${PYTHON3_EXEC}"] or not cmd[1].endswith(".py"):
            continue                    # a C++ test or a lua one under valkey
        tests[name] = {"name": name, "script": cmd[1], "args": cmd[2:], "env": [],
                       "labels": set(), "depends": [], "locks": [], "timeout": 600.0}
    keys = {"ENVIRONMENT", "LABELS", "DEPENDS", "RESOURCE_LOCK", "TIMEOUT",
            "FIXTURES_REQUIRED", "FIXTURES_SETUP", "WORKING_DIRECTORY", "RUN_SERIAL",
            "COST", "WILL_FAIL", "PASS_REGULAR_EXPRESSION", "SKIP_RETURN_CODE"}
    for call in cmake_calls(text, "set_tests_properties"):
        words = cmake_words(call)
        if "PROPERTIES" not in words:
            continue
        names = words[:words.index("PROPERTIES")]
        rest = words[words.index("PROPERTIES") + 1:]
        props, key = {}, None
        for w in rest:
            if w in keys:
                key = w
                props.setdefault(key, [])
            elif key:
                props[key].extend(v for v in w.split(";") if v)
        for n in names:
            t = tests.get(n)
            if not t:
                continue
            t["env"] += [e for e in props.get("ENVIRONMENT", []) if "${_" not in e]
            t["labels"] |= set(props.get("LABELS", []))
            t["depends"] += props.get("DEPENDS", [])
            t["locks"] += props.get("RESOURCE_LOCK", [])
            if props.get("TIMEOUT"):
                t["timeout"] = float(props["TIMEOUT"][0])
    return list(tests.values())


def read_skips(path):
    skips = {}
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.split("#", 1)[0].strip()
                if line:
                    name, _, why = line.partition(" ")
                    skips[name] = why.strip() or "no reason given"
    return skips


def has_cluster(build):
    """Whether the build was configured with BARCH_CLUSTER on."""
    try:
        with open(os.path.join(build, "CMakeCache.txt")) as f:
            return any(re.match(r"BARCH_CLUSTER:BOOL=(ON|TRUE|1|YES)\s*$", line, re.I)
                       for line in f)
    except OSError:
        return False


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build", required=True,
                    help="directory with barchd(.exe), barch.py and _barch.pyd")
    ap.add_argument("--python", default=None,
                    help="command that runs the python under test (split like a shell "
                         "would); default: the python running this")
    ap.add_argument("--path-prefix", default="",
                    help="put in front of every absolute path handed to the tests (Z: for wine)")
    ap.add_argument("--root", help="where the tests work; default <build>/testroot")
    ap.add_argument("-R", dest="include", help="only tests whose name matches this regex")
    ap.add_argument("-E", dest="exclude", help="leave out tests whose name matches this regex")
    ap.add_argument("-L", dest="label", help="only tests with this label, e.g. short")
    ap.add_argument("-j", type=int, default=1, help="tests at once")
    ap.add_argument("--port-base", type=int, default=20000,
                    help="first port handed out; ctest on Linux uses 20000, so pick "
                         "another range to run both on one machine at once - under "
                         "32768, where Linux's outgoing connections take theirs")
    ap.add_argument("--skips", default=os.path.join(HERE, "test_skips.txt"))
    ap.add_argument("--list", action="store_true", help="list what would run and stop")
    ap.add_argument("--report-only", action="store_true",
                    help="exit 0 even when tests fail")
    a = ap.parse_args()

    build = os.path.abspath(a.build)
    root = os.path.abspath(a.root or os.path.join(build, "testroot"))
    exe = os.path.join(build, "barchd.exe")
    if not os.path.exists(exe):
        exe = os.path.join(build, "barchd")
    os.makedirs(os.path.join(root, "logs"), exist_ok=True)

    def child_path(p):
        return a.path_prefix + p if os.path.isabs(p) else p

    tests = read_tests(os.path.join(ROOT, "CMakeLists.txt"))
    for port, t in enumerate(tests):
        t["port"] = a.port_base + 20 * port     # the same blocks ctest hands out
    skips = read_skips(a.skips)
    if not has_cluster(build):
        # the cluster tests sit inside `if (BARCH_CLUSTER ...)`, which read_tests
        # doesn't follow, and the Windows build never has the cluster
        for t in tests:
            if "cluster" in t["labels"]:
                skips.setdefault(t["name"], "this build has no cluster (BARCH_CLUSTER)")
    chosen = [t for t in tests
              if (not a.include or re.search(a.include, t["name"]))
              and (not a.exclude or not re.search(a.exclude, t["name"]))
              and (not a.label or a.label in t["labels"])]
    if a.list:
        for t in chosen:
            print(f"{t['name']:36} {os.path.basename(t['script']):32}"
                  f"{'  SKIP: ' + skips[t['name']] if t['name'] in skips else ''}")
        print(f"{len(chosen)} tests")
        return 0

    subst = {"${CMAKE_BINARY_DIR}": root,
             "${TEST_SOURCE_PATH}": os.path.join(ROOT, "test"),
             "${TEST_BUILD_PATH}": os.path.join(root, "test-build")}

    def expand(s):
        for k, v in subst.items():
            s = s.replace(k, v)
        return s

    def barchd_or(path):
        # BARCHD=${CMAKE_BINARY_DIR}/barchd names the test root, with a forward
        # slash however the root is spelled; point it at the build under test
        if os.path.normcase(os.path.normpath(path)) == \
                os.path.normcase(os.path.normpath(os.path.join(root, "barchd"))):
            return exe
        return path

    # the default is a path, not a command line: on Windows it's full of
    # backslashes, which a POSIX split would take for escapes
    if a.python is None:
        python = [sys.executable]
    else:
        python = shlex.split(a.python, posix=os.name != "nt")
    results = {}
    lock = threading.Lock()
    running_locks = set()
    pending = list(chosen)
    started = time.time()

    def run(t):
        name = t["name"]
        log_path = os.path.join(root, "logs", name + ".log")
        env = dict(os.environ)
        # the build directory first, so `import barch` finds the module under test
        env["PYTHONPATH"] = child_path(build)
        env["BARCHD"] = child_path(exe)
        env["BARCH_TEST_PORT"] = str(t["port"])
        env["BARCH_TEST_NAME"] = name
        env["BARCH_TEST_ROOT"] = child_path(root)
        env["PYTHONUNBUFFERED"] = "1"
        # the tests live in the repository, and importing scale.py would otherwise
        # drop __pycache__ beside it - harmless under ctest, a dirty tree here
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        for e in t["env"]:
            k, _, v = expand(e).partition("=")
            v = barchd_or(v)
            env[k] = child_path(v) if os.path.isabs(v) else v
        cmd = python + [child_path(expand(t["script"]))] + \
            [child_path(expand(x)) for x in t["args"]]
        t0 = time.time()
        with open(log_path, "wb") as log:
            log.write(("$ " + " ".join(cmd) + "\n").encode())
            log.flush()
            # a pipe copied into the log, not the log file as stdout: python under
            # wine can't start with a plain file for its standard streams
            p = subprocess.Popen(cmd, cwd=root, env=env, stdin=subprocess.DEVNULL,
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

            def pump():
                # a server the test left running still holds the pipe open, and
                # this thread is the only thing reading it. It must not keep the
                # runner alive when that happens, and must not outlive the log it
                # writes to - both were a hang and a spurious traceback. TODO 599
                try:
                    while True:
                        b = p.stdout.read1(65536)
                        if not b:
                            break
                        try:
                            log.write(b)
                            log.flush()
                        except ValueError:            # the log is closed
                            break
                except (OSError, ValueError):
                    pass

            copier = threading.Thread(target=pump, daemon=True)
            copier.start()
            try:
                rc = p.wait(timeout=t["timeout"])
                status = "PASS" if rc == 0 else f"FAIL ({rc})"
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
                status = f"TIMEOUT ({t['timeout']:g}s)"
            copier.join(timeout=10)
            if copier.is_alive():
                p.stdout.close()                      # let read1 return, so it ends
        if status == "PASS":
            # a test that can't run here says "SKIP: why" and exits 0; that's a
            # skip, and counting it as a pass hid a wrong BARCHD for a whole run
            with open(log_path, "rb") as f:
                for line in f.read().decode(errors="replace").splitlines():
                    if line.startswith("SKIP"):
                        status = "SKIP (" + line.partition(":")[2].strip()[:80] + ")"
                        break
        return status, time.time() - t0, log_path

    def worker():
        while True:
            with lock:
                t = None
                for c in pending:
                    deps_done = all(d in results or d not in {x["name"] for x in chosen}
                                    for d in c["depends"])
                    if deps_done and not (set(c["locks"]) & running_locks):
                        t = c
                        break
                if t is None:
                    if not pending:
                        return
                else:
                    pending.remove(t)
                    running_locks.update(t["locks"])
            if t is None:
                time.sleep(0.2)
                continue
            if t["name"] in skips:
                status, took, log_path = "SKIP", 0.0, ""
            else:
                try:
                    status, took, log_path = run(t)
                except Exception as e:
                    # a test that can't even start is a failure, not a missing
                    # result: a dead worker thread used to leave "0 passed, 0
                    # failed" and a green job behind it
                    status, took, log_path = f"ERROR ({type(e).__name__}: {e})", 0.0, ""
            with lock:
                running_locks.difference_update(t["locks"])
                results[t["name"]] = (status, took, log_path)
                done = len(results)
                print(f"{done:3}/{len(chosen)} {t['name']:36} {status:14} {took:7.1f}s"
                      + (f"  {skips[t['name']]}" if status == "SKIP" else ""), flush=True)

    threads = [threading.Thread(target=worker) for _ in range(max(1, a.j))]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    failed = [n for n, (s, _, _) in results.items()
              if s != "PASS" and not s.startswith("SKIP")]
    skipped = [n for n, (s, _, _) in results.items() if s.startswith("SKIP")]
    print(f"\n{len(results) - len(failed) - len(skipped)} passed, {len(failed)} failed, "
          f"{len(skipped)} skipped, in {time.time() - started:.0f}s")
    for n in failed:
        print(f"  {n}: {results[n][0]}  log: {results[n][2]}")
    missing = [t["name"] for t in chosen if t["name"] not in results]
    if missing:
        print(f"  {len(missing)} never ran: {', '.join(missing[:10])}")
    # report-only lets failing tests through, not a run that didn't happen
    errors = [n for n, (st, _, _) in results.items() if st.startswith("ERROR")]
    if missing or errors or (not results and chosen):
        return 1
    return 0 if (not failed or a.report_only) else 1


if __name__ == "__main__":
    sys.exit(main())
