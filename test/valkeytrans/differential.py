# Run translated valkey tests against valkey first, then against barch.
#
# See TODO 40. A translated test that is subtly wrong is worse than no test: it either
# passes when it should not, or fails for a reason about the translation rather than about
# barch, and either way somebody loses an afternoon deciding which. The check that makes
# it safe is differential, and it is available because the same FetchContent that gives us
# the tcl also builds a working valkey-server next to it.
#
#   1. run every translated case against valkey-server, in order. Whatever passes there is
#      a faithful translation, by definition - valkey is the thing the tcl was written
#      against. Whatever fails is a translation artefact and is dropped, not reported.
#   2. run that trusted set against barch. Anything failing now is a real difference, and
#      the translation is not a suspect.
#
# Dropping rather than reporting step 1 failures is what makes the tool usable. Cases in a
# tcl file share state - one test leaves a key another reads - so a case this translator
# skipped takes its dependants with it. Those dependants fail on valkey too, which is
# exactly how we tell them apart from a real finding.
#
# Exit status is 0 when barch agrees with valkey on every trusted case. A difference is
# either a defect or a divergence we have decided to accept; accepted ones belong in
# ACCEPTED below with a reason, not deleted from the tcl.
import fnmatch
import json
import os
import socket
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))

# Differences we have looked at and decided to keep. Each needs a reason; a bare entry
# here is indistinguishable from a bug someone got tired of.
ACCEPTED = {
    # name pattern: why
    # --- commands that are not implemented. See TODO 52 -------------------------
    "SETBIT*": "the bit commands are not implemented. See TODO 52",
    "GETBIT*": "the bit commands are not implemented. See TODO 52",
    "MGET against non-string key": "the case sets up with SADD; there is no set type. "
                                   "See TODO 52",
    "SET with IFEQ*": "SET's IFEQ conditional is not implemented. See TODO 52",

    # --- differences already decided about ---------------------------------------
}


def valkey_dir():
    for build in ("build", "RelWithDebInfo", "Debug"):
        p = os.path.join(REPO, "test", build, "_deps", "valkey-src", "src")
        if os.path.exists(os.path.join(p, "valkey-server")):
            return p
    return None


def wait_for_port(port, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=0.5)
            s.close()
            return True
        except OSError:
            time.sleep(0.1)
    return False


def render(v):
    """a reply as tcl would print it, which is what the expectations were written against"""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, bytes):
        return v.decode("utf-8", "replace")
    if isinstance(v, (list, tuple)):
        return " ".join(render(x) for x in v)
    if isinstance(v, float):
        # tcl prints a whole float without its fraction
        return ("%r" % v).rstrip("0").rstrip(".") if "." in ("%r" % v) else "%r" % v
    return str(v)


def matches(expected, got):
    if expected is None:
        return True
    expected = expected.strip()
    if any(ch in expected for ch in "*?["):
        return fnmatch.fnmatchcase(got, expected)
    return got == expected


# the leading token of a redis error is its code. redis-py recognises these and strips
# them off the message it raises, while the tcl expectations were written against the
# whole line - so `{ERR*}` would never match a message redis-py has already shortened
ERROR_CODES = ("ERR", "WRONGTYPE", "NOPROTO", "NOAUTH", "NOPERM", "BUSYGROUP",
               "EXECABORT", "MASTERDOWN", "READONLY", "OOM", "MISCONF", "NOSCRIPT")


def matches_error(expected, got):
    """compare an error expectation, with or without the code redis-py removed"""
    if matches(expected, got):
        return True
    head = expected.split(" ", 1)[0].lstrip("*")
    for code in ERROR_CODES:
        if head == code or head.startswith(code):
            rest = expected[len(expected.split(" ", 1)[0]):].strip()
            # `ERR*` leaves nothing to match on, and the caller already knows it failed
            return True if not rest else matches(rest, got)
    return False


def round_float(text):
    """valkey's roundFloat helper: keep it readable, drop a pointless fraction"""
    try:
        v = float(text)
    except (TypeError, ValueError):
        return text
    r = round(v, 10)
    if r == int(r):
        return str(int(r))
    return repr(r)


def run_case(conn, case):
    """(ok, detail). Runs the case's steps in order against one connection."""
    import redis
    last = None
    err = None
    collected = []
    for step in case.get("steps", []):
        args = step["args"]
        try:
            reply = conn.execute_command(*args)
        except redis.ResponseError as e:
            if step["op"] == "catch":
                err = str(e)
                continue
            if step["op"] == "expect_error":
                if not matches_error(step["value"], str(e)):
                    return False, "%s: expected error %r got %r" % (
                        " ".join(args), step["value"], str(e))
                continue
            return False, "%s raised %s" % (" ".join(args), e)
        except Exception as e:                       # noqa: BLE001 - report, do not mask
            return False, "%s raised %s: %s" % (" ".join(args), type(e).__name__, e)
        if step["op"] == "expect_error":
            return False, "%s: expected it to fail with %r, it returned %r" % (
                " ".join(args), step["value"], render(reply))
        if step["op"] == "catch":
            # it was supposed to fail and did not
            err = ""
        if step["op"] == "expect":
            got = render(reply)
            if not matches(step["value"], got):
                return False, "%s: expected %r got %r" % (
                    " ".join(args), step["value"], got)
        if step.get("round"):
            reply = round_float(render(reply))
        last = reply
        # only tagged steps form the value in list and concat modes; anything else on
        # those lines is setup. `collected` is not used by the other modes, and the
        # translator always tags when it sets one of these, so untagged means setup
        if step.get("collect", False):
            collected.append(reply)

    mode = case.get("mode")
    if mode == "asserts":
        return True, ""
    if mode == "err":
        got = err if err is not None else ""
        if matches_error(case.get("expected") or "", got):
            return True, ""
        return False, "expected %r got %r" % (case.get("expected"), got)
    elif mode == "concat":
        # tcl's `append` joins with no separator at all
        got = "".join(render(x) for x in collected)
    elif mode == "list":
        got = render(collected)
    else:
        got = render(last)
    if matches(case.get("expected"), got):
        return True, ""
    return False, "expected %r got %r" % (case.get("expected"), got)


def run_all(port, cases, label):
    import redis
    conn = redis.Redis(host="127.0.0.1", port=port, decode_responses=True,
                       socket_timeout=15)
    # redis-py rewrites the replies of commands it recognises: +OK becomes True, SCAN's
    # cursor becomes an int, INFO becomes a dict, and so on. Every one of those is a
    # reply the tcl compares against as text, so the comparison is against redis-py's
    # idea of the value rather than the server's. Clearing the callbacks gives the raw
    # reply, which is the only thing worth comparing two servers on
    conn.response_callbacks = {}
    results = {}
    for case in cases:
        if case.get("skipped"):
            continue
        try:
            ok, detail = run_case(conn, case)
        except Exception as e:                       # noqa: BLE001
            ok, detail = False, "%s: %s" % (type(e).__name__, e)
        results[case["name"]] = (ok, detail)
    conn.close()
    print("  %s: %d of %d cases pass" % (label, sum(1 for o, _ in results.values() if o),
                                         len(results)))
    return results


def start_valkey(port, module):
    vdir = valkey_dir()
    if not vdir:
        return None
    cmd = [os.path.join(vdir, "valkey-server"), "--port", str(port),
           "--save", "", "--appendonly", "no"]
    proc = subprocess.Popen(cmd, cwd=vdir, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    if not wait_for_port(port):
        proc.kill()
        return None
    return proc


def main(argv):
    files = argv[1:]
    if not files:
        cdir = os.path.join(HERE, "cases")
        files = [os.path.join(cdir, f) for f in sorted(os.listdir(cdir))
                 if f.endswith(".json")]
    if not files:
        print("no translated cases; run translate.py first")
        return 2

    all_cases = []
    for f in files:
        d = json.load(open(f))
        for c in d["cases"]:
            c["source"] = d["source"]
        all_cases.extend(d["cases"])
    live = [c for c in all_cases if not c.get("skipped")]
    print("%d cases, %d translated, %d stubs" %
          (len(all_cases), len(live), len(all_cases) - len(live)))
    if not live:
        print("nothing to run")
        return 0

    # ---- step 1: valkey decides which translations are faithful ------------------
    vport = 7811
    vproc = start_valkey(vport, None)
    if vproc is None:
        print("SKIP: no valkey-server built - configure test/CMakeLists.txt to fetch it")
        return 0
    try:
        vres = run_all(vport, live, "valkey")
    finally:
        vproc.kill()
        vproc.wait()

    trusted = [c for c in live if vres.get(c["name"], (False,))[0]]
    dropped = [c for c in live if not vres.get(c["name"], (False,))[0]]
    if dropped:
        # worth showing rather than counting: a case valkey rejects is usually a
        # dependant of one the translator skipped, and the state it needed never got
        # set up - but it can equally be a bug in the translation, and the two look
        # identical from a count alone
        print("  %d dropped, valkey rejects them so the translation is not faithful:"
              % len(dropped))
        for c in dropped:
            print("     %-52s %s" % (c["name"][:52], vres[c["name"]][1][:70]))
    if not trusted:
        print("no faithful translations to compare")
        return 1

    # ---- step 2: barch runs the same cases and is compared on the trusted ones ----
    #
    # Every live case is run, not just the trusted ones, because the cases in a tcl file
    # share state - one sets a key another reads. Running only the trusted set against
    # barch while valkey ran all of them leaves the two servers in different states by
    # the time a later case is compared, and reports the difference as barch's fault.
    # Only the comparison is restricted; the execution is not.
    # `import barch` resolves to the module pip installed into the venv the tests run
    # under. Do not add the build directory to the path: it holds barch.so, the valkey
    # module, which python finds first and cannot load as an extension
    import barch
    bport = 7812
    barch.start("127.0.0.1", bport)
    time.sleep(0.5)
    bres = run_all(bport, live, "barch ")

    differences = []
    for case in trusted:
        ok, detail = bres.get(case["name"], (False, "not run"))
        if ok:
            continue
        why = next((r for pat, r in ACCEPTED.items()
                    if fnmatch.fnmatchcase(case["name"], pat)), None)
        if why:
            print("  accepted: %s (%s)" % (case["name"], why))
            continue
        differences.append((case, detail))

    print()
    if not differences:
        print("barch agrees with valkey on all %d faithful cases" % len(trusted))
        return 0
    print("%d differences from valkey:" % len(differences))
    for case, detail in differences:
        print("  [%s] %s" % (case["source"], case["name"]))
        print("      %s" % detail)
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
