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
#
# The reasons used to cite TODO 52 and TODO 63. Both were closed months ago - 52 in
# DONE 43 and 63 in DONE 65 - and neither was ever about most of what was hung on it, so
# the pointers said "tracked and pending" about work that was finished. Two of the reasons
# had also gone stale as facts: MULTI/EXEC do exist, and so does LMOVE. Checking the one
# that blamed LMOVE turned up a real bug it had been sitting on top of - see TODO 129.
# Reasons now state what was measured, and a pointer is only here when something is
# genuinely open. See DONE 121.
ACCEPTED = {
    # name pattern: why
    # --- commands that are not implemented. Each checked against a running barch --
    "SETBIT*": "SETBIT is an unknown command",
    "GETBIT*": "GETBIT is an unknown command",
    "MGET against non-string key": "the case sets up with SADD, which is an unknown "
                                   "command; there is no set type",
    "SET with IFEQ*": "SET's IFEQ conditional is a syntax error here",

    # --- cases that need something other than the command under test ------------
    # MULTI and EXEC do exist and this case's effect is right; what differs is the reply
    # to a queued command, which is a nil here and +QUEUED there. DISCARD is unknown
    "BRPOPLPUSH inside a transaction": "a queued command answers nil rather than +QUEUED",

    # --- cases whose setup uses a type or command barch does not have -------------
    "ZDIFFSTORE with a regular set*": "the case sets up with SADD; there is no set type",
    "ZINTERSTORE with a regular set*": "the case sets up with SADD",
    "ZUNIONSTORE with a regular set*": "the case sets up with SADD",
    "ZINTERSTORE #516*": "the case sets up with SADD",
    "ZINTERSTORE regression with two sets*": "the case sets up with SADD",
    "ZADD XX option without key*": "TYPE is an unknown command",
    "PERSIST can undo an EXPIRE": "the case reads the value back with a command whose "
                                  "setup uses TYPE, which is an unknown command",


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


def render(v, in_list=False):
    """a reply as tcl would print it, which is what the expectations were written against"""
    if v is None:
        # tcl prints a nil on its own as nothing, but an empty element inside a list as
        # {} - so MGET over four keys with one missing reads `a b c {}`, not `a b c `
        return "{}" if in_list else ""
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, bytes):
        return v.decode("utf-8", "replace")
    if isinstance(v, (list, tuple)):
        # a nested list is braced, the way tcl prints one. Flattening it made BZMPOP's
        # reply - a key and a list of {member score} pairs - render the same as BZPOPMIN's
        # flat one, so `zset {{a 0}}` compared as `zset a 0` and valkey rejected its own
        # answer. Only nesting is braced; the outermost list is still bare
        inner = " ".join(render(x, True) for x in v)
        return "{%s}" % inner if in_list else inner
    if isinstance(v, float):
        # tcl prints a whole float without its fraction
        return ("%r" % v).rstrip("0").rstrip(".") if "." in ("%r" % v) else "%r" % v
    return str(v)


def matches(expected, got):
    if expected is None:
        return True
    # deliberately not stripped. The translator keeps a quoted or braced expectation
    # verbatim because its whitespace is part of the value - GETRANGE of "Hello World"
    # from 5 answers " World" - and stripping here would undo that

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


class Deferred:
    """
    A connection that sends without reading, so a command can be left parked while the
    test does something else on the main one. redis-py has no deferring client, but its
    Connection does exactly this: `send_command` writes and returns, `read_response`
    collects when we are ready.

    zset.tcl's blocking tests are written around this - park a BZPOPMIN, check the server
    says a client is blocked, ZADD from the other connection, then read what came back.
    """
    def __init__(self, port):
        import redis
        self.client = redis.Redis(host="127.0.0.1", port=port, decode_responses=True,
                                  socket_timeout=20)
        self.client.response_callbacks = {}
        try:
            self.conn = self.client.connection_pool.get_connection("defer")
        except TypeError:                            # redis-py 6 dropped the argument
            self.conn = self.client.connection_pool.get_connection()

    def send(self, args):
        self.conn.send_command(*args)

    def read(self):
        return self.conn.read_response()

    def close(self):
        try:
            self.conn.disconnect()
        except Exception:                            # noqa: BLE001
            pass


def blocked_clients(conn):
    """what `INFO clients` says is parked, which is what wait_for_blocked_client reads"""
    try:
        info = conn.execute_command("INFO", "clients")
    except Exception:                                # noqa: BLE001
        return None
    if isinstance(info, bytes):
        info = info.decode("utf-8", "replace")
    for line in str(info).split("\n"):
        if line.startswith("blocked_clients:"):
            try:
                return int(line.split(":", 1)[1].strip())
            except ValueError:
                return None
    return None


def wait_blocked(conn, count, timeout=5.0):
    """poll until that many clients are parked. False if they never are"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        n = blocked_clients(conn)
        if n is None:
            return False
        if n >= count:
            return True
        time.sleep(0.05)
    return False


def run_case(conn, case, port=None):
    """(ok, detail). Runs the case's steps in order against one connection."""
    import redis
    last = None
    err = None
    collected = []
    named = {}
    # a case that parks a client opens its own connections, and they have to be shut at
    # the end however the case leaves: a connection still parked when the next case runs
    # keeps blocked_clients above zero and every wait_for_blocked_client after it passes
    # for the wrong reason
    deferred = {}
    try:
        return _run_steps(conn, case, port, deferred)
    finally:
        for d in deferred.values():
            d.close()
        # and wait for the server to notice. Closing the socket is not the same as the
        # block being released, and a case that inherits a parked client from the one
        # before it sees wait_for_blocked_client pass for the wrong reason and then has
        # its wake stolen. That is DONE 116's lesson in a different costume
        if deferred:
            deadline = time.time() + 2.0
            while time.time() < deadline and (blocked_clients(conn) or 0) > 0:
                time.sleep(0.05)


def _run_steps(conn, case, port, deferred):
    import redis
    last = None
    err = None
    collected = []
    named = {}
    for step in case.get("steps", []):
        if step["op"] == "sleep":
            time.sleep(step["ms"] / 1000.0)
            continue
        if step["op"] == "wait_blocked":
            if not wait_blocked(conn, step["count"]):
                return False, "waited for %d blocked client(s), INFO clients says %s" % (
                    step["count"], blocked_clients(conn))
            continue
        if step["op"] == "defer":
            name = step["conn"]
            if name not in deferred:
                deferred[name] = Deferred(port)
            deferred[name].send(step["args"])
            continue
        if step["op"] == "read":
            name = step["conn"]
            if name not in deferred:
                return False, "read from %s before anything was sent on it" % name
            try:
                reply = deferred[name].read()
            except Exception as e:                   # noqa: BLE001
                return False, "deferred read on %s raised %s: %s" % (
                    name, type(e).__name__, e)
            got = render(reply)
            if not matches(step["value"], got):
                return False, "deferred read: expected %r got %r" % (step["value"], got)
            continue
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
            if step.get("as"):
                named[step["as"]] = reply
            continue
        if step.get("round"):
            reply = round_float(render(reply))
        last = reply
        if step.get("as"):
            named[step["as"]] = reply
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
    elif mode == "bind":
        got = render(named.get(case.get("bind")))
    elif mode == "list":
        if case.get("vars"):
            got = render([named.get(v) for v in case["vars"]])
        else:
            got = render(collected)
    else:
        got = render(last)
    if matches(case.get("expected"), got):
        return True, ""
    return False, "expected %r got %r" % (case.get("expected"), got)



# Commands that leave the connection in a different state than they found it. MULTI is
# the one that caught us out: a queued command that the server does not know leaves the
# transaction in a state the next case inherits, and every reply after it came back
# empty - which read as barch failing forty odd cases it had never been asked about
_STATEFUL = {"hello", "reset", "select", "auth", "subscribe", "psubscribe", "swapdb",
             "multi", "exec", "discard", "watch", "unwatch"}


def _switches_protocol(case):
    for step in case.get("steps") or []:
        args = step.get("args") or []
        if args and args[0].lower() in _STATEFUL:
            return True
    return False


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
    def fresh():
        c = redis.Redis(host="127.0.0.1", port=port, decode_responses=True,
                        socket_timeout=15)
        c.response_callbacks = {}
        return c

    results = {}
    source = None
    for case in cases:
        if case.get("skipped"):
            continue
        # every tcl file is written against an empty db. Running all eight into one
        # server left state from one file breaking the setup of a case in the next, and
        # a case valkey rejects is dropped as an unfaithful translation rather than
        # compared - so the pollution did not show up as a failure, it quietly removed
        # cases from the comparison. The ZUNIONSTORE NaN case was one of them: it failed
        # on valkey with WRONGTYPE because `z{t}` already held a string by the time zset
        # was reached, and it was hiding a real difference. See DONE 116
        if case.get("source") != source:
            source = case.get("source")
            try:
                conn.execute_command("FLUSHALL")
            except Exception:                        # noqa: BLE001
                conn = fresh()
                conn.execute_command("FLUSHALL")
        try:
            ok, detail = run_case(conn, case, port)
        except Exception as e:                       # noqa: BLE001
            ok, detail = False, "%s: %s" % (type(e).__name__, e)
            # One case can break the connection for every case after it, and then the
            # differences it causes look like the server's fault rather than the client's.
            # `HELLO 3` did exactly that: the reply is a RESP3 map, redis-py is parsing
            # RESP2, and every command after the protocol error came back empty - 47 of
            # them, all reported as barch disagreeing with valkey. A case that fails at
            # the protocol level gets a new connection before the next one starts
            try:
                conn.close()
            except Exception:                        # noqa: BLE001
                pass
            conn = fresh()
        results[case["uid"]] = (ok, detail)
        # HELLO does not fail - it succeeds, and leaves the connection speaking a protocol
        # the client is not parsing, so every reply after it is misread rather than
        # refused. That is worse than an error: the run carries on and reports the damage
        # as differences. Any case that changes the connection's mode gets a fresh one
        if _switches_protocol(case):
            try:
                conn.close()
            except Exception:                        # noqa: BLE001
                pass
            conn = fresh()
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
        # results were keyed by case name, and two cases in expire.tcl share one - both
        # are called "EXPIRE with unsupported options". The second overwrote the first,
        # so one of them was run and then never compared. The position in its file is
        # what makes a case unique
        for i, c in enumerate(d["cases"]):
            c["source"] = d["source"]
            c["uid"] = "%s#%d" % (d["source"], i)
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

    trusted = [c for c in live if vres.get(c["uid"], (False,))[0]]
    dropped = [c for c in live if not vres.get(c["uid"], (False,))[0]]
    if dropped:
        # worth showing rather than counting: a case valkey rejects is usually a
        # dependant of one the translator skipped, and the state it needed never got
        # set up - but it can equally be a bug in the translation, and the two look
        # identical from a count alone
        print("  %d dropped, valkey rejects them so the translation is not faithful:"
              % len(dropped))
        for c in dropped:
            print("     %-52s %s" % (c["name"][:52], vres[c["uid"]][1][:70]))
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
    # barch loads whatever .dat files are in the working directory, and valkey starts
    # empty. A leftover key made `HSET hash f a` answer WRONGTYPE - correctly, because
    # `hash` really did hold a string from some earlier session - and the harness read
    # that as barch disagreeing with valkey. The fixture has to match or the comparison
    # is between two different databases
    clear = socket.create_connection(("127.0.0.1", bport), timeout=10)
    clear.sendall(b"*1\r\n$8\r\nFLUSHALL\r\n")
    clear.recv(4096)
    clear.close()
    bres = run_all(bport, live, "barch ")

    differences = []
    for case in trusted:
        ok, detail = bres.get(case["uid"], (False, "not run"))
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
