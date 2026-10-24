"""
The read log keeps no copy of the keys a transaction reads - TODO 755.

The overlay already holds a copy of every key the transaction reads, so the log only
counts them. For a key that's then written, the log keeps the hash of what was read,
taken just before the write. This checks the cases where that could go wrong:
  - a key read, then written, then changed by someone else, conflicts;
  - the same with many keys written, past what the log keeps in its small map, so the
    rest are kept in a scratch space;
  - a key read as missing, set and then deleted in the transaction, which someone
    else creates meanwhile, conflicts (the overlay keeps a tomb for it rather than
    erasing it), as long as it writes something else: a transaction that ends up
    writing nothing commits without a check;
  - INCR in a loop, which writes in place, commits when nothing else changed it and
    conflicts when something did;
  - none of these conflict when nobody else wrote.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14795)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


DATA = os.path.join(os.getcwd(), "txreadlog_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txreadlog.log"), "a")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "txrl" + str(int(time.time()))


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", SPACE)
    return c


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def committed(got):
    return got in (True, "OK")


def conflicted(got):
    return "TXCONFLICT" in str(got)


try:
    other = client()

    for begin, kind in ((["BEGINTX"], "optimistic"), (["BEGINTX", "PESSIMISTIC"], "pessimistic")):
        print(kind, flush=True)
        pessimistic = kind == "pessimistic"

        # read, written, then changed underneath
        for race in (False, True):
            other.set("a", "1")
            c = client()
            c.execute_command(*begin)
            c.get("a")
            c.set("a", "2")
            if race and not pessimistic:
                other.set("a", "x")
            got = answer(c, "COMMITTX")
            if race and not pessimistic:
                check(conflicted(got), "a key read and written, then changed, conflicts (%s)" % str(got)[:40])
            else:
                check(committed(got) and other.get("a") == "2", "a key read and written commits (%s)" % str(got)[:40])

        # past the small map: the early keys are kept there and the late ones in scratch
        many = 8000
        for victim in (None, 3, many - 3):
            for i in range(many):
                other.set("m%05d" % i, "v")
            c = client()
            c.execute_command(*begin)
            # read, then written: a blind write isn't a read, and doesn't conflict
            for i in range(many):
                c.get("m%05d" % i)
                c.set("m%05d" % i, "w")
            if victim is not None and not pessimistic:
                other.set("m%05d" % victim, "x")
            got = answer(c, "COMMITTX")
            if victim is not None and not pessimistic:
                check(conflicted(got), "%d keys written, key %d changed, conflicts (%s)"
                      % (many, victim, str(got)[:30]))
            else:
                check(committed(got) and other.get("m%05d" % (many - 1)) == "w",
                      "%d keys written commit (%s)" % (many, str(got)[:30]))

        # set then deleted in the transaction, created meanwhile
        for race in (False, True):
            other.delete("fresh")
            c = client()
            c.execute_command(*begin)
            c.get("fresh")
            c.set("fresh", "mine")
            c.delete("fresh")
            check(c.get("fresh") is None, "a key set then deleted reads as gone")
            # a write that stays (a value it already had isn't one): a transaction that
            # writes nothing commits unchecked, as a read only one
            c.incr("other")
            if race and not pessimistic:
                other.set("fresh", "theirs")
            got = answer(c, "COMMITTX")
            if race and not pessimistic:
                check(conflicted(got), "set, deleted, created by another, conflicts (%s)" % str(got)[:30])
            else:
                check(committed(got) and other.get("fresh") is None,
                      "set then deleted commits, and leaves nothing (%s)" % str(got)[:30])

        # INCR in a loop writes in place
        for race in (False, True):
            other.set("n", "0")
            c = client()
            c.execute_command(*begin)
            for _ in range(50):
                c.incr("n")
            if race and not pessimistic:
                other.set("n", "100")
            got = answer(c, "COMMITTX")
            if race and not pessimistic:
                check(conflicted(got), "INCR x50, then changed by another, conflicts (%s)" % str(got)[:30])
            else:
                check(committed(got) and other.get("n") == "50", "INCR x50 commits 50 (%s, %s)"
                      % (str(got)[:30], other.get("n")))
finally:
    p.terminate()
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()

print("\n%s" % ("all read log checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
