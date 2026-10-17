#!/usr/bin/env python3
"""
A space saved during a transaction loads again - TODO 678.

`_save` wrote the live root and arenas, which don't make a pair a load takes while the
transaction's pages are copied on write. Stopping barchd during a transaction (its
shutdown saves) left shard files that failed with "invalid page", and the space came
back without the keys saved before BEGIN. A save in a transaction now writes the space
as it was at BEGIN, so a restart comes back as if the transaction had rolled back.

What has to hold:
  - ten keys, SAVE, BEGIN, 300 more keys and an overwrite, stop, start: the ten keys
    are back as they were, nothing written since BEGIN is, and no shard file fails
    to load;
  - the space takes writes again, and a transaction committed before a restart keeps
    its writes.

And ROLLBACK in each key order - TODO 679. An unordered shard keeps every key in its
hash index, and a rollback left that index as it was inside the transaction: a key the
transaction added answered "use after free" (in a TEST_OD build; another reads freed
memory). In unordered, ordered and hybrid spaces, after 50 keys, BEGIN, an overwrite
that grows a value, a new key and a delete, then ROLLBACK, every key reads as it was
at BEGIN.
"""
import os
import re
import shutil
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402

scale.workdir()
PORT = scale.port(default=14710)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DIR = os.path.join(os.getcwd(), "txrestart")
LOG = DIR + ".log"
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def start():
    # output to a file: an unread pipe fills and stops barchd
    proc = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DIR],
                            stdout=open(LOG, "a"), stderr=subprocess.STDOUT)
    end = time.time() + 30
    while time.time() < end:
        try:
            redis.Redis(port=PORT).ping()
            return proc
        except redis.RedisError:
            time.sleep(0.1)
    raise AssertionError("barchd didn't come up")


def stop(proc):
    proc.terminate()
    check(proc.wait(30) == 0, "barchd stops cleanly")


def client():
    c = redis.Redis(port=PORT, decode_responses=True, single_connection_client=True)
    c.execute_command("USE", "tx1")
    return c


def bad_loads():
    text = re.sub(r"\x1b\[[0-9;]*m", "", open(LOG).read())
    return len(re.findall(r"invalid page|could not load", text))


shutil.rmtree(DIR, ignore_errors=True)
os.makedirs(DIR)
if os.path.exists(LOG):
    os.remove(LOG)

print("ten keys saved, then a transaction under way when barchd stops")
proc = None
try:
    proc = start()
    c = client()
    for i in range(10):
        c.set("k%d" % i, "before")
    c.execute_command("SAVE")
    c.execute_command("BEGIN")
    for i in range(300):
        c.set("a%d" % i, "x")
    c.set("k0", "during")
    stop(proc)

    print("started again")
    before = bad_loads()
    proc = start()
    c = client()
    check(bad_loads() == before, "every shard file loads")
    check([c.get("k%d" % i) for i in range(10)] == ["before"] * 10, "the ten keys are back as they were")
    check(c.get("a0") is None and c.get("a299") is None, "nothing written since BEGIN is")
    check(c.dbsize() == 10, "and the space holds those ten (%d)" % c.dbsize())

    print("a transaction committed before a restart keeps its writes")
    c.execute_command("BEGIN")
    c.set("k1", "committed")
    c.execute_command("COMMIT")
    c.execute_command("SAVE")
    stop(proc)
    proc = start()
    c = client()
    check(c.get("k1") == "committed", "k1 holds the committed write")
    c.set("z", "1")
    check(c.get("z") == "1", "and the space takes writes")

    print("ROLLBACK in each key order")
    for mode in ("UNORDERED", "ORDERED", "HYBRID"):
        m = redis.Redis(port=PORT, decode_responses=True, single_connection_client=True)
        m.execute_command("USE", "order_" + mode.lower())
        m.execute_command("KSOPTIONS", "SET", mode)
        for i in range(50):
            m.set("p%d" % i, "v")
        m.set("k", "a")
        m.execute_command("BEGIN")
        m.set("k", "b" * 40)
        m.set("n", "x")
        m.delete("p3")
        m.execute_command("ROLLBACK")
        try:
            got = (m.get("k"), m.get("n"), m.get("p3"), m.dbsize())
        except redis.RedisError as e:
            got = str(e)
        check(got == ("a", None, "v", 51), "%s: every key as it was at BEGIN (%s)" % (mode.lower(), got))
    stop(proc)
finally:
    # stopped whatever happened above, so the next run finds the port and the directory free
    if proc and proc.poll() is None:
        proc.terminate()
        proc.wait(30)

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
