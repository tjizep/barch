# The last page of an arena is mapped only as far as it's used - TODO 639.
#
# With many shards most arenas hold one page with a few KB on it, and each was
# mapped and counted whole - a thousand shards with two thousand small keys
# reported 2.2 GB for 70 MB of RSS. Now page 0 isn't mapped, no spare page is,
# and the last page grows in 64 KB steps, doubling, while callers still see a
# whole page.
#
# Checked, against barchd, on a space with one shard:
#   - one small key costs a step, not a page
#   - values written across every step of the last page and onto the next page
#     all read back, and what's mapped stays under whole pages
#   - BEGIN maps the last page whole (a transaction copies whole pages), and
#     COMMIT and ROLLBACK trim it again, with the right keys in either case
#   - SAVE and a restart bring every value back, at the same mapped size
import os
import shutil
import signal
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14639)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "lastpage_data")
QUIET = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000"]
PAGE = 512 * 1024
STEP = 64 * 1024
SPACE = "lp"

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def start():
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA] + QUIET,
                         stdout=open(os.path.join(DATA, "..", "lastpage_barchd.log"), "ab"),
                         stderr=subprocess.STDOUT)
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            raise AssertionError("barchd exited with %s" % p.returncode)
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % PORT)


def stop(p, sig=signal.SIGTERM):
    if p is None or p.poll() is not None:
        return
    p.send_signal(sig)
    try:
        p.wait(timeout=120)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=30)


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=120)


def mapped(r):
    return int(r.info("memory")["barch_vmm_bytes_allocated"])


def value(i):
    # about a kilobyte, and different for each key so a wrong read shows
    return ("%06d" % i) * 170


def write(r, keys):
    pipe = r.pipeline(transaction=False)
    for i in keys:
        pipe.execute_command(SPACE + ":SET", "k%06d" % i, value(i))
    pipe.execute()


def missing(r, keys):
    pipe = r.pipeline(transaction=False)
    for i in keys:
        pipe.execute_command(SPACE + ":GET", "k%06d" % i)
    got = pipe.execute()
    return [i for i, v in zip(keys, got) if v != value(i).encode()]


shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)
p = start()
try:
    r = client()
    r.execute_command("configuration:SET", SPACE + ".shards", "1")
    r.execute_command("configuration:SAVE")
finally:
    stop(p)

p = start()
try:
    r = client()
    base = mapped(r)
    r.execute_command(SPACE + ":SET", "one", "small")
    one = mapped(r) - base
    # two arenas, nodes and leaves, and each has a step at most
    check(0 < one <= 2 * STEP, "one small key maps a step, not a page (%d KB)" % (one // 1024))

    # past every step of the last page and onto the next one, a batch at a time
    done = []
    sizes = []
    for batch in range(12):
        keys = list(range(batch * 100, (batch + 1) * 100))
        write(r, keys)
        done += keys
        sizes.append(mapped(r) - base)
    bad = missing(r, done)
    check(not bad, "%d keys across the steps and the next page read back (%d don't)"
          % (len(done), len(bad)))
    grew = sizes[-1]
    check(grew > PAGE, "the leaves went onto a second page (%d KB mapped)" % (grew // 1024))
    steps = sorted(set(sizes))
    check(len(steps) >= 4, "the mapping grew in steps, not by whole pages (%s KB)"
          % [s // 1024 for s in steps])
    check(all(s % STEP == 0 for s in sizes), "and only ever by whole steps")

    before = mapped(r)
    r.execute_command(SPACE + ":BEGIN")
    inside = list(range(5000, 5050))
    write(r, inside)
    check(r.execute_command(SPACE + ":COMMIT") == b"OK", "COMMIT answers OK")
    done += inside
    bad = missing(r, done)
    check(not bad, "after COMMIT every key reads back (%d don't)" % len(bad))
    after = mapped(r)
    check(after < before + PAGE, "and the last page is trimmed again (%d KB before, %d after)"
          % (before // 1024, after // 1024))

    before = mapped(r)
    r.execute_command(SPACE + ":BEGIN")
    dropped = list(range(6000, 6050))
    write(r, dropped)
    check(r.execute_command(SPACE + ":ROLLBACK") == b"OK", "ROLLBACK answers OK")
    bad = missing(r, done)
    check(not bad, "after ROLLBACK the committed keys read back (%d don't)" % len(bad))
    gone = r.execute_command(SPACE + ":GET", "k%06d" % dropped[0])
    check(gone is None, "and the rolled back ones are gone")
    check(mapped(r) == before, "and what's mapped is what it was at BEGIN (%d KB, %d KB)"
          % (before // 1024, mapped(r) // 1024))

    check(r.execute_command(SPACE + ":SAVE") == b"OK", "SAVE answers OK")
    saved = mapped(r)
    check(saved == before, "a SAVE leaves the mapping as it was (%d KB, %d KB)"
          % (before // 1024, saved // 1024))
finally:
    stop(p)

p = start()
try:
    r = client()
    bad = missing(r, done)
    check(not bad, "after a restart every key reads back (%d don't)" % len(bad))
    check(r.execute_command(SPACE + ":GET", "one") == b"small", "the small one too")
    loaded = mapped(r)
    # the load maps only what each last page used, so it lands where the save was
    check(loaded <= saved + 2 * STEP, "and maps what it did before the restart (%d KB, %d KB)"
          % (saved // 1024, loaded // 1024))
finally:
    stop(p)

shutil.rmtree(DATA, ignore_errors=True)
if failures:
    print("FAILURES above")
    sys.exit(1)
print("all passed")
