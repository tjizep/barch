# Writes made while a save runs are kept, and the files are one moment - TODO 465.
#
# A save freezes the shard's pages, writes the files from them with no latch
# held, and merges the pages written in the meantime back when it's done. So
# three things have to hold, and each is checked here while writers are busy
# the whole time the saves run:
#
#   1. nothing written during a save is lost when it merges: the live space is
#      exactly what the clients were told
#   2. the change log and the files agree: after kill -9 the files plus the log
#      are everything that was acknowledged, so the log's mark was taken at the
#      freeze, not before or after
#   3. the files alone are one moment: loaded without the log, every key that
#      was there before the saves is there, and every value is one that key
#      really had
#
# Run for a range-sharded space, where every shard freezes together and keys move
# between shards during the save, and for a hash-sharded one, where each shard
# freezes on its own. BEGIN during a save is checked as well: it waits for the
# freeze instead of dropping it.
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14467)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "savecow_data")
LOGS = os.path.join(os.getcwd(), "savecow_logs")
LOGS_ASIDE = os.path.join(os.getcwd(), "savecow_logs_aside")

KEYS = scale.env_int("SAVECOW_KEYS", 200000, floor=20000)
WRITERS = 3
SAVES = 4
VALUE = "v" * 200
# no interval saves: the saves are the ones this test starts. `timer` is
# enough for kill -9, which leaves the page cache alone
ARGS = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000",
        "-c", "aof_durability=timer"]


def start():
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA] + ARGS,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            out = p.stdout.read().decode(errors="replace")
            raise AssertionError("barchd exited with %s:\n%s" % (p.returncode, out[-2000:]))
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % PORT)


def stop(p, sig=signal.SIGTERM):
    p.send_signal(sig)
    try:
        p.wait(timeout=120)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=30)


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=300)


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def base(i):
    return "k%08d" % i


def fill(r, space, names, value):
    pipe = r.pipeline(transaction=False)
    for n, name in enumerate(names):
        pipe.execute_command(space + ":SET", name, value)
        if n % 5000 == 4999:
            pipe.execute()
    pipe.execute()


def read_all(r, space, names):
    pipe = r.pipeline(transaction=False)
    out = []
    for n, name in enumerate(names):
        pipe.execute_command(space + ":GET", name)
        if n % 5000 == 4999:
            out.extend(pipe.execute())
    out.extend(pipe.execute())
    return dict(zip(names, out))


class writer(threading.Thread):
    """sets, overwrites and deletes keys only it touches, and remembers the last
    answer for each, and every value each key ever had"""

    def __init__(self, space, n, stop_at):
        super().__init__()
        self.space, self.n, self.stop_at = space, n, stop_at
        self.expect = {}        # key -> last acknowledged value, None when deleted
        self.history = {}       # key -> set of values it has had
        self.error = None

    def run(self):
        c = client()
        rnd = random.Random(self.n)
        i = 0
        try:
            while not self.stop_at.is_set():
                i += 1
                roll = rnd.random()
                if roll < 0.5:
                    # overwrite a base key in this writer's stripe: never deleted
                    k = base(rnd.randrange(KEYS // WRITERS) * WRITERS + self.n)
                    v = "w%d-%d" % (self.n, i)
                    c.execute_command(self.space + ":SET", k, v)
                    self.expect[k] = v
                elif roll < 0.85:
                    # a new key below every base key, so a range space moves keys
                    k = "a%d-%07d" % (self.n, rnd.randrange(50000))
                    v = "n%d-%d" % (self.n, i)
                    c.execute_command(self.space + ":SET", k, v)
                    self.expect[k] = v
                else:
                    k = "a%d-%07d" % (self.n, rnd.randrange(50000))
                    c.execute_command(self.space + ":DEL", k)
                    self.expect[k] = None
                    v = None
                self.history.setdefault(k, set()).add(v)
        except Exception as e:           # reported by the main thread
            self.error = e


def run_space(space, range_sharded):
    print("%s space" % ("range-sharded" if range_sharded else "hash-sharded"), flush=True)
    for d in (DATA, LOGS, LOGS_ASIDE):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)
    names = [base(i) for i in range(KEYS)]

    proc = start()
    try:
        r = client()
        r.execute_command("configuration:SET", space + ".aof_dir", LOGS)
        if range_sharded:
            r.execute_command("configuration:SET", space + ".ordered", "1")
            r.execute_command("configuration:SET", space + ".shards", "4")
            r.execute_command("configuration:SET", space + ".range_sharded", "1")
        else:
            r.execute_command("configuration:SET", space + ".shards", "4")
        r.execute_command("configuration:SAVE")
    finally:
        stop(proc)

    proc = start()
    writers = []
    try:
        r = client()
        fill(r, space, names, VALUE)
        check(r.execute_command(space + ":SAVE") in (b"OK", "OK"), "the first SAVE works")

        stop_at = threading.Event()
        writers = [writer(space, n, stop_at) for n in range(WRITERS)]
        for w in writers:
            w.start()
        time.sleep(0.3)
        saves_ok = 0
        began = False
        for n in range(SAVES):
            if n == SAVES - 1:
                # BEGIN from another client while this save runs: it has to wait
                # for the freeze rather than drop it. COMMIT, not ROLLBACK: a
                # transaction is the whole space's, and a rollback would take back
                # what the writers wrote in it
                def begin_commit():
                    b = client()
                    b.execute_command("USE", space)
                    b.execute_command("BEGIN")
                    b.execute_command("COMMIT")
                t = threading.Thread(target=begin_commit)
                t.start()
            if r.execute_command(space + ":SAVE") in (b"OK", "OK"):
                saves_ok += 1
            if n == SAVES - 1:
                t.join(timeout=120)
                began = not t.is_alive()
        time.sleep(0.3)
        stop_at.set()
        for w in writers:
            w.join(timeout=120)
        errors = [w.error for w in writers if w.error]
        if errors:
            print("  a writer failed: %s" % errors[0], flush=True)
        check(not errors, "the writers were answered throughout")
        check(saves_ok == SAVES, "every SAVE during the writes worked (%d of %d)" % (saves_ok, SAVES))
        check(began, "a BEGIN during a save came back")

        expect = {}
        history = {}
        for w in writers:
            expect.update(w.expect)
            for k, vs in w.history.items():
                history.setdefault(k, set()).update(vs)
        writes = sum(len(w.expect) for w in writers)
        print("  %d keys written during %d saves" % (writes, SAVES), flush=True)

        # 1. the live space after the saves merged
        live = read_all(r, space, list(expect))
        wrong = [k for k, v in expect.items()
                 if live[k] != (v.encode() if v is not None else None)]
        if wrong:
            k = wrong[0]
            print("  first wrong: %s is %r, was told %r" % (k, live[k], expect[k]), flush=True)
        check(not wrong, "nothing written during a save was lost (%d were)" % len(wrong))
        want_size = KEYS + sum(1 for k, v in expect.items() if v is not None and k[0] == "a")
        check(r.execute_command(space + ":DBSIZE") == want_size, "and the size is right")
    finally:
        for w in writers:
            w.stop_at.set()
        # no shutdown save: the files are the last SAVE's
        stop(proc, signal.SIGKILL)

    # 2. files plus the log are everything acknowledged
    proc = start()
    try:
        r = client()
        back = read_all(r, space, list(expect))
        lost = [k for k, v in expect.items()
                if back[k] != (v.encode() if v is not None else None)]
        if lost:
            k = lost[0]
            print("  first wrong: %s is %r, was told %r" % (k, back[k], expect[k]), flush=True)
        check(not lost, "after kill -9, files and log hold every answer (%d don't)" % len(lost))
        check(r.execute_command(space + ":DBSIZE") == want_size, "and the size is right")
    finally:
        stop(proc, signal.SIGKILL)

    # 3. the files alone: one moment, whole
    for name in os.listdir(LOGS):
        shutil.move(os.path.join(LOGS, name), os.path.join(LOGS_ASIDE, name))
    proc = start()
    try:
        r = client()
        files = read_all(r, space, names + [k for k in history if k[0] == "a"])
        missing = [k for k in names if files[k] is None]
        check(not missing, "every key from before the saves is in the files (%d aren't)"
              % len(missing))
        odd = []
        for k, v in files.items():
            had = history.get(k, set()) | ({VALUE} if k[0] == "k" else {None})
            if (v.decode() if v is not None else None) not in had:
                odd.append(k)
        if odd:
            print("  first odd: %s is %r" % (odd[0], files[odd[0]]), flush=True)
        check(not odd, "and every value is one that key really had (%d aren't)" % len(odd))
        present = sum(1 for v in files.values() if v is not None)
        check(r.execute_command(space + ":DBSIZE") == present,
              "and the size agrees with what reads back")
    finally:
        stop(proc)


run_space("cr", True)
run_space("ch", False)

print("\n%s" % ("all save copy-on-write checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
