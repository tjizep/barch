#!/usr/bin/env python3
"""
COMMITTX and a USE of a new space at the same time - TODO 714.

COMMITTX held the space's tx_gate alone while it made the writes, and each write looked
its space up by name, which takes the keyspace registry lock. A USE of a space that
doesn't exist yet holds that registry lock while it builds the space, and building one
runs AUTH through an internal caller, which takes the default space's tx_gate shared
once a transaction has run there. So the two could wait on each other for good. TSan
reported it as a lock-order inversion in TestRespClientLocal. The same went for any
read that takes the gate and looks a space up, SPACES say: building a space no longer
takes a gate under the registry lock.

Checked against one barchd: one client commits transactions in the default space, with
many writes each so the gate is held a while, while another keeps USE-ing spaces that
don't exist yet and a third lists the spaces from the default space. All three have to
finish, every COMMITTX answering OK, and the keys of the last transaction have to be
there. The SPACES half only hangs where a waiting writer holds readers back, so it's
mostly for a TSan run to check the lock order.

It made 800 new spaces until TODO 748. Each one started a maintenance thread then, and
on a 4 vCPU coverage runner 800 of them starved the commits, which crawled until the
300 s ran out without a command ever timing out. Spaces share a pool now
(TestMaintenancePool), and 100 is plenty: the deadlock showed on the first round.
"""
import os
import socket
import subprocess
import sys
import threading
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14714)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

ROUNDS = scale.scaled(400, floor=20)
WRITES = 50
# new spaces made while the commits run - TODO 748
NEW_SPACES = scale.scaled(100, floor=20)
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


DATA = os.path.join(os.getcwd(), "txuse_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txuse.log"), "a")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

TAG = str(int(time.time()))


def client():
    # a hang is what's being looked for, so it shows up as a timeout, not a stuck test.
    # No retries: redis-py 6 and up resend a timed out command on a new connection, up
    # to ten times, so a stall in CI ran out the 300 s join with no error said - TODO 748
    return redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=20, decode_responses=True,
                       single_connection_client=True,
                       retry=redis.retry.Retry(redis.backoff.NoBackoff(), 0))


stacks_taken = threading.Lock()


def stacks(why):
    """Every barchd thread's stack, printed once, the first time a command stalls - TODO
    748. The stall shows in CI and not locally, so CI has to say where it waits. Needs gdb,
    which the coverage job has; without it this says so and goes on."""
    if not stacks_taken.acquire(blocking=False):
        return
    print("barchd stalled (%s); its threads:" % why, flush=True)
    gdb = ["gdb", "-p", str(p.pid), "-batch", "-nx", "-ex", "set pagination off", "-ex", "thread apply all bt"]
    out = ""
    # as is, then through sudo: a CI runner's ptrace_scope lets only a parent attach
    for cmd in (gdb, ["sudo", "-n"] + gdb):
        try:
            out = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120,
                                 text=True).stdout
        except (OSError, subprocess.SubprocessError) as e:
            out = "no stacks: %r" % e
        if "Thread 1 " in out:
            break
    print(out, flush=True)


def stalled(e, what):
    if isinstance(e, redis.TimeoutError):
        stacks(what)
    return "%s: %r" % (what, e)


results = {"commits": 0, "commit_errors": [], "uses": 0, "use_errors": [], "lists": 0, "list_errors": []}


def committing():
    c = client()
    c.execute_command("USE", "0")
    what = "USE 0"
    try:
        for i in range(ROUNDS):
            what = "BEGINTX, round %d" % i
            c.execute_command("BEGINTX")
            for j in range(WRITES):
                what = "SET %d, round %d" % (j, i)
                c.execute_command("SET", "tu%s:%d" % (TAG, j), str(i))
            what = "COMMITTX, round %d" % i
            got = c.execute_command("COMMITTX")
            if got != "OK":
                results["commit_errors"].append(str(got))
            results["commits"] += 1
    except Exception as e:
        results["commit_errors"].append(stalled(e, what))


def using():
    c = client()
    i = 0
    try:
        for i in range(NEW_SPACES):
            c.execute_command("USE", "tu%s_%d" % (TAG, i))
            results["uses"] += 1
    except Exception as e:
        results["use_errors"].append(stalled(e, "USE %d" % i))


def listing():
    c = client()
    c.execute_command("USE", "0")
    i = 0
    try:
        for i in range(ROUNDS):
            c.execute_command("SPACES")
            results["lists"] += 1
    except Exception as e:
        results["list_errors"].append(stalled(e, "SPACES %d" % i))


def server_load():
    """barchd's thread count and CPU so far, where /proc has them (Linux)"""
    try:
        with open("/proc/%d/status" % p.pid) as f:
            threads = next(line.split()[1] for line in f if line.startswith("Threads:"))
        with open("/proc/%d/stat" % p.pid) as f:
            fields = f.read().rsplit(")", 1)[1].split()
        cpu = (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
        return "%s threads, %.1f s of CPU" % (threads, cpu)
    except (OSError, StopIteration, ValueError, IndexError):
        return "no /proc for barchd"


try:
    print("COMMITTX in the default space while new spaces are made (%d rounds)" % ROUNDS)
    started = time.time()
    threads = [threading.Thread(target=t) for t in (committing, using, listing)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(max(1.0, 300 - (time.time() - started)))
    if any(t.is_alive() for t in threads):
        # still going: a hang, or a crawl where no one command takes the 20 s a
        # timeout needs - TODO 748. Say how far each got, and where barchd is
        print("still running after %.0f s: %d of %d commits, %d of %d USEs, %d of %d SPACES; barchd: %s"
              % (time.time() - started, results["commits"], ROUNDS, results["uses"], NEW_SPACES,
                 results["lists"], ROUNDS, server_load()), flush=True)
        stacks("threads still running after %.0f s" % (time.time() - started))
    check(not any(t.is_alive() for t in threads), "all three finish")
    check(results["commits"] == ROUNDS and not results["commit_errors"],
          "every COMMITTX answers OK (%d, %s)" % (results["commits"], results["commit_errors"][:2]))
    check(results["uses"] == NEW_SPACES and not results["use_errors"],
          "every USE answers (%d, %s)" % (results["uses"], results["use_errors"][:2]))
    check(results["lists"] == ROUNDS and not results["list_errors"],
          "every SPACES answers (%d, %s)" % (results["lists"], results["list_errors"][:2]))
    try:
        c = client()
        c.execute_command("USE", "0")
        last = [c.get("tu%s:0" % TAG), c.get("tu%s:%d" % (TAG, WRITES - 1))]
    except redis.RedisError as e:
        last = [repr(e)]               # a deadlocked server answers nobody
    check(last == [str(ROUNDS - 1)] * 2, "the last transaction's writes are there (%s)" % last[:1])
finally:
    p.kill()
    p.wait()

if failures:
    # barchd's own log, which CI shows nowhere else: a latch timeout writes its dump there
    log.flush()
    with open(os.path.join(os.getcwd(), "txuse.log"), errors="replace") as f:
        tail = f.readlines()[-80:]
    print("the end of barchd's log:\n" + "".join(tail), flush=True)
print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
