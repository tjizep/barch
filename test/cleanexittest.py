# barchd stops cleanly while its background threads are busy - TODO 533.
#
# Statics are destroyed in the reverse order they were built, and a function
# static is built on first use. The space registry is one of the first, so every
# function static built after it is destroyed before it - while the maintenance
# threads it joins are still running. Any other thread nobody joined before
# main() returned is in the same position with every static it reads. DONE 499
# fixed four of these one at a time as TSan found them.
#
# Checked, against barchd, each stopped with SIGTERM:
#   - with `functions_dir` set, so the function sync thread runs. Nothing
#     stopped it, and its std::thread was destroyed still joinable, which is
#     std::terminate: every clean stop ended in SIGABRT after the save.
#   - while maintenance is busy with what reaches function statics: an index's
#     queue (perm_index's chains), a refused cgroup limit (said once, from a
#     static string), LRU eviction, with writers and KEYS still going. Exit 0
#     here only says it didn't crash; under TSan or ASan, run by hand with
#     log_path, the reports are what count.
#   - and that it stops in reasonable time. A KEYS reply still streaming when
#     the socket threads stopped used to hold the stop for the whole
#     `rpc_client_max_wait_ms`, 30 s by default - TODO 538.
import os
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
PORT = scale.port(default=14533)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "cleanexit_data")
FUNCTIONS = os.path.join(os.getcwd(), "cleanexit_functions")
ROUNDS = max(2, int(scale.env_float("BARCH_CLEANEXIT_ROUNDS", 4, 2)))

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def start(args):
    err = open(os.path.join(DATA, "stderr.txt"), "wb")
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA] + args,
                         stdout=subprocess.DEVNULL, stderr=err)
    end = time.time() + 120
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


def stop(p):
    """SIGTERM, and what it exited with, the last thing it said and how long it took"""
    began = time.time()
    p.send_signal(signal.SIGTERM)
    try:
        code = p.wait(timeout=180)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()
        return "hung", "", time.time() - began
    took = time.time() - began
    with open(os.path.join(DATA, "stderr.txt"), "rb") as f:
        tail = f.read().decode(errors="replace").strip().splitlines()[-1:]
    return code, tail[0] if tail else "", took


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=30)


def fresh():
    for d in (DATA, FUNCTIONS):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)


print("with functions_dir set, a SIGTERM stops barchd cleanly", flush=True)
fresh()
for i in range(2):
    p = start(["-c", "functions_dir=" + FUNCTIONS])
    time.sleep(0.5)
    code, said, _ = stop(p)
    check(code == 0, "run %d exits 0 (%s: %s)" % (i + 1, code, said[-60:]))

print("a SIGTERM while maintenance, writers and KEYS are busy", flush=True)
fresh()
BUSY = ["-c", "maintenance_poll_delay=5",
        "-c", "cgroup_memory_control=on",
        # a path that can't be written, so every tick refuses and says why once
        "-c", "cgroup_memory_path=" + os.path.join(DATA, "no", "such", "memory.max")]
for i in range(ROUNDS):
    p = start(BUSY)
    r = client()
    r.execute_command("USE", "cx")
    if i == 0:
        for n in range(200):
            r.set("a%d|b%d|c%d|%d" % (n % 7, n % 5, n % 3, n), "v")
        r.execute_command("INDEX", "CREATE", "tri", "COMPOSITE", 3)
        r.execute_command("INDEX", "BUILD", "tri", "ALL")
        r.execute_command("KSPACE", "OPTION", "SET", "LRU", "ON")
        r.execute_command("SAVE")

    going = threading.Event()
    going.set()

    def writer(seed):
        c = client()
        n = 0
        try:
            c.execute_command("USE", "cx")
            while going.is_set():
                c.set("a%d|b%d|c%d|w%d_%d" % (n % 7, n % 5, n % 3, seed, n), "v" * 32)
                n += 1
        except Exception:
            pass        # the server going away mid write is the point

    def globber():
        c = client()
        try:
            c.execute_command("USE", "cx")
            while going.is_set():
                c.execute_command("KEYS", "a1*")
        except Exception:
            pass

    threads = [threading.Thread(target=writer, args=(k,)) for k in range(3)]
    threads.append(threading.Thread(target=globber))
    for t in threads:
        t.start()
    time.sleep(1.0)
    code, said, took = stop(p)
    going.clear()
    for t in threads:
        t.join(timeout=30)
    check(code == 0, "round %d exits 0 (%s: %s)" % (i + 1, code, said[-60:]))
    # well under the 30 s a stalled stream held it for, with room for a sanitizer
    check(took < 20, "round %d stops in %.1f s" % (i + 1, took))

print("\n%s" % ("clean exit checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
