# Replication survives a replica restart, and keeps writes in order - TODO 485.
#
# The embedded module sends every KeyValue write to the destinations named with
# barch.publish(). repl::consumers::distribute takes the buffered writes and
# sends them one call at a time. Three things in it are checked here:
#
#   - after the replica restarts, rpc_impl::tcall keeps the old socket: it only
#     reconnects when the socket is closed, and a read or write error doesn't
#     close it. So every later call fails on the dead connection and the
#     replica never gets another write;
#   - when a call fails, distribute breaks out of the batch and the rest of it
#     is dropped for that destination, with no retry;
#   - every key space's maintenance thread calls distribute, so two batches can
#     be in flight at once and land in either order. A key set to 1 and then 2
#     can end at 1 on the replica.
#
# The replica is a barchd in its own process, so it can be killed and started
# again on the same port. The writer is this process, through the module.
import os
import shutil
import signal
import socket
import subprocess
import sys
import time

import scale
import redis
import barch

scale.workdir()
PORT = scale.port(default=14485)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "replreconnect_data")
shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)

N = 200
WAIT = 20                       # seconds a batch gets to reach the replica
ORDER_ROUNDS = 5
FILLER = 20000                  # writes in the long batch
EXTRA_SPACES = 8                # more maintenance threads calling distribute


def start():
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA],
                         stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
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
    p.send_signal(sig)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=30)


def replica():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=30)


def arrived(prefix, n=N, seconds=WAIT):
    """how many of prefix0..prefix{n-1} the replica holds, waiting up to `seconds`"""
    deadline = time.time() + seconds
    have = 0
    while True:
        try:
            r = replica()
            pipe = r.pipeline(transaction=False)
            for i in range(n):
                pipe.execute_command("GET", "%s%d" % (prefix, i))
            have = sum(1 for i, v in enumerate(pipe.execute())
                       if v == ("%s%d" % (prefix, i)).encode())
        except redis.RedisError:
            have = 0
        if have == n or time.time() >= deadline:
            return have
        time.sleep(0.5)


def write(kv, prefix, n=N):
    for i in range(n):
        kv.set("%s%d" % (prefix, i), "%s%d" % (prefix, i))


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


proc = start()
try:
    # maintenance ticks often, so distribute runs often and on many threads
    barch.setConfiguration("maintenance_poll_delay", "20")
    barch.publish("127.0.0.1", str(PORT))
    kv = barch.KeyValue()

    write(kv, "a")
    got = arrived("a")
    check(got == N, "writes reach the replica (%d of %d)" % (got, N))

    # ordering: every key space's maintenance thread calls distribute, so with
    # several spaces two batches can be sent at once. The first batch is long -
    # FILLER writes then k=1 - so a thread is still working through it when k=2
    # is written and another thread takes that as a batch of its own and sends
    # it straight away. The replica then gets k=2 before k=1 and keeps 1.
    # Before the restart, while the replica is receiving at all
    if got == N:
        spaces = [barch.KeyValue("replorder%d" % s) for s in range(EXTRA_SPACES)]
        wrong = []
        r = replica()
        for round_ in range(ORDER_ROUNDS):
            k = "ordered%d" % round_
            for j in range(FILLER):
                kv.set("filler%d" % j, str(round_))
            kv.set(k, "1")
            kv.set(k + "_first", "1")        # the end of the long batch
            time.sleep(0.1)                  # long enough for a tick to take it
            kv.set(k, "2")
            kv.set(k + "_second", "1")
            # wait until both batches are through, so what's read is final
            deadline = time.time() + WAIT * 3
            while time.time() < deadline and not (
                    r.execute_command("GET", k + "_first") == b"1"
                    and r.execute_command("GET", k + "_second") == b"1"):
                time.sleep(0.2)
            final = r.execute_command("GET", k)
            if final != b"2":
                wrong.append((k, final))
        if wrong:
            print("  ended wrong:", wrong[:5], flush=True)
        check(not wrong, "a key set to 1 then 2 ends at 2 on the replica (%d of %d rounds wrong)"
              % (len(wrong), ORDER_ROUNDS))
    else:
        print("  ordering not checked: the replica isn't receiving", flush=True)

    # the replica goes away and comes back on the same port
    stop(proc, signal.SIGKILL)
    proc = start()

    # the first batch after the restart meets the dead connection
    write(kv, "b")
    got_b = arrived("b")
    # and one well after it, which a reconnect would carry
    write(kv, "c")
    got_c = arrived("c")
    check(got_c == N, "writes reach the replica after it restarts (%d of %d)" % (got_c, N))
    check(got_b == N, "the batch that met the dead connection isn't dropped (%d of %d)"
          % (got_b, N))
finally:
    stop(proc)
    barch.stop()

print("\n%s" % ("all replication reconnect checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
