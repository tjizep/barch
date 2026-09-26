# RETRIEVE copies a key space from another barch - TODO 480.
#
# It pinged the other side and answered OK, and loaded nothing: the code that read
# the stream was compiled out. Now the other side freezes every shard at one
# moment and streams them as shard files; this side takes them all before
# changing anything, then installs and loads them the way LOAD does, with a change
# log checkpoint.
#
# Two servers, for a hash-sharded and a range-sharded space:
#   - the local space becomes the remote one, the keys it had before are gone,
#     and it stays that way after kill -9
#   - with a writer busy on the remote the whole time, what arrives is one moment:
#     every key from before is there, and every value is one the remote had
#   - an empty remote space empties the local one
#   - an unreachable remote, a wrong secret, a user without read rights, a space
#     the remote doesn't have and a different shard count all fail and leave the
#     local space as it was
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
LOCAL = scale.port(default=14480)
REMOTE = LOCAL + 1

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

LOCAL_DATA = os.path.join(os.getcwd(), "retrieve_local")
REMOTE_DATA = os.path.join(os.getcwd(), "retrieve_remote")
LOGS = os.path.join(os.getcwd(), "retrieve_logs")
N = 5000
ARGS = ["-c", "save_interval=86400000", "-c", "max_modifications_before_save=1000000000",
        "-c", "aof_durability=timer"]


def start(port, data):
    p = subprocess.Popen([BINARY, "--port", str(port), "--bind", "127.0.0.1",
                          "--dir", data] + ARGS,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            out = p.stdout.read().decode(errors="replace")
            raise AssertionError("barchd exited with %s:\n%s" % (p.returncode, out[-2000:]))
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % port)


def stop(p, sig=signal.SIGTERM):
    if p is None or p.poll() is not None:
        return
    p.send_signal(sig)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=30)


def client(port):
    return redis.Redis(host="127.0.0.1", port=port, protocol=2, socket_timeout=120)


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def answer(r, *args):
    try:
        return r.execute_command(*args)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def ok(reply):
    return reply in (b"OK", "OK")


def write(r, space, prefix, value, n=N):
    pipe = r.pipeline(transaction=False)
    for i in range(n):
        pipe.execute_command(space + ":SET", "%s%06d" % (prefix, i), value)
        if i % 5000 == 4999:
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


def configure(port, data, space, range_sharded, shards=2, logs=None):
    shutil.rmtree(data, ignore_errors=True)
    os.makedirs(data)
    p = start(port, data)
    try:
        r = client(port)
        r.execute_command("configuration:SET", space + ".shards", str(shards))
        if logs:
            shutil.rmtree(logs, ignore_errors=True)
            os.makedirs(logs)
            r.execute_command("configuration:SET", space + ".aof_dir", logs)
        if range_sharded:
            r.execute_command("configuration:SET", space + ".ordered", "1")
            r.execute_command("configuration:SET", space + ".range_sharded", "1")
        r.execute_command("configuration:SAVE")
    finally:
        stop(p)


def run_space(space, range_sharded):
    print("%s space" % ("range-sharded" if range_sharded else "hash-sharded"), flush=True)
    configure(REMOTE, REMOTE_DATA, space, range_sharded)
    configure(LOCAL, LOCAL_DATA, space, range_sharded, logs=LOGS)
    remote = start(REMOTE, REMOTE_DATA)
    local = start(LOCAL, LOCAL_DATA)
    names = ["r%06d" % i for i in range(N)]
    try:
        rr, lr = client(REMOTE), client(LOCAL)
        write(rr, space, "r", "remote")
        write(lr, space, "l", "local")

        # the refusals first, while the local space is known
        def unchanged(what, *args):
            reply = answer(lr, space + ":RETRIEVE", *args)
            print("  %s answered: %s" % (what, reply), flush=True)
            check(not ok(reply), "RETRIEVE %s fails" % what)
            check(lr.execute_command(space + ":DBSIZE") == N
                  and lr.execute_command(space + ":GET", "l000000") == b"local",
                  "and leaves the local space as it was")

        unchanged("from nowhere", "127.0.0.1", "1")
        unchanged("with a wrong secret", "127.0.0.1", str(REMOTE), "default", "not-it")
        # a login that can't read the space gets nothing a RESP client couldn't
        rr.execute_command("ACL", "SETUSER", "writeonly", "on", ">wpass", "+write")
        rr.execute_command("ACL", "SETUSER", "reader", "on", ">rpass", "+read")
        unchanged("as a user without read rights", "127.0.0.1", str(REMOTE), "writeonly", "wpass")
        answer(rr, "zz:SET", "x", "1")        # a space the remote has; this one it doesn't
        reply = answer(client(LOCAL), "nosuch:RETRIEVE", "127.0.0.1", str(REMOTE))
        check(not ok(reply), "RETRIEVE of a space the remote hasn't got fails (%s)" % (reply,))

        # the real thing, as the user that can read
        reply = answer(lr, space + ":RETRIEVE", "127.0.0.1", str(REMOTE), "reader", "rpass")
        check(ok(reply), "RETRIEVE works (%s)" % (reply,))
        got = read_all(lr, space, names)
        wrong = [k for k, v in got.items() if v != b"remote"]
        check(not wrong, "every remote key is here with its value (%d aren't)" % len(wrong))
        check(lr.execute_command(space + ":GET", "l000000") is None, "and the local ones are gone")
        check(lr.execute_command(space + ":DBSIZE") == N, "and nothing else")
        write(lr, space, "after", "after", n=100)
    finally:
        stop(local, signal.SIGKILL)
    local = start(LOCAL, LOCAL_DATA)
    try:
        lr = client(LOCAL)
        got = read_all(lr, space, names)
        check(all(v == b"remote" for v in got.values()), "after kill -9 the retrieved keys are here")
        check(lr.execute_command(space + ":DBSIZE") == N + 100,
              "with the writes after it and nothing else")

        # a writer busy on the remote the whole time
        stop_at = threading.Event()
        history = {}

        def writer():
            c = client(REMOTE)
            i = 0
            while not stop_at.is_set():
                k = "r%06d" % (i % N)
                v = "w%d" % i
                c.execute_command(space + ":SET", k, v)
                history.setdefault(k, set()).add(v.encode())
                c.execute_command(space + ":SET", "a%06d" % i, "new")     # moves range shards
                i += 1

        t = threading.Thread(target=writer)
        t.start()
        time.sleep(0.3)
        reply = answer(lr, space + ":RETRIEVE", "127.0.0.1", str(REMOTE))
        time.sleep(0.2)
        stop_at.set()
        t.join(timeout=60)
        check(ok(reply), "RETRIEVE while the remote is written to works (%s)" % (reply,))
        got = read_all(lr, space, names)
        odd = [k for k, v in got.items() if v != b"remote" and v not in history.get(k, set())]
        check(not odd, "every base key is here with a value the remote had (%d aren't)" % len(odd))
        added = lr.execute_command(space + ":DBSIZE") - N
        pipe = lr.pipeline(transaction=False)
        for i in range(added):
            pipe.execute_command(space + ":EXISTS", "a%06d" % i)
        prefix = sum(pipe.execute())
        check(prefix == added, "and the new keys are a prefix of what was written (%d of %d)"
              % (prefix, added))

        # an empty remote space empties this one
        rr = client(REMOTE)
        rr.execute_command("USE", space)
        rr.execute_command("FLUSHDB")
        reply = answer(lr, space + ":RETRIEVE", "127.0.0.1", str(REMOTE))
        check(ok(reply) and lr.execute_command(space + ":DBSIZE") == 0,
              "RETRIEVE of an empty space empties this one (%s)" % (reply,))
    finally:
        stop(local, signal.SIGKILL)
    local = start(LOCAL, LOCAL_DATA)
    try:
        check(client(LOCAL).execute_command(space + ":DBSIZE") == 0, "and it stays empty after kill -9")
    finally:
        stop(local)
        stop(remote)

    # a different shard count on the remote
    configure(REMOTE, REMOTE_DATA, space, range_sharded, shards=3)
    remote = start(REMOTE, REMOTE_DATA)
    local = start(LOCAL, LOCAL_DATA)
    try:
        write(client(REMOTE), space, "r", "remote", n=100)
        lr = client(LOCAL)
        write(lr, space, "l", "local", n=100)
        reply = answer(lr, space + ":RETRIEVE", "127.0.0.1", str(REMOTE))
        print("  a different shard count answered: %s" % (reply,), flush=True)
        check(not ok(reply) and lr.execute_command(space + ":DBSIZE") == 100,
              "a different shard count fails and changes nothing")
    finally:
        stop(local)
        stop(remote)


run_space("rh", False)
run_space("rg", True)

print("\n%s" % ("all retrieve checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
