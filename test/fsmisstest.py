# A remembered miss goes when its ttl does - TODO 544.
#
# With `<space>.missing_ttl` set, a path the file source doesn't have is
# remembered as a key, `fs:miss:<path>`, so the source isn't asked again for a
# while. The key was written with no expiry and nothing ever deleted it: the ttl
# was only compared when the same path was asked for again. So every made-up
# path anyone asked for - through a `source = true` HTTP route, say - left a key
# for good.
#
# Checked, against barchd: 2000 made-up paths through FS FETCH leave their miss
# keys, a repeat inside the ttl still doesn't ask the source again, and once the
# ttl has passed the keys are gone from KEYS, the expiry sweep takes them when
# memory is short, and a repeat asks again.
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
PORT = scale.port(default=14544)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "fsmiss_data")
TTL_MS = 3000
PATHS = 2000

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA,
                      "--no-save-on-exit"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    end = time.time() + 60
    while True:
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            break
        except OSError:
            if time.time() > end or p.poll() is not None:
                raise AssertionError("barchd did not start")
            time.sleep(0.1)
    r = redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=60)
    # before the space is first used: it reads these when it's built
    r.execute_command("configuration:SET", "fm.fs_source", "fetcher")
    r.execute_command("configuration:SET", "fm.missing_ttl", str(TTL_MS))
    # one shard, so a sweep under a tiny ceiling empties it: with several, each
    # stops once memory is back under the threshold (DONE 493)
    r.execute_command("configuration:SET", "fm.shards", "1")
    r.execute_command("fm:SETF", "fetcher", """function call(path)
        barch.call("INCRBY", "asked", 1)
        return nil
    end""")

    def misses():
        return len(r.execute_command("fm:KEYS", "fs:miss:*"))

    def asked():
        return int(r.execute_command("fm:GET", "asked") or 0)

    began = time.time()
    for i in range(PATHS):
        r.execute_command("fm:FS", "FETCH", "/made/up/%d" % i)
    check(asked() == PATHS, "the source was asked about every path (%d)" % asked())
    check(misses() == PATHS, "and each miss is remembered (%d keys)" % misses())
    r.execute_command("fm:FS", "FETCH", "/made/up/0")
    if time.time() - began < TTL_MS / 1000.0:
        check(asked() == PATHS, "a repeat inside the ttl doesn't ask the source again")
    time.sleep(TTL_MS / 1000.0 + 1.5)
    left = misses()
    check(left == 0, "once the ttl has passed the miss keys are gone (%d left)" % left)
    # not only hidden: an expired key is taken out of the space by the expiry
    # sweep, which runs when memory is wanted, as it does for any expired key.
    # Without an expiry nothing but an LRU policy could ever take a miss key
    r.execute_command("CONFIG", "SET", "maxmemory", "4096")
    deadline = time.time() + 60
    size = r.execute_command("fm:DBSIZE")
    while time.time() < deadline and size > 10:
        time.sleep(0.5)
        size = r.execute_command("fm:DBSIZE")
    r.execute_command("CONFIG", "SET", "maxmemory", str(1 << 34))
    check(size <= 10, "and when memory is wanted they're taken out, not just hidden (DBSIZE %d)"
          % size)
    r.execute_command("fm:FS", "FETCH", "/made/up/1")
    check(asked() == PATHS + 1, "and a repeat asks the source again (%d)" % asked())
finally:
    p.send_signal(signal.SIGKILL)
    p.wait(timeout=30)

print("\n%s" % ("fs miss checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
