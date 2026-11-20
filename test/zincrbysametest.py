"""
ZINCRBY that leaves the score where it was keeps the member - TODO 950.

ZINCRBY moves a member by writing its new score key and removing the old one. When
the score does not change, the new key is the old key. The write did nothing, and
the remove took the only copy. ZCARD dropped, ZRANGE lost the member, and ZSCORE
still answered from the member index.

Checked, for an increment of 0, of -0, and of one too small to change the double:
  - the reply is the old score;
  - ZCARD, ZRANGE and ZSCORE agree the member is there with that score;
  - a later ZINCRBY by 1 adds to that score, and ZREM removes it. On the old
    code it found no member and wrote a new one at 1.
Also checked: an increment that does change the score still leaves one entry.
"""
import os
import socket
import subprocess
import sys
import time

import redis

import scale

scale.workdir()
PORT = scale.port(default=14950)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-74s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


# (score the member starts at, increment)
CASES = [
    ("1", "0"),
    ("1", "-0"),
    ("1", "1e-300"),
    ("1e300", "1"),
]

DATA = os.path.join(os.getcwd(), "zincrbysame_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "zincrbysame.log"), "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

try:
    r = redis.Redis(port=PORT, socket_timeout=60)
    for start, incr in CASES:
        r.execute_command("FLUSHALL")
        r.zadd("z", {"m": float(start), "o": 5})
        old = float(start)
        what = "ZINCRBY by %s from %s" % (incr, start)
        got = float(r.execute_command("ZINCRBY", "z", incr, "m"))
        check(got == old, "%s answers the old score (%r)" % (what, got))
        check(r.zcard("z") == 2, "%s keeps ZCARD at 2 (%d)" % (what, r.zcard("z")))
        rng = r.zrange("z", 0, -1, withscores=True)
        check((b"m", old) in rng and len(rng) == 2, "%s keeps m in ZRANGE (%r)" % (what, rng))
        check(r.zscore("z", "m") == old, "%s keeps ZSCORE (%r)" % (what, r.zscore("z", "m")))
        moved = float(r.execute_command("ZINCRBY", "z", "1", "m"))
        rng = r.zrange("z", 0, -1, withscores=True)
        check(moved == old + 1 and len(rng) == 2 and (b"m", old + 1) in rng,
              "%s, then by 1, adds to it (%r)" % (what, rng))
        check(r.zrem("z", "m") == 1 and r.zcard("z") == 1, "%s, then ZREM removes it" % what)

    r.execute_command("FLUSHALL")
    r.zadd("z", {"m": 1})
    r.execute_command("ZINCRBY", "z", "2", "m")
    rng = r.zrange("z", 0, -1, withscores=True)
    check(rng == [(b"m", 3.0)], "ZINCRBY by 2 leaves one entry at 3 (%r)" % rng)
finally:
    p.kill()
    p.wait()
    log.close()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
