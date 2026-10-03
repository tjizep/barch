# redis-cli --pipe against barchd - TODO 589.
#
# The shop's catalog load is a few thousand SETs through `redis-cli --pipe`, and it
# failed twice over: redis-cli sends an empty line before the ECHO it ends with,
# which the request parser took as part of the next header ("invalid array size"),
# and barchd had no ECHO, so the reply redis-cli waits for never came. Checked:
#   1. ECHO answers its argument, and is an arity error otherwise
#   2. a pipe of thousands of SETs, values up to 65 KB, ends with no errors and exit 0
#   3. what it set is there
import os
import random
import shutil
import signal
import subprocess
import sys

import scale
import redis

scale.workdir()
PORT = scale.port(default=14991)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)
CLI = shutil.which("redis-cli")
if not CLI:
    print("SKIP: no redis-cli")
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "redispipe_data")
shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)
LOG = os.path.join(DATA, "barchd.log")
COUNT = scale.scaled(3000, floor=300)

print("start redis-cli --pipe test with %s" % BINARY, flush=True)


def cmd(*args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        a = a if isinstance(a, bytes) else a.encode()
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


log = open(LOG, "wb")
proc = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA,
                         "--config", "internal_shards=7"], stdout=log, stderr=subprocess.STDOUT)
log.close()
scale.wait_for_port(PORT, proc=proc, what="barchd")
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=30)
    print("ECHO", flush=True)
    assert r.execute_command("ECHO", "hello") == b"hello"
    assert r.execute_command("ECHO", b"\x00\r\nbinary\xff") == b"\x00\r\nbinary\xff"
    for args in ((), ("a", "b")):
        try:
            r.execute_command("ECHO", *args)
            raise AssertionError("ECHO %r was not an arity error" % (args,))
        except redis.exceptions.ResponseError as e:
            assert "wrong number of arguments" in str(e), e

    print("a pipe of %d SETs, like the shop's catalog load" % COUNT, flush=True)
    rng = random.Random(589)
    want = {}
    buf = [cmd("USE", "piped")]
    for i in range(COUNT):
        size = rng.choice((40, 300, 1500, 20000, 65000))
        value = ("%d:" % i + "x" * size).encode()
        want["p:%d" % i] = value
        buf.append(cmd("SET", "p:%d" % i, value))
    done = subprocess.run([CLI, "-p", str(PORT), "--pipe"], input=b"".join(buf),
                          capture_output=True, timeout=300)
    out = done.stdout.decode(errors="replace") + done.stderr.decode(errors="replace")
    assert done.returncode == 0, out
    assert "errors: 0, replies: %d" % (COUNT + 1) in out, out

    for key in ("p:0", "p:%d" % (COUNT // 2), "p:%d" % (COUNT - 1)):
        assert r.execute_command("piped:GET", key) == want[key], key
    r.close()
finally:
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        proc.kill()
        raise AssertionError("barchd did not stop")
assert proc.returncode == 0, "barchd exited with %s:\n%s" % (
    proc.returncode, open(LOG, "rb").read().decode(errors="replace")[-3000:])
print("redis-cli --pipe test complete", flush=True)
