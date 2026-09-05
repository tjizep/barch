# The barchd program - TODO 237.
#
# Python drives the test, but not the server: the whole point of barchd is a RESP
# listener with no python in the process, so this starts the binary, talks to it over
# a socket, and checks /proc/<pid>/maps has nothing python shaped in it.
import os
import signal
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14350)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "barchd_data")
os.makedirs(DATA, exist_ok=True)
for f in os.listdir(DATA):
    if f.endswith(".dat"):
        os.remove(os.path.join(DATA, f))

print("start barchd test with %s" % BINARY, flush=True)


def start(*args, env=None):
    e = dict(os.environ)
    if env:
        e.update(env)
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA] + list(args),
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=e)
    # wait for the port rather than a fixed sleep: loading shards takes as long as it
    # takes, and on a loaded machine that is not a constant
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            out = p.stdout.read().decode(errors="replace")
            raise AssertionError("barchd exited with %s:\n%s" % (p.returncode, out[-2000:]))
        try:
            s = socket.create_connection(("127.0.0.1", PORT), timeout=0.5)
            s.close()
            return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % PORT)


def stop(p, sig=signal.SIGTERM, timeout=60):
    p.send_signal(sig)
    end = time.time() + timeout
    while time.time() < end:
        if p.poll() is not None:
            return p.stdout.read().decode(errors="replace")
        time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not exit on signal %s" % sig)


# --- it answers RESP, with no python in it --------------------------------
print("it serves RESP", flush=True)
proc = start()
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=10)
    assert r.ping() is True          # redis-py turns PING into a bool
    assert r.execute_command("FLUSHDB") is not None
    r.set("k", "hello")
    assert r.get("k") == b"hello"
    r.set("durable", "survives a restart")

    with open("/proc/%d/maps" % proc.pid) as fh:
        maps = fh.read().lower()
    assert "python" not in maps, "barchd has python mapped into it"

    # --- version and help, which are the other two things a program owes you
    v = subprocess.run([BINARY, "--version"], capture_output=True, text=True)
    assert v.returncode == 0 and v.stdout.strip(), v
    h = subprocess.run([BINARY, "--help"], capture_output=True, text=True)
    assert h.returncode == 0 and "--config" in h.stdout, h.stdout[:400]
finally:
    log = stop(proc)

# --- SIGTERM saves, and the data is there next time -----------------------
print("SIGTERM saves and the keys survive a restart", flush=True)
assert "barchd saving" in log, log[-1500:]
assert "barchd stopped" in log, log[-1500:]

proc = start()
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=10)
    assert r.get("durable") == b"survives a restart"
    assert r.get("k") == b"hello"
finally:
    stop(proc)

# --- configuration from the environment and the command line --------------
print("the environment configures it, and --config overrides", flush=True)
proc = start("--config", "hybrid_keys=off", env={"BARCH_MAX_MEMORY_BYTES": "134217728",
                                                 "BARCH_HYBRID_KEYS": "on"})
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=10)
    assert r.execute_command("CONFIG", "GET", "max_memory_bytes")[1] == b"134217728"
    # exported on, asked off on the command line: the command line is applied second
    assert r.execute_command("CONFIG", "GET", "hybrid_keys")[1] == b"off"
finally:
    stop(proc)

# --- what it does with nonsense -------------------------------------------
print("bad arguments are refused, with a code that says so", flush=True)
for args, code in ((["--config", "nonsense=1"], 2),
                   (["--port"], 2),
                   (["--not-an-option"], 2),
                   (["--dir", "/no/such/place/at/all"], 1)):
    got = subprocess.run([BINARY] + args, capture_output=True, text=True, timeout=120)
    assert got.returncode == code, (args, got.returncode, got.stderr[:300])


# --- importing at boot, into a named key space - TODO 240 -----------------
print("--load-keys and --load-fs, into a space of their own", flush=True)
import shutil
import tempfile

src = tempfile.mkdtemp(prefix="bdload")
try:
    os.makedirs(os.path.join(src, "sub"))
    # not hello.luau: the stem becomes the command name and HELLO is the RESP
    # handshake, which wins - a stored function cannot shadow a builtin
    open(os.path.join(src, "greeting.luau"), "w").write(
        'function call() return "hi from a loaded function" end')
    open(os.path.join(src, "sub", "data.txt"), "wb").write(b"a value")

    proc = start("--load-keys", src + "@site", "--load-fs", src + ":/files@media")
    try:
        r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=10)
        # the default space got neither
        assert r.execute_command("GET", "sub:data.txt") is None
        assert r.execute_command("GET", "fs:m:/files/sub/data.txt") is None

        r.execute_command("USE", "site")
        assert r.execute_command("GET", "sub:data.txt") == b"a value"
        assert r.execute_command("greeting") == b"hi from a loaded function"

        r.execute_command("USE", "media")
        assert r.execute_command("GET", "fs:m:/files/sub/data.txt") is not None
        assert r.execute_command("GET", "fs:d:/files/sub/data.txt|00000000") == b"a value"
    finally:
        stop(proc)

    print("one listener, not three", flush=True)
    # setting server_port through the configuration restarts the server on a thread
    # of its own, so --port used to start the listener up to three times. See TODO 241
    proc = start()
    try:
        log_so_far = ""
        r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=10)
        assert r.ping() is True
    finally:
        log_so_far = stop(proc)
    starts = log_so_far.count("setting static_bloom_filter")
    assert starts == 1, "the listener was started %d times:\n%s" % (starts, log_so_far[-800:])

    print("a bad space name is refused before anything is written", flush=True)
    got = subprocess.run([BINARY, "--port", str(PORT), "--dir", DATA,
                          "--load-keys", src + "@not a name"],
                         capture_output=True, text=True, timeout=120)
    assert got.returncode == 1, got.returncode
    assert "not a key space name" in got.stdout + got.stderr, (got.stdout + got.stderr)[-400:]
finally:
    shutil.rmtree(src, ignore_errors=True)

print("barchd test complete", flush=True)
