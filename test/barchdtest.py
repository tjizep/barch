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
    # what it was doing matters more than the fact that it was not ready: a start
    # that hangs says so in its own log and this used to throw that away
    try:
        out = p.stdout.read().decode(errors="replace")
    except Exception:
        out = "(no output)"
    raise AssertionError("barchd did not listen on %d:\n%s" % (PORT, out[-3000:]))


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
        assert r.execute_command("GET", "fs:n:/files/sub/data.txt") is None

        r.execute_command("USE", "site")
        assert r.execute_command("GET", "sub:data.txt") == b"a value"
        assert r.execute_command("greeting") == b"hi from a loaded function"

        r.execute_command("USE", "media")
        # through the file API rather than the keys behind it - fs.h owns the layout
        r.execute_command("SETF", "fsget", "function call(p) return barch.fs.get(p) end")
        assert r.execute_command("fsget", "/files/sub/data.txt") == b"a value"
    finally:
        stop(proc)

    print("one listener, not three, and the port is recorded - TODO 241", flush=True)
    # setting server_port used to restart the server on a thread of its own, so
    # --port started the listener up to three times, and an exported BARCH_SERVER_PORT
    # started one nobody asked for. Start-up records now and only CONFIG SET acts
    other = PORT + 7
    proc = start(env={"BARCH_SERVER_PORT": str(other)})
    try:
        log_so_far = ""
        r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=10)
        assert r.ping() is True
        # the exported port was recorded and not opened
        try:
            s = socket.create_connection(("127.0.0.1", other), timeout=1)
            s.close()
            raise AssertionError("BARCH_SERVER_PORT started a listener of its own")
        except (ConnectionRefusedError, OSError):
            pass
        # and what is reported is the port this process is really on, which used to
        # be whatever the configuration happened to hold
        assert r.execute_command("CONFIG", "GET", "server_port")[1] == str(PORT).encode()
        assert r.execute_command("CONFIG", "GET", "server_binding")[1] == b"127.0.0.1"

    finally:
        log_so_far = stop(proc)
    starts = log_so_far.count("setting static_bloom_filter")
    assert starts == 1, "the listener was started %d times:\n%s" % (starts, log_so_far[-800:])

    print("CONFIG SET still moves a running listener", flush=True)
    # the other half of it: recording must not act, and asking must
    moved = PORT + 8
    proc = start()
    try:
        r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=10)
        r.execute_command("CONFIG", "SET", "server_port", str(moved))
        end = time.time() + 20
        ok = False
        while time.time() < end:
            try:
                m = redis.Redis(host="127.0.0.1", port=moved, db=0, protocol=2,
                                socket_timeout=5)
                ok = m.ping() is True
                m.close()
                break
            except Exception:
                time.sleep(0.2)
        assert ok, "CONFIG SET server_port did not move the listener"
    finally:
        stop(proc)

    print("arena_dir maps the pages from files - TODO 239", flush=True)
    arenas = os.path.join(src, "arenas")
    proc = start("--config", "arena_dir=" + arenas)
    try:
        r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
        assert r.execute_command("CONFIG", "GET", "arena_dir")[1] == arenas.encode()
        for i in range(2000):
            r.set("arena%05d" % i, "v" * 400)
        assert r.get("arena01999") == b"v" * 400

        files = [f for f in os.listdir(arenas) if f.endswith(".arena")]
        assert files, "arena_dir was set and nothing was mapped from it"
        # one per allocator per shard, named after the arena
        assert any(f.startswith("leaves_") for f in files), files[:4]
        assert any(f.startswith("nodes_") for f in files), files[:4]

        # and not one descriptor held for them: an mmap keeps its own reference, and
        # 347 shards times two allocators would exhaust a 1024 limit on its own
        fds = os.listdir("/proc/%d/fd" % proc.pid)
        held = 0
        for fd in fds:
            try:
                if os.readlink("/proc/%d/fd/%s" % (proc.pid, fd)).endswith(".arena"):
                    held += 1
            except OSError:
                pass
        assert held == 0, "%d arena descriptors are being held open" % held

        # the pages are file backed, so they are evictable rather than anonymous
        anon = 0
        with open("/proc/%d/status" % proc.pid) as fh:
            for line in fh:
                if line.startswith("RssAnon:"):
                    anon = int(line.split()[1])
        assert anon > 0, "could not read RssAnon"
    finally:
        stop(proc)

    # the data still comes back from the shard file, which is what is authoritative
    proc = start("--config", "arena_dir=" + arenas)
    try:
        r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
        assert r.get("arena01999") == b"v" * 400, "a mapped arena lost its data"
    finally:
        stop(proc)

    print("a clean stop leaves snapshots, and the next start maps - TODO 262", flush=True)
    snaps = os.path.join(src, "mapped")
    proc = start("--config", "arena_dir=" + snaps)
    try:
        r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
        for i in range(3000):
            r.set("mapped%05d" % i, "v%d" % i)
        assert r.get("mapped02999") == b"v2999"
    finally:
        clean_log = stop(proc)
    assert "arena snapshots" in clean_log, clean_log[-600:]
    metas = [f for f in os.listdir(snaps) if f.endswith(".meta")]
    assert metas, "an orderly shutdown wrote no snapshots"

    proc = start("--config", "arena_dir=" + snaps)
    try:
        r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
        assert r.get("mapped02999") == b"v2999", "a mapped arena lost its data"
        assert r.get("mapped00001") == b"v1"
        # read once and gone, checked while it is running: a crash from here on has
        # to find nothing to trust. The clean stop below writes them again, which is
        # what they are for
        assert not [f for f in os.listdir(snaps) if f.endswith(".meta")], \
            "a snapshot survived being read"
    finally:
        mapped_log = stop(proc)
    assert "arenas back rather than loading" in mapped_log, "nothing was mapped back"

    print("a hard kill falls back to the shard file", flush=True)
    proc = start("--config", "arena_dir=" + snaps)
    r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
    r.set("afterkill", "here")
    r.execute_command("SAVE")
    proc.kill()
    proc.wait(timeout=30)
    assert not [f for f in os.listdir(snaps) if f.endswith(".meta")], \
        "a killed process left a snapshot behind"
    proc = start("--config", "arena_dir=" + snaps)
    try:
        r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
        assert r.get("afterkill") == b"here"
        assert r.get("mapped02999") == b"v2999", "the fallback lost data"
    finally:
        killed_log = stop(proc)
    assert "arenas back rather than loading" not in killed_log, \
        "a snapshot was trusted after a hard kill"

    print("a bad space name is refused before anything is written", flush=True)
    got = subprocess.run([BINARY, "--port", str(PORT), "--dir", DATA,
                          "--load-keys", src + "@not a name"],
                         capture_output=True, text=True, timeout=120)
    assert got.returncode == 1, got.returncode
    assert "not a key space name" in got.stdout + got.stderr, (got.stdout + got.stderr)[-400:]
finally:
    shutil.rmtree(src, ignore_errors=True)

print("barchd test complete", flush=True)
