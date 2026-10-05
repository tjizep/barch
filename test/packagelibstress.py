# Readers never see a torn library pin set - TODO 595.
#
# lib and lib2 are libraries with five versions each, tagged v1..v5, and each version is
# large: 120 files of about 20kB on top of init.luau. The app pins both at the same tag.
# Four readers, each on its own connection, call a function that requires @lib, does a
# little work, then requires @lib2, and returns both versions. Meanwhile the app is
# re-pinned through v2, v3, v4, v5 and back to v1, v2, v3 - the way back reinstalls
# versions the collector has already removed, with the readers still running.
#
# Every answer has to be one version of both, "k|k": never lib at one version and lib2
# at another, and never an error. Before 595 each library's pin moved in a commit of its
# own and a call resolved each require on its own, so a call spanning a re-pin got a mix.
#
# Then one long call, in a space whose deadline cap is far above the server's: it starts
# on v3, the app moves to v4 and the collector runs well after a replaced pin set used to
# be dropped, and the call still finishes on v3 for both - TODO 596.
import os
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14998)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

HERE = os.getcwd()
base = scale.fwd(tempfile.mkdtemp(prefix="bdlibs"))
LIB = scale.fwd(os.path.join(base, "lib"))
LIB2 = scale.fwd(os.path.join(base, "lib2"))
APP = scale.fwd(os.path.join(base, "app"))
VERSIONS = 5
FILES = 120
READERS = 4

print("start package library stress test with %s" % BINARY, flush=True)


def git(repo, *args):
    return subprocess.check_output(["git", "-C", repo, *args],
                                   stderr=subprocess.DEVNULL).decode().strip()


def write(repo, rel, body):
    path = os.path.join(repo, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)


def commit(repo, msg):
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", msg)


def make(repo):
    subprocess.check_call(["git", "init", "-q", "-b", "main", repo])
    git(repo, "config", "user.email", "t@t")
    git(repo, "config", "user.name", "t")


rnd = random.Random(595)
for repo in (LIB, LIB2):
    make(repo)
    write(repo, "package.luau",
          'function setup() return { kind = "dependency" } end\n')
    for k in range(1, VERSIONS + 1):
        write(repo, "init.luau", "function v() return %d end\n" % k)
        for i in range(FILES):
            line = "".join(rnd.choice("abcdefghij") for _ in range(100))
            write(repo, "blob/b%03d.txt" % i, ("%d %s\n" % (k, line)) * 200)
        commit(repo, "v%d" % k)
        git(repo, "tag", "v%d" % k)


def app_package(k):
    return """
function setup()
    return {
        depends = {
            { name = "lib", url = "%s", tag = "v%d" },
            { name = "lib2", url = "%s", tag = "v%d" },
        },
        load = { { path = "code", space = "appsp" } },
        -- a cap of its own well above the server's - TODO 596
        spaces = { appsp = { function_deadline_max_ms = 20000,
                             function_deadline_ms = 20000 } },
    }
end
""" % (LIB, k, LIB2, k)


make(APP)
write(APP, "package.luau", app_package(1))
# the work between the two requires is what makes a call span a re-pin
write(APP, "code/both.luau", """
function call()
    local a = require("@lib").v()
    local n = 0
    for i = 1, 200000 do n = n + i end
    local b = require("@lib2").v()
    return a .. "|" .. b
end
""")
# requires @lib, then runs for longer than any grace worked out from the server's cap,
# then requires @lib2 - TODO 596
write(APP, "code/long.luau", """
function call(spins)
    local a = require("@lib").v()
    local n = 0
    for i = 1, tonumber(spins) do n = n + i end
    local b = require("@lib2").v()
    return a .. "|" .. b
end
""")
commit(APP, "app")


def start(data, *args):
    os.makedirs(data, exist_ok=True)
    log = open(os.path.join(data, "barchd.log"), "ab")
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", data]
                         + list(args), stdout=log, stderr=subprocess.STDOUT)
    log.close()
    scale.wait_for_port(PORT, proc=p, what="barchd", timeout=300)
    return p


def stop(p, data):
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(timeout=120)
    except subprocess.TimeoutExpired:
        p.kill()
        raise AssertionError("barchd did not stop")
    assert p.returncode == 0, "barchd exited with %s:\n%s" % (
        p.returncode, open(os.path.join(data, "barchd.log"), "rb").read()
        .decode(errors="replace")[-3000:])


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=120)


stopping = threading.Event()
answers = [[] for _ in range(READERS)]
errors = []


def reader(n):
    r = client()
    while not stopping.is_set():
        try:
            answers[n].append(r.execute_command("appsp.BOTH").decode())
        except Exception as e:   # every error is a failure, whatever it is
            errors.append(repr(e))
            time.sleep(0.01)


try:
    data = os.path.join(HERE, "stress_data")
    shutil.rmtree(data, ignore_errors=True)
    proc = start(data, "-g", APP, "pull=on", "user=default")
    try:
        r = client()
        # a replaced pin set goes 2s after it was replaced, once no call holds it, so
        # the collector removes versions while the readers run and the way back
        # reinstalls them. The server's cap is low so that appsp's own, 20s, is the
        # one the long call below runs under - TODO 595, 596
        r.execute_command("CONFIG", "SET", "function_deadline_max_ms", "1000")
        assert r.execute_command("appsp.BOTH") == b"1|1"
        threads = [threading.Thread(target=reader, args=(n,)) for n in range(READERS)]
        for t in threads:
            t.start()
        for k in (2, 3, 4, 5, 1, 2, 3):
            write(APP, "package.luau", app_package(k))
            commit(APP, "v%d" % k)
            t0 = time.time()
            assert r.execute_command("FUNCTIONS", "SYNC", "app") == b"OK"
            print("  re-pinned to v%d in %.2fs" % (k, time.time() - t0), flush=True)
            time.sleep(0.2)
        time.sleep(0.5)
        stopping.set()
        for t in threads:
            t.join(timeout=120)

        total = sum(len(a) for a in answers)
        torn = [a for each in answers for a in each if a.split("|")[0] != a.split("|")[1]]
        seen = sorted({a for each in answers for a in each})
        print("  %d answers, %d torn, %d errors, seen %s"
              % (total, len(torn), len(errors), seen), flush=True)
        assert not errors, errors[:5]
        assert not torn, torn[:10]
        assert total > 50, "the readers hardly ran"
        assert len(seen) > 2, "the readers saw too few re-pins to mean anything"
        assert r.execute_command("appsp.BOTH") == b"3|3"
        time.sleep(2.5)
        assert r.execute_command("FUNCTIONS", "SYNC", "app") == b"OK"
        for lib in ("lib", "lib2"):
            left = r.execute_command("repository:GRAPH", "LS", "/packages/%s/versions" % lib)
            assert len(left) == 1, "%s keeps %d versions after the grace" % (lib, len(left))

        # TODO 596: a call that outlives the old grace keeps its pin set. appsp's cap is
        # 20s and the server's 1s, so the 595 grace, twice the server's cap, was 2s.
        # Called as appsp:LONG, not appsp.LONG: a dotted call runs against the
        # connection's own space, and gets that space's limits
        print("a long call keeps the pin set it started on", flush=True)
        long_answer = []
        # scripts have no clock, so time a short run and scale it to about 6s
        t0 = time.time()
        client().execute_command("appsp:LONG", "20000000")
        spins = int(20000000 * 6.0 / max(time.time() - t0, 0.01))

        def long_call():
            try:
                long_answer.append(client().execute_command("appsp:LONG", str(spins)).decode())
            except Exception as e:
                long_answer.append(repr(e))
        t = threading.Thread(target=long_call)
        t.start()
        time.sleep(0.5)                 # it has required @lib by now
        write(APP, "package.luau", app_package(4))
        commit(APP, "v4 under a long call")
        assert r.execute_command("FUNCTIONS", "SYNC", "app") == b"OK"
        time.sleep(3)                   # past the old grace
        # the collector runs after every sync: v3's pin set is replaced and past the
        # grace, and only the long call still needs it
        assert r.execute_command("FUNCTIONS", "SYNC", "app") == b"OK"
        t.join(timeout=60)
        assert long_answer == ["3|3"], long_answer
        assert r.execute_command("appsp.BOTH") == b"4|4"
        # and once it has ended, the pin set and its versions go
        time.sleep(2.5)
        assert r.execute_command("FUNCTIONS", "SYNC", "app") == b"OK"
        for lib in ("lib", "lib2"):
            left = r.execute_command("repository:GRAPH", "LS", "/packages/%s/versions" % lib)
            assert len(left) == 1, "%s keeps %d versions after the long call" % (lib, len(left))
    finally:
        stopping.set()
        stop(proc, data)
finally:
    shutil.rmtree(base, ignore_errors=True)

print("package library stress test complete", flush=True)
