# package.luau `depends`: a package that pulls more repositories - TODO 585.
#
# Three local origins: app depends on libone, libone depends on libtwo and back on
# app, libtwo has no package at all. Checked:
#   1. barchd -g app installs the whole chain before it listens, the deepest first:
#      app's after hook already sees libtwo's key
#   2. a dependency inherits the user, is marked as added by its package, and the
#      cycle back to app ends instead of looping
#   3. a dependency the package stops naming is removed, with the one it brought
#   4. with no user on the repository, nothing is cloned
#   5. a name that is already a repository with another url is refused, and so is a
#      url git would read as an option
import os
import shutil
import signal
import subprocess
import sys
import tempfile

import scale
import redis

scale.workdir()
PORT = scale.port(default=14990)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

HERE = os.getcwd()
base = tempfile.mkdtemp(prefix="bddeps")
APP = os.path.join(base, "app")
ONE = os.path.join(base, "one")
TWO = os.path.join(base, "two")

print("start package depends test with %s" % BINARY, flush=True)


def git(repo, *args):
    subprocess.check_call(["git", "-C", repo, *args],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


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


def app_package(depends):
    return """
function setup()
    return {
        depends = { %s },
        load = { { path = "code", space = "appsp" } },
        hooks = { after = { space = "appsp", call = "AFTER" } },
    }
end
""" % depends


LIBONE = '{ name = "libone", url = "%s", pull = true }' % ONE

make(TWO)
write(TWO, "depb/two.luau", 'function call() return "two" end\n')
write(TWO, "depb/two.txt", "from two")
commit(TWO, "two")

make(ONE)
write(ONE, "package.luau", """
function setup()
    return {
        depends = {
            { name = "libtwo", url = "%s" },
            -- back to the repository that started it: already there, so a cycle ends
            { name = "app", url = "%s" },
        },
        load = { { path = "code", space = "depa" } },
    }
end
""" % (TWO, APP))
write(ONE, "code/one.luau", 'function call() return "one" end\n')
commit(ONE, "one")

make(APP)
write(APP, "package.luau", app_package(LIBONE))
write(APP, "code/app.luau", 'function call() return "app" end\n')
# what was there when the hook ran: the grandchild has to be installed first
write(APP, "code/after.luau", """
function call(repo, from, to)
    -- another space from a script is barch.space, not a space: prefix
    local ok, v = pcall(function() return barch.space["depb"]:get("two.txt") end)
    barch.call("SET", "seen", tostring(v))
    return "ok"
end
""")
commit(APP, "app")


def start(data, *args):
    os.makedirs(data, exist_ok=True)
    log = open(os.path.join(data, "barchd.log"), "ab")
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", data]
                         + list(args), stdout=log, stderr=subprocess.STDOUT)
    log.close()
    scale.wait_for_port(PORT, proc=p, what="barchd")
    return p


def stop(p, data):
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        raise AssertionError("barchd did not stop")
    assert p.returncode == 0, "barchd exited with %s:\n%s" % (
        p.returncode, open(os.path.join(data, "barchd.log"), "rb").read()
        .decode(errors="replace")[-3000:])


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)


def conf(r, key):
    v = r.execute_command("configuration:GET", key)
    return v.decode() if v is not None else None


def line_of(r, name):
    for line in r.execute_command("FUNCTIONS", "STATUS").decode().splitlines():
        if line.startswith("name=%s " % name):
            return line
    return None


def refused(r, repo, words):
    try:
        r.execute_command("FUNCTIONS", "SYNC", repo)
    except redis.exceptions.ResponseError as e:
        assert words in str(e), e
        return
    raise AssertionError("FUNCTIONS SYNC %s was not refused" % repo)


try:
    data = os.path.join(HERE, "deps_data")
    shutil.rmtree(data, ignore_errors=True)
    print("barchd -g app installs the chain before it listens", flush=True)
    proc = start(data, "-g", APP, "pull=on", "user=default")
    try:
        r = client()
        assert r.execute_command("depb:GET", "two.txt") == b"from two"
        assert r.execute_command("depb.TWO") == b"two"
        assert r.execute_command("depa.ONE") == b"one"
        assert r.execute_command("appsp.APP") == b"app"
        assert r.execute_command("appsp:GET", "seen") == b"from two", \
            "app's after hook ran before its dependencies were in"

        print("dependencies inherit the user and say who added them", flush=True)
        for name, by in (("libone", "app"), ("libtwo", "libone")):
            assert conf(r, "git/repositories/%s/user" % name) == "default"
            assert conf(r, "git/repositories/%s/package/added_by" % name) == by
            line = line_of(r, name)
            assert line and "added_by=" + by in line and "state=ok" in line, line
        assert conf(r, "git/repositories/app/package/depends") == "libone\n"
        assert conf(r, "git/repositories/libone/package/depends") == "libtwo\n"
        # the cycle: app was already there, so libone's dependency on it was met, not added
        assert conf(r, "git/repositories/app/package/added_by") is None
        assert "depends=ok" in line_of(r, "app")
        assert r.execute_command("FUNCTIONS", "SYNC", "app") == b"OK"

        print("a dependency it stops naming goes, with what that one brought", flush=True)
        write(APP, "package.luau", app_package(""))
        commit(APP, "no deps")
        assert r.execute_command("FUNCTIONS", "SYNC", "app") == b"OK"
        left = sorted(k.decode() for k in r.execute_command("configuration:KEYS", "git/*"))
        assert line_of(r, "libone") is None and line_of(r, "libtwo") is None, \
            (r.execute_command("FUNCTIONS", "STATUS").decode(), left)
        assert conf(r, "git/repositories/libone/url") is None
        assert conf(r, "git/repositories/libtwo/package/added_by") is None
        assert conf(r, "git/repositories/app/package/depends") is None
        assert r.execute_command("appsp.APP") == b"app"
    finally:
        stop(proc, data)

    print("with no user on the repository nothing is cloned", flush=True)
    write(APP, "package.luau", app_package(LIBONE))
    commit(APP, "deps again")
    data = os.path.join(HERE, "deps_data_nouser")
    shutil.rmtree(data, ignore_errors=True)
    proc = start(data, "-g", APP, "pull=on")
    try:
        r = client()
        assert "depends=skipped:no_user" in line_of(r, "app"), line_of(r, "app")
        assert line_of(r, "libone") is None
        assert conf(r, "git/repositories/libone/url") is None

        print("a name that is someone else's repository, and a url like an option",
              flush=True)
        r.execute_command("configuration:SET", "git/repositories/app/user", "default")
        r.execute_command("configuration:SET", "git/repositories/libone/url", TWO)
        refused(r, "app", "with another url")
        r.execute_command("configuration:REM", "git/repositories/libone/url")
        write(APP, "package.luau",
              app_package('{ name = "evil", url = "--upload-pack=touch %s/pwned" }' % base))
        commit(APP, "evil")
        refused(r, "app", "cannot start with -")
        assert not os.path.exists(os.path.join(base, "pwned"))
        assert conf(r, "git/repositories/evil/url") is None
    finally:
        stop(proc, data)
finally:
    shutil.rmtree(base, ignore_errors=True)

print("package depends test complete", flush=True)
