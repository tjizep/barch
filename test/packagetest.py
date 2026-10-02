# package.luau, a git repository that says how it is installed - TODO 582.
#
# barchd is started with -g on a local origin whose package.luau sets space
# settings, maps three folders into two spaces, starts an HTTP server and names a
# before and an after hook. Checked:
#   1. the settings are in place before the spaces open: pkga comes up with 3 shards
#   2. the folders land where `load` says, and package.luau is not imported itself
#   3. HTTP serves, and still serves after a restart
#   4. the after hook runs on the first sync and once per process after that; the
#      before hook only when a new commit is about to replace the checkout
#   5. a setting the package stops listing is taken back
#   6. a shard change on an existing space is refused and nothing moves
#   7. with no `user` on the repository the hooks are skipped
#   8. a bad package is refused before anything is applied
#   9. a hook that sends FUNCTIONS SYNC gets an error, not a deadlock
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request

import scale
import redis

scale.workdir()
PORT = scale.port(default=14986)
HTTP_PORT = scale.port(1, default=14987)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "package_data")
shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)
LOG = os.path.join(DATA, "barchd.log")
base = tempfile.mkdtemp(prefix="bdpkg")
origin = os.path.join(base, "shoprepo")
REPO = "shoprepo"

print("start package test with %s" % BINARY, flush=True)


def git(*args):
    subprocess.check_call(["git", "-C", origin, *args],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def write(rel, body):
    path = os.path.join(origin, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)


def commit(msg):
    git("add", "-A")
    git("commit", "-q", "-m", msg)


def package(shards_a=3, ordered=True, extra=""):
    spaces = "pkga = { shards = %d%s }, pkgb = { shards = 2%s }, pkgc = {}" % (
        shards_a, ", ordered = true" if ordered else "", extra)
    return """
function setup()
    return {
        spaces = { %s },
        load = {
            { path = "code", space = "pkga" },
            { path = "site", space = "pkga", as = "fs", fs_root = "/site" },
            { path = "more", space = "pkgb" },
        },
        http = { space = "pkga", key = "CONF", port = %d, bind = "127.0.0.1" },
        hooks = {
            before = { space = "pkga", call = "BEFOREHOOK", args = { "b" } },
            after  = { space = "pkga", call = "AFTERHOOK", args = { "a" } },
        },
    }
end
""" % (spaces, HTTP_PORT)


def page(version):
    return """
function call() return "page" end
function hello(req, res)
    res.body = "%s"
    res.code = 200
end
function service()
    return { kind = "resource", route = "/hello", methods = { GET = hello } }
end
""" % version


HOOK = """
function call(repo, from, to, extra)
    barch.call("INCR", "hooks:%(name)s")
    barch.call("SET", "hooks:%(name)s:args", repo .. "|" .. from .. "|" .. to .. "|" .. tostring(extra))
    %(more)s
    return "ok"
end
"""

subprocess.check_call(["git", "init", "-q", "-b", "main", origin])
git("config", "user.email", "t@t")
git("config", "user.name", "t")
write("package.luau", package())
write("code/conf.luau", """
function call() return "conf" end
function service()
    return { kind = "http", bind = "127.0.0.1", user = "default", keys = { "PAGE" } }
end
""")
write("code/page.luau", page("v1"))
write("code/beforehook.luau", HOOK % {"name": "before", "more": ""})
# a sync from inside a hook would wait for the sync running the hook
write("code/afterhook.luau", HOOK % {"name": "after", "more": """
    local ok, e = pcall(barch.call, "FUNCTIONS", "SYNC")
    barch.call("SET", "hooks:resync", tostring(e))"""})
write("more/thing.txt", "a value")
write("site/index.html", "<p>hi</p>")
commit("v1")


def head():
    return subprocess.check_output(["git", "-C", origin, "rev-parse", "HEAD"], text=True).strip()


def start(*args):
    logfile = open(LOG, "ab")
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA]
                         + list(args), stdout=logfile, stderr=subprocess.STDOUT)
    logfile.close()
    scale.wait_for_port(PORT, proc=p, what="barchd")
    return p


def stop(p):
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        raise AssertionError("barchd did not stop")
    assert p.returncode == 0, "barchd exited with %s:\n%s" % (
        p.returncode, open(LOG, "rb").read().decode(errors="replace")[-3000:])


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=30)


def conf(r, key):
    return r.execute_command("configuration:GET", key)


def http(path="/hello"):
    with urllib.request.urlopen("http://127.0.0.1:%d%s" % (HTTP_PORT, path), timeout=10) as f:
        return f.read().decode()


def status(r):
    return r.execute_command("FUNCTIONS", "STATUS").decode()


def count(r, name):
    v = r.execute_command("pkga:GET", "hooks:" + name)
    return int(v) if v is not None else 0


def live(space, setting):
    """what the open space really runs with, from barch.config() inside it"""
    c = client()
    try:
        c.execute_command("USE", space)
        c.execute_command("SETF", "LIVECONF",
                          "function call(k) return tostring(barch.config()[k]) end")
        return c.execute_command("LIVECONF", setting).decode()
    finally:
        c.close()


def sync(r):
    return r.execute_command("FUNCTIONS", "SYNC", REPO)


try:
    print("the first start applies the package", flush=True)
    proc = start("-g", origin, "pull=on", "user=default")
    try:
        r = client()
        assert conf(r, "pkga.shards") == b"3"
        assert conf(r, "pkga.ordered") == b"1"
        assert conf(r, "pkgb.shards") == b"2"
        # in place before the spaces opened, so they opened with them
        assert live("pkga", "shards") == "3" and live("pkga", "ordered") == "1"
        assert live("pkgb", "shards") == "2"
        # listed with nothing to load and no settings, and it exists all the same
        assert r.execute_command("KSPACE", "EXIST", "pkgc") == 1
        assert r.execute_command("pkga.PAGE") == b"page"
        assert r.execute_command("pkgb:GET", "thing.txt") == b"a value"
        # through the file API in the space itself, the way barchdtest reads one
        f = client()
        f.execute_command("USE", "pkga")
        f.execute_command("SETF", "FSGET", "function call(p) return barch.fs.get(p) end")
        assert f.execute_command("FSGET", "/site/index.html") == b"<p>hi</p>"
        f.close()
        # the installer is not something it installs
        try:
            r.execute_command("PACKAGE")
            raise AssertionError("package.luau was imported as a function")
        except redis.exceptions.ResponseError:
            pass
        assert http() == "v1"
        # a fresh clone: no before, one after with no old commit
        assert count(r, "before") == 0
        assert count(r, "after") == 1
        assert r.execute_command("pkga:GET", "hooks:after:args").decode() == \
            "%s||%s|a" % (REPO, head())
        assert b"wait for itself" in r.execute_command("pkga:GET", "hooks:resync")
        st = status(r)
        assert "package=applied" in st and "hooks=after=ok" in st and "user=default" in st, st

        print("a sync with no new commit runs no hook", flush=True)
        assert sync(r) == b"OK"
        assert count(r, "before") == 0 and count(r, "after") == 1
    finally:
        stop(proc)

    print("after a restart HTTP is back, and after runs once more", flush=True)
    proc = start()
    try:
        r = client()
        assert http() == "v1"
        assert count(r, "after") == 2 and count(r, "before") == 0
        # loaded from its files with the count it was saved with
        assert live("pkga", "shards") == "3" and live("pkgb", "shards") == "2"

        print("a new commit runs before, then after, and drops what is no longer listed",
              flush=True)
        old = head()
        write("code/page.luau", page("v2"))
        write("package.luau", package(ordered=False))
        commit("v2")
        assert sync(r) == b"OK"
        assert count(r, "before") == 1 and count(r, "after") == 3
        assert r.execute_command("pkga:GET", "hooks:before:args").decode() == \
            "%s|%s|%s|b" % (REPO, old, head())
        assert conf(r, "pkga.ordered") is None, "a setting the package stopped listing stayed"
        assert conf(r, "pkga.shards") == b"3"
        assert r.execute_command("pkga.PAGE") == b"page"
        assert http() == "v2", "the running server kept the old handler"

        print("a shard change on a space that exists is refused", flush=True)
        write("package.luau", package(shards_a=5, ordered=False))
        write("code/page.luau", page("v3"))
        commit("v3")
        try:
            sync(r)
            raise AssertionError("a shard change was applied")
        except redis.exceptions.ResponseError as e:
            assert "cannot change" in str(e), e
        assert conf(r, "pkga.shards") == b"3"
        assert "package=failed" in status(r), status(r)
        assert http() == "v2"

        print("no user on the repository, no hooks", flush=True)
        r.execute_command("configuration:REM", "git/repositories/%s/user" % REPO)
        before, after = count(r, "before"), count(r, "after")
        write("package.luau", package(ordered=False))
        write("code/page.luau", page("v4"))
        commit("v4")
        assert sync(r) == b"OK"
        assert (count(r, "before"), count(r, "after")) == (before, after)
        st = status(r)
        assert "skipped:no_user" in st and "package=applied" in st, st
        assert http() == "v4"

        print("a package that does not check out is refused whole", flush=True)
        write("package.luau", package(ordered=False, extra=", shardz = 4"))
        write("code/page.luau", page("v5"))
        commit("v5")
        try:
            sync(r)
            raise AssertionError("a misspelt setting was accepted")
        except redis.exceptions.ResponseError as e:
            assert "not a key space setting" in str(e), e
        assert conf(r, "pkgb.shardz") is None
        assert r.execute_command("pkga.PAGE") == b"page"
        assert http() == "v4", "a refused package still replaced the code"
    finally:
        stop(proc)
finally:
    shutil.rmtree(base, ignore_errors=True)

print("package test complete", flush=True)
