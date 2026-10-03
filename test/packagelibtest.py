# Library packages stored by commit in the repository graph - TODO 593, phase 1.
#
# lib and lib2 say kind = "dependency" in their package.luau. lib has two commits:
# c1, tagged v1, and c2 on main, which depends on lib2. Three apps pin it:
#   app1 by tag v1, app2 by branch main, app3 by commit c1.
# Checked:
#   1. both versions of lib are in the graph under /packages/lib/versions/<sha>,
#      with their files under content/, and lib never becomes a repository
#   2. each app's /apps/<app>/current/deps/lib is a link to its own version, and app3's
#      is the very node app1's is: a second pin shares, it does not copy
#   3. a version's own dependencies are links too: c2's deps/lib2 is lib2's version
#   4. the refs: tag v1 and branch main point at c1 and c2
#   5. an app that stops naming lib loses its link, and the version stays
#   6. refused: a library with spaces, a tag and a commit that disagree, a name
#      already used with another url, and a library that depends on an application
# and, phase 2, require:
#   7. require("@lib") gets the version the app's own package pinned: app1 and
#      app3 get v1, app2 gets v2, all three called on one connection, so two
#      versions of lib live in one session
#   8. inside a library a bare path is its own module, and "@lib2" is the version
#      that library pinned; the table form works too
#   9. a name the package doesn't depend on, and a version in require, are refused,
#      and an app that drops lib can't require it any more
# and, phase 3:
#  10. FUNCTIONS STATUS says what an app pins, and a diamond: app2 pins lib2 at a
#      commit other than the one lib pins, and both answer
#  11. FUNCTIONS LIBRARIES lists every version with who pins it, which versions
#      link to it and which refs point at it
#  12. a sync that fails leaves the app's pins where they were
#  13. a version nothing reaches goes, with its refs; a package with none left
#      goes, its name is free for another url, and unused mirrors go from disk
# and TODO 594, the repository space is the server's to write:
#  14. SET and GRAPH PUT there are refused, and GRAPH's reads still work; a
#      refused space:CMD leaves the connection in the space it was in
#  15. a per space ACL that says +all doesn't give write back
#  16. a script can't write it through barch.store, barch.call, barch.graph or
#      barch.space, whether it runs there or reaches over; it can still read
#  17. a package can't name it, and the sync still installs libraries into it
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14996)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

HERE = os.getcwd()
base = tempfile.mkdtemp(prefix="bdlib")
LIB = os.path.join(base, "lib")
LIB2 = os.path.join(base, "lib2")
PLAIN = os.path.join(base, "plain")
APPS = {n: os.path.join(base, n) for n in ("app1", "app2", "app3", "bad")}

print("start package library test with %s" % BINARY, flush=True)


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
    return git(repo, "rev-parse", "HEAD")


def make(repo):
    subprocess.check_call(["git", "init", "-q", "-b", "main", repo])
    git(repo, "config", "user.email", "t@t")
    git(repo, "config", "user.name", "t")


def library(depends=""):
    return """
function setup()
    return { kind = "dependency", depends = { %s } }
end
""" % depends


def app(name, depends):
    return """
function setup()
    return {
        depends = { %s },
        load = { { path = "code", space = "%ssp" } },
    }
end
""" % (depends, name)


make(LIB2)
write(LIB2, "package.luau", library())
write(LIB2, "two.luau", "function two() return 2 end\n")
write(LIB2, "init.luau", "function two() return 2 end\n")
LIB2_SHA = commit(LIB2, "lib2")
write(LIB2, "init.luau", "function two() return 22 end\n")
LIB2_B = commit(LIB2, "lib2 b")

make(LIB)
write(LIB, "package.luau", library())
write(LIB, "mod.luau", "function v() return 1 end\n")
write(LIB, "sub/util.luau", "function u() return 'u' end\n")
write(LIB, "init.luau", """
local util = require("sub/util")
function v() return 1 end
function u() return util.u() end
""")
C1 = commit(LIB, "v1")
git(LIB, "tag", "v1")
write(LIB, "package.luau", library('{ name = "lib2", url = "%s", commit = "%s" }'
                                   % (LIB2, LIB2_SHA)))
write(LIB, "mod.luau", "function v() return 2 end\n")
write(LIB, "init.luau", """
local two = require("@lib2")
function v() return 2 + two.two() end
function u() return "u2" end
""")
C2 = commit(LIB, "v2")

# an application-kind repository: no kind, so the default
make(PLAIN)
write(PLAIN, "plainsp/p.luau", "function call() return 'p' end\n")
commit(PLAIN, "plain")

# app1 requires inside call(), app2 at its top level, app3 with the table form
CODE = {
    "app1": 'function call() local lib = require("@lib") return lib.v() end\n',
    "app2": 'local lib = require("@lib")\nfunction call() return lib.v() end\n',
    "app3": 'function call() return require({ name = "lib" }).u() end\n',
}
for name, dep in (("app1", 'tag = "v1"'), ("app2", 'branch = "main"'),
                  ("app3", 'commit = "%s"' % C1)):
    make(APPS[name])
    write(APPS[name], "package.luau",
          app(name, '{ name = "lib", url = "%s", %s }' % (LIB, dep)))
    write(APPS[name], "code/%s.luau" % name, CODE[name])
    commit(APPS[name], name)
write(APPS["app2"], "code/nope.luau",
      'function call() return require("@nothere").v() end\n')
write(APPS["app2"], "code/pinned.luau",
      'function call() return require({ name = "lib", tag = "v1" }).v() end\n')
write(APPS["app2"], "code/util.luau",
      'function call() return require("@lib/sub/util").u() end\n')
commit(APPS["app2"], "more")

make(APPS["bad"])
write(APPS["bad"], "code/bad.luau", 'function call() return "bad" end\n')
commit(APPS["bad"], "bad")


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


def add_repo(r, name, url):
    for k, v in (("url", url), ("pull", "on"), ("user", "default")):
        r.execute_command("configuration:SET", "git/repositories/%s/%s" % (name, k), v)


def node(r, path):
    """the node id at a path in the repository graph, None when there is none"""
    line = r.execute_command("repository:GRAPH", "STAT", path)
    if line is None:
        return None
    fields = dict(f.split("=", 1) for f in line.decode().split(" ") if "=" in f)
    return int(fields["id"])


def names(r, path):
    return sorted(l.decode().split(" ", 4)[4]
                  for l in r.execute_command("repository:GRAPH", "LS", path))


def body(r, path):
    v = r.execute_command("repository:GRAPH", "GET", path)
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


def set_bad(r, package):
    write(APPS["bad"], "package.luau", package)
    commit(APPS["bad"], "bad again")


try:
    data = os.path.join(HERE, "lib_data")
    shutil.rmtree(data, ignore_errors=True)
    proc = start(data, "-g", APPS["app1"], "pull=on", "user=default")
    try:
        r = client()
        # a replaced pin set goes 2s after it was replaced, once no call holds it -
        # TODO 595, 596
        GRACE = 2.5
        for name in ("app2", "app3"):
            add_repo(r, name, APPS[name])
            assert r.execute_command("FUNCTIONS", "SYNC", name) == b"OK"

        print("both versions of lib are in the graph, and lib is no repository", flush=True)
        assert names(r, "/packages/lib/versions") == sorted([C1, C2])
        assert body(r, "/packages/lib/versions/%s/content/mod.luau" % C1) == \
            "function v() return 1 end\n"
        assert body(r, "/packages/lib/versions/%s/content/mod.luau" % C2) == \
            "function v() return 2 end\n"
        assert body(r, "/packages/lib/versions/%s/content/sub/util.luau" % C1) is not None
        assert body(r, "/packages/lib/source") == LIB
        assert conf(r, "git/repositories/lib/url") is None
        assert conf(r, "git/repositories/app1/package/depends") is None

        print("each app links to its own version, and a second pin shares", flush=True)
        v1 = node(r, "/packages/lib/versions/" + C1)
        v2 = node(r, "/packages/lib/versions/" + C2)
        assert v1 and v2 and v1 != v2
        assert node(r, "/apps/app1/current/deps/lib") == v1
        assert node(r, "/apps/app2/current/deps/lib") == v2
        assert node(r, "/apps/app3/current/deps/lib") == v1
        assert r.execute_command("FUNCTIONS", "SYNC", "app3") == b"OK"
        assert names(r, "/packages/lib/versions") == sorted([C1, C2])
        assert names(r, "/apps/app3/current/deps") == ["lib"], "a second sync linked twice"
        assert names(r, "/apps/app3/pinsets") == ["1"], "a sync that changed nothing re-pinned"

        print("a version's own dependencies are links too", flush=True)
        assert node(r, "/packages/lib/versions/%s/deps/lib2" % C2) == \
            node(r, "/packages/lib2/versions/" + LIB2_SHA)
        assert node(r, "/packages/lib/versions/%s/deps/lib2" % C1) is None

        print("refs point at the commits they resolved to", flush=True)
        assert node(r, "/packages/lib/refs/tag/v1") == v1
        assert node(r, "/packages/lib/refs/branch/main") == v2

        print("require gets the version the app's package pinned", flush=True)
        assert r.execute_command("app1sp.APP1") == 1
        assert r.execute_command("app2sp.APP2") == 4, "v2 and its lib2"
        assert r.execute_command("app3sp.APP3") == b"u"
        assert r.execute_command("app2sp.UTIL") == b"u"
        assert r.execute_command("app1sp.APP1") == 1, "v1 and v2 side by side in one session"
        assert node(r, "/spaces/app1sp") == node(r, "/apps/app1")

        print("refused: a name it doesn't depend on, and a version in require", flush=True)
        for fn, words in (("NOPE", "nothere is not a dependency of key space app2sp"),
                          ("PINNED", "takes no tag")):
            try:
                r.execute_command("app2sp." + fn)
                raise AssertionError(fn + " was not refused")
            except redis.exceptions.ResponseError as e:
                assert words in str(e), e

        print("an app that stops naming lib loses its link, the version stays", flush=True)
        write(APPS["app1"], "package.luau", app("app1", ""))
        commit(APPS["app1"], "no lib")
        assert r.execute_command("FUNCTIONS", "SYNC", "app1") == b"OK"
        assert node(r, "/apps/app1/current/deps/lib") is None
        assert node(r, "/packages/lib/versions/" + C1) == v1
        try:
            r.execute_command("app1sp.APP1")
            raise AssertionError("app1 still reached lib")
        except redis.exceptions.ResponseError as e:
            assert "lib is not a dependency" in str(e), e
        print("a top level require of a library the package doesn't name fails the sync",
              flush=True)
        write(APPS["app1"], "code/app1.luau", CODE["app2"])
        commit(APPS["app1"], "top level")
        refused(r, "app1", "lib is not a dependency of package app1")

        print("refused: what a library can't be", flush=True)
        add_repo(r, "bad", APPS["bad"])
        write(LIB2, "package.luau", """
function setup()
    return { kind = "dependency", spaces = { x = { shards = 2 } } }
end
""")
        with_spaces = commit(LIB2, "spaces")
        set_bad(r, app("bad", '{ name = "lib2", url = "%s", commit = "%s" }'
                       % (LIB2, with_spaces)))
        refused(r, "bad", "a library cannot")
        set_bad(r, app("bad", '{ name = "lib", url = "%s", tag = "v1", commit = "%s" }'
                       % (LIB, C2)))
        refused(r, "bad", "tag v1 is not")
        set_bad(r, app("bad", '{ name = "lib", url = "%s", commit = "%s" }'
                       % (LIB2, LIB2_SHA)))
        refused(r, "bad", "another url")
        write(LIB2, "package.luau", library('{ name = "plain", url = "%s" }' % PLAIN))
        on_app = commit(LIB2, "on an application")
        set_bad(r, app("bad", '{ name = "lib2", url = "%s", commit = "%s" }'
                       % (LIB2, on_app)))
        refused(r, "bad", "is an application")
        assert conf(r, "git/repositories/plain/url") is None
        assert names(r, "/packages/lib2/versions") == [LIB2_SHA]

        def libraries():
            return [l.decode() for l in r.execute_command("FUNCTIONS", "LIBRARIES")]

        def app2_package(*deps):
            return app("app2", ", ".join(deps))
        LIB_MAIN = '{ name = "lib", url = "%s", branch = "main" }' % LIB
        LIB2_ON_B = '{ name = "lib2", url = "%s", commit = "%s" }' % (LIB2, LIB2_B)

        print("STATUS says what an app pins, and a diamond", flush=True)
        assert "libraries=lib@%s" % C2[:12] in line_of(r, "app2"), line_of(r, "app2")
        assert "diamond=" not in line_of(r, "app2")
        write(APPS["app2"], "package.luau", app2_package(LIB_MAIN, LIB2_ON_B))
        write(APPS["app2"], "code/both.luau", 'function call() '
              'return require("@lib").v() * 100 + require("@lib2").two() end\n')
        commit(APPS["app2"], "lib2 too")
        assert r.execute_command("FUNCTIONS", "SYNC", "app2") == b"OK"
        assert r.execute_command("app2sp.BOTH") == 422, "lib's lib2 is 2, app2's is 22"
        line = line_of(r, "app2")
        assert "libraries=lib@%s,lib2@%s" % (C2[:12], LIB2_B[:12]) in line, line
        assert "diamond=lib2@%s@%s" % tuple(sorted([LIB2_SHA[:12], LIB2_B[:12]])) in line, line

        print("FUNCTIONS LIBRARIES lists every version and who holds it", flush=True)
        libs = libraries()
        for want in ("name=lib version=%s pinned_by=app3 used_by=- refs=tag/v1" % C1,
                     "name=lib version=%s pinned_by=app2 used_by=- refs=branch/main" % C2,
                     "name=lib2 version=%s pinned_by=- used_by=lib@%s refs=-"
                     % (LIB2_SHA, C2[:12]),
                     "name=lib2 version=%s pinned_by=app2 used_by=- refs=-" % LIB2_B):
            assert want in libs, (want, libs)

        print("a sync that fails leaves the pins where they were", flush=True)
        write(APPS["app2"], "package.luau", app2_package(
            '{ name = "lib", url = "%s", tag = "v1" }' % LIB, LIB2_ON_B))
        write(APPS["app2"], "code/broken.luau", "function call( end\n")
        commit(APPS["app2"], "broken")
        try:
            r.execute_command("FUNCTIONS", "SYNC", "app2")
            raise AssertionError("a broken file synced")
        except redis.exceptions.ResponseError:
            pass
        assert node(r, "/apps/app2/current/deps/lib") == v2
        # the failed one's pin set stays beside it until the next switch
        assert node(r, "/apps/app2/current") != node(r, "/apps/app2/pinsets/" +
                                                     max(names(r, "/apps/app2/pinsets"), key=int))
        assert r.execute_command("app2sp.APP2") == 4
        os.remove(os.path.join(APPS["app2"], "code/broken.luau"))
        write(APPS["app2"], "package.luau", app2_package(LIB_MAIN, LIB2_ON_B))
        commit(APPS["app2"], "fixed")
        assert r.execute_command("FUNCTIONS", "SYNC", "app2") == b"OK"
        assert len(names(r, "/apps/app2/pinsets")) > 1, "a replaced pin set went at once"
        time.sleep(GRACE)
        assert r.execute_command("FUNCTIONS", "SYNC", "app2") == b"OK"
        left = names(r, "/apps/app2/pinsets")
        assert len(left) == 1 and \
            node(r, "/apps/app2/pinsets/" + left[0]) == node(r, "/apps/app2/current"), \
            "pin sets past their grace are still there: %s" % left

        print("a version nothing reaches goes, with its refs", flush=True)
        write(APPS["app3"], "package.luau", app("app3", ""))
        commit(APPS["app3"], "no lib")
        assert r.execute_command("FUNCTIONS", "SYNC", "app3") == b"OK"
        assert C1 in names(r, "/packages/lib/versions"), \
            "a version went while a call could still be on the pin set holding it"
        time.sleep(GRACE)
        assert r.execute_command("FUNCTIONS", "SYNC", "app3") == b"OK"
        assert names(r, "/packages/lib/versions") == [C2]
        assert node(r, "/packages/lib/refs/tag/v1") is None
        assert node(r, "/packages/lib/refs/branch/main") == v2
        assert names(r, "/packages/lib2/versions") == sorted([LIB2_SHA, LIB2_B])

        print("a package nothing pins goes, with its mirror, and its name is free",
              flush=True)
        write(APPS["app2"], "package.luau", app2_package())
        write(APPS["app2"], "code/app2.luau", CODE["app1"])
        commit(APPS["app2"], "nothing")
        assert r.execute_command("FUNCTIONS", "SYNC", "app2") == b"OK"
        time.sleep(GRACE)
        assert r.execute_command("FUNCTIONS", "SYNC", "app2") == b"OK"
        assert node(r, "/packages/lib") is None and node(r, "/packages/lib2") is None
        assert libraries() == []
        assert "libraries=" not in line_of(r, "app2")
        mirrors = os.path.join(data, "functions", ".mirrors")
        assert os.listdir(mirrors) == [], os.listdir(mirrors)
        write(APPS["app2"], "package.luau", app2_package(
            '{ name = "lib", url = "%s", commit = "%s" }' % (LIB2, LIB2_SHA)))
        commit(APPS["app2"], "lib is lib2 now")
        assert r.execute_command("FUNCTIONS", "SYNC", "app2") == b"OK"
        assert body(r, "/packages/lib/source") == LIB2

        print("the repository space is the server's to write", flush=True)
        version = names(r, "/packages/lib/versions")[0]
        init = "/packages/lib/versions/%s/content/init.luau" % version
        before = body(r, init)

        def denied(*cmd, conn=None):
            try:
                (conn or r).execute_command(*cmd)
            except redis.exceptions.ResponseError as e:
                return str(e)
            raise AssertionError("%s was not refused" % (cmd,))

        assert "not authorized" in denied("repository:SET", "x", "y")
        # a refused space:CMD used to leave the connection in that space
        r.execute_command("SET", "after_refusal", "1")
        assert r.execute_command("EXISTS", "after_refusal") == 1
        r.execute_command("DEL", "after_refusal")
        assert "not authorized" in denied("repository:GRAPH", "PUT", init, "evil")
        assert "not authorized" in denied("repository:GRAPH", "RM", "/packages", "RECURSIVE")
        assert body(r, init) == before
        assert r.execute_command("repository:EXISTS", "x") == 0

        print("a per space ACL can't give write back", flush=True)
        assert r.execute_command("KSPACE", "ACL", "repository", "SETUSER", "default", "on",
                                 "+all") == b"OK"
        fresh = client()
        assert "not authorized" in denied("repository:SET", "x", "y", conn=fresh)
        assert node(fresh, "/packages") is not None
        fresh.close()

        print("nor can a script, from inside or across", flush=True)
        for fn, src in (
                ("STOREW", 'function call() barch.store.set("x", "y") return "wrote" end'),
                ("CALLW", 'function call() return barch.call("SET", "x", "y") end'),
                ("GRAPHW", 'function call() barch.graph.put("/x.txt", "y", "text/plain") '
                           'return "wrote" end'),
                ("SPACEW", 'function call() return barch.space["repository"]:set("x", "y") end'),
                ("READS", 'function call() '
                          'return barch.call("GRAPH", "STAT", "/packages") end')):
            assert r.execute_command("probe:SETF", fn, src) == b"OK"
        for call in ("repository:probe.STOREW", "repository:probe.CALLW",
                     "repository:probe.GRAPHW", "probe:SPACEW"):
            denied(call)
        assert r.execute_command("repository:EXISTS", "x") == 0
        assert node(r, "/x.txt") is None
        assert b"path=/packages" in r.execute_command("repository:probe.READS")

        print("a package can't name it, and the sync still installs into it", flush=True)
        set_bad(r, """
function setup()
    return { spaces = { repository = { shards = 2 } } }
end
""")
        refused(r, "bad", "is not a key space a package can use")
        write(APPS["app2"], "package.luau", app2_package(
            '{ name = "lib", url = "%s", commit = "%s" }' % (LIB2, LIB2_SHA), LIB2_ON_B))
        commit(APPS["app2"], "lib2 again")
        assert r.execute_command("FUNCTIONS", "SYNC", "app2") == b"OK"
        assert names(r, "/packages/lib2/versions") == [LIB2_B]
    finally:
        stop(proc, data)
finally:
    shutil.rmtree(base, ignore_errors=True)

print("package library test complete", flush=True)
