# Several git repositories, configured as a directory of little files in the
# configuration space - TODO 252.
#
# Before this there were six flat globals, so there was one checkout, one remote and
# one branch. What this checks is the part that could not be done at all: two
# repositories at once, two branches of the same origin landing in different key
# spaces, a clone barch does itself, and the refusal when two of them want one space.
import os
import subprocess
import tempfile

import scale
import redis
import barch

scale.workdir()
PORT = scale.port(default=14700)

print("start git repositories test")
base = tempfile.mkdtemp(prefix="barch-repos-")
origin = os.path.join(base, "origin")
os.makedirs(origin)


def git(cwd, *args, out=False):
    cmd = ["git", "-C", cwd, *args]
    if out:
        return subprocess.check_output(cmd, text=True).strip()
    subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def write(rel, body):
    path = os.path.join(origin, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)


# one origin, two branches: main says v1, next says v2
git(origin, "init", "-b", "main")
git(origin, "config", "user.email", "t@t")
git(origin, "config", "user.name", "t")
write("ver.luau", 'function call() return "v1" end\n')
write("notes.txt", "from main\n")
git(origin, "add", "-A")
git(origin, "commit", "-m", "v1")
git(origin, "checkout", "-q", "-b", "next")
write("ver.luau", 'function call() return "v2" end\n')
git(origin, "add", "-A")
git(origin, "commit", "-m", "v2")
git(origin, "checkout", "-q", "main")

barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")


def conf(repo, setting, value):
    c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
    try:
        c.execute_command("USE", "configuration")
        c.execute_command("SET", "git/repositories/%s/%s" % (repo, setting), value)
    finally:
        c.close()


def unconf(repo, setting):
    c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
    try:
        c.execute_command("USE", "configuration")
        c.execute_command("REM", "git/repositories/%s/%s" % (repo, setting))
    finally:
        c.close()


def in_space(space, *cmd):
    c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
    try:
        c.execute_command("USE", space)
        return c.execute_command(*cmd)
    finally:
        c.close()


FSGET = "function call(p) return barch.fs.get(p) end"
FSSTAT = "function call(p) local e = barch.fs.stat(p) if e == nil then return nil end return tostring(e.size) end"


def fs_in(space, fn, path):
    """the file store as a file store: what a stored file is belongs to fs.h, and a
    test that names its keys is another copy of the layout"""
    c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
    try:
        c.execute_command("USE", space)
        c.execute_command("SETF", "fsget", FSGET)
        c.execute_command("SETF", "fsstat", FSSTAT)
        return c.execute_command(fn, path)
    finally:
        c.close()


def status():
    return r.execute_command("FUNCTIONS", "STATUS").decode()


try:
    # the checkouts land under functions_dir/<name>, since neither names a dir
    r.execute_command("CONFIG", "SET", "functions_dir", os.path.join(base, "checkouts"))

    print("two branches of one origin, into two key spaces", flush=True)
    for name, branch, space in (("site", "main", "site"), ("nxt", "next", "sitenext")):
        conf(name, "url", origin)
        conf(name, "branch", branch)
        conf(name, "space", space)
        conf(name, "pull", "on")

    assert r.execute_command("FUNCTIONS", "SYNC") == b"OK"
    # barch cloned both itself - there was no checkout to start from
    assert os.path.isdir(os.path.join(base, "checkouts", "site", ".git"))
    assert os.path.isdir(os.path.join(base, "checkouts", "nxt", ".git"))
    assert in_space("site", "ver") == b"v1"
    assert in_space("sitenext", "ver") == b"v2"
    # a non-luau file is a key, and it went to the declared space, not the default
    assert in_space("site", "GET", "notes.txt") == b"from main\n"
    assert r.execute_command("GET", "notes.txt") is None

    print("status has a line per repository", flush=True)
    st = status()
    assert len(st.splitlines()) == 2, st
    assert "name=site " in st and "name=nxt " in st, st
    assert "state=ok" in st, st
    assert "space=sitenext" in st, st

    print("one repository by name", flush=True)
    write("ver.luau", 'function call() return "v3" end\n')
    git(origin, "add", "-A")
    git(origin, "commit", "-m", "v3")
    assert r.execute_command("FUNCTIONS", "SYNC", "site") == b"OK"
    assert in_space("site", "ver") == b"v3"
    assert in_space("sitenext", "ver") == b"v2"
    # one argument that is not a repository is a commit, which is what it always
    # meant - but it cannot mean it about several repositories at once
    try:
        r.execute_command("FUNCTIONS", "SYNC", "deadbeef")
        assert False, "a bare commit should not apply to every repository"
    except redis.exceptions.ResponseError as e:
        assert "needs a repository" in str(e), e
    try:
        r.execute_command("FUNCTIONS", "SYNC", "nosuch", "deadbeef")
        assert False, "an unknown repository should not sync"
    except redis.exceptions.ResponseError as e:
        assert "no repository" in str(e), e

    print("two repositories cannot own one key space", flush=True)
    conf("nxt", "space", "site")
    try:
        r.execute_command("FUNCTIONS", "SYNC")
        assert False, "the overlap should be refused"
    except redis.exceptions.ResponseError as e:
        assert "both want key space site" in str(e), e
    st = status()
    assert st.count("state=conflict") == 2, st
    # neither ran, so the space still holds what the last good sync left
    assert in_space("site", "ver") == b"v3"
    conf("nxt", "space", "sitenext")
    assert r.execute_command("FUNCTIONS", "SYNC") == b"OK"

    print("a deploy key setting holds a reference, not the key", flush=True)
    conf("nxt", "ssh_key", "-----BEGIN OPENSSH PRIVATE KEY-----\nnope\n")
    st = status()
    assert "state=disabled" in st, st
    assert "not the key itself" in st, st
    # the other repository is untouched by its neighbour being wrong
    assert r.execute_command("FUNCTIONS", "SYNC", "site") == b"OK"
    unconf("nxt", "ssh_key")

    print("a repository can land in the file store instead of in keys", flush=True)
    # a directory of things that are not key values - which is the point of as=fs
    write("assets/logo.svg", "<svg/>\n")
    write("assets/app.js", "console.log(1)\n")
    git(origin, "add", "-A")
    git(origin, "commit", "-m", "assets")
    conf("files", "url", origin)
    conf("files", "space", "media")
    conf("files", "pull", "on")
    conf("files", "as", "fs")
    conf("files", "fs_root", "/repo")
    assert r.execute_command("FUNCTIONS", "SYNC", "files") == b"OK"
    assert "as=fs" in status() and "root=/repo" in status(), status()
    assert fs_in("media", "fsstat", "/repo/assets/logo.svg") is not None, \
        "the file store has no metadata for the imported file"
    assert fs_in("media", "fsget", "/repo/assets/logo.svg") == b"<svg/>\n"
    # a .luau in an fs repository stays a file: it is content, not a function
    assert fs_in("media", "fsstat", "/repo/ver.luau") is not None

    print("a file deleted upstream leaves the store", flush=True)
    os.remove(os.path.join(origin, "assets", "app.js"))
    git(origin, "add", "-A")
    git(origin, "commit", "-m", "drop app.js")
    assert r.execute_command("FUNCTIONS", "SYNC", "files") == b"OK"
    assert fs_in("media", "fsstat", "/repo/assets/app.js") is None
    assert fs_in("media", "fsget", "/repo/assets/app.js") is None
    assert fs_in("media", "fsstat", "/repo/assets/logo.svg") is not None

    for setting in ("url", "space", "pull", "as", "fs_root"):
        unconf("files", setting)

    print("the old settings still describe one repository called default", flush=True)
    for name in ("site", "nxt"):
        for setting in ("url", "branch", "space", "pull"):
            unconf(name, setting)
    legacy = os.path.join(base, "legacy")
    os.makedirs(legacy)
    with open(os.path.join(legacy, "old.luau"), "w", encoding="utf-8") as f:
        f.write('function call() return "legacy" end\n')
    r.execute_command("CONFIG", "SET", "functions_dir", legacy)
    st = status()
    assert "name=default" in st, st
    assert r.execute_command("FUNCTIONS", "SYNC") == b"OK"
    assert r.execute_command("old") == b"legacy"

    print("complete git repositories test")
finally:
    try:
        barch.stop()
    except Exception:
        pass
