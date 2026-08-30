import os
import subprocess
import tempfile

import redis
import barch

# A checkout of .luau files is SETF'd by FUNCTIONS SYNC. Other files are SET as
# keys, keeping the extension; a nested path becomes dir:name. A broken file
# must not leave a half-applied space, and a deleted file is REMF / REM after
# a successful sync. Keys someone SET by hand stay.

PORT = 14000

print("start function sync test")
root = tempfile.mkdtemp(prefix="barch-fns-")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")


def write(rel, body):
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)


def remove(rel):
    os.remove(os.path.join(root, rel))


try:
    write("greet.luau", 'function call(who) return "hi " .. who end\n')
    r.execute_command("CONFIG", "SET", "functions_dir", root)
    assert r.execute_command("FUNCTIONS", "SYNC") == b"OK"
    assert r.execute_command("greet", "sam").decode() == "hi sam"

    # a broken file in the same space refuses the whole apply
    write("broken.luau", "function call( end\n")
    try:
        r.execute_command("FUNCTIONS", "SYNC")
        assert False, "broken luau should refuse the sync"
    except redis.exceptions.ResponseError:
        pass
    assert r.execute_command("greet", "sam").decode() == "hi sam"
    names = {n.decode() for n in r.execute_command("KEYSF")}
    assert "BROKEN" not in names, names
    assert "GREET" in names

    remove("broken.luau")

    # require order: AMOD needs ZMOD, and A sorts first
    write("zmod.luau", '''
        function twice(n) return n * 2 end
        function call() return "z" end
    ''')
    write("amod.luau", '''
        local z = require("zmod")
        function call(n) return z.twice(tonumber(n)) end
    ''')
    assert r.execute_command("FUNCTIONS", "SYNC") == b"OK"
    assert r.execute_command("amod", "21") == 42

    remove("greet.luau")
    assert r.execute_command("FUNCTIONS", "SYNC") == b"OK"
    names = {n.decode() for n in r.execute_command("KEYSF")}
    assert "GREET" not in names, names
    assert "AMOD" in names and "ZMOD" in names
    # this connection still has GREET compiled (TODO 137); a new one must not
    fresh = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
    try:
        fresh.execute_command("greet", "sam")
        assert False, "removed function should not still run as a command"
    except redis.exceptions.ResponseError:
        pass
    fresh.close()

    write("hnsw/put.luau", 'function call(w) return "put " .. w end\n')
    r.execute_command("configuration:SET", "hnsw.shards", "1")
    assert r.execute_command("FUNCTIONS", "SYNC") == b"OK"
    assert r.execute_command("hnsw:put", "x").decode() == "put x"

    # non-luau files become keys; nested dirs get dir: prepended
    write("notes.md", "hello notes")
    write("hnsw/words.txt", "cat dog")
    write("hnsw/data/foo.json", '{"a":1}')
    r.set("user-key", "hand")
    r.execute_command("hnsw:SET", "keep-me", "hand")
    assert r.execute_command("FUNCTIONS", "SYNC") == b"OK"
    assert r.get("notes.md") == b"hello notes"
    assert r.execute_command("hnsw:GET", "words.txt") == b"cat dog"
    assert r.execute_command("hnsw:GET", "data:foo.json") == b'{"a":1}'
    assert r.get("user-key") == b"hand"
    assert r.execute_command("hnsw:GET", "keep-me") == b"hand"

    # a broken luau refuses the space, including pending key writes
    write("broken.luau", "function call( end\n")
    write("notes.md", "should not land")
    try:
        r.execute_command("FUNCTIONS", "SYNC")
        assert False, "broken luau should refuse the sync"
    except redis.exceptions.ResponseError:
        pass
    assert r.get("notes.md") == b"hello notes"
    remove("broken.luau")
    write("notes.md", "hello notes")

    # dropping a data file REMs it; hand-written keys stay
    remove("hnsw/words.txt")
    remove("notes.md")
    assert r.execute_command("FUNCTIONS", "SYNC") == b"OK"
    assert r.get("notes.md") is None
    assert r.execute_command("hnsw:GET", "words.txt") is None
    assert r.execute_command("hnsw:GET", "data:foo.json") == b'{"a":1}'
    assert r.get("user-key") == b"hand"
    assert r.execute_command("hnsw:GET", "keep-me") == b"hand"

    # pin a checkout to one commit so later origin commits do not land
    origin = tempfile.mkdtemp(prefix="barch-fns-origin-")
    clone = tempfile.mkdtemp(prefix="barch-fns-clone-")
    os.rmdir(clone)
    def git(cwd, *args, out=False):
        cmd = ["git", "-C", cwd, *args]
        if out:
            return subprocess.check_output(cmd, text=True).strip()
        subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    git(origin, "init", "-b", "main")
    git(origin, "config", "user.email", "t@t")
    git(origin, "config", "user.name", "t")
    with open(os.path.join(origin, "ver.luau"), "w", encoding="utf-8") as f:
        f.write('function call(who) return "v1 " .. who end\n')
    git(origin, "add", "ver.luau")
    git(origin, "commit", "-m", "v1")
    sha1 = git(origin, "rev-parse", "HEAD", out=True)
    subprocess.check_call(["git", "clone", "--quiet", origin, clone])
    def call_ver():
        c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
        try:
            return c.execute_command("ver", "sam").decode()
        finally:
            c.close()
    r.execute_command("CONFIG", "SET", "functions_dir", clone)
    r.execute_command("CONFIG", "SET", "functions_git_pull", "on")
    r.execute_command("CONFIG", "SET", "functions_git_commit", sha1)
    assert r.execute_command("FUNCTIONS", "SYNC") == b"OK"
    assert call_ver() == "v1 sam"
    with open(os.path.join(origin, "ver.luau"), "w", encoding="utf-8") as f:
        f.write('function call(who) return "v2 " .. who end\n')
    git(origin, "add", "ver.luau")
    git(origin, "commit", "-m", "v2")
    sha2 = git(origin, "rev-parse", "HEAD", out=True)
    assert r.execute_command("FUNCTIONS", "SYNC") == b"OK"
    assert call_ver() == "v1 sam"
    r.execute_command("CONFIG", "SET", "functions_git_commit", sha2)
    assert r.execute_command("FUNCTIONS", "SYNC") == b"OK"
    assert call_ver() == "v2 sam"
    r.execute_command("CONFIG", "SET", "functions_git_commit", "off")
    with open(os.path.join(origin, "ver.luau"), "w", encoding="utf-8") as f:
        f.write('function call(who) return "v3 " .. who end\n')
    git(origin, "add", "ver.luau")
    git(origin, "commit", "-m", "v3")
    assert r.execute_command("FUNCTIONS", "SYNC") == b"OK"
    assert call_ver() == "v3 sam"
    # one-shot pin on the command, without changing the config
    assert r.execute_command("FUNCTIONS", "SYNC", sha1) == b"OK"
    assert call_ver() == "v1 sam"
    st = r.execute_command("FUNCTIONS", "STATUS").decode()
    assert "pin=off" in st, st
    assert "commit=" in st, st

    print("complete function sync test")
finally:
    try:
        barch.stop()
    except Exception:
        pass
