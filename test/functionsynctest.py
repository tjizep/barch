import os
import tempfile

import redis
import barch

# A checkout of .luau files is SETF'd by FUNCTIONS SYNC. A broken file must not
# leave a half-applied space, and a deleted file is REMF after a successful sync.

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

    print("complete function sync test")
finally:
    try:
        barch.stop()
    except Exception:
        pass
