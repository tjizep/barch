# DIR - a key namespace walked as a tree. TODO 254.
#
# FS is the file store, which has a layout behind it. This is the other half: keys
# that are a tree only because somebody named them that way. The configuration
# space is full of them - git/repositories/<name>/<setting> - and so is anything
# LOADKEYS imported, which joins directories with a colon.
import scale
import redis
import barch

scale.workdir()
PORT = scale.port(default=14900)

print("start dir test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")


def ls(*args):
    return [x.decode() for x in r.execute_command("DIR", "LS", *args)]


try:
    for k, v in (("git:repositories:site:url", "https://x"),
                 ("git:repositories:site:branch", "main"),
                 ("git:repositories:nxt:url", "https://y"),
                 ("git:repositories", "top"),
                 ("other:thing", "z")):
        r.execute_command("SET", k, v)

    print("one level, with a name that is both a key and a node", flush=True)
    # `git:repositories` holds a value *and* has children under it, which the file
    # store cannot do and a raw key tree can - so `both` is a real answer
    assert ls("git") == ["both 3 repositories"], ls("git")
    assert ls("git:repositories") == ["node 0 nxt", "node 0 site"], ls("git:repositories")
    assert ls("git:repositories:site") == ["key 4 branch", "key 9 url"]
    assert ls("git:repositories:site:url") == []

    print("the whole space, which has no prefix to stop at", flush=True)
    assert ls("") == ["node 0 git", "node 0 other"], ls("")

    print("paging", flush=True)
    assert ls("git:repositories:site", "LIMIT", "1") == ["key 4 branch"]
    assert ls("git:repositories:site", "AFTER", "branch") == ["key 9 url"]

    print("counting is the keys under it, and itself when it is one", flush=True)
    assert r.execute_command("DIR", "COUNT", "git") == 4
    assert r.execute_command("DIR", "COUNT", "git:repositories:site") == 2
    assert r.execute_command("DIR", "COUNT", "nothing") == 0

    print("the separator is an argument, not a convention", flush=True)
    r.execute_command("SET", "a/b/c", "1")
    r.execute_command("SET", "a/b/d", "2")
    assert ls("a", "SEP", "/") == ["node 0 b"]
    assert ls("a/b", "SEP", "/") == ["key 1 c", "key 1 d"]
    # with the default separator that whole thing is one name
    assert ls("") == ["key 1 a/b/c", "key 1 a/b/d", "node 0 git", "node 0 other"], ls("")
    for bad in ("", "::"):
        try:
            r.execute_command("DIR", "LS", "a", "SEP", bad)
            raise AssertionError("SEP %r should have been refused" % bad)
        except redis.exceptions.ResponseError as e:
            assert "one byte" in str(e), e

    print("moving a subtree rewrites the keys under it", flush=True)
    assert r.execute_command("DIR", "MV", "git:repositories:site",
                             "git:repositories:renamed") == 2
    assert ls("git:repositories") == ["node 0 nxt", "node 0 renamed"]
    assert r.execute_command("GET", "git:repositories:renamed:url") == b"https://x"
    assert r.execute_command("GET", "git:repositories:site:url") is None
    # the key at the root of the move comes too when there is one
    r.execute_command("SET", "tree", "root")
    r.execute_command("SET", "tree:leaf", "under")
    assert r.execute_command("DIR", "MV", "tree", "moved") == 2
    assert r.execute_command("GET", "moved") == b"root"
    assert r.execute_command("GET", "moved:leaf") == b"under"

    print("copying leaves the original, removing takes the lot", flush=True)
    assert r.execute_command("DIR", "CP", "git:repositories:renamed",
                             "git:repositories:clone") == 2
    assert r.execute_command("GET", "git:repositories:renamed:url") == b"https://x"
    assert r.execute_command("GET", "git:repositories:clone:url") == b"https://x"
    assert r.execute_command("DIR", "RM", "git:repositories:clone") == 2
    assert ls("git:repositories") == ["node 0 nxt", "node 0 renamed"]
    assert r.execute_command("DIR", "RM", "git:repositories:clone") == 0

    print("what a move refuses", flush=True)
    for args, why in ((("git", "git:deeper"), "into itself"),
                      (("git", "git"), "same")):
        try:
            r.execute_command("DIR", "MV", *args)
            raise AssertionError("DIR MV %s should have been refused" % (args,))
        except redis.exceptions.ResponseError as e:
            assert why in str(e), (args, str(e))

    print("dir test complete", flush=True)
finally:
    try:
        barch.stop()
    except Exception:
        pass
