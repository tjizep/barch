# The graph store beside FS: nodes joined by edges, as ids - TODO 382.
#
# FS stores a path literally in the key name, so a file can only live at one
# path. Here paths are not stored at all: the edge table maps (parent, name)
# to a node id and any node may be edged from any number of parents. Leaf
# bytes reuse the FS inode/chunk keys verbatim; only the node/edge tables are
# new.
#
# Both doors are exercised: GRAPH * over RESP and barch.graph from Luau. The
# walk's visited set and queue live in a private one-shard scratch space, not
# in heap containers - a walk over millions of nodes is keys, not memory.
import scale

import redis
import barch

scale.workdir()
PORT = scale.port(default=14450)

print("start graph test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
# Never USE: the connection's unset space is the store the swig Caller reads
# through (KeyValue("default") binds "node", unnamed). USE default would name
# "default_", a *different* space - probed 20-09-2026 while STAT read null.
r.execute_command("FLUSHDB")

failures = []


def check(ok, what):
    if not ok:
        print("  FAIL " + what, flush=True)
        failures.append(what)


def stat(path):
    line = r.execute_command("GRAPH", "STAT", path)
    if line is None:
        return None
    out = {}
    for part in line.decode().split(" "):
        k, _, v = part.partition("=")
        out[k] = v
    return out


def node_id(path):
    return int(stat(path)["id"])


try:
    print("a leaf round trips", flush=True)
    assert r.execute_command("GRAPH", "MKDIR", "/a") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/a/hello.txt", "hello-world",
                             "TYPE", "text/plain") == b"OK"
    check(r.execute_command("GRAPH", "GET", "/a/hello.txt") == b"hello-world",
          "GET answers what PUT wrote")
    check(r.execute_command("GRAPH", "GET", "/a/hello.txt", "FROM", "1", "LEN", "3") == b"ell",
          "FROM/LEN clips the leaf")
    st = stat("/a/hello.txt")
    check(st["kind"] == "leaf" and st["refs"] == "1" and st["size"] == "11"
          and st["type"] == "text/plain" and "edge" in st,
          "STAT names kind, refs, size, type and the edge (%r)" % (st,))
    check(stat("/")["id"] == "0", "the root is node 0")
    check(r.execute_command("GRAPH", "STAT", "/missing") is None,
          "STAT of nothing is null")
    check(r.execute_command("GRAPH", "GET", "/missing") is None,
          "GET of nothing is null")
    ls = [x.decode() for x in r.execute_command("GRAPH", "LS", "/a")]
    check(len(ls) == 1 and ls[0].split(" ", 4) == ["leaf", ls[0].split(" ")[1],
          ls[0].split(" ")[2], "1", "hello.txt"],
          "LS is kind edge node refs name (%r)" % (ls,))

    print("two empty siblings list each other, not themselves", flush=True)
    assert r.execute_command("GRAPH", "MKDIR", "/x") == b"OK"
    assert r.execute_command("GRAPH", "MKDIR", "/y") == b"OK"
    root_ls = sorted(x.decode() for x in r.execute_command("GRAPH", "LS", "/"))
    check(any(x.endswith(" x") for x in root_ls)
          and any(x.endswith(" y") for x in root_ls),
          "LS / names both new directories (%r)" % (root_ls,))
    # an empty directory lists nothing: LS names a node's children, and /x
    # and /y have none. This once read as a listing bug (TODO 388) - it
    # is the correct answer, pinned here so it stays read that way.
    check(r.execute_command("GRAPH", "LS", "/x") == []
          and r.execute_command("GRAPH", "LS", "/y") == [],
          "LS of an empty directory is empty")

    print("one node at two paths", flush=True)
    assert r.execute_command("GRAPH", "PUT", "/shared.txt", "shared-bytes") == b"OK"
    assert r.execute_command("GRAPH", "MKDIR", "/b") == b"OK"
    shared = node_id("/shared.txt")
    assert r.execute_command("GRAPH", "LINK", shared, "/b/alias.txt") == b"OK"
    check(r.execute_command("GRAPH", "GET", "/b/alias.txt") == b"shared-bytes",
          "the second edge reads the same bytes")
    check(stat("/shared.txt")["refs"] == "2", "LINK raised the refcount to 2")
    check(node_id("/b/alias.txt") == shared, "both paths name one node")
    assert r.execute_command("GRAPH", "UNLINK", "/b/alias.txt") == b"OK"
    check(stat("/shared.txt")["refs"] == "1", "UNLINK dropped it back to 1")
    check(r.execute_command("GRAPH", "GET", "/shared.txt") == b"shared-bytes",
          "the survivor keeps the bytes")
    assert r.execute_command("GRAPH", "LINK", shared, "/b/alias.txt") == b"OK"
    assert r.execute_command("GRAPH", "RM", "/shared.txt") == b"OK"
    check(r.execute_command("GRAPH", "GET", "/shared.txt") is None
          and r.execute_command("GRAPH", "GET", "/b/alias.txt") is None,
          "RM takes the node everywhere, not just under one path")

    print("duplicate names are distinct edges", flush=True)
    assert r.execute_command("GRAPH", "PUT", "/dup1.txt", "first") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/dup2.txt", "second") == b"OK"
    n1, n2 = node_id("/dup1.txt"), node_id("/dup2.txt")
    check(n1 != n2, "two PUTs are two nodes")
    assert r.execute_command("GRAPH", "MKDIR", "/d") == b"OK"
    assert r.execute_command("GRAPH", "LINK", n1, "/d/same") == b"OK"
    assert r.execute_command("GRAPH", "LINK", n2, "/d/same") == b"OK"
    got = [x.decode() for x in r.execute_command("GRAPH", "LS", "/d")]
    check(len(got) == 2 and got[0].split(" ")[4] == "same"
          and got[1].split(" ")[4] == "same" and got[0] != got[1],
          "LS shows both edges (%r)" % (got,))
    check(r.execute_command("GRAPH", "GET", "/d/same") == b"first",
          "resolution takes the first-created edge")
    assert r.execute_command("GRAPH", "RM", "/d/same") == b"OK"
    check(r.execute_command("GRAPH", "GET", "/d/same") == b"second",
          "removing the first edge reveals the second")

    print("edge ids run the full 64 bits", flush=True)
    assert r.execute_command("GRAPH", "MKDIR", "/wide") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/wide/big.txt", "big-edge") == b"OK"
    big_node = node_id("/wide/big.txt")
    # A hand-made wide edge only proves the parsers; resolve_parent and
    # the batch planner must also see the new parent. So link for real:
    # /widedup/dup.txt is a live second edge under a fresh parent, and the
    # fat key is crafted under that same parent. LS then answers both.
    assert r.execute_command("GRAPH", "MKDIR", "/widedup") == b"OK"
    assert r.execute_command("GRAPH", "LINK", big_node, "/widedup/dup.txt") == b"OK"
    wide_parent = node_id("/widedup")
    fat = 2**32 + 5
    fat_hex = "%016x" % fat
    parent_hex = "%016x" % wide_parent
    big_stat = stat("/wide/big.txt")
    r.execute_command("SET", "graph:e:%s:%s" % (parent_hex, fat_hex),
                      '{"node":%d,"name":"fat.txt"}' % big_node)
    r.execute_command("SET", "graph:r:%016x:%s" % (big_node, fat_hex), parent_hex)
    # and the node's own refcount, which LINK would have bumped
    cur = int(big_stat["refs"])
    node_hex = "%016x" % big_node
    raw_node = r.execute_command("GET", "graph:n:" + node_hex).decode()
    inode = None
    for part in raw_node.strip("{}").split(","):
        k, _, v = part.partition(":")
        if k.strip().strip('"') == "inode":
            inode = v.strip()
    assert inode is not None, "node record carries its inode (%r)" % (raw_node,)
    r.execute_command("SET", "graph:n:" + node_hex,
                      '{"kind":"leaf","refs":%d,' % (cur + 1)
                      + '"size":8,"chunk":65536,"chunks":1,"version":1,'
                      + '"type":"","inode":%s}' % inode)
    alias_edge = fat
    wide = [x.decode() for x in r.execute_command("GRAPH", "LS", "/widedup")]
    check(len(wide) == 2, "both edges list (%r)" % (wide,))
    # LS prints kind edge node refs name; edge is field index 1
    check(any(int(x.split(" ")[1]) == fat for x in wide),
          "the 16-hex edge id lists (%r)" % (wide,))
    check(alias_edge > 2**32,
          "the new edge id needs more than 8 hex (%d)" % alias_edge)
    check(node_id("/widedup/fat.txt") == big_node,
          "a >2^32 edge still resolves to its node")
    check(stat("/widedup/fat.txt")["edge"] == str(alias_edge),
          "STAT names the wide edge id")
    check(r.execute_command("GRAPH", "GET", "/widedup/fat.txt") == b"big-edge",
          "GET through the wide edge reads the bytes")
    bfs_ids = [int(x.decode().split(" ", 1)[0])
               for x in r.execute_command("GRAPH", "BFS", "/widedup")]
    check(big_node in bfs_ids, "BFS reaches the node behind a wide edge")
    assert r.execute_command("GRAPH", "UNLINK", "/widedup/fat.txt") == b"OK"
    check(stat("/wide/big.txt")["refs"] == "2"
          and r.execute_command("GRAPH", "GET", "/wide/big.txt") == b"big-edge",
          "UNLINK drops just the wide edge")

    print("a cycle is a walk, not a loop", flush=True)
    assert r.execute_command("GRAPH", "MKDIR", "/t") == b"OK"
    assert r.execute_command("GRAPH", "MKDIR", "/t/sub") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/t/sub/doc.txt", "doc") == b"OK"
    assert r.execute_command("GRAPH", "LINK", node_id("/a"), "/t/loopback") == b"OK"
    bfs = [x.decode() for x in r.execute_command("GRAPH", "BFS", "/t")]
    paths = [x.split(" ", 1)[1] for x in bfs]
    check(paths[0] == "/t" and "/t/sub" in paths and "/t/sub/doc.txt" in paths
          and "/t/loopback" in paths and "/t/loopback/hello.txt" in paths,
          "BFS reaches through the linked node (%r)" % (paths,))
    check(len(paths) == len(set(x.split(" ", 1)[0] for x in bfs)),
          "every node listed once despite the cycle")
    dfs = [x.decode().split(" ", 1)[1]
           for x in r.execute_command("GRAPH", "DFS", "/t")]
    check(dfs[0] == "/t" and set(dfs[1:3]) == {"/t/sub", "/t/loopback"}
          and set(dfs[3:]) == {"/t/sub/doc.txt", "/t/loopback/hello.txt"},
          "DFS visits every node once, subtrees before siblings (%r)" % (dfs,))
    check([x.decode().split(" ", 1)[1]
           for x in r.execute_command("GRAPH", "BFS", "/t", "DEPTH", "1")] ==
          ["/t", "/t/sub", "/t/loopback"],
          "DEPTH 1 stops at the children")
    check([x.decode().split(" ", 1)[1]
           for x in r.execute_command("GRAPH", "BFS", "/t", "OFFSET", "1", "LIMIT", "1")] ==
          paths[1:2],
          "OFFSET/LIMIT page the walk")
    by_id = [x.decode() for x in
             r.execute_command("GRAPH", "BFS", str(node_id("/t/sub")), "LIMIT", "2")]
    check(len(by_id) == 2 and by_id[0].endswith(str(node_id("/t/sub"))),
          "a walk starts from a node id too (%r)" % (by_id,))

    print("moves, copies and removes", flush=True)
    assert r.execute_command("GRAPH", "PUT", "/cp.txt", "copy-me") == b"OK"
    assert r.execute_command("GRAPH", "CP", "/cp.txt", "/cp2.txt") == b"OK"
    check(node_id("/cp.txt") != node_id("/cp2.txt"),
          "CP duplicates the node, LINK shares it")
    assert r.execute_command("GRAPH", "PUT", "/cp.txt", "changed") == b"OK"
    check(r.execute_command("GRAPH", "GET", "/cp2.txt") == b"copy-me",
          "the copy keeps the old bytes")
    assert r.execute_command("GRAPH", "MV", "/cp2.txt", "/moved.txt") == b"OK"
    check(r.execute_command("GRAPH", "GET", "/moved.txt") == b"copy-me"
          and r.execute_command("GRAPH", "GET", "/cp2.txt") is None,
          "MV moves the edge, not the bytes")
    try:
        r.execute_command("GRAPH", "RM", "/t")
        check(False, "RM of a full directory should be refused")
    except redis.ResponseError as e:
        check("not empty" in str(e), "RM refuses a full directory (%r)" % (e,))
    assert r.execute_command("GRAPH", "RM", "/t", "RECURSIVE") == b"OK"
    check(r.execute_command("GRAPH", "GET", "/t/sub/doc.txt") is None,
          "recursive RM takes the subtree")
    check(r.execute_command("GRAPH", "GET", "/a/hello.txt") == b"hello-world",
          "RM /t takes the link edge, not the linked node: /a survives it")
    check(stat("/a")["refs"] == "1", "and /a keeps its one remaining edge")

    print("the Luau door", flush=True)
    assert r.execute_command("SETF", "gstat", """function call(path)
    local st = barch.graph.stat(path)
    if st == nil then return nil end
    return st.id
end""") == b"OK"
    check(int(r.execute_command("GSTAT", "/a/hello.txt")) == node_id("/a/hello.txt"),
          "graph.stat answers the node id")
    assert r.execute_command("SETF", "glist", """function call(path)
    local got = barch.graph.list(path)
    local out = {}
    for i, e in ipairs(got) do out[#out + 1] = e.name .. "=" .. e.id end
    return out
end""") == b"OK"
    check([x.decode() for x in r.execute_command("GLIST", "/b")] == [],
          "graph.list of an emptied directory is empty")
    assert r.execute_command("SETF", "gwrite", """function call()
    barch.graph.mkdir("/lu")
    barch.graph.put("/lu/n.txt", "luau-bytes", "text/plain")
    return barch.graph.get("/lu/n.txt")
end""") == b"OK"
    check(r.execute_command("GWRITE") == b"luau-bytes"
          and r.execute_command("GRAPH", "GET", "/lu/n.txt") == b"luau-bytes",
          "graph.put through Luau lands in the same store")
    assert r.execute_command("SETF", "gwalk", """function call()
    local got = barch.graph.bfs("/", 0, 100, 0)
    local out = {}
    for i, e in ipairs(got) do out[#out + 1] = e.path end
    return out
end""") == b"OK"
    got = [x.decode() for x in r.execute_command("GWALK")]
    check("/a/hello.txt" in got and "/lu/n.txt" in got,
          "graph.bfs from Luau walks the store (%d hits)" % len(got))
    assert r.execute_command("SETF", "gspace", """function call()
    local sp = barch.graph.space("nosuchspace")
    return "unexpected"
end""") == b"OK"
    try:
        r.execute_command("GSPACE")
        check(False, "graph.space of nothing should be refused")
    except redis.ResponseError as e:
        check("no key space" in str(e), "graph.space refuses it (%r)" % (e,))

    if failures:
        raise AssertionError("%d checks failed, first: %s" % (len(failures), failures[0]))
    print("graph test complete", flush=True)
finally:
    barch.stop()
