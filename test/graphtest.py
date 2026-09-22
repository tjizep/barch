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
import json
import threading

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


def node_record(nid):
    """the stored node record, or None once the node is gone"""
    raw = r.execute_command("GET", "graph:n:%016x" % nid)
    return None if raw is None else json.loads(raw)


def fnv1a(name):
    """the hash graph:x: keys carry - FNV-1a, the same as gname_hash"""
    h = 1469598103934665603
    for c in name.encode():
        h ^= c
        h = (h * 1099511628211) & 0xFFFFFFFFFFFFFFFF
    return h


def refused(want, *cmd):
    """the command is an error, and the error says `want`"""
    try:
        got = r.execute_command(*cmd)
    except redis.ResponseError as e:
        check(want in str(e), "%s: %r says %r" % (" ".join(map(str, cmd)), str(e), want))
        return
    check(False, "%s should be refused, got %r" % (" ".join(map(str, cmd)), got))


def lua(name, body, *args):
    r.execute_command("SETF", name, body)
    return r.execute_command(name.upper(), *args)


try:
    # Both of these run first, while the id counters are fresh.
    print("FS ids start past the graph counter", flush=True)
    # graph leaves written before TODO 407 took their FS inode ids from the
    # "graph" sequence, so a fresh FS block starts past that counter rather
    # than landing on one of them
    r.execute_command("SET", "ids:graph", "5000")
    r.execute_command("FS", "PUT", "/floor.txt", "floor")
    inodes = [int(k.decode().split(":")[2], 16) for k in r.execute_command("KEYS", "fs:i:*")]
    check(inodes and min(inodes) >= 5000,
          "the first FS inode id is past the graph counter (%r)" % (inodes,))
    r.execute_command("FLUSHDB")

    print("graph leaves and FS files keep their own bytes", flush=True)
    # Both write fs:i:/fs:c: keys. Graph leaves took their inode ids from
    # "graph" and FS files from "fs", both counting from 1, so the second FS
    # file wrote over the first graph leaf (TODO 407).
    r.execute_command("FS", "PUT", "/x.txt", "fs-one")
    assert r.execute_command("GRAPH", "PUT", "/g.txt", "graph-bytes") == b"OK"
    r.execute_command("FS", "PUT", "/y.txt", "fs-two")
    check(r.execute_command("GRAPH", "GET", "/g.txt") == b"graph-bytes",
          "an FS PUT leaves a graph leaf's bytes alone")
    check(r.execute_command("FS", "GET", "/x.txt") == b"fs-one"
          and r.execute_command("FS", "GET", "/y.txt") == b"fs-two",
          "and a graph PUT leaves FS files alone")
    assert r.execute_command("GRAPH", "RM", "/g.txt") == b"OK"
    check(r.execute_command("FS", "GET", "/x.txt") == b"fs-one"
          and r.execute_command("FS", "GET", "/y.txt") == b"fs-two",
          "removing the leaf takes only its own bytes")
    r.execute_command("FLUSHDB")

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
    # and its name index entry, which is how a path step finds it
    r.execute_command("SET", "graph:x:%s:%016x:%s" % (parent_hex, fnv1a("fat.txt"), fat_hex),
                      "%016x" % big_node)
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
    # pre-order: /t/sub's whole subtree before /t/loopback. This used to pin
    # the level-order answer a head-popping "DFS" gave (TODO 407).
    check(dfs == ["/t", "/t/sub", "/t/sub/doc.txt", "/t/loopback", "/t/loopback/hello.txt"],
          "DFS visits every node once, each subtree before the next sibling (%r)" % (dfs,))
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

    print("a new leaf's ids stay its own", flush=True)
    # a new-leaf PUT reserved two ids and staged three, so the next LINK or MV
    # into the same directory got the leaf's edge id and wrote over its edge
    assert r.execute_command("GRAPH", "MKDIR", "/ids") == b"OK"
    assert r.execute_command("GRAPH", "MKDIR", "/ids-other") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/ids/f1", "one") == b"OK"
    assert r.execute_command("GRAPH", "LINK", node_id("/ids-other"), "/ids/g") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/ids/f2", "two") == b"OK"
    assert r.execute_command("GRAPH", "MV", "/ids-other", "/ids/o2") == b"OK"
    names = sorted(x.decode().split(" ", 4)[4] for x in r.execute_command("GRAPH", "LS", "/ids"))
    check(names == ["f1", "f2", "g", "o2"], "a LINK and an MV after a PUT keep its edge (%r)" % (names,))
    check(r.execute_command("GRAPH", "GET", "/ids/f1") == b"one"
          and r.execute_command("GRAPH", "GET", "/ids/f2") == b"two", "and its bytes")

    print("concurrent writers keep the count exact", flush=True)
    # refs was read in the plan and written in the stage with nothing between,
    # so eight clients linking one node lost most of the counts
    assert r.execute_command("GRAPH", "PUT", "/race.txt", "raced") == b"OK"
    race = node_id("/race.txt")
    workers, per = 8, scale.scaled(50, floor=10)
    for t in range(workers):
        assert r.execute_command("GRAPH", "MKDIR", "/race%d" % t) == b"OK"

    def link_many(t):
        c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
        for i in range(per):
            c.execute_command("GRAPH", "LINK", race, "/race%d/l%d" % (t, i))

    threads = [threading.Thread(target=link_many, args=(t,)) for t in range(workers)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    want = 1 + workers * per
    check(stat("/race.txt")["refs"] == str(want),
          "refs counts every edge (%s of %d)" % (stat("/race.txt")["refs"], want))
    check(len(r.execute_command("KEYS", "graph:r:%016x:*" % race)) == want,
          "one reverse entry per edge")
    for t in range(workers):
        assert r.execute_command("GRAPH", "RM", "/race%d" % t, "RECURSIVE") == b"OK"
    check(stat("/race.txt")["refs"] == "1"
          and r.execute_command("GRAPH", "GET", "/race.txt") == b"raced",
          "RM of the linking directories leaves it one edge and its bytes")

    print("a directory can't move under itself", flush=True)
    for p in ("/m", "/m/b", "/m/b/c"):
        assert r.execute_command("GRAPH", "MKDIR", p) == b"OK"
    # the old check looked two levels up, so the third went through and
    # orphaned /m and everything under it
    for to in ("/m/x", "/m/b/x", "/m/b/c/x"):
        refused("into itself", "GRAPH", "MV", "/m", to)
    check(stat("/m") is not None and stat("/m/b/c") is not None, "and /m stays where it was")

    print("nothing is left behind unreachable", flush=True)
    # a directory linked under itself never reaches refs 0
    assert r.execute_command("GRAPH", "MKDIR", "/u") == b"OK"
    u = node_id("/u")
    assert r.execute_command("GRAPH", "LINK", u, "/u/me") == b"OK"
    check(stat("/u")["refs"] == "2", "a self-link counts")
    assert r.execute_command("GRAPH", "UNLINK", "/u") == b"OK"
    check(node_record(u) is None, "UNLINK of its last route takes a self-linked directory")
    # the last edge of a directory with something in it takes what's under it
    assert r.execute_command("GRAPH", "MKDIR", "/z") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/z/f", "zz") == b"OK"
    z, zf = node_id("/z"), node_id("/z/f")
    zinode = node_record(zf)["inode"]
    assert r.execute_command("GRAPH", "UNLINK", "/z") == b"OK"
    check(node_record(z) is None and node_record(zf) is None
          and r.execute_command("GET", "fs:i:%016x" % zinode) is None,
          "UNLINK of a directory's last edge takes what only it held, bytes too")
    # an alias going leaves the directory and its count where they were
    assert r.execute_command("GRAPH", "MKDIR", "/keep") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/keep/f", "kk") == b"OK"
    assert r.execute_command("GRAPH", "LINK", node_id("/keep"), "/alias") == b"OK"
    assert r.execute_command("GRAPH", "UNLINK", "/alias") == b"OK"
    check(stat("/keep")["refs"] == "1" and r.execute_command("GRAPH", "GET", "/keep/f") == b"kk",
          "UNLINK of an alias keeps the directory")
    # RM RECURSIVE took the self-link for a hold from outside and kept /t2/a
    assert r.execute_command("GRAPH", "MKDIR", "/t2") == b"OK"
    assert r.execute_command("GRAPH", "MKDIR", "/t2/a") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/t2/a/keep.txt", "orphan-bytes") == b"OK"
    a2, keep2 = node_id("/t2/a"), node_id("/t2/a/keep.txt")
    keep_inode = node_record(keep2)["inode"]
    assert r.execute_command("GRAPH", "LINK", a2, "/t2/a/self") == b"OK"
    assert r.execute_command("GRAPH", "RM", "/t2", "RECURSIVE") == b"OK"
    check(node_record(a2) is None and node_record(keep2) is None,
          "RM RECURSIVE takes a self-linked directory under it")
    check(r.execute_command("GET", "fs:i:%016x" % keep_inode) is None
          and r.execute_command("KEYS", "graph:e:%016x:*" % a2) == [],
          "with its bytes and its edges")

    print("a directory past 1024 entries", flush=True)
    # children_of stopped at 1024 edges without a word. The count has to be
    # past that whatever the scale, and the name index keeps it quick.
    assert r.execute_command("GRAPH", "MKDIR", "/wd") == b"OK"
    wide_n = 1100
    for i in range(wide_n):
        r.execute_command("GRAPH", "PUT", "/wd/f%04d" % i, "v%d" % i)
    check(len(r.execute_command("GRAPH", "LS", "/wd")) == wide_n, "LS names every entry")
    check(r.execute_command("GRAPH", "GET", "/wd/f1050") == b"v1050", "an entry past 1024 resolves")
    assert r.execute_command("GRAPH", "PUT", "/wd/f1050", "again") == b"OK"
    check(len(r.execute_command("GRAPH", "LS", "/wd")) == wide_n
          and r.execute_command("GRAPH", "GET", "/wd/f1050") == b"again",
          "a PUT past 1024 overwrites rather than adding a duplicate")
    refused("exists", "GRAPH", "MKDIR", "/wd/f1099")
    seen, after = [], None
    while True:
        cmd = ["GRAPH", "LS", "/wd", "LIMIT", "100"] + (["AFTER", after] if after else [])
        page = [x.decode() for x in r.execute_command(*cmd)]
        if not page:
            break
        seen += [x.split(" ", 4)[4] for x in page]
        after = page[-1].split(" ")[1]
    check(len(seen) == wide_n and len(set(seen)) == wide_n,
          "AFTER pages through every entry once (%d)" % len(seen))
    wd = node_id("/wd")
    assert r.execute_command("GRAPH", "RM", "/wd", "RECURSIVE") == b"OK"
    check(stat("/wd") is None and r.execute_command("KEYS", "graph:e:%016x:*" % wd) == []
          and r.execute_command("KEYS", "graph:x:%016x:*" % wd) == [],
          "RM RECURSIVE takes all of it, index entries included")

    print("LS AFTER is an edge id", flush=True)
    # a name can't say which of its edges a page stopped at
    assert r.execute_command("GRAPH", "MKDIR", "/pg") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/pg/x", "x1") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/pg/y", "y") == b"OK"
    assert r.execute_command("GRAPH", "LINK", node_id("/pg/y"), "/pg/x") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/pg/z", "z") == b"OK"
    every = [x.decode() for x in r.execute_command("GRAPH", "LS", "/pg")]
    paged, after = [], None
    while True:
        cmd = ["GRAPH", "LS", "/pg", "LIMIT", "1"] + (["AFTER", after] if after else [])
        page = [x.decode() for x in r.execute_command(*cmd)]
        if not page:
            break
        paged += page
        after = page[-1].split(" ")[1]
    check(len(every) == 4 and paged == every,
          "LIMIT 1 pages see every edge once, the repeated name included (%r)" % (paged,))
    refused("edge id", "GRAPH", "LS", "/pg", "AFTER", "x")
    y_edge = stat("/pg/y")["edge"]
    assert r.execute_command("GRAPH", "UNLINK", "/pg/y") == b"OK"
    rest = [x.decode().split(" ", 4)[4]
            for x in r.execute_command("GRAPH", "LS", "/pg", "AFTER", y_edge)]
    check(rest == ["x", "z"], "a cursor whose edge went away still resumes after it (%r)" % (rest,))

    print("EDGE names one edge of a repeated name", flush=True)
    assert r.execute_command("GRAPH", "MKDIR", "/e") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/e/one", "1") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/e/two", "2") == b"OK"
    assert r.execute_command("GRAPH", "LINK", node_id("/e/one"), "/e/n") == b"OK"
    assert r.execute_command("GRAPH", "LINK", node_id("/e/two"), "/e/n") == b"OK"
    first, second = [x.decode().split(" ")[1] for x in r.execute_command("GRAPH", "LS", "/e")
                     if x.decode().endswith(" n")]
    check(r.execute_command("GRAPH", "GET", "/e/n") == b"1"
          and r.execute_command("GRAPH", "GET", "/e/n", "EDGE", second) == b"2",
          "GET EDGE reads the duplicate the first one hides")
    check(r.execute_command("GRAPH", "STAT", "/e/n", "EDGE", second).decode().find(
          "edge=" + second) >= 0, "STAT EDGE names it")
    assert r.execute_command("GRAPH", "UNLINK", "/e/n", "EDGE", second) == b"OK"
    check(r.execute_command("GRAPH", "GET", "/e/n") == b"1" and stat("/e/two")["refs"] == "1",
          "UNLINK EDGE drops just that edge")
    assert r.execute_command("GRAPH", "LINK", node_id("/e/two"), "/e/n") == b"OK"
    second = [x.decode().split(" ")[1] for x in r.execute_command("GRAPH", "LS", "/e")
              if x.decode().endswith(" n")][1]
    assert r.execute_command("GRAPH", "MV", "/e/n", "/e/m", "EDGE", second) == b"OK"
    check(r.execute_command("GRAPH", "GET", "/e/m") == b"2"
          and r.execute_command("GRAPH", "GET", "/e/n") == b"1", "MV EDGE moves just that edge")
    assert r.execute_command("GRAPH", "RM", "/e/n", "EDGE", first) == b"OK"
    check(r.execute_command("GRAPH", "GET", "/e/one") is None, "RM EDGE takes that edge's node")
    refused("no such edge", "GRAPH", "UNLINK", "/e/m", "EDGE", "999999999")
    refused("GRAPH UNLINK", "GRAPH", "UNLINK", "/e/m", "EDGE", "0")

    print("CHUNK and stray options are checked", flush=True)
    for bad in ("-1", "abc", "1.5", str(10 ** 7)):
        refused("CHUNK", "GRAPH", "PUT", "/chunk.txt", "abcdef", "CHUNK", bad)
        refused("CHUNK", "FS", "PUT", "/chunk.txt", "abcdef", "CHUNK", bad)
    check(r.execute_command("GRAPH", "STAT", "/chunk.txt") is None, "and nothing was stored")
    assert r.execute_command("GRAPH", "PUT", "/chunk.txt", "abcdef", "CHUNK", "4") == b"OK"
    check(stat("/chunk.txt")["chunks"] == "2"
          and r.execute_command("GRAPH", "GET", "/chunk.txt") == b"abcdef",
          "a good CHUNK splits the bytes")
    # a word left over at the end used to be skipped without a sound
    refused("GRAPH GET", "GRAPH", "GET", "/chunk.txt", "FROM")
    refused("GRAPH PUT", "GRAPH", "PUT", "/o.txt", "x", "TYPE")
    refused("GRAPH CP", "GRAPH", "CP", "/chunk.txt", "/c2.txt", "TYPE")
    refused("GRAPH BFS", "GRAPH", "BFS", "/", "DEPTH")
    refused("GRAPH LS", "GRAPH", "LS", "/", "LIMIT")
    refused("FS GET", "FS", "GET", "/x.txt", "FROM")

    print("CP copies a directory whole, in its own shape", flush=True)
    assert r.execute_command("GRAPH", "MKDIR", "/src") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/src/f", "ff", "TYPE", "text/plain") == b"OK"
    assert r.execute_command("GRAPH", "MKDIR", "/src/sub") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/src/sub/g", "gg") == b"OK"
    assert r.execute_command("GRAPH", "LINK", node_id("/src/f"), "/src/sub/f2") == b"OK"
    assert r.execute_command("GRAPH", "LINK", node_id("/src"), "/src/sub/back") == b"OK"
    # this failed with "no such path" for any directory with something in it
    assert r.execute_command("GRAPH", "CP", "/src", "/dst") == b"OK"
    check(r.execute_command("GRAPH", "GET", "/dst/f") == b"ff"
          and r.execute_command("GRAPH", "GET", "/dst/sub/g") == b"gg"
          and r.execute_command("GRAPH", "GET", "/dst/sub/f2") == b"ff",
          "every leaf is there")
    check(node_id("/dst/f") == node_id("/dst/sub/f2") != node_id("/src/f"),
          "a node shared inside stays shared, and is a new one")
    check(node_id("/dst/sub/back") == node_id("/dst"), "a cycle comes out a cycle")
    check(stat("/dst")["refs"] == "2" and stat("/dst/f")["refs"] == "2"
          and stat("/dst/f")["type"] == "text/plain", "counts and types come across")
    assert r.execute_command("GRAPH", "PUT", "/src/f", "changed") == b"OK"
    check(r.execute_command("GRAPH", "GET", "/dst/f") == b"ff", "the bytes are a copy")
    refused("exists", "GRAPH", "CP", "/src", "/dst")
    refused("leaf copy", "GRAPH", "CP", "/src", "/dst2", "TYPE", "x")
    assert r.execute_command("GRAPH", "RM", "/src", "RECURSIVE") == b"OK"
    assert r.execute_command("GRAPH", "RM", "/dst", "RECURSIVE") == b"OK"
    check(stat("/src") is None and stat("/dst") is None, "and both go with RM RECURSIVE")

    print("DFS is pre-order, first path wins", flush=True)
    assert r.execute_command("GRAPH", "MKDIR", "/o") == b"OK"
    assert r.execute_command("GRAPH", "MKDIR", "/o/p") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/o/p/leaf1", "1") == b"OK"
    assert r.execute_command("GRAPH", "MKDIR", "/o/q") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/o/q/leaf2", "2") == b"OK"
    assert r.execute_command("GRAPH", "LINK", node_id("/o/p/leaf1"), "/o/q/again") == b"OK"
    dfs = [x.decode().split(" ", 1)[1] for x in r.execute_command("GRAPH", "DFS", "/o")]
    check(dfs == ["/o", "/o/p", "/o/p/leaf1", "/o/q", "/o/q/leaf2"],
          "DFS goes down before across, and lists a shared node at its first path (%r)" % (dfs,))

    print("a layout 2 store is reindexed by its first write", flush=True)
    # what an older build leaves behind: layout 2, and a name index that is
    # missing or stale
    assert r.execute_command("GRAPH", "MKDIR", "/old") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/old/f", "old") == b"OK"
    r.execute_command("SET", "graph:layout", "2")
    for k in r.execute_command("KEYS", "graph:x:*"):
        r.execute_command("DEL", k)
    r.execute_command("SET", "graph:x:%016x:%016x:%016x" % (node_id("/old"), fnv1a("ghost"), 7),
                      "%016x" % 7)
    check(r.execute_command("GRAPH", "GET", "/old/f") == b"old", "reads scan while it's missing")
    assert r.execute_command("GRAPH", "MKDIR", "/old/g") == b"OK"
    check(r.execute_command("GET", "graph:layout") == b"3", "the write stamps layout 3")
    check(len(r.execute_command("KEYS", "graph:x:*")) == len(r.execute_command("KEYS", "graph:e:*")),
          "one index key per edge, the stale one gone")
    check(r.execute_command("GRAPH", "GET", "/old/f") == b"old" and stat("/old/g") is not None,
          "and paths resolve through it")
    r.execute_command("SET", "graph:layout", "9")
    refused("layout 9", "GRAPH", "MKDIR", "/nine")
    r.execute_command("SET", "graph:layout", "3")

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
    local got = barch.graph.bfs("/", 0, 1000, 0)
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

    # the Luau verbs mean what the GRAPH ones do: remove was rename under
    # another name, and rm without true unlinked (TODO 407)
    assert r.execute_command("GRAPH", "MKDIR", "/lu2") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/lu2/a", "A") == b"OK"
    assert r.execute_command("GRAPH", "LINK", node_id("/lu2/a"), "/lu2/b") == b"OK"
    check(lua("gremove", "function call(p) barch.graph.remove(p) return 'ok' end", "/lu2/b") == b"ok"
          and stat("/lu2/a") is None and stat("/lu2/b") is None,
          "graph.remove takes the node everywhere, like RM")
    assert r.execute_command("GRAPH", "PUT", "/lu2/c", "C") == b"OK"
    assert r.execute_command("GRAPH", "LINK", node_id("/lu2/c"), "/lu2/d") == b"OK"
    check(lua("gunlink", "function call(p) barch.graph.unlink(p) return 'ok' end", "/lu2/d") == b"ok"
          and stat("/lu2/d") is None and stat("/lu2/c")["refs"] == "1",
          "graph.unlink drops one edge")
    check(lua("grename", "function call(a, b) barch.graph.rename(a, b) return 'ok' end",
              "/lu2/c", "/lu2/e") == b"ok"
          and r.execute_command("GRAPH", "GET", "/lu2/e") == b"C" and stat("/lu2/c") is None,
          "graph.rename moves an edge")
    try:
        lua("grename1", "function call(a) barch.graph.rename(a) return 'ok' end", "/lu2/e")
        check(False, "graph.rename without a destination should be refused")
    except redis.ResponseError:
        pass
    assert r.execute_command("GRAPH", "MKDIR", "/lu2/sub") == b"OK"
    assert r.execute_command("GRAPH", "PUT", "/lu2/sub/f", "F") == b"OK"
    try:
        lua("grm", "function call(p) barch.graph.rm(p) return 'ok' end", "/lu2/sub")
        check(False, "graph.rm of a full directory without true should be refused")
    except redis.ResponseError as e:
        check("not empty" in str(e), "graph.rm refuses a full directory (%r)" % (e,))
    check(lua("grmr", "function call(p) barch.graph.rm(p, true) return 'ok' end", "/lu2/sub") == b"ok"
          and stat("/lu2/sub") is None, "graph.rm with true takes the subtree")
    assert r.execute_command("GRAPH", "MKDIR", "/lu3") == b"OK"
    for n in ("x", "y", "z"):
        assert r.execute_command("GRAPH", "PUT", "/lu3/" + n, n) == b"OK"
    x_edge = stat("/lu3/x")["edge"]
    got = lua("glistafter", """function call(p, after)
    local out = {}
    for i, e in ipairs(barch.graph.list(p, tonumber(after), 0, 0)) do out[#out + 1] = e.name end
    return out
end""", "/lu3", x_edge)
    check([x.decode() for x in got] == ["y", "z"], "graph.list pages by edge id (%r)" % (got,))
    try:
        lua("glocked", """function call()
    return barch.store.locked("x", function() barch.graph.mkdir("/inlock") end)
end""")
        check(False, "a graph write inside a locked region should be refused")
    except redis.ResponseError as e:
        check("not allowed inside a locked region" in str(e),
              "a graph write inside a locked region is refused (%r)" % (e,))

    if failures:
        raise AssertionError("%d checks failed, first: %s" % (len(failures), failures[0]))
    print("graph test complete", flush=True)
finally:
    barch.stop()
