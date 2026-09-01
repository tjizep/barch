#!/usr/bin/env python3
"""Load the HNSW Luau functions into a 1-shard space and optionally run a demo.

    python3 deploy.py --start --demo

Then, from redis-cli -p 14000:

    USE HNSW
    HNSW.SET hello
    HNSW.CLOSEST helo
    HNSW.TUNE fast
    HNSW.PARAMS

SET/CLOSEST/TUNE/PARAMS all come from the one HNSW key, through a resp
transport(). FUNCTIONS COMMANDS lists them with the rights each one needs.
"""
import argparse
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
LUAU = os.path.join(HERE, "luau")
SPACE = "HNSW"
PORT = 14000

# GRAPH first: hnsw.luau requires it at SETF time
ORDER = ("graph.luau", "hnsw.luau")

DEMO_WORDS = [
    "cat", "cats", "car", "cart", "card", "care",
    "dog", "dogs", "dot", "door", "doll",
    "hello", "hallo", "help", "helm", "held",
    "redis", "redish", "radius", "ready",
    "barch", "birch", "batch", "beach",
    "graph", "graft", "grasp",
]

# queries chosen so the nearest neighbour is obvious on this tiny set
DEMO_QUERIES = [
    ("cta", "cat"),
    ("helo", "hello"),
    ("reds", "redis"),
    ("barc", "barch"),
    ("doog", "dog"),
]


def read_luau(name):
    path = os.path.join(LUAU, name)
    with open(path, encoding="utf-8") as f:
        return f.read()


def connect(host, port):
    import redis
    r = redis.Redis(host=host, port=port, db=0, protocol=2)
    r.ping()
    return r


def prepare_space(r):
    # these are read when the space is first built, so they have to land first
    r.execute_command("configuration:SET", f"{SPACE}.shards", "1")
    r.execute_command("configuration:SET", f"{SPACE}.function_slice_insns", "20000000")
    r.execute_command("configuration:SET", f"{SPACE}.function_deadline_ms", "30000")


def deploy(r):
    prepare_space(r)
    for fname in ORDER:
        name = os.path.splitext(fname)[0]
        src = read_luau(fname)
        r.execute_command(f"{SPACE}:SETF", name, src)
        print(f"SETF {SPACE}:{name.upper()}  ({len(src)} bytes)")


def demo(r):
    # dotted form: definition from HNSW, data in the current space
    r.execute_command("USE", SPACE)
    t0 = time.time()
    for w in DEMO_WORDS:
        r.execute_command(f"{SPACE}.SET", w)
    dt = time.time() - t0
    print(f"added {len(DEMO_WORDS)} words in {dt:.3f}s")
    stats = r.execute_command(f"{SPACE}.PARAMS")
    print("stats count,M,efc,efs,heur,entry:", stats)
    ok = True
    for q, want in DEMO_QUERIES:
        got = r.execute_command(f"{SPACE}.CLOSEST", q)
        if isinstance(got, bytes):
            got = got.decode()
        print(f"  CLOSEST {q!r} -> {got!r}  (want {want!r})")
        if got != want:
            ok = False
    top = r.execute_command(f"{SPACE}.CLOSEST", "hello", "3")
    print("  CLOSEST hello 3 ->", top)
    # one key, four commands, and what each of them needs
    for line in r.execute_command("FUNCTIONS", "COMMANDS"):
        print("  ", line.decode() if isinstance(line, bytes) else line)
    return ok


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=PORT)
    p.add_argument("--start", action="store_true",
                   help="start an embedded barch server on --port")
    p.add_argument("--demo", action="store_true",
                   help="insert a small word list and query a few typos")
    p.add_argument("--tune", default="default",
                   choices=("fast", "default", "accurate"),
                   help="build/search preset used for --demo")
    args = p.parse_args()

    if args.start:
        import barch
        barch.start("0.0.0.0", args.port)
        barch.ping("127.0.0.1", args.port)

    r = connect(args.host, args.port)
    deploy(r)
    if args.demo:
        r.execute_command(f"{SPACE}:TUNE", args.tune)
        ok = demo(r)
        if not ok:
            print("some CLOSEST answers were not the nearest of the tiny set")
            sys.exit(1)
        print("demo ok")
    if args.start and not args.demo:
        print(f"HNSW functions loaded on port {args.port}, Ctrl+C to stop")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
