#!/usr/bin/env python3
"""Load build/ into the `inventory` key space as plain keys.

  p:<asin>     the full product record, as prepare.py wrote it
  index:<n>    the compact rows, 200 to a key as a JSON array with its brackets
               off, so /api/index joins them with commas; index:n is how many
               (one 2.3 MB key is over the largest value a key can hold)
  names        slug -> display name
  categories   the category tree, [{slug, name, subs: [{slug, name}]}]

Usage: load_inventory.py PORT     (talks RESP to redis-cli --pipe)
"""
import glob, json, os, subprocess, sys

port = sys.argv[1] if len(sys.argv) > 1 else "14000"
here = os.path.dirname(os.path.abspath(__file__))
build = os.path.join(here, "build")

def cmd(*args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        a = a if isinstance(a, bytes) else a.encode()
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)

index_raw = open(os.path.join(build, "meta", "index.json"), "rb").read()
names_raw = open(os.path.join(build, "meta", "names.json"), "rb").read()
names = json.loads(names_raw)

tree = {}
for row in json.loads(index_raw):
    tree.setdefault(row["top"], set()).add(row["sub"])
categories = [{"slug": t, "name": names.get(t, t),
               "subs": [{"slug": s, "name": names.get(s, s)} for s in sorted(tree[t])]}
              for t in sorted(tree)]

buf = [cmd("USE", "inventory")]
n = 0
for path in sorted(glob.glob(os.path.join(build, "catalog", "*", "*", "*.json"))):
    asin = os.path.basename(path)[:-5]
    buf.append(cmd("SET", "p:" + asin, open(path, "rb").read()))
    n += 1
rows = json.loads(index_raw)
CHUNK = 200
parts = [rows[i:i + CHUNK] for i in range(0, len(rows), CHUNK)]
for i, part in enumerate(parts):
    buf.append(cmd("SET", "index:%d" % i, json.dumps(part, separators=(",", ":"))[1:-1]))
buf.append(cmd("SET", "index:n", str(len(parts))))
buf.append(cmd("SET", "names", names_raw))
buf.append(cmd("SET", "categories", json.dumps(categories, separators=(",", ":"))))

r = subprocess.run(["redis-cli", "-p", port, "--pipe"], input=b"".join(buf),
                   capture_output=True)
sys.stdout.write(r.stdout.decode())
sys.stderr.write(r.stderr.decode())
print("loaded %d products into inventory" % n)
sys.exit(r.returncode)
