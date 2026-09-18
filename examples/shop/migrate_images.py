#!/usr/bin/env python3
"""One-off: turn the pictures held as keys in `images` (i:<asin>, t:<asin>) into
files at /img/<asin> in the same space, with their content type.

A key holds at most 512 kB and a picture may be bigger, so pictures are files.
Leaves the keys where they are; delete them once the files are checked.
"""
import socket, subprocess

PORT = 14100

def cmd(*a):
    o = [b"*%d\r\n" % len(a)]
    for x in a:
        x = x if isinstance(x, bytes) else x.encode()
        o.append(b"$%d\r\n%s\r\n" % (len(x), x))
    return b"".join(o)

def cli(text):
    return subprocess.run(["redis-cli", "-p", str(PORT), "-3", "--raw"], input=text,
                          capture_output=True, text=True).stdout.split("\n")

asins, cur = [], "0"
while True:
    out = cli("USE images\nSCAN %s MATCH i:* COUNT 500\n" % cur)
    cur = out[1]
    asins += [k[2:] for k in out[2:] if k.startswith("i:")]
    if cur == "0":
        break

s = socket.create_connection(("127.0.0.1", PORT)); s.settimeout(20)
f = s.makefile("rb")

def reply():
    l = f.readline()
    if l[:1] == b"$":
        n = int(l[1:])
        return None if n < 0 else f.read(n + 2)[:-2]
    return l.strip()

def rt(*a):
    s.sendall(cmd(*a)); return reply()

rt("USE", "images")
n = 0
for a in asins:
    body, kind = rt("GET", "i:" + a), rt("GET", "t:" + a)
    if body is None:
        continue
    r = rt("FS", "PUT", "/img/" + a, body, "TYPE", (kind or b"image/jpeg").decode())
    if r is None or r[:1] not in (b"+", b":"):
        print("failed", a, r); continue
    n += 1
print("wrote %d of %d files" % (n, len(asins)))
