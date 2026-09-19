#!/usr/bin/env python3
"""Write app/commands.json - the RESP console's command list - out of the command
reference that docs/index.html carries as `const CMDS={...}`.

Usage: make_commands.py [path/to/docs/index.html]
Defaults to ../../docs/index.html, which is where it is in the barch repo. Run it
again when the reference changes; the page reads whatever this last wrote.
"""
import html, json, os, re, sys

here = os.path.dirname(os.path.abspath(__file__))
src = sys.argv[1] if len(sys.argv) > 1 else os.path.join(here, "..", "..", "docs", "index.html")
text = open(src, encoding="utf-8").read()
start = text.index("const CMDS=") + len("const CMDS=")
cmds, _ = json.JSONDecoder().raw_decode(text[start:])

def plain(s):
    return html.unescape(re.sub(r"<[^>]+>", "", s or "")).strip()

out = []
for name, c in sorted(cmds.items()):
    if c.get("asy"):
        continue                    # KEYS, VALUES, RANGE: a script may not call them
    out.append({
        "name": name,
        "family": plain(c.get("family")),
        "syntax": plain(c.get("syntax")),
        "summary": plain(c.get("summary")),
        "example": c.get("example") or "",
        "dangerous": bool(c.get("dangerous")),
        "write": "write" in (c.get("cats") or []),
    })
dst = os.path.join(here, "app", "commands.json")
with open(dst, "w", encoding="utf-8") as f:
    json.dump(out, f, separators=(",", ":"))
print("wrote %d commands to %s" % (len(out), dst))
