"""
The pure cores include nothing that does I/O, locks or keeps time - TODO 920.

A core is a header of plain decisions that a test can run through every short
sequence of events. It stays that way only while it can't reach a socket, a mutex
or a clock. This fails on any include outside the list below. docs/TRACTABILITY.md
says which modules have a core.
"""
import os
import re
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
CORES = [
    "src/rpc/park_core.h",
]
ALLOWED = {
    "<array>", "<cstddef>", "<cstdint>", "<limits>", "<optional>", "<type_traits>",
    "<string_view>", "<utility>",
}

failures = 0
for core in CORES:
    path = os.path.join(ROOT, core)
    with open(path) as f:
        for n, line in enumerate(f, 1):
            m = re.match(r'\s*#\s*include\s*([<"][^>"]+[>"])', line)
            if m and m.group(1) not in ALLOWED:
                print("%s:%d includes %s, which a core may not" % (core, n, m.group(1)))
                failures += 1
print("FAILED" if failures else "all %d cores clean" % len(CORES))
sys.exit(1 if failures else 0)
