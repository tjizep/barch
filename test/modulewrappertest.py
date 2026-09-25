# Every Valkey module wrapper calls the handler it's named after - TODO 455.
#
# The module commands are thin wrappers, `cmd_X` calling `vk_call(..., X)`, and
# they get written by copying the one above. Four of them had kept the handler
# they were copied from: B.SAVEALL and B.SIZEALL ran CLEAR and emptied the
# space, B.CLEARALL cleared only the current space, and B.UINCRBY ran INCRBY.
# Nothing tested the module path, so none of it showed. This reads the source
# and fails on any wrapper whose handler has a different name.
import glob
import os
import re
import sys

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")

# a wrapper that's meant to call another handler goes here, with the reason
ALLOWED = {
}

wrapper = re.compile(r"int\s+cmd_(\w+)\s*\([^)]*\)\s*\{(.*?)\n\}", re.S)
call = re.compile(r"vk_call\(\s*[^,]+,\s*[^,]+,\s*[^,]+,\s*([\w:]+)\s*\)")

checked = 0
wrong = []
for path in sorted(glob.glob(os.path.join(SRC, "**", "*.cpp"), recursive=True)):
    with open(path, errors="replace") as f:
        text = f.read()
    for m in wrapper.finditer(text):
        name = m.group(1)
        for handler in call.findall(m.group(2)):
            checked += 1
            handler = handler.split("::")[-1]
            if handler != name and ALLOWED.get(name) != handler:
                line = text.count("\n", 0, m.start()) + 1
                wrong.append("%s:%d cmd_%s calls %s" % (os.path.relpath(path, SRC), line,
                                                        name, handler))

print("%d module wrappers checked" % checked)
for w in wrong:
    print("  WRONG HANDLER", w)
if checked < 50:
    print("  FAIL: that's too few, so the pattern no longer matches the source")
    sys.exit(1)
sys.exit(1 if wrong else 0)
