# A directory import stays inside the directory it was given - TODO 541.
#
# The walks behind LOADFS, LOADKEYS, the function sync and a git repository
# imported as a file store followed every symlink. A link to a file outside the
# tree read that file into the store, a link to a directory outside it read the
# whole directory, and two links back to the tree itself made the walk go round
# for good. For a repository, that's a remote deciding which of the server's
# files end up in the store.
#
# Checked, against barchd, with a tree holding:
#   inside.txt -> real.txt          a link to a file in the tree: still read
#   leak.txt   -> ../outside/secret a link to a file outside: skipped
#   up         -> ../outside        a link to a directory outside: skipped
#   loop/a, loop/b -> .             links back into the tree: not followed
# through LOADFS and LOADKEYS, each finishing well inside its timeout.
import os
import shutil
import signal
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14541)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "symlinkwalk_data")
BASE = os.path.join(os.getcwd(), "symlinkwalk_tree")
TREE = os.path.join(BASE, "site")

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


for d in (DATA, BASE):
    shutil.rmtree(d, ignore_errors=True)
os.makedirs(DATA)
os.makedirs(os.path.join(TREE, "loop"))
os.makedirs(os.path.join(BASE, "outside"))
with open(os.path.join(BASE, "outside", "secret"), "w") as f:
    f.write("not for the store")
with open(os.path.join(TREE, "real.txt"), "w") as f:
    f.write("in the tree")
with open(os.path.join(TREE, "loop", "f.txt"), "w") as f:
    f.write("in the loop")
os.symlink("real.txt", os.path.join(TREE, "inside.txt"))
os.symlink("../outside/secret", os.path.join(TREE, "leak.txt"))
os.symlink("../outside", os.path.join(TREE, "up"))
os.symlink(".", os.path.join(TREE, "loop", "a"))
os.symlink(".", os.path.join(TREE, "loop", "b"))

p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA,
                      "--no-save-on-exit"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    end = time.time() + 60
    while True:
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            break
        except OSError:
            if time.time() > end or p.poll() is not None:
                raise AssertionError("barchd did not start")
            time.sleep(0.1)
    # no retries: a walk that never ends should fail here, not be sent again
    r = redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=60, retry_on_timeout=False)

    print("LOADFS", flush=True)
    began = time.time()
    try:
        r.execute_command("site:LOADFS", TREE)
        loaded = True
    except redis.exceptions.TimeoutError:
        loaded = False
    check(loaded, "LOADFS finishes (%.1f s)" % (time.time() - began))
    if loaded:
        get = lambda path: r.execute_command("site:FS", "GET", path)
        check(get("/real.txt") == b"in the tree", "a file in the tree is imported")
        check(get("/inside.txt") == b"in the tree", "a link to a file in the tree is too")
        check(get("/leak.txt") is None, "a link to a file outside is not")
        check(get("/up/secret") is None, "nor is a directory outside, through a link")
        check(get("/loop/f.txt") == b"in the loop", "a real directory is walked")
        check(get("/loop/a/f.txt") is None, "and a link back into the tree isn't followed")

    print("LOADKEYS", flush=True)
    began = time.time()
    try:
        r.execute_command("keys:LOADKEYS", TREE, "t")
        loaded = True
    except redis.exceptions.TimeoutError:
        loaded = False
    check(loaded, "LOADKEYS finishes (%.1f s)" % (time.time() - began))
    if loaded:
        values = [r.execute_command("keys:GET", k) for k in r.execute_command("keys:KEYS", "*")]
        check(b"not for the store" not in values, "no key holds the file outside (%d keys)" % len(values))
        check(b"in the tree" in values, "a file in the tree is a key")
finally:
    p.send_signal(signal.SIGKILL)
    p.wait(timeout=30)

print("\n%s" % ("symlink walk checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
