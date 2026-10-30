#!/usr/bin/env python3
"""
A repository set up through a cluster's configuration space syncs straight away, and
keeps polling at its interval, after a restart too - TODO 788.

The function sync poller sleeps for up to a minute when it has nothing due. A write
to the configuration space under git/repositories/ is meant to wake it, so a new or
changed repository is read at once. In a cluster it wasn't: the poller only found
the repository on its idle minute, and after a restart the first poll was a minute
in as well.

Here a one-node cluster has a repository set up through its configuration space,
polling every 200ms into a replicated space. What has to hold:
  - the repository's key is in the space within a few seconds of the setup;
  - a commit upstream after that is in the space within a few seconds;
  - after a restart, another commit upstream is in the space within a few seconds.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for  # noqa: E402

scale.workdir()
BASE = scale.port(default=19540)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
# well under the poller's idle minute, and room for a slow (TSan) build
SOON = 15
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


ORIGIN = tempfile.mkdtemp(prefix="clustersyncwake")


def git(*args):
    subprocess.check_call(["git", "-C", ORIGIN, *args], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def push(body, message):
    with open(os.path.join(ORIGIN, "data.txt"), "w", encoding="utf-8") as f:
        f.write(body)
    git("add", "-A")
    git("commit", "-q", "-m", message)


def value():
    try:
        return node.client(SPACE).execute_command("GET", "data.txt")
    except Exception:  # noqa: BLE001 - not up yet, or not leading yet
        return None


def arrives(body, what):
    began = time.time()
    ok = wait_for(SOON, lambda: value() == body)
    check(ok, "%s (%.1fs)" % (what, time.time() - began))


git("init", "-q", "-b", "main")
git("config", "user.email", "t@t")
git("config", "user.name", "t")
push("one", "v1")

node = clusternodes.Node(0, BASE, BINARY)
try:
    print("a one-node cluster, with a repository set up through its configuration")
    node.start()
    node.client().execute_command("CLUSTER", "INIT")
    conf = node.client("configuration")
    conf.execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(60, lambda: node.leads(SPACE)), "it leads the space's group")
    node.client().execute_command("CONFIG", "SET", "functions_dir", os.path.join(node.dir, "checkouts"))
    for setting, val in (("url", ORIGIN), ("space", SPACE), ("pull", "on"), ("ms", "200")):
        conf.execute_command("SET", "git/repositories/app/" + setting, val)
    arrives("one", "the repository's key is in the space soon after the setup")

    print("a commit upstream")
    push("two", "v2")
    arrives("two", "is in the space soon after")

    print("a restart, and another commit upstream")
    node.stop()
    node.start()
    check(wait_for(60, lambda: node.leads(SPACE)), "it leads the space's group again")
    push("three", "v3")
    arrives("three", "is in the space soon after")
finally:
    node.stop()
    shutil.rmtree(ORIGIN, ignore_errors=True)

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
