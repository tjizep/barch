#!/usr/bin/env python3
"""
A group stops only once the appends already in it are done - TODO 787.

On SIGTERM, barchd refuses new Raft writes and then stops its groups. But a write
already on its way into NuRaft kept going, and nothing waited for it: NuRaft's
shutdown ran beside the append, and TSan caught the two touching the same state. It
showed up as a function sync write in a TSan run of TestClusterFunctionSync, since the
sync thread is only stopped after the groups are.

Here a one-node cluster syncs a repository into a replicated space, and a test knob
(BARCH_TEST_APPEND_HOLD_FILE) holds each of the space's appends for 1.5s before NuRaft
gets it. With the knob on, a change is pushed upstream, and SIGTERM is sent once the
sync's write is being held. What has to hold:
  - the held write isn't handed to a group that has already shut down, which NuRaft
    answers with nothing ("by callback, return null" in the log): the group waits
    for it instead;
  - barchd exits 0 (a TSan report makes it 66, a crash a signal);
  - its log has no ThreadSanitizer report and says it stopped;
  - it stops within 15s, so the wait has a bound.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for  # noqa: E402

scale.workdir()
BASE = scale.port(default=19500)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


ORIGIN = tempfile.mkdtemp(prefix="clusterstop")


def git(*args):
    subprocess.check_call(["git", "-C", ORIGIN, *args], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def write(rel, body):
    with open(os.path.join(ORIGIN, rel), "w", encoding="utf-8") as f:
        f.write(body)


def commit(message):
    git("add", "-A")
    git("commit", "-q", "-m", message)


def log_text(n):
    with open(n.dir + ".log", encoding="utf-8", errors="replace") as f:
        return f.read()


git("init", "-q", "-b", "main")
git("config", "user.email", "t@t")
git("config", "user.name", "t")
write("data.txt", "one")
commit("v1")

hold = os.path.join(os.getcwd(), "hold")
if os.path.exists(hold):
    os.remove(hold)
# the space's group is the second, after the cluster group
node = clusternodes.Node(0, BASE, BINARY, env={"BARCH_TEST_APPEND_HOLD_FILE": hold,
                                                "BARCH_TEST_APPEND_HOLD_GROUP": "1",
                                                "BARCH_TEST_APPEND_HOLD_MS": "1500"})
try:
    print("a one-node cluster syncing a repository into a replicated space")
    node.start()
    node.client().execute_command("CLUSTER", "INIT")
    conf = node.client("configuration")
    conf.execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(60, lambda: node.leads(SPACE)), "it leads the space's group")
    node.client().execute_command("CONFIG", "SET", "functions_dir", os.path.join(node.dir, "checkouts"))
    for setting, value in (("url", ORIGIN), ("space", SPACE), ("pull", "on"), ("ms", "200")):
        conf.execute_command("SET", "git/repositories/app/" + setting, value)
    check(wait_for(30, lambda: node.client(SPACE).execute_command("GET", "data.txt") == "one"),
          "the repository's key is in the space")

    print("SIGTERM while the sync's write is held on its way into NuRaft")
    open(hold, "w").close()
    write("data.txt", "two")
    commit("v2")
    held = wait_for(30, lambda: "raft group 1 test: holding an append" in log_text(node))
    check(held, "a write is being held")
    began = time.time()
    node.proc.send_signal(signal.SIGTERM)
    try:
        code = node.proc.wait(30)
    except subprocess.TimeoutExpired:
        node.proc.kill()
        code = node.proc.wait()
    took = time.time() - began
    node.proc = None
    text = log_text(node)
    after = text[text.find("raft group 1 test: holding an append"):]
    check("by callback, return null" not in after, "the held write went in before its group stopped")
    check(code == 0, "barchd exits 0 (%s)" % code)
    check("ThreadSanitizer" not in text, "with no ThreadSanitizer report")
    check("barchd stopped" in text, "and says it stopped")
    check(took < 15, "within 15s (%.1fs)" % took)
finally:
    if node.proc:
        node.stop()
    shutil.rmtree(ORIGIN, ignore_errors=True)

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
