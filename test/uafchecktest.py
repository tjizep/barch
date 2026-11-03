# The use after free check is a startup option, off by default - TODO 812.
#
# Every address an allocator frees used to go into a set that every read looked
# up, which was about 5% of an insert (DONE 763). Now `use_after_free_check` turns
# that on, and an allocator reads it when it's built. INFO memory's
# use_after_free_tracked counts the freed addresses put in that set, so this needs
# no timing. Checked, on a barchd started each way:
#   1. by default the option reads off, and freeing tracks nothing
#   2. started with use_after_free_check=on, it reads on, and freeing tracks addresses
#   3. either way, what was written and deleted reads back right
import os
import signal
import shutil
import subprocess
import sys

import scale
import redis

scale.workdir()
PORT = scale.port(default=14159)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

N = 2000


def run(extra):
    data = os.path.join(os.getcwd(), "uafcheck_data")
    shutil.rmtree(data, ignore_errors=True)
    os.makedirs(data)
    log_path = os.path.join(data, "barchd.log")
    log = open(log_path, "wb")
    # the sanitizer jobs turn the check on for every server through the environment,
    # and the default is what's checked here
    env = {k: v for k, v in os.environ.items() if k != "BARCH_USE_AFTER_FREE_CHECK"}
    proc = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", data] + extra,
                            stdout=log, stderr=subprocess.STDOUT, env=env)
    log.close()
    scale.wait_for_port(PORT, proc=proc, what="barchd")
    try:
        r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=30)
        setting = r.config_get("use_after_free_check").get("use_after_free_check")
        before = int(r.info("memory").get("use_after_free_tracked", -1))
        for i in range(N):
            r.set("k:%d" % i, "v" * (i % 50 + 1))
        for i in range(0, N, 2):
            assert r.delete("k:%d" % i) == 1
        for i in range(N):
            assert r.get("k:%d" % i) == (None if i % 2 == 0 else b"v" * (i % 50 + 1)), i
        tracked = int(r.info("memory").get("use_after_free_tracked", -1)) - before
        r.close()
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            scale.stop_hung(proc, signal.SIGTERM, 60, log=log_path)
    assert proc.returncode == 0, "barchd exited with %s:\n%s" % (
        proc.returncode, open(log_path, "rb").read().decode(errors="replace")[-3000:])
    return setting, before, tracked


print("start use after free check test with %s" % BINARY, flush=True)

setting, before, tracked = run([])
print("default: %r, %d freed addresses tracked" % (setting, tracked), flush=True)
assert setting == "off", setting
assert before == 0, "no use_after_free_tracked in INFO memory"
assert tracked == 0, "%d freed addresses tracked with the check off" % tracked

setting, before, tracked = run(["--config", "use_after_free_check=on"])
print("on: %r, %d freed addresses tracked" % (setting, tracked), flush=True)
assert setting == "on", setting
assert tracked >= N // 2, "only %d freed addresses tracked for %d deletes" % (tracked, N // 2)

print("use after free check test complete", flush=True)
