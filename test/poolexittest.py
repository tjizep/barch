#!/usr/bin/env python3
"""
The Python module's exit hook stops the maintenance pool's threads - TODO 799.

Since TODO 748 every space's maintenance runs on one small pool. The module's exit
hook (stop_background_threads, from Py_AtExit) took each space off the pool, but
left the pool's threads waiting for work, to be joined by the pool's destructor
at static destruction. Under Linux that works. In a Windows DLL statics go at
DLL_PROCESS_DETACH, after ExitProcess has killed every other thread, and the join
there never returned: every Windows test that loads _barch.pyd hung until its
600s ran out, while the barchd.exe tests passed.

A child Python opens a space and writes to it, so the pool starts, then:
  - where it can see its threads (Linux, /proc/self/task) and call the hook
    (an exported symbol, through ctypes), it calls the hook and checks every
    thread the module started is gone;
  - and it has to exit, which is the check that counts on Windows.
"""
import os
import subprocess
import sys

import scale

scale.workdir()

CHILD = r'''
import ctypes, os, sys, threading, time
import barch

def threads():
    try:
        return len(os.listdir("/proc/self/task"))
    except OSError:
        return None

# TSan starts a thread of its own with the first one made, so one is made and
# done with before counting. A joined thread can stay in /proc for a moment after
# join returns (ASan showed one), so this waits for the count to settle
t = threading.Thread(target=lambda: None)
t.start()
t.join()
before = threads()
for _ in range(20):
    time.sleep(0.05)
    now = threads()
    if now == before:
        break
    before = now
k = barch.KeyValue()
k.set("pool", "started")
time.sleep(0.3)
during = threads()
print("threads before %s, with a space open %s" % (before, during), flush=True)
hook = None
if before is not None:
    try:
        hook = ctypes.CDLL(barch._barch.__file__)._ZN5barch23stop_background_threadsEv
    except (AttributeError, OSError):
        pass
if hook is not None:
    hook()
    time.sleep(0.3)
    after = threads()
    print("threads after the exit hook %s" % after, flush=True)
    if after > before:
        print("FAIL: the exit hook left %d threads running" % (after - before), flush=True)
        sys.exit(2)
else:
    print("no threads to count here, so only the exit is checked", flush=True)
'''

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


env = dict(os.environ)
if scale.SANITIZER_PRELOAD:
    # scale took it out of this process's environment, and a sanitizer build of
    # the module can't load without it
    env["LD_PRELOAD"] = scale.SANITIZER_PRELOAD
try:
    r = subprocess.run([sys.executable, "-c", CHILD], cwd=os.getcwd(), env=env, timeout=120,
                       stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                       stderr=subprocess.STDOUT)
    out = r.stdout.decode(errors="replace")
    code = r.returncode
except subprocess.TimeoutExpired as e:
    out = (e.stdout or b"").decode(errors="replace")
    code = None
for line in out.splitlines():
    if not line.startswith("[") and "BARCH" not in line:
        print("    " + line)
check("FAIL:" not in out, "no thread of the module outlives its exit hook")
check(code is not None, "the child exits within 120s")
check(code == 0, "and exits 0 (%s)" % code)
print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
