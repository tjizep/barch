#!/usr/bin/env python3
"""
barchd says what and where when it crashes - TODO 646.

A crash used to leave nothing behind: TestLoadResult's barchd died in Windows CI with
a log that just stops (TODO 644). barchd now reports a std::terminate, an unhandled
Windows exception, and (on Linux) a fatal signal on stderr, which is its log, with a
stack.

BARCH_TEST_CRASH crashes it on purpose at the start of main: "terminate" throws from
a thread nothing catches it on, "segv" writes through a null pointer.

What has to hold, each way:
  - barchd exits with a failure;
  - its log has a "barchd crash:" line saying what happened, and a stack;
  - for terminate, the exception's own message.
Under ASan or TSan barchd leaves fatal signals to the sanitizer, so for segv the
sanitizer's report is taken instead.

On Linux, "dump" holds a latch on one thread and times out a wait for it on another,
three seconds in (TODO 741). The log should then have a dump of every thread that
names the holder, and barchd should still be running.
"""
import os
import subprocess
import sys

import scale

scale.workdir()
PORT = scale.port(default=14540)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY) and not os.path.exists(BINARY + ".exe"):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def crash(how):
    log = os.path.join(os.getcwd(), "crash_%s.log" % how)
    env = dict(os.environ)
    env["BARCH_TEST_CRASH"] = how
    with open(log, "wb") as out:
        p = subprocess.run([BINARY, "--port", str(PORT), "--bind", "127.0.0.1"],
                           stdout=out, stderr=subprocess.STDOUT, env=env, timeout=60)
    with open(log, "rb") as f:
        return p.returncode, f.read().decode("utf-8", "replace")


print("terminate: an exception nothing catches")
code, text = crash("terminate")
check(code != 0, "barchd exits with a failure (%d)" % code)
check("barchd crash: terminate" in text, "the log says it was a terminate")
check("BARCH_TEST_CRASH asked for it" in text, "with the exception's own message")
check("stack (" in text, "and a stack")

print("segv: a write through a null pointer")
code, text = crash("segv")
check(code != 0, "barchd exits with a failure (%d)" % code)
sanitized = "Sanitizer" in text
if sanitized:
    check("SEGV" in text, "the sanitizer reports the SEGV itself")
else:
    check("barchd crash:" in text and ("signal 11" in text or "access violation" in text),
          "the log says what it was")
    check("stack (" in text, "and a stack")
    if os.name == "nt":
        check("writing 0x0" in text, "a write to address 0")

hows = ["terminate", "segv"]
if sys.platform.startswith("linux"):
    # a latch wait that times out dumps every thread - TODO 741
    print("dump: a latch wait times out")
    hows.append("dump")
    import time
    log = os.path.join(os.getcwd(), "crash_dump.log")
    env = dict(os.environ)
    env["BARCH_TEST_CRASH"] = "dump"
    with open(log, "wb") as out:
        p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1"],
                             stdout=out, stderr=subprocess.STDOUT, env=env)
    text = ""
    end = time.time() + 120
    while time.time() < end and "end of thread dump" not in text and p.poll() is None:
        time.sleep(0.5)
        text = open(log, "rb").read().decode("utf-8", "replace")
    alive = p.poll() is None
    p.kill()
    p.wait()
    check("end of thread dump" in text, "the log has a thread dump")
    check(alive, "and barchd is still up after it")
    holder = None
    for ln in text.splitlines():
        if ln.startswith("held by: "):
            holder = ln.split()[2]
    check(holder is not None and holder.isdigit(), "it names the thread holding the latch (%s)" % holder)
    check(holder is not None and ("-- thread %s " % holder) in text and "<- THIS" in text,
          "and that thread's section shows the hold")
    check(text.count("-- thread ") >= 4, "every thread is there (%d)" % text.count("-- thread "))
    check("crash_if_asked" in text or "barchd+" in text, "with barchd's own frames")

if failures:
    for how in hows:
        print("---- crash_%s.log" % how)
        print(open(os.path.join(os.getcwd(), "crash_%s.log" % how), "rb").read()
              .decode("utf-8", "replace")[-3000:])
print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
