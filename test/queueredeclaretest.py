# A queue follows its declaration after it's opened - TODO 516.
#
# The queue registry used to take a queue's declaration the first time the queue
# was opened and keep it. A changed call or user kept running the old function as
# the old user until a restart (an authorization problem, not just stale state), a
# changed durability, max_attempts or dir did nothing, and a removed declaration
# still took QUEUE PUSH with no consumer ever coming for it.
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
PORT = scale.port(default=14395)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

ROOT = os.path.join(os.getcwd(), "queueredeclare")
shutil.rmtree(ROOT, ignore_errors=True)
DATA, QDIR, QDIR2 = (os.path.join(ROOT, d) for d in ("data", "queues", "queues2"))
for d in (DATA, QDIR, QDIR2):
    os.makedirs(d)
LOG_PATH = os.path.join(ROOT, "barchd.log")


def wait_until(pred, timeout=10.0):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.05)
    return pred()


def status_of(r, name):
    for line in r.execute_command("QUEUE", "STATUS").decode().splitlines():
        if (" name=%s " % name) in line + " ":
            return dict(kv.split("=", 1) for kv in line.split(" ") if "=" in kv)
    return {}


def declare(r, key, name, call, user="default", durability="each", extra=""):
    r.execute_command("configuration:SETF", "queues/" + key, '''
        function transport()
            return { kind = "queue", name = "%s", space = "default", call = "%s",
                     user = "%s", durability = "%s", poll = "1h"%s }
        end''' % (name, call, user, durability, extra))


print("start queue redeclare test with %s" % BINARY, flush=True)
log = open(LOG_PATH, "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA,
                      "--config", "queue_dir=" + QDIR], stdout=log, stderr=subprocess.STDOUT)
try:
    end = time.time() + 60
    while True:
        try:
            socket.create_connection(("127.0.0.1", PORT), 0.5).close()
            break
        except OSError:
            assert time.time() < end, "barchd did not listen on %d" % PORT
            time.sleep(0.1)
    r = redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=30)
    for n in ("ONE", "TWO"):
        r.execute_command("SETF", "TAKE" + n, '''
            function call(message, sequence, attempts)
                barch.store.set("who:" .. message, "%s")
                return "ok"
            end''' % n)
    # no categories at all: a queue run as this user can't call its function
    r.execute_command("ACL", "SETUSER", "qnone", "on", ">pw")

    # --- the call ------------------------------------------------------------------
    print("a new call is what the next message runs", flush=True)
    declare(r, "work", "work", "TAKEONE")
    r.execute_command("QUEUE", "PUSH", "work", "a")
    assert wait_until(lambda: r.get("who:a") == b"ONE"), r.get("who:a")
    declare(r, "work", "work", "TAKETWO")
    r.execute_command("QUEUE", "PUSH", "work", "b")
    assert wait_until(lambda: r.get("who:b") is not None), "b was never handled"
    assert r.get("who:b") == b"TWO", "the old call still ran: %r" % r.get("who:b")

    # --- the user, and max_attempts ------------------------------------------------
    print("a new user, and max_attempts, apply from the next message", flush=True)
    declare(r, "work", "work", "TAKETWO", user="qnone", extra=", max_attempts = 1")
    r.execute_command("QUEUE", "PUSH", "work", "c")
    assert wait_until(lambda: status_of(r, "work").get("dead") == "1"), \
        "run as qnone it can't call TAKETWO, and with max_attempts 1 it's dead lettered: %s" \
        % status_of(r, "work")
    assert r.get("who:c") is None, "c ran as someone allowed to: %r" % r.get("who:c")
    line = [l for l in r.execute_command("QUEUE", "STATUS").decode().splitlines() if " name=work " in l]
    assert line and "'qnone' is not authorized" in line[0], line
    declare(r, "work", "work", "TAKETWO")

    # --- durability ----------------------------------------------------------------
    print("a new durability applies to the open queue", flush=True)
    declare(r, "dur", "dur", "TAKEONE", durability="timer", extra=", enabled = false")
    r.execute_command("QUEUE", "PUSH", "dur", "d1")
    declare(r, "dur", "dur", "TAKEONE", durability="each", extra=", enabled = false")
    r.execute_command("QUEUE", "PUSH", "dur", "d2")
    # `each` syncs every add before the push answers, so nothing is left unsynced
    unsynced = status_of(r, "dur").get("unsynced")
    assert unsynced == "0", "still behaving as timer after the change: unsynced=%s" % unsynced

    # --- dir -------------------------------------------------------------------------
    print("a new dir moves the queue, and says what stays behind", flush=True)
    declare(r, "work", "work", "TAKETWO", extra=', dir = "%s"' % QDIR2)
    r.execute_command("QUEUE", "PUSH", "work", "e")
    assert wait_until(lambda: r.get("who:e") == b"TWO"), r.get("who:e")
    assert "work.queue" in os.listdir(QDIR2), os.listdir(QDIR2)

    # --- removal, and declaring it again ------------------------------------------
    print("a removed queue refuses a push, and comes back to its file", flush=True)
    declare(r, "held", "held", "TAKEONE", extra=", enabled = false")
    r.execute_command("QUEUE", "PUSH", "held", "h1")
    assert r.execute_command("configuration:REMF", "queues/held") == 1
    try:
        r.execute_command("QUEUE", "PUSH", "held", "h2")
        raise AssertionError("a push to a queue whose declaration was removed was taken")
    except redis.exceptions.ResponseError as e:
        assert "no queue called 'held'" in str(e), str(e)
    declare(r, "held", "held", "TAKEONE", extra=", enabled = false")
    r.execute_command("QUEUE", "PUSH", "held", "h3")
    assert wait_until(lambda: status_of(r, "held").get("waiting") == "2"), \
        "h1 from before the removal and h3 should both be waiting: %s" % status_of(r, "held")
finally:
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()
    log.close()

with open(LOG_PATH, errors="replace") as f:
    out = f.read()
for said in ("follows its new declaration", "moved from", "is no longer declared",
             "is declared again"):
    assert said in out, "the log never said '%s'" % said
assert "WARNING: ThreadSanitizer" not in out and "ERROR: AddressSanitizer" not in out, \
    out[out.find("Sanitizer") - 200:][:4000]
print("queue redeclare test passed")
