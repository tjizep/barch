import os
import time

import scale

import redis
import barch

# TODO 366: the queue consumer. A message published to a declared queue is
# handed to the function the declaration names, on the server's own threads,
# woken by the publish rather than waited for by a poll.

scale.workdir()

PORT = scale.port(default=14000)
QDIR = os.path.join(os.getcwd(), "queues")

# scale.workdir() keeps the directory between runs on purpose, so a failure can
# be looked at. A queue file left there would be delivered as soon as the
# consumer armed, and this test counts deliveries - so it starts from nothing
if os.path.isdir(QDIR):
    for f in os.listdir(QDIR):
        os.remove(os.path.join(QDIR, f))

print("start queue consumer test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")
r.execute_command("configuration:FLUSHDB")
assert r.execute_command("CONFIG", "SET", "queue_dir", QDIR) == b"OK"

failures = []


def check(ok, what):
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures.append(what)


def wait_until(pred, timeout=15.0, step=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(step)
    return pred()


def status_of(name):
    for line in r.execute_command("QUEUE", "STATUS").decode().splitlines():
        if (" name=%s " % name) in line + " ":
            return dict(kv.split("=", 1) for kv in line.split(" ") if "=" in kv)
    return {}


try:
    print("a message reaches its handler")
    assert r.execute_command("SETF", "TAKE", '''
        function call(message, sequence, attempts)
            local n = tonumber(barch.store.get("taken") or "0") or 0
            barch.store.set("taken", tostring(n + 1))
            barch.store.set("last", message)
            barch.store.set("last_seq", sequence)
            barch.store.set("last_attempts", attempts)
            return "ok"
        end
    ''') == b"OK"
    assert r.execute_command("configuration:SETF", "queues/work", '''
        function transport()
            return {
                kind = "queue", name = "work", space = "default",
                call = "TAKE", user = "default", durability = "each",
                max_attempts = 3, poll = "1h"
            }
        end
    ''') == b"OK"

    # poll is an hour, so anything that arrives proves the publish did the waking
    seq = r.execute_command("QUEUE", "PUSH", "work", "hello")
    check(isinstance(seq, int) and seq >= 1, "PUSH answers the sequence (%s)" % seq)
    check(wait_until(lambda: r.get("last") == b"hello"),
          "the handler ran, with the poll an hour away - the publish woke it")
    check(r.get("last_seq") == str(seq).encode(),
          "and was told the sequence it was published with")
    check(r.get("last_attempts") == b"0", "with no attempts against it yet")
    check(wait_until(lambda: status_of("work").get("waiting") == "0"),
          "the message was removed once the handler succeeded")
    check(status_of("work").get("delivered") == "1", "and counted as delivered")

    print("a backlog drains in order")
    r.execute_command("DEL", "taken")
    for i in range(20):
        r.execute_command("QUEUE", "PUSH", "work", "m%d" % i)
    check(wait_until(lambda: r.get("taken") == b"20"), "twenty messages all handled")
    check(r.get("last") == b"m19", "the last one handled is the last one published")
    check(status_of("work").get("waiting") == "0", "nothing left waiting")

    print("a handler that throws keeps the message")
    assert r.execute_command("SETF", "BOOM", '''
        function call(message, sequence, attempts)
            local n = tonumber(barch.store.get("tries") or "0") or 0
            barch.store.set("tries", tostring(n + 1))
            barch.store.set("boom_attempts", attempts)
            error("no thanks")
        end
    ''') == b"OK"
    assert r.execute_command("configuration:SETF", "queues/bad", '''
        function transport()
            return {
                kind = "queue", name = "bad", space = "default",
                call = "BOOM", user = "default", durability = "each",
                max_attempts = 3, poll = "200ms"
            }
        end
    ''') == b"OK"
    r.execute_command("QUEUE", "PUSH", "bad", "doomed")
    # three attempts, then the dead letter queue
    check(wait_until(lambda: status_of("bad").get("dead") == "1"),
          "after max_attempts it is in the dead letter queue")
    tries = int(r.get("tries") or 0)
    check(tries == 3, "the handler saw it exactly max_attempts times (%d)" % tries)
    check(r.get("boom_attempts") == b"2",
          "and was told how many attempts had already been made")
    check(status_of("bad").get("waiting") == "0", "it is out of the live queue")
    check("no thanks" in r.execute_command("QUEUE", "STATUS").decode(),
          "the status says why it failed")

    print("a queue does not block on its poison")
    r.execute_command("QUEUE", "PUSH", "bad", "also doomed")
    check(wait_until(lambda: status_of("bad").get("dead") == "2"),
          "the next message is attempted rather than stuck behind the dead one")

    print("what a publish will not do")
    try:
        r.execute_command("QUEUE", "PUSH", "nosuch", "x")
        check(False, "publishing to an undeclared queue is refused")
    except redis.ResponseError as e:
        check("no queue called" in str(e),
              "publishing to an undeclared queue is refused, and says so")

    print("barch.queue publishes from inside a function")
    assert r.execute_command("SETF", "FANOUT", '''
        function call(n)
            local last = 0
            for i = 1, tonumber(n) do
                last = barch.queue("work", "from luau " .. i)
            end
            return last
        end
    ''') == b"OK"
    r.execute_command("DEL", "taken")
    seq = r.execute_command("FANOUT", "5")
    check(isinstance(seq, int) and seq > 0,
          "barch.queue answers the sequence (%s)" % seq)
    check(wait_until(lambda: r.get("taken") == b"5"),
          "all five reached the handler")
    check(r.get("last") == b"from luau 5", "and the last one is the last published")
    check(r.get("last_seq") == str(seq).encode(),
          "the sequence barch.queue answered is the one the handler was told")

    print("what barch.queue will not do quietly")
    assert r.execute_command("SETF", "BADQ", '''
        function call()
            return barch.queue("nosuch", "x")
        end
    ''') == b"OK"
    try:
        r.execute_command("BADQ")
        check(False, "publishing to an undeclared queue from luau raises")
    except redis.ResponseError as e:
        check("no queue called" in str(e),
              "publishing to an undeclared queue from luau raises, and says so")
    # and a script that wants to decide for itself can
    assert r.execute_command("SETF", "TRYQ", '''
        function call()
            local ok, err = pcall(function() return barch.queue("nosuch", "x") end)
            if ok then return "published" end
            return "caught"
        end
    ''') == b"OK"
    check(r.execute_command("TRYQ") == b"caught", "and pcall can catch it")

    print("a handler can publish onward")
    assert r.execute_command("configuration:SETF", "queues/stage2", '''
        function transport()
            return { kind = "queue", name = "stage2", space = "default",
                     call = "STAGE2", user = "default", durability = "each",
                     poll = "1h" }
        end
    ''') == b"OK"
    assert r.execute_command("SETF", "STAGE2", '''
        function call(message)
            barch.store.set("stage2", message)
            return "ok"
        end
    ''') == b"OK"
    assert r.execute_command("SETF", "STAGE1", '''
        function call(message)
            barch.queue("stage2", message .. " and on")
            return "ok"
        end
    ''') == b"OK"
    assert r.execute_command("configuration:SETF", "queues/stage1", '''
        function transport()
            return { kind = "queue", name = "stage1", space = "default",
                     call = "STAGE1", user = "default", durability = "each",
                     poll = "1h" }
        end
    ''') == b"OK"
    r.execute_command("QUEUE", "PUSH", "stage1", "start")
    check(wait_until(lambda: r.get("stage2") == b"start and on"),
          "a message handled by one queue can be published to the next")

    print("messages outlive the process")
    assert r.execute_command("configuration:SETF", "queues/later", '''
        function transport()
            return {
                kind = "queue", name = "later", space = "default",
                call = "MISSING_ON_PURPOSE", user = "default",
                durability = "each", poll = "1h", enabled = false
            }
        end
    ''') == b"OK"
    # enabled = false, so nothing consumes it and the messages stay on disk
    a = r.execute_command("QUEUE", "PUSH", "later", "one")
    b = r.execute_command("QUEUE", "PUSH", "later", "two")
    check(b == a + 1, "sequences count up within a queue")
    check(os.path.exists(os.path.join(QDIR, "later.queue")),
          "the queue file is where queue_dir says")
    check(status_of("later").get("waiting") == "2",
          "a disabled queue keeps its messages rather than dropping them")
finally:
    barch.stop()

print()
if failures:
    print("FAILURES: %d" % len(failures))
    for f in failures:
        print("  " + f)
    raise SystemExit(1)
print("all queue consumer checks pass")
