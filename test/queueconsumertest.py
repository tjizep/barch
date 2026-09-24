import http.server
import os
import socketserver
import threading
import time

import scale

import redis
import barch

# TODO 366: the queue consumer. A message published to a declared queue is
# handed to the function the declaration names, on the server's own threads,
# woken by the publish rather than waited for by a poll.

scale.workdir()

PORT = scale.port(default=14000)
WEB_PORT = scale.port(1, default=14001)
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

    print("a handler waiting on I/O holds no consumer thread")
    # TODO 436. The consumer's context runs on a handful of worker threads, and a
    # delivery used to hold one of them until the handler finished - parked on
    # http or not. Twelve queues whose handlers each wait a second on an upstream
    # took ceil(12 / threads) seconds that way. Now the handler's outcome comes
    # back from the function pool, so they all wait at once.
    class Slow(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            time.sleep(1.0)
            self.send_response(200)
            self.send_header("Content-Length", "4")
            self.end_headers()
            self.wfile.write(b"done")

    class Web(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    web = Web(("127.0.0.1", WEB_PORT), Slow)
    threading.Thread(target=web.serve_forever, daemon=True).start()
    try:
        assert r.execute_command("SETF", "WAITUP", '''
            function call(message)
                local got = http.request("http://127.0.0.1:%d/"):timeout(5000):get()
                barch.store.set("waited:" .. message, got.body or got.error or "?")
                return "ok"
            end
        ''' % WEB_PORT) == b"OK"
        QN = 12
        for i in range(QN):
            assert r.execute_command("configuration:SETF", "queues/wait%d" % i, '''
                function transport()
                    return { kind = "queue", name = "wait%d", space = "default",
                             call = "WAITUP", user = "default", durability = "each",
                             poll = "1h" }
                end
            ''' % i) == b"OK"
        t0 = time.time()
        for i in range(QN):
            r.execute_command("QUEUE", "PUSH", "wait%d" % i, "m%d" % i)
        all_done = wait_until(lambda: all(r.get("waited:m%d" % i) == b"done" for i in range(QN)))
        took = time.time() - t0
        check(all_done, "all %d handlers got their upstream's answer" % QN)
        check(took < 2.5, "and waited on it together: %.2fs for %d one-second waits" % (took, QN))
        check(wait_until(lambda: all(status_of("wait%d" % i).get("delivered") == "1" for i in range(QN))),
              "each counted as delivered once its handler ended")
    finally:
        web.shutdown()

    print("a handler's state is kept between messages, per user")
    # TODO 437. A queue handler used to get a new Luau state, and a fresh compile,
    # for every message. The states are pooled now, so a top level local lives
    # on from one message to the next - as it does across calls on one
    # connection. A redefinition is still what runs next, and a queue that runs
    # as a different user gets a state of its own.
    assert r.execute_command("SETF", "COUNTS", '''
        local seen = 0
        function call(message)
            seen += 1
            barch.store.set("counts:" .. message, "v1:" .. seen)
            return "ok"
        end
    ''') == b"OK"
    assert r.execute_command("configuration:SETF", "queues/counts", '''
        function transport()
            return { kind = "queue", name = "counts", space = "default",
                     call = "COUNTS", user = "default", durability = "each",
                     poll = "1h" }
        end
    ''') == b"OK"
    for i in range(5):
        r.execute_command("QUEUE", "PUSH", "counts", "a%d" % i)
    check(wait_until(lambda: r.get("counts:a4") is not None), "five messages handled")
    seen = [r.get("counts:a%d" % i) for i in range(5)]
    check(seen == [b"v1:%d" % (i + 1) for i in range(5)],
          "one state served all five, so the local counted up: %r" % seen)

    # declared up front: a declaration is a SETF too, and would clear the pool
    r.execute_command("ACL", "SETUSER", "qother", "on", ">pw", "+read", "+write", "+keys",
                      "+data", "+function")
    assert r.execute_command("configuration:SETF", "queues/counts2", '''
        function transport()
            return { kind = "queue", name = "counts2", space = "default",
                     call = "COUNTS", user = "qother", durability = "each",
                     poll = "1h" }
        end
    ''') == b"OK"
    # unlike a connection's cache, a plain SETF is enough: a handler always ran
    # the latest source when every call compiled afresh, and still does
    assert r.execute_command("SETF", "COUNTS", '''
        local seen = 100
        function call(message)
            seen += 1
            barch.store.set("counts:" .. message, "v2:" .. seen)
            return "ok"
        end
    ''') == b"OK"
    r.execute_command("QUEUE", "PUSH", "counts", "b0")
    check(wait_until(lambda: r.get("counts:b0") is not None) and r.get("counts:b0") == b"v2:101",
          "a plain SETF is what the next message runs: %r" % r.get("counts:b0"))
    r.execute_command("QUEUE", "PUSH", "counts", "b1")
    check(wait_until(lambda: r.get("counts:b1") is not None) and r.get("counts:b1") == b"v2:102",
          "and the state is kept again after it: %r" % r.get("counts:b1"))

    r.execute_command("QUEUE", "PUSH", "counts2", "c0")
    check(wait_until(lambda: r.get("counts:c0") is not None) and r.get("counts:c0") == b"v2:101",
          "another user's queue starts from a state of its own, not default's at 102: %r"
          % r.get("counts:c0"))

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
