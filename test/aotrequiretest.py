# A native function that requires a module inside call() - TODO 567.
#
# A native call runs with the interrupt hook down, and the module it requires is
# interpreted, so the module runs hookless too. What stops a runaway then is the
# deadline watch: it puts the hook back once the call's slice or deadline is up.
# A require at the top level is loaded before the call starts; one inside call()
# only as it runs, which is the case this covers:
#
# - a module that loops for ever still ends at the caller's deadline
# - a module that parks on I/O comes back with the right answer
# - a module that runs for several slices finishes with the right answer
#
# each through a native caller, and through an interpreted one as the control.
#
# Nothing here is timed - TODO 805. A stop is told by the answer and the timeout
# counter, several slices by INFO's luau_slices, and the slow web server answers
# once INFO's luau_parks says the call is parked on it, not after a sleep.
import http.server
import os
import threading
import time

import scale
import redis
from redis.backoff import NoBackoff
from redis.retry import Retry
import barch

scale.workdir()
PORT = scale.port(default=14620)
WEB = scale.port(1, default=14621)
DEADLINE_MS = 400


def counter(name):
    """a counter from INFO memory, on a connection of its own: the web server's
    thread reads it too"""
    c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=20)
    try:
        return int(c.info("memory").get(name, 0))
    finally:
        c.close()


# what luau_parks was before the call the web server is answering
parks_before = [0]


class Slow(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        # answer once the call is parked on this request, so the answer reaches a
        # parked call however the threads are scheduled. The deadline is only so a
        # call that never parks still gets an answer, and the check below fails
        end = time.monotonic() + 10
        while counter("luau_parks") <= parks_before[0] and time.monotonic() < end:
            time.sleep(0.01)
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *a):
        pass


web = http.server.ThreadingHTTPServer(("127.0.0.1", WEB), Slow)
threading.Thread(target=web.serve_forever, daemon=True).start()

print("start aot require test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
# well past any answer here, but short enough that a call nothing stops fails
# the test rather than hanging it
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=20)
r.execute_command("USE", "ar")

failures = []


def check(ok, what):
    print("  %-72s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures.append(what)


def native_compiled():
    return counter("luau_native_compiled")


def call(name, *args):
    """on a connection of its own, so a call that never answers can't leave the
    next one on a reconnected connection that has forgotten its USE"""
    # and no retry: redis-py sends a timed out command again on a new connection,
    # which has forgotten the USE and answers "unknown command" instead
    c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=20,
                    retry=Retry(NoBackoff(), 0), retry_on_timeout=False)
    c.execute_command("USE", "ar")
    try:
        out = c.execute_command(name, *args)
    except redis.ResponseError as e:
        out = "error: " + str(e)
    except redis.TimeoutError:
        out = "no answer in 20s"
    c.close()
    return out


def fsput(path, text):
    r.execute_command("ARFSPUT", path, text)


assert r.execute_command("SETF", "ARFSPUT", '''
function call(path, content) return barch.fs.put(path, content, "text/plain", 65536) end
''') == b"OK"

# the modules: plain Luau, no --!native, so they run interpreted
fsput("/spin.luau", "function run() local x = 0 while true do x = x + 1 end end")
fsput("/park.luau", '''
function run()
    local got = http.request("http://127.0.0.1:%d/slow"):timeout(15000):get()
    return tostring(got.status) .. got.body
end''' % WEB)
fsput("/count.luau", '''
function run(n)
    local x = 0
    for i = 1, n do x = x + i % 7 end
    return x
end''')


def caller(native, module, args="", deadline=DEADLINE_MS):
    return ('%s--@barch {"deadline_ms": %d}\n'
            'function call(...)\n'
            '    local m = require(":/%s.luau")\n'
            '    return m.run(%s)\n'
            'end\n') % ("--!native\n" if native else "", deadline, module, args)


# long enough to run for many slices - a slice is about 20ms, a million
# instructions - under a deadline that leaves it room to finish
N = 20000000
COUNT_DEADLINE_MS = 10000
expect_count = (N // 7) * 21 + sum(range(1, N % 7 + 1))

try:
    for native in (True, False):
        kind = "native" if native else "interpreted"
        print("%s caller, module required inside call()" % kind)
        for name, module, args, deadline in (
                ("SPIN", "spin", "", DEADLINE_MS), ("PARK", "park", "", DEADLINE_MS),
                ("COUNT", "count", "tonumber((...))", COUNT_DEADLINE_MS)):
            fn = ("N" if native else "I") + name
            assert r.execute_command("SETF", fn, caller(native, module, args, deadline)) == b"OK"

        before = native_compiled()
        timeouts = int(r.info("errors").get("function_timeouts", 0))
        out = call(("N" if native else "I") + "SPIN")
        if native:
            check(native_compiled() - before == 1, "the caller compiled to native code")
        # the deadline is what stops it: nothing else answers a call that never
        # returns, and a call the watch lets go of never answers (TODO 802)
        check(isinstance(out, str) and "timeout" in out.lower(),
              "a module that loops for ever is stopped: %r" % (out,))
        check(int(r.info("errors").get("function_timeouts", 0)) - timeouts == 1,
              "by the caller's %dms deadline" % DEADLINE_MS)

        parks_before[0] = counter("luau_parks")
        out = call(("N" if native else "I") + "PARK")
        check(out == b"200ok", "a module that parks on I/O answers: %r" % (out,))
        check(counter("luau_parks") > parks_before[0], "and the call was parked while it waited")

        slices = counter("luau_slices")
        out = call(("N" if native else "I") + "COUNT", str(N))
        check(out == expect_count or out == str(expect_count).encode(),
              "a module running many slices finishes right: %r" % (out,))
        # a slower machine only adds slices. A native slice is 20ms of the clock, and
        # N takes 0.7s here, so it'd take a machine 35 times faster to fit in one
        check(counter("luau_slices") - slices >= 2,
              "and it did run for more than one slice (%d)" % (counter("luau_slices") - slices))

    check(r.ping(), "the server still answers")
finally:
    if failures:
        # a call nothing stopped is still running on a worker, and stop() would
        # wait for it for ever - report and go
        print("\nFAILURES: %d" % len(failures))
        for f in failures:
            print("  " + f)
        os._exit(1)
    barch.stop()
    web.shutdown()

print()
print("all aot require checks pass")
