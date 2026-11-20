# A raise between park_call and the yield, caught by the script - TODO 942.
#
# park_call marks the call parked. The C function then starts what it waits for
# and yields through park_yield. A raise in between (a failed allocation, a thread
# that won't start) is a Luau error, and a script's pcall catches it. The call ran
# on still marked parked. Its next budget yield was taken for the park, and it
# waited for a completion nobody had started. The client got no answer, ever: the
# wait ran out, the call was told to stop, and the session waited with no timer for
# a call that would never be resumed.
#
# BARCH_TEST_FAULT=park makes park_call raise at that point.
import os

os.environ["BARCH_TEST_FAULT"] = "park"

import scale
import redis
import barch

scale.workdir()
PORT = scale.port(default=14942)

print("start park raise test", flush=True)

# the loop runs well past the inline slice, so the call yields for its budget
# and goes on on the pool
CAUGHT = '''
function call(url)
    local ok, e = pcall(function() return http.request(url):timeout(1000):get() end)
    local n = 0
    for i = 1, 5000000 do n = n + i end
    if ok then return "parked " .. n end
    return "caught " .. tostring(e) .. " " .. n
end
'''

barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
# a backstop for the old code, which never answers. Not a time the test measures
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
r.execute_command("FLUSHDB")
try:
    assert r.execute_command("SETF", "caught", CAUGHT) == b"OK"
    try:
        got = r.execute_command("CAUGHT", "http://127.0.0.1:1/")
    except redis.exceptions.TimeoutError:
        raise AssertionError("a call that caught a raise after park_call never answered")
    print("answer:", got, flush=True)
    assert got.startswith(b"caught "), got
    assert b"test fault park" in got, got
    assert got.endswith(b" 12500002500000"), got
    # the same connection goes on
    assert r.ping()
    got = r.execute_command("CAUGHT", "http://127.0.0.1:1/")
    assert got.startswith(b"caught "), got
    print("park raise test passed", flush=True)
finally:
    barch.stop()
