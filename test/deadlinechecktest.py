import scale
import redis
import barch

# A running function checks its deadline at an interrupt, and that check reads
# the clock - TODO 809. It used to read it at every interrupt, which was about 7%
# of an insert done from Luau. Now it reads it at one interrupt in every few. This
# counts the reads, through INFO, so it needs no timing: a loop of K turns fires
# at least K interrupts, and should cost a small fraction of K clock reads, but
# still some, and a function that never ends must still be stopped.

scale.workdir()

PORT = scale.port(default=14158)

print("start deadline check test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")


def checks():
    return int(r.info("memory").get("luau_deadline_checks", 0))


K = 200000
assert r.execute_command(
    "SETF", "dlspin",
    "function call(n) local s = 0; for i = 1, tonumber(n) do s = s + i end; return s end") == b"OK"
assert r.execute_command("dlspin", "10") == 55        # compiled outside the count

before = checks()
assert r.execute_command("dlspin", str(K)) == K * (K + 1) // 2
made = checks() - before
print(f"{K} loop turns, {made} clock reads")
# one read per interrupt was the old way; one in eight is far more than it is now
assert 0 < made <= K // 8, f"{made} clock reads for {K} interrupts"

# a call that comes back from a long barch.call past its deadline is stopped at its
# next interrupt, not 64 interrupts on, which a function that only has a few left
# never gets to. The inner one has a deadline of its own and runs until the outer
# one's has gone by - a condition, not a guess at how long anything takes
assert r.execute_command(
    "SETF", "dlinner",
    '''--[[@barch{"deadline_ms": 30000}]]
function call() local t0 = barch.time(); while barch.time() - t0 < 100 do end; return "inner" end''') == b"OK"
assert r.execute_command(
    "SETF", "dlouter",
    '''--[[@barch{"deadline_ms": 20}]]
function call() local r = barch.call("CALLF", "dlinner"); for i = 1, 3 do end; return r end''') == b"OK"
try:
    got = r.execute_command("dlouter")
    raise AssertionError(f"came back past its deadline with {got!r}")
except redis.exceptions.ResponseError as e:
    assert "timeout" in str(e), str(e)

# a timeout a pcall caught is raised again at the next interrupt, so the call still
# ends with it instead of going on to return
assert r.execute_command(
    "SETF", "dlcaught",
    '''--[[@barch{"deadline_ms": 20}]]
function call() local ok = pcall(function() while true do end end); for i = 1, 3 do end; return "caught" end''') == b"OK"
try:
    got = r.execute_command("dlcaught")
    raise AssertionError(f"a caught timeout let it return {got!r}")
except redis.exceptions.ResponseError as e:
    assert "timeout" in str(e), str(e)

# and a loop that never ends still runs into its deadline
r.execute_command("CONFIG", "SET", "function_deadline_ms", "200")
assert r.execute_command("SETF", "dlforever", "function call() while true do end end") == b"OK"
try:
    r.execute_command("dlforever")
    raise AssertionError("a loop with no end came back")
except redis.exceptions.ResponseError as e:
    assert "timeout" in str(e), str(e)

barch.stop()
print("deadline check test ok")
