# Space handles kept past the call that made them - TODO 427.
#
# A module keeps its handles: `local cfg = barch.space.configuration` at the top of a
# stored function is built once, when the module loads. The call interface those
# handles pointed into is replaced when the connection runs a call in another space,
# so requiring another space's module and then running a call in that space left the
# module holding pointers into a freed map, and barchd died inside hide_secrets'
# wrapper. The s3 backup testing found it with require("s3.S3").
import os
import sys

import scale

import redis
import barch

scale.workdir()
PORT = scale.port(default=14495)
EXTERNAL = os.environ.get("SPACEHANDLE_PORT")   # point at a running barchd to try it

if not EXTERNAL:
    print("start space handle test", flush=True)
    barch.start("0.0.0.0", PORT)
    barch.ping("127.0.0.1", PORT)
    port = PORT
else:
    port = int(EXTERNAL)

failures = []


def check(ok, what):
    if not ok:
        print("  FAIL " + what, flush=True)
        failures.append(what)


r = redis.Redis(host="127.0.0.1", port=port, db=0, protocol=2, decode_responses=True)
r.execute_command("configuration:SET", "hsx.endpoint", "http://minio:9000")

r.execute_command("USE", "hsmod")
r.set("k", "in hsmod")
r.execute_command("HSET", "box", "f", "in the box")
# handles can't be opened while a module's top level runs, so a module keeps them the
# way the s3 module does: opened on first use, then held in an upvalue from then on
assert r.execute_command("SETF", "MOD", """
    local cfg, cur, named, box
    local function handles()
        cfg = cfg or barch.space.configuration
        cur = cur or barch.current()
        named = named or barch.space.hsmod
        box = box or barch.space.hsmod:container("box")
    end
    function endpoint()
        handles()
        return cfg["hsx.endpoint"]
    end
    function call()
        handles()
        return { cfg["hsx.endpoint"], cur:get("k") or "none", named:get("k") or "none",
                 box["f"] or "none" }
    end
""") == "OK"

r.execute_command("USE", "hsuse")
r.set("k", "in hsuse")
assert r.execute_command("SETF", "CROSS", """
    local m = require("hsmod.MOD")
    function call()
        return { barch.space.configuration["hsx.endpoint"], m.endpoint() }
    end
""") == "OK"

for round in range(3):
    got = r.execute_command("hsuse.CROSS")
    check(got == ["http://minio:9000", "http://minio:9000"], "round %d: CROSS answered %r" % (round, got))
    # the call that used to take barchd down: a call in the module's own space
    got = r.execute_command("hsmod.MOD")
    # hsmod.MOD called from a connection using hsuse runs in hsuse (a function reads
    # the space the call runs in, not the one it's defined in), so barch.current() is
    # hsuse. It is still a new call interface - the defined-in space changed - which is
    # what freed the one the kept handles pointed into
    check(got == ["http://minio:9000", "in hsuse", "in hsmod", "in the box"],
          "round %d: MOD answered %r" % (round, got))
    check(r.ping(), "round %d: still answering" % round)

# a current() handle made in one space follows the call it is used in
r.execute_command("USE", "hsuse")
assert r.execute_command("SETF", "VIA", """
    local m = require("hsmod.MOD")
    function call() return m.call()[2] end
""") == "OK"
check(r.execute_command("hsuse.VIA") == "in hsuse", "a kept current() handle reads the running call's space")

# --- kept handles whose interface is gone - TODO 430 -----------------------------------
# A connection keeps a few interfaces now (4), so going back and forth between two
# spaces no longer frees one. Calls in five other spaces do: MOD's interface is pushed
# out and freed, and its kept handles have to find their spaces again
for i in range(5):
    r.execute_command("USE", "hsother%d" % i)
    r.execute_command("SETF", "NOP", "function call() return 1 end")
r.execute_command("USE", "hsuse")
check(r.execute_command("hsmod.MOD") == ["http://minio:9000", "in hsuse", "in hsmod", "in the box"],
      "MOD before its interface is pushed out")
for i in range(5):
    r.execute_command("hsother%d.NOP" % i)
check(r.execute_command("hsmod.MOD") == ["http://minio:9000", "in hsuse", "in hsmod", "in the box"],
      "MOD's kept handles after its interface was freed")
check(r.ping(), "still answering")

# --- a walk kept past its call - TODO 430 ----------------------------------------------
# A module keeps a walk in a coroutine and hands out one key per call. With more keys
# than a page (256), the page it refills in a later call has to come from the call
# running then: in between, a call in another space replaced the interface the walk
# was started under
r.execute_command("USE", "hsmod")
for i in range(600):
    r.set("w:%04d" % i, "v")
assert r.execute_command("SETF", "STEP", """
    local co
    function call(what)
        if what == "start" then
            co = coroutine.wrap(function()
                for row in barch.space.hsmod do
                    if string.sub(row.key, 1, 2) == "w:" then coroutine.yield(row.key) end
                end
                coroutine.yield(nil)
            end)
        end
        return co()
    end
""") == "OK"
walked = [r.execute_command("hsmod.STEP", "start")]
while True:
    # calls in other spaces every step, enough to push the walk's interface out
    r.execute_command("hsuse.CROSS")
    if len(walked) % 50 == 0:
        for i in range(5):
            r.execute_command("hsother%d.NOP" % i)
    k = r.execute_command("hsmod.STEP", "next")
    if k is None:
        break
    walked.append(k)
check(walked == ["w:%04d" % i for i in range(600)],
      "a walk kept past its call reads every key once: %d keys" % len(walked))

# --- a barch.art() walk outlives its handle - TODO 431 -----------------------------------
# The handle isn't referenced once the loop has started, and the garbage the loop
# makes collects it. The walk used to keep the handle's store as a raw pointer and
# read through freed memory on its next page: the process died with SIGSEGV
assert r.execute_command("SETF", "ARTWALK", """
    local function make()
        local a = barch.art()
        for i = 1, 1000 do a["k" .. i] = tostring(i) end
        return a
    end
    function call()
        local n = 0
        for row in make() do
            local junk = {}
            for i = 1, 3000 do junk[i] = { i, tostring(i) } end
            n = n + 1
        end
        return n
    end
""") == "OK"
check(r.execute_command("hsmod.ARTWALK") == 1000, "a barch.art() walk keeps its space alive")
check(r.ping(), "still answering after the art walk")

if failures:
    print("space handle test FAILED: %d" % len(failures), flush=True)
    raise SystemExit(1)
print("space handle test ok", flush=True)
if not EXTERNAL:
    barch.stop()
